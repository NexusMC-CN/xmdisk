package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"xingmeng/internal/meta"
)

func countTrash(t *testing.T, s *Store) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.cfg.DataDir, "trash"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read trash: %v", err)
	}
	return len(entries)
}

func TestGCTrashAndSweep(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)

	c1 := randBytes(t, 64<<10)
	c2 := randBytes(t, 64<<10)
	mustPut(t, s, "bkt", "k1", c1, PutOptions{})
	oi2 := mustPut(t, s, "bkt", "k2", c2, PutOptions{})
	if err := s.Delete(ctx, "bkt", "k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 第一轮：无主 blob 入 trash；宽限期内不清扫
	s.cfg.GCGrace = time.Hour
	res, err := s.RunGCOnce(ctx)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if res.Trashed != 1 || res.Swept != 0 || !res.GateOpen {
		t.Fatalf("gc round1: %+v", res)
	}
	if got := countTrash(t, s); got != 1 {
		t.Fatalf("trash = %d, want 1", got)
	}
	// k2 完好
	rc, _, err := s.Get(ctx, "bkt", "k2", nil)
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	rc.Close()
	if !s.blobs.Exists(oi2.Blobs[0].Sha256) {
		t.Fatalf("live blob should stay")
	}

	// 宽限期归零：清扫生效，blob 物理删除
	s.cfg.GCGrace = 0
	res, err = s.RunGCOnce(ctx)
	if err != nil || res.Swept != 1 || res.Trashed != 0 {
		t.Fatalf("gc round2: %v %+v", err, res)
	}
	if got := countTrash(t, s); got != 0 {
		t.Fatalf("trash after sweep = %d, want 0", got)
	}
}

func TestGCResurrect(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)

	c := randBytes(t, 64<<10)
	mustPut(t, s, "bkt", "k", c, PutOptions{})
	s.Delete(ctx, "bkt", "k")
	s.cfg.GCGrace = time.Hour
	if _, err := s.RunGCOnce(ctx); err != nil {
		t.Fatalf("gc1: %v", err)
	}
	if countTrash(t, s) != 1 {
		t.Fatalf("expect 1 in trash")
	}

	// 同内容重新写入：blobs/ 重新出现该哈希，trash 留有旧副本
	mustPut(t, s, "bkt", "k", c, PutOptions{})
	s.cfg.GCGrace = 0
	res, err := s.RunGCOnce(ctx)
	if err != nil {
		t.Fatalf("gc2: %v", err)
	}
	if res.Resurrected != 1 || res.Swept != 0 {
		t.Fatalf("resurrect: %+v", res)
	}
	if got := countTrash(t, s); got != 0 {
		t.Fatalf("trash after resurrect = %d, want 0", got)
	}
	rc, _, err := s.Get(ctx, "bkt", "k", nil)
	if err != nil {
		t.Fatalf("get after resurrect: %v", err)
	}
	rc.Close()
}

func TestGCSlaveGate(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)
	mustPut(t, s, "bkt", "k1", randBytes(t, 64<<10), PutOptions{})
	mustPut(t, s, "bkt", "k2", randBytes(t, 64<<10), PutOptions{})
	s.Delete(ctx, "bkt", "k1")

	s.cfg.GCGrace = time.Hour
	if _, err := s.RunGCOnce(ctx); err != nil {
		t.Fatalf("gc1: %v", err)
	}
	m1, err := s.meta.GCMarkSeq(ctx)
	if err != nil || m1 <= 0 {
		t.Fatalf("mark seq: %v %d", err, m1)
	}

	// 从节点落后于标记点：门控关闭，宽限期归零也不清扫
	s.meta.SetSlaveAck(ctx, "s1", m1-1, time.Now().UnixNano())
	s.cfg.GCGrace = 0
	res, err := s.RunGCOnce(ctx)
	if err != nil {
		t.Fatalf("gc2: %v", err)
	}
	if res.GateOpen || res.Swept != 0 {
		t.Fatalf("gate should be closed: %+v", res)
	}
	if countTrash(t, s) != 1 {
		t.Fatalf("blob must survive closed gate")
	}

	// 从节点追平：放行清扫
	s.meta.SetSlaveAck(ctx, "s1", m1, time.Now().UnixNano())
	res, err = s.RunGCOnce(ctx)
	if err != nil {
		t.Fatalf("gc3: %v", err)
	}
	if !res.GateOpen || res.Swept != 1 {
		t.Fatalf("gate should be open: %+v", res)
	}
	if countTrash(t, s) != 0 {
		t.Fatalf("trash should be empty")
	}
}

func TestGCStaleUploads(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)
	now := time.Now().UnixNano()
	s.cfg.MultipartAbortAfter = 24 * time.Hour

	// 超时会话：part 引用真实 blob；先记入 pending（multipart API 将来负责加）
	oldData := randBytes(t, 32<<10)
	h1, _, err := s.blobs.Put(bytes.NewReader(oldData))
	if err != nil {
		t.Fatalf("put part blob: %v", err)
	}
	if err := s.meta.InsertUpload(ctx, "up-old", "bkt", "old.bin", "{}", now-int64(200*time.Hour)); err != nil {
		t.Fatalf("insert upload: %v", err)
	}
	if err := s.meta.InsertUploadPart(ctx, "up-old", 1, h1, int64(len(oldData)), "e1"); err != nil {
		t.Fatalf("insert part: %v", err)
	}
	// 先记入 pending（multipart API 将来负责在 MpPut 时加）
	if err := s.meta.WithWriteTx(ctx, func(t *meta.Tx) error {
		return t.AdjustUsage("bkt", 0, int64(len(oldData)), 0)
	}); err != nil {
		t.Fatalf("bump pending: %v", err)
	}

	// 在途会话：受保护
	freshData := randBytes(t, 32<<10)
	h2, _, err := s.blobs.Put(bytes.NewReader(freshData))
	if err != nil {
		t.Fatalf("put part blob: %v", err)
	}
	if err := s.meta.InsertUpload(ctx, "up-new", "bkt", "new.bin", "{}", now); err != nil {
		t.Fatalf("insert upload: %v", err)
	}
	if err := s.meta.InsertUploadPart(ctx, "up-new", 1, h2, int64(len(freshData)), "e2"); err != nil {
		t.Fatalf("insert part: %v", err)
	}

	s.cfg.GCGrace = 0
	res, err := s.RunGCOnce(ctx)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if res.UploadsAborted != 1 {
		t.Fatalf("uploads aborted = %d, want 1", res.UploadsAborted)
	}
	// 老会话行已删（parts 为空），新会话保留
	if parts, _ := s.meta.UploadParts(ctx, "up-old"); len(parts) != 0 {
		t.Fatalf("stale upload parts should be gone")
	}
	if parts, _ := s.meta.UploadParts(ctx, "up-new"); len(parts) != 1 {
		t.Fatalf("fresh upload must survive")
	}
	// 老 part 的 blob 被清扫，新 part 的 blob 仍在
	if s.blobs.Exists(h1) {
		t.Fatalf("stale part blob should be swept")
	}
	if !s.blobs.Exists(h2) {
		t.Fatalf("fresh part blob must stay")
	}
	// 老 pending 已随会话清理退还
	bs, err := s.BucketStat(ctx, "bkt")
	if err != nil || bs.Pending != 0 {
		t.Fatalf("pending after abort: %v %+v", err, bs)
	}
}
