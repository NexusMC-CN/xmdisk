package store

import (
	"bytes"
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"testing"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// 测试用小阈值：小文件 1MB、分块 4MB，避免大内存与长耗时。
func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Config{
		DataDir:        t.TempDir(),
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func mustPut(t *testing.T, s *Store, bucket, key string, data []byte, opts PutOptions) *ObjInfo {
	t.Helper()
	oi, err := s.Put(context.Background(), bucket, key, bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return oi
}

func countBlobFiles(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(s.cfg.DataDir, "blobs"), func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk blobs: %v", err)
	}
	return n
}

func md5hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func TestPutGetRoundtrip(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.CreateBucket(ctx, "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	data := randBytes(t, 1<<20-100) // 单 blob 路径
	oi := mustPut(t, s, "bkt", "x/y.bin", data, PutOptions{ContentType: "application/octet-stream", Meta: map[string]string{"k": "v"}})

	if oi.Size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", oi.Size, len(data))
	}
	if oi.ETag != md5hex(data) {
		t.Fatalf("etag = %s, want md5 %s", oi.ETag, md5hex(data))
	}
	if len(oi.Blobs) != 1 {
		t.Fatalf("blob refs = %d, want 1", len(oi.Blobs))
	}
	if oi.UserMeta["k"] != "v" {
		t.Fatalf("user meta lost: %v", oi.UserMeta)
	}

	rc, info, err := s.Get(ctx, "bkt", "x/y.bin", nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("roundtrip mismatch: %d bytes", len(got))
	}
	if info.ETag != oi.ETag || info.ContentType != "application/octet-stream" {
		t.Fatalf("info mismatch: %+v", info)
	}

	st, err := s.Stat(ctx, "bkt", "x/y.bin")
	if err != nil || st.Size != int64(len(data)) {
		t.Fatalf("stat: %v %+v", err, st)
	}
	bs, err := s.BucketStat(ctx, "bkt")
	if err != nil || bs.Used != int64(len(data)) || bs.Objects != 1 {
		t.Fatalf("bucket stat: %v %+v", err, bs)
	}
}

func TestPutChunkedAndRange(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)

	data := randBytes(t, 5<<20) // 阈值 1MB / 分块 4MB → 两块
	oi := mustPut(t, s, "bkt", "big.bin", data, PutOptions{})
	if len(oi.Blobs) != 2 {
		t.Fatalf("blob refs = %d, want 2 (4MB+1MB)", len(oi.Blobs))
	}
	if oi.Blobs[0].Size != 4<<20 || oi.Blobs[1].Size != 1<<20 {
		t.Fatalf("chunk sizes: %d+%d", oi.Blobs[0].Size, oi.Blobs[1].Size)
	}
	if oi.ETag != md5hex(data) {
		t.Fatalf("chunked etag should still be whole-stream md5")
	}
	if n := countBlobFiles(t, s); n != 2 {
		t.Fatalf("physical blob files = %d, want 2", n)
	}

	// 跨块区读：从第 1 块末尾读进第 2 块开头
	start := 4<<20 - 10
	rc, _, err := s.Get(ctx, "bkt", "big.bin", &Range{Start: int64(start), Length: 20})
	if err != nil {
		t.Fatalf("range get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data[start:start+20]) {
		t.Fatalf("cross-chunk range mismatch")
	}

	// 越界区间
	if _, _, err := s.Get(ctx, "bkt", "big.bin", &Range{Start: int64(len(data)) + 1, Length: 5}); !errors.Is(err, errs.ErrRangeInvalid) {
		t.Fatalf("want ErrRangeInvalid, got %v", err)
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)

	mustPut(t, s, "bkt", "k", randBytes(t, 100<<10), PutOptions{})
	mustPut(t, s, "bkt", "k", randBytes(t, 300<<10), PutOptions{})
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Used != 300<<10 || bs.Objects != 1 {
		t.Fatalf("after overwrite: used=%d objects=%d", bs.Used, bs.Objects)
	}

	if err := s.Delete(ctx, "bkt", "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Stat(ctx, "bkt", "k"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("want KeyNotFound after delete, got %v", err)
	}
	bs, _ = s.BucketStat(ctx, "bkt")
	if bs.Used != 0 || bs.Objects != 0 {
		t.Fatalf("after delete: used=%d objects=%d", bs.Used, bs.Objects)
	}

	// oplog 顺序与连续性：bucket_create, entry_put, entry_put, entry_delete
	ops, err := s.meta.OpsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ops: %v", err)
	}
	wantTypes := []string{meta.OpBucketCreate, meta.OpEntryPut, meta.OpEntryPut, meta.OpEntryDelete}
	if len(ops) != len(wantTypes) {
		t.Fatalf("oplog len = %d, want %d", len(ops), len(wantTypes))
	}
	for i, op := range ops {
		if op.Type != wantTypes[i] {
			t.Fatalf("op[%d].Type = %s, want %s", i, op.Type, wantTypes[i])
		}
		if op.Seq != int64(i+1) {
			t.Fatalf("op[%d].Seq = %d, want %d (contiguous)", i, op.Seq, i+1)
		}
	}
}

func TestQuota(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	quota := int64(1 << 20)
	if err := s.CreateBucket(ctx, "qta", &quota); err != nil {
		t.Fatalf("create: %v", err)
	}
	mustPut(t, s, "qta", "a", randBytes(t, 600_000), PutOptions{})
	_, err := s.Put(ctx, "qta", "bkt", bytes.NewReader(randBytes(t, 700_000)), PutOptions{})
	if !errors.Is(err, errs.ErrQuotaExceeded) {
		t.Fatalf("want QuotaExceeded, got %v", err)
	}
	bs, _ := s.BucketStat(ctx, "qta")
	if bs.Used != 600_000 || bs.Objects != 1 {
		t.Fatalf("after quota reject: used=%d objects=%d", bs.Used, bs.Objects)
	}
	// 覆盖写允许负增量（腾出空间）
	mustPut(t, s, "qta", "a", randBytes(t, 500_000), PutOptions{})
	bs, _ = s.BucketStat(ctx, "qta")
	if bs.Used != 500_000 {
		t.Fatalf("after shrink overwrite: used=%d, want 500000", bs.Used)
	}
}

func TestDedup(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)
	data := randBytes(t, 128<<10)
	mustPut(t, s, "bkt", "k1", data, PutOptions{})
	mustPut(t, s, "bkt", "k2", data, PutOptions{})
	if n := countBlobFiles(t, s); n != 1 {
		t.Fatalf("physical blobs = %d, want 1 (dedup)", n)
	}
	// 计费口径是逻辑字节：两份都计
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Used != int64(2*len(data)) {
		t.Fatalf("logical used = %d, want %d", bs.Used, 2*len(data))
	}
}

func TestDirAndList(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)

	if err := s.Mkdir(ctx, "bkt", "d"); err != nil {
		t.Fatalf("mkdir d: %v", err)
	}
	if err := s.Mkdir(ctx, "bkt", "d/sub"); err != nil {
		t.Fatalf("mkdir d/sub: %v", err)
	}
	mustPut(t, s, "bkt", "d/f1", randBytes(t, 16), PutOptions{})
	mustPut(t, s, "bkt", "d/sub/f2", randBytes(t, 16), PutOptions{})
	mustPut(t, s, "bkt", "top", randBytes(t, 16), PutOptions{})

	// 根层：1 item + 1 CP
	r, err := s.List(ctx, "bkt", "", "/", "", 10)
	if err != nil {
		t.Fatalf("list root: %v", err)
	}
	if len(r.Items) != 1 || r.Items[0].Key != "top" || len(r.CommonPrefixes) != 1 || r.CommonPrefixes[0] != "d/" {
		t.Fatalf("root list: items=%v cps=%v", r.Items, r.CommonPrefixes)
	}

	// d/ 层：目录标记 d/ 自身也作为 item 返回（S3 对零字节标记对象同样如此，
	// 树形协议适配层过滤 key == prefix 的条目即可）
	r, _ = s.List(ctx, "bkt", "d/", "/", "", 10)
	if got := keysOf(r.Items); len(got) != 2 || got[0] != "d/" || got[1] != "d/f1" ||
		len(r.CommonPrefixes) != 1 || r.CommonPrefixes[0] != "d/sub/" {
		t.Fatalf("d/ list: items=%v cps=%v", got, r.CommonPrefixes)
	}

	// 分页 max=1：四轮走完全部结果
	r, _ = s.List(ctx, "bkt", "d/", "/", "", 1)
	if got := keysOf(r.Items); len(got) != 1 || got[0] != "d/" || !r.Truncated || r.NextAfter != "d/" {
		t.Fatalf("page1: %+v", r)
	}
	r, _ = s.List(ctx, "bkt", "d/", "/", r.NextAfter, 1)
	if got := keysOf(r.Items); len(got) != 1 || got[0] != "d/f1" || !r.Truncated || r.NextAfter != "d/f1" {
		t.Fatalf("page2: %+v", r)
	}
	r, _ = s.List(ctx, "bkt", "d/", "/", r.NextAfter, 1)
	if len(r.CommonPrefixes) != 1 || r.CommonPrefixes[0] != "d/sub/" || r.Truncated {
		t.Fatalf("page3: %+v", r)
	}
	// CP token（key+\x00）续传：其子键已归并，直接为空
	r, _ = s.List(ctx, "bkt", "d/", "/", "d/sub/\x00", 1)
	if len(r.Items) != 0 || len(r.CommonPrefixes) != 0 || r.Truncated {
		t.Fatalf("after CP token: %+v", r)
	}

	// 无 delim：全部条目平铺（含目录条目）
	r, _ = s.List(ctx, "bkt", "d/", "", "", 100)
	if got := keysOf(r.Items); len(got) != 4 || got[0] != "d/" || got[1] != "d/f1" || got[2] != "d/sub/" || got[3] != "d/sub/f2" {
		t.Fatalf("flat list keys = %v, want [d/ d/f1 d/sub/ d/sub/f2]", got)
	}

	// rmdir 语义
	if err := s.Rmdir(ctx, "bkt", "d"); !errors.Is(err, errs.ErrDirNotEmpty) {
		t.Fatalf("want DirNotEmpty, got %v", err)
	}
	if err := s.Delete(ctx, "bkt", "d/"); !errors.Is(err, errs.ErrIsDirectory) {
		t.Fatalf("delete dir via Delete: want IsDirectory, got %v", err)
	}
	s.Delete(ctx, "bkt", "d/f1")
	s.Delete(ctx, "bkt", "d/sub/f2")
	if err := s.Rmdir(ctx, "bkt", "d/sub"); err != nil {
		t.Fatalf("rmdir d/sub: %v", err)
	}
	if err := s.Rmdir(ctx, "bkt", "d"); err != nil {
		t.Fatalf("rmdir d: %v", err)
	}
}

func keysOf(items []ObjInfo) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.Key)
	}
	return out
}

func TestReadOnly(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)
	s.SetReadOnly(true)
	defer s.SetReadOnly(false)
	if _, err := s.Put(ctx, "bkt", "k", bytes.NewReader([]byte("x")), PutOptions{}); !errors.Is(err, errs.ErrReadOnly) {
		t.Fatalf("want ReadOnly, got %v", err)
	}
	if err := s.Delete(ctx, "bkt", "nope"); errors.Is(err, errs.ErrReadOnly) == false && err == nil {
		t.Fatalf("delete under readonly: %v", err)
	}
	// 读不受影响
	if _, err := s.List(ctx, "bkt", "", "", "", 10); err != nil {
		t.Fatalf("list under readonly should work: %v", err)
	}
}

func TestBucketOps(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.CreateBucket(ctx, "Bad_Name", nil); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if err := s.CreateBucket(ctx, "okb", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateBucket(ctx, "okb", nil); !errors.Is(err, errs.ErrBucketExists) {
		t.Fatalf("want BucketExists, got %v", err)
	}
	if err := s.DeleteBucket(ctx, "okb"); err != nil {
		t.Fatalf("delete empty bucket: %v", err)
	}
	if _, err := s.BucketStat(ctx, "okb"); !errors.Is(err, errs.ErrBucketNotFound) {
		t.Fatalf("want BucketNotFound, got %v", err)
	}
	// 非空桶拒绝删除
	s.CreateBucket(ctx, "full", nil)
	mustPut(t, s, "full", "k", []byte("data"), PutOptions{})
	if err := s.DeleteBucket(ctx, "full"); !errors.Is(err, errs.ErrDirNotEmpty) {
		t.Fatalf("want DirNotEmpty, got %v", err)
	}
}

func TestEmptyAndMissing(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)

	// 空对象
	oi := mustPut(t, s, "bkt", "empty", nil, PutOptions{})
	if oi.Size != 0 || len(oi.Blobs) != 1 || oi.Blobs[0].Size != 0 {
		t.Fatalf("empty object: %+v", oi)
	}
	rc, _, err := s.Get(ctx, "bkt", "empty", nil)
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) != 0 {
		t.Fatalf("empty get returned %d bytes", len(got))
	}

	// 不存在的对象 / 桶
	if _, _, err := s.Get(ctx, "bkt", "nope", nil); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("want KeyNotFound, got %v", err)
	}
	if _, _, err := s.Get(ctx, "nobucket", "k", nil); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("missing bucket behaves as KeyNotFound at store layer: %v", err)
	}
	if err := s.Delete(ctx, "bkt", "nope"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestOplogPayloadShape(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)
	mustPut(t, s, "bkt", "k", []byte("hello"), PutOptions{})
	ops, _ := s.meta.OpsAfter(ctx, 0, 10)
	var put meta.Op
	for _, op := range ops {
		if op.Type == meta.OpEntryPut {
			put = op
		}
	}
	if put.Payload == "" {
		t.Fatalf("no entry_put in oplog")
	}
	if put.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", put.Epoch)
	}
	// payload 是合法 JSON 且含关键字段
	var m map[string]any
	if err := json.Unmarshal([]byte(put.Payload), &m); err != nil {
		t.Fatalf("payload not valid json: %v", err)
	}
	if m["bucket"] != "bkt" || m["etag"] != md5hex([]byte("hello")) {
		t.Fatalf("payload fields: %v", m)
	}
	if _, ok := m["blobs"].([]any); !ok {
		t.Fatalf("payload missing blobs array: %v", m)
	}
}
