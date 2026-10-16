package repl

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

func newStore(t *testing.T, name string) *store.Store {
	t.Helper()
	s, err := store.Open(store.Config{
		DataDir:        t.TempDir() + "/" + name,
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
		GCGrace:        time.Hour,
	})
	if err != nil {
		t.Fatalf("open store %s: %v", name, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func randData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func mustPutS(t *testing.T, s *store.Store, bucket, key string, n int, opts store.PutOptions) *store.ObjInfo {
	t.Helper()
	oi, err := s.Put(context.Background(), bucket, key, bytes.NewReader(randData(t, n)), opts)
	if err != nil {
		t.Fatalf("put %s/%s: %v", bucket, key, err)
	}
	return oi
}

func drain(t *testing.T, ctx context.Context, sl *Slave) {
	t.Helper()
	for i := 0; i < 100; i++ {
		n, err := sl.SyncOnce(ctx)
		if err != nil {
			t.Fatalf("sync once: %v", err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatalf("slave did not catch up in 100 rounds")
}

func TestReplicationBasic(t *testing.T) {
	ctx := context.Background()
	mst := newStore(t, "m")
	slvStore := newStore(t, "s")

	// 主节点造数据：两桶、小文件、分块大文件、覆盖写、目录、move、tree_move、删除
	if err := mst.CreateBucket(ctx, "alpha", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	quota := int64(1 << 30)
	mst.CreateBucket(ctx, "beta", &quota)
	mustPutS(t, mst, "alpha", "small.txt", 512<<10, store.PutOptions{ContentType: "text/plain"})
	mustPutS(t, mst, "alpha", "big.bin", 5<<20, store.PutOptions{})     // 两块 blob
	mustPutS(t, mst, "alpha", "small.txt", 300<<10, store.PutOptions{}) // 覆盖
	mst.Mkdir(ctx, "alpha", "d")
	mustPutS(t, mst, "alpha", "d/x", 4<<10, store.PutOptions{})
	mst.SetQuota(ctx, "beta", nil)
	if err := mst.Move(ctx, "alpha", "d/x", "d/y"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if err := mst.Move(ctx, "alpha", "d/", "e/"); err != nil {
		t.Fatalf("tree move: %v", err)
	}
	mustPutS(t, mst, "alpha", "gone", 8<<10, store.PutOptions{})
	if err := mst.Delete(ctx, "alpha", "gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	master := NewMaster(mst, "tok")
	srv := httptest.NewServer(master.Handler())
	defer srv.Close()
	sl, err := NewSlave(SlaveConfig{ID: "s1", MasterURL: srv.URL, Token: "tok", Batch: 3}, slvStore)
	if err != nil {
		t.Fatalf("new slave: %v", err)
	}
	if err := sl.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	drain(t, ctx, sl)

	// 条目一致，且每个 blob 引用在从节点数据面就位
	for _, c := range []struct{ bucket, key string }{
		{"alpha", "small.txt"}, {"alpha", "big.bin"}, {"alpha", "e/"}, {"alpha", "e/y"},
	} {
		mi, merr := mst.Stat(ctx, c.bucket, c.key)
		si, serr := slvStore.Stat(ctx, c.bucket, c.key)
		if merr != nil || serr != nil {
			t.Fatalf("stat %v: master=%v slave=%v", c, merr, serr)
		}
		if mi.Size != si.Size || mi.ETag != si.ETag || mi.IsDir != si.IsDir {
			t.Fatalf("entry mismatch %v: master=%+v slave=%+v", c, mi, si)
		}
		for _, ref := range mi.Blobs {
			if !slvStore.HasBlob(ref.Sha256) {
				t.Fatalf("slave missing blob %s for %v", ref.Sha256, c)
			}
		}
	}
	if _, err := slvStore.Stat(ctx, "alpha", "gone"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("deleted key must not be live on slave: %v", err)
	}
	if _, err := slvStore.Stat(ctx, "alpha", "d/"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("tree-moved prefix must be gone: %v", err)
	}
	// 桶计数一致（计费口径在从节点同步保持，提升后即接管）
	mb, _ := mst.BucketStat(ctx, "alpha")
	sb, _ := slvStore.BucketStat(ctx, "alpha")
	if mb.Used != sb.Used || mb.Objects != sb.Objects {
		t.Fatalf("bucket stat mismatch: master=%+v slave=%+v", mb, sb)
	}
	// 日志历史逐条一致（同历史续传的前提，99-D013）
	ml, _ := mst.LastSeq(ctx)
	slq, _ := slvStore.LastSeq(ctx)
	if ml == 0 || ml != slq {
		t.Fatalf("last seq: master=%d slave=%d", ml, slq)
	}
	mops, _ := mst.Ops(ctx, 0, 10000)
	sops, _ := slvStore.Ops(ctx, 0, 10000)
	if len(mops) != len(sops) {
		t.Fatalf("oplog length: master=%d slave=%d", len(mops), len(sops))
	}
	for i := range mops {
		if mops[i].Seq != sops[i].Seq || mops[i].Type != sops[i].Type {
			t.Fatalf("op[%d] mismatch: %v vs %v", i, mops[i], sops[i])
		}
	}
	// 增量：主节点再写一条，从节点一轮追上
	mustPutS(t, mst, "alpha", "more", 16<<10, store.PutOptions{})
	n, err := sl.SyncOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("incremental sync: n=%d err=%v", n, err)
	}
	if _, err := slvStore.Stat(ctx, "alpha", "more"); err != nil {
		t.Fatalf("incremental entry: %v", err)
	}
}

// TestTombstoneSkip 验证"删除与同步赛跑"场景（99-D006）：
// 主节点标记 GC 后 blob 进入墓碑态，晚来的从节点重放 entry_put 时
// GET blob 得 410，跳过下载照常回放，后续 entry_delete 移除条目——最终态一致，无重试风暴。
func TestTombstoneSkip(t *testing.T) {
	ctx := context.Background()
	mst := newStore(t, "m")
	slvStore := newStore(t, "s")

	mst.CreateBucket(ctx, "bkt", nil)
	oi := mustPutS(t, mst, "bkt", "k", 64<<10, store.PutOptions{})
	if err := mst.Delete(ctx, "bkt", "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := mst.RunGCOnce(ctx); err != nil { // grace=1h：标记入 trash，不清扫
		t.Fatalf("gc: %v", err)
	}

	master := NewMaster(mst, "")
	srv := httptest.NewServer(master.Handler())
	defer srv.Close()
	sl, _ := NewSlave(SlaveConfig{ID: "s1", MasterURL: srv.URL}, slvStore)
	drain(t, ctx, sl)

	if _, err := slvStore.Stat(ctx, "bkt", "k"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("k must not be live: %v", err)
	}
	if slvStore.HasBlob(oi.Blobs[0].Sha256) {
		t.Fatalf("tombstoned blob must not be downloaded")
	}
	ml, _ := mst.LastSeq(ctx)
	slq, _ := slvStore.LastSeq(ctx)
	if ml != slq {
		t.Fatalf("seq: %d vs %d", ml, slq)
	}
}

// TestResnapshotBootstrap 验证晚加入从节点路径（99-D004）：
// 日志被截断后新从节点 tail 得 409，转快照重建，blob 保留只补缺失。
func TestResnapshotBootstrap(t *testing.T) {
	ctx := context.Background()
	mst := newStore(t, "m")
	slvStore := newStore(t, "s")

	mst.CreateBucket(ctx, "bkt", nil)
	mustPutS(t, mst, "bkt", "a", 100<<10, store.PutOptions{})
	mustPutS(t, mst, "bkt", "b", 100<<10, store.PutOptions{})
	mustPutS(t, mst, "bkt", "c", 100<<10, store.PutOptions{})
	last, _ := mst.LastSeq(ctx)

	// 模拟日志截断：只保留最后一条
	if _, err := mst.TruncateOplog(ctx, last); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	master := NewMaster(mst, "")
	srv := httptest.NewServer(master.Handler())
	defer srv.Close()
	sl, _ := NewSlave(SlaveConfig{ID: "s2", MasterURL: srv.URL}, slvStore)

	// tail 起点低于保留期 → 409
	if _, err := sl.SyncOnce(ctx); !errors.Is(err, ErrNeedResnapshot) {
		t.Fatalf("want ErrNeedResnapshot, got %v", err)
	}
	// 快照重建 + 追平
	if err := sl.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	drain(t, ctx, sl)

	for _, k := range []string{"a", "b", "c"} {
		mi, merr := mst.Stat(ctx, "bkt", k)
		si, serr := slvStore.Stat(ctx, "bkt", k)
		if merr != nil || serr != nil || mi.Size != si.Size {
			t.Fatalf("after bootstrap %s: %v %v", k, merr, serr)
		}
		if !slvStore.HasBlob(mi.Blobs[0].Sha256) {
			t.Fatalf("blob for %s missing after bootstrap", k)
		}
	}
	ml, _ := mst.LastSeq(ctx)
	slq, _ := slvStore.LastSeq(ctx)
	if ml != slq {
		t.Fatalf("seq after bootstrap: %d vs %d", ml, slq)
	}
}
