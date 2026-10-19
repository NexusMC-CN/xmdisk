package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

func TestMoveFile(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)

	data := randBytes(t, 64<<10)
	oi := mustPut(t, s, "bkt", "a/f1", data, PutOptions{Meta: map[string]string{"k": "v"}})
	if err := s.Move(ctx, "bkt", "a/f1", "b/f2"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, err := s.Stat(ctx, "bkt", "a/f1"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("src should be gone: %v", err)
	}
	got, err := s.Stat(ctx, "bkt", "b/f2")
	if err != nil {
		t.Fatalf("dst stat: %v", err)
	}
	if got.Size != oi.Size || got.ETag != oi.ETag || got.UserMeta["k"] != "v" {
		t.Fatalf("moved entry mismatch: %+v", got)
	}
	if got.Blobs[0].Sha256 != oi.Blobs[0].Sha256 {
		t.Fatalf("blob refs must move unchanged")
	}
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Used != int64(len(data)) || bs.Objects != 1 {
		t.Fatalf("move must not change usage: %+v", bs)
	}
	// ops：put + put(dst) + delete(src) 相邻
	ops, _ := s.Ops(ctx, 0, 100)
	if len(ops) < 3 || ops[len(ops)-2].Type != meta.OpEntryPut || ops[len(ops)-1].Type != meta.OpEntryDelete {
		t.Fatalf("move ops tail: %v", ops)
	}
}

func TestMoveFileOverwrite(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)
	mustPut(t, s, "bkt", "src", randBytes(t, 100<<10), PutOptions{})
	mustPut(t, s, "bkt", "dst", randBytes(t, 40<<10), PutOptions{})
	if err := s.Move(ctx, "bkt", "src", "dst"); err != nil {
		t.Fatalf("move: %v", err)
	}
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Used != 100<<10 || bs.Objects != 1 {
		t.Fatalf("overwrite move accounting: used=%d objects=%d", bs.Used, bs.Objects)
	}
}

func TestMoveTree(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)
	if err := s.Mkdir(ctx, "bkt", "d"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := s.Mkdir(ctx, "bkt", "d/sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mustPut(t, s, "bkt", "d/f1", randBytes(t, 16<<10), PutOptions{})
	mustPut(t, s, "bkt", "d/sub/f2", randBytes(t, 32<<10), PutOptions{})
	before, _ := s.BucketStat(ctx, "bkt")

	opsBefore := len(mustOps(t, s))
	if err := s.Move(ctx, "bkt", "d/", "e/"); err != nil {
		t.Fatalf("tree move: %v", err)
	}
	// 全部条目迁移
	for _, k := range []string{"d/", "d/f1", "d/sub/", "d/sub/f2"} {
		if _, err := s.Stat(ctx, "bkt", k); !errors.Is(err, errs.ErrKeyNotFound) {
			t.Fatalf("%s should be gone: %v", k, err)
		}
	}
	for _, k := range []string{"e/", "e/f1", "e/sub/", "e/sub/f2"} {
		if _, err := s.Stat(ctx, "bkt", k); err != nil {
			t.Fatalf("%s should exist: %v", k, err)
		}
	}
	after, _ := s.BucketStat(ctx, "bkt")
	if after.Used != before.Used || after.Objects != before.Objects {
		t.Fatalf("tree move accounting: before=%+v after=%+v", before, after)
	}
	// 单条 tree_move op（99-D011）
	ops := mustOps(t, s)
	if len(ops) != opsBefore+1 || ops[len(ops)-1].Type != meta.OpTreeMove {
		t.Fatalf("want exactly one tree_move op appended, got %d ops, last=%s", len(ops)-opsBefore, ops[len(ops)-1].Type)
	}
	// 移入自身拒绝
	if err := s.Move(ctx, "bkt", "e/", "e/x/"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("move into itself: %v", err)
	}
}

func mustOps(t *testing.T, s *Store) []meta.Op {
	t.Helper()
	ops, err := s.Ops(context.Background(), 0, 10000)
	if err != nil {
		t.Fatalf("ops: %v", err)
	}
	return ops
}

func TestSetAttrs(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil, false)
	oi := mustPut(t, s, "bkt", "f", []byte("hello"), PutOptions{ContentType: "text/plain"})

	mode := uint32(0o600)
	mt := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := s.SetAttrs(ctx, "bkt", "f", AttrPatch{
		Mode: &mode, MTime: &mt, ContentType: strPtr("application/json"),
		Meta: map[string]string{"a": "1"},
	}); err != nil {
		t.Fatalf("setattrs: %v", err)
	}
	got, err := s.Stat(ctx, "bkt", "f")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got.Mode != 0o600 || got.ContentType != "application/json" || got.UserMeta["a"] != "1" {
		t.Fatalf("attrs not applied: %+v", got)
	}
	if got.Size != oi.Size || got.ETag != oi.ETag {
		t.Fatalf("size/etag must not change: %+v", got)
	}
	if !got.MTime.Truncate(time.Second).Equal(mt) {
		t.Fatalf("mtime = %v, want %v", got.MTime, mt)
	}
	// 产生 entry_put（全量状态 op）
	ops := mustOps(t, s)
	if ops[len(ops)-1].Type != meta.OpEntryPut {
		t.Fatalf("setattrs should emit entry_put, got %s", ops[len(ops)-1].Type)
	}
}

func strPtr(s string) *string { return &s }
