package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"xingmeng/internal/errs"
)

func TestMultipartLifecycle(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)

	p1, p2, p3 := randBytes(t, 100<<10), randBytes(t, 50<<10), randBytes(t, 30<<10)
	id, err := s.MpBegin(ctx, "bkt", "big.bin", PutOptions{ContentType: "application/x-test"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// 乱序上传 + part2 重传（记账差值）
	e1, _, _ := s.MpPut(ctx, id, 1, bytes.NewReader(p1))
	s.MpPut(ctx, id, 2, bytes.NewReader(p2))
	e2 := md5hex(p2) // 重传同内容，etag 稳定
	s.MpPut(ctx, id, 2, bytes.NewReader(p2))
	e3, _, _ := s.MpPut(ctx, id, 3, bytes.NewReader(p3))
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Pending != int64(len(p1)+len(p2)+len(p3)) || bs.Used != 0 {
		t.Fatalf("pending accounting: %+v", bs)
	}
	// part etag = 内容 md5
	if e3 != md5hex(p3) {
		t.Fatalf("part etag = %s, want md5", e3)
	}
	// 列 parts
	parts, _ := s.MpParts(ctx, id)
	if len(parts) != 3 || parts[0].No != 1 || parts[2].No != 3 {
		t.Fatalf("parts: %+v", parts)
	}
	// 合成 etag 期望值：md5(md5(p1)||md5(p2)||md5(p3))-3
	h := md5.New()
	for _, p := range [][]byte{p1, p2, p3} {
		sum := md5.Sum(p)
		h.Write(sum[:])
	}
	wantETag := fmt.Sprintf("%s-3", hex.EncodeToString(h.Sum(nil)))

	oi, err := s.MpComplete(ctx, id, []PartRef{{1, e1}, {2, e2}, {3, e3}})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if oi.Size != int64(len(p1)+len(p2)+len(p3)) || oi.ETag != wantETag {
		t.Fatalf("complete result: size=%d etag=%s want %s", oi.Size, oi.ETag, wantETag)
	}
	if oi.ContentType != "application/x-test" {
		t.Fatalf("content type lost: %q", oi.ContentType)
	}
	// 内容拼接正确
	rc, _, err := s.Get(ctx, "bkt", "big.bin", nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var buf bytes.Buffer
	buf.ReadFrom(rc)
	rc.Close()
	if !bytes.Equal(buf.Bytes(), append(append(append([]byte{}, p1...), p2...), p3...)) {
		t.Fatalf("content mismatch")
	}
	// pending 归零、used 接管；会话消失
	bs, _ = s.BucketStat(ctx, "bkt")
	if bs.Pending != 0 || bs.Used != int64(len(p1)+len(p2)+len(p3)) || bs.Objects != 1 {
		t.Fatalf("after complete: %+v", bs)
	}
	if parts, err := s.MpParts(ctx, id); err == nil && len(parts) != 0 {
		t.Fatalf("session must be gone after complete")
	}
}

func TestMultipartCompleteValidation(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "bkt", nil)
	p1, p2, p3 := randBytes(t, 16<<10), randBytes(t, 16<<10), randBytes(t, 16<<10)

	id, _ := s.MpBegin(ctx, "bkt", "obj", PutOptions{})
	e1, _, _ := s.MpPut(ctx, id, 1, bytes.NewReader(p1))
	s.MpPut(ctx, id, 2, bytes.NewReader(p2))
	e3, _, _ := s.MpPut(ctx, id, 3, bytes.NewReader(p3))

	// 错误 etag → 拒绝，会话保留可重试
	if _, err := s.MpComplete(ctx, id, []PartRef{{1, e1}, {2, "deadbeef"}}); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if _, err := s.MpParts(ctx, id); err != nil {
		t.Fatalf("session must survive failed complete: %v", err)
	}

	// 只选部分 part：对象 = 选中部分，全部 pending 退还（S3 语义）
	oi, err := s.MpComplete(ctx, id, []PartRef{{1, e1}, {3, e3}})
	if err != nil {
		t.Fatalf("partial complete: %v", err)
	}
	if oi.Size != int64(len(p1)+len(p3)) {
		t.Fatalf("partial size: %d", oi.Size)
	}
	bs, _ := s.BucketStat(ctx, "bkt")
	if bs.Pending != 0 {
		t.Fatalf("pending must fully refund: %+v", bs)
	}
	// 未选中的 part2 blob 无引用，GC 应回收
	res, err := s.RunGCOnce(ctx)
	if err != nil || res.Trashed != 1 {
		t.Fatalf("gc should trash unreferenced part: %v %+v", err, res)
	}
}

func TestMultipartAbortAndQuota(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	quota := int64(1 << 20)
	s.CreateBucket(ctx, "qta", &quota)

	id, _ := s.MpBegin(ctx, "qta", "obj", PutOptions{})
	if _, _, err := s.MpPut(ctx, id, 1, bytes.NewReader(randBytes(t, 600<<10))); err != nil {
		t.Fatalf("part1: %v", err)
	}
	// 配额 1MB：600KB part 已 pending，再传 600KB 超限
	if _, _, err := s.MpPut(ctx, id, 2, bytes.NewReader(randBytes(t, 600<<10))); !errors.Is(err, errs.ErrQuotaExceeded) {
		t.Fatalf("want QuotaExceeded, got %v", err)
	}
	// abort 退还
	if err := s.MpAbort(ctx, id); err != nil {
		t.Fatalf("abort: %v", err)
	}
	bs, _ := s.BucketStat(ctx, "qta")
	if bs.Pending != 0 || bs.Used != 0 {
		t.Fatalf("after abort: %+v", bs)
	}
	if _, err := s.MpParts(ctx, id); !errors.Is(err, errs.ErrUploadNotFound) {
		t.Fatalf("session gone: %v", err)
	}
	// 未知会话
	if _, _, err := s.MpPut(ctx, "nope", 1, bytes.NewReader([]byte("x"))); !errors.Is(err, errs.ErrUploadNotFound) {
		t.Fatalf("want UploadNotFound, got %v", err)
	}
}
