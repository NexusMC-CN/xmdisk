package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"xingmeng/internal/errs"
)

// countingReader 统计从源实际拉取的字节数：验证配额守卫"早中断"，
// 而不是"流完 32MB 后事务才拒绝"（用户提出的绕过场景，99-D019）。
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func TestStreamingQuotaGuardPut(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	quota := int64(1 << 20)
	s.CreateBucket(ctx, "qta", &quota, false)

	big := bytes.NewReader(randBytes(t, 32<<20))
	cr := &countingReader{r: big}
	_, err := s.Put(ctx, "qta", "huge.bin", cr, PutOptions{})
	if !errors.Is(err, errs.ErrQuotaExceeded) {
		t.Fatalf("want QuotaExceeded, got %v", err)
	}
	// 早中断：从源消费的量 ~ 额度（1MB）+ bufio 预读缓冲，远小于 32MB
	if cr.n > quota+(2<<20) {
		t.Fatalf("guard must abort early: consumed %d bytes", cr.n)
	}
	// 磁盘零残留（临时文件在错误路径清理，无 blob 入库）
	if n := countBlobFiles(t, s); n != 0 {
		t.Fatalf("no blob should be committed, got %d", n)
	}
	if _, err := s.Stat(ctx, "qta", "huge.bin"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("no entry expected: %v", err)
	}

	// 覆盖写抵扣：先存 900KB，再流式覆盖大文件仍被守卫拦住
	mustPut(t, s, "qta", "k", randBytes(t, 900<<10), PutOptions{})
	cr2 := &countingReader{r: bytes.NewReader(randBytes(t, 32<<20))}
	_, err = s.Put(ctx, "qta", "k", cr2, PutOptions{})
	if !errors.Is(err, errs.ErrQuotaExceeded) {
		t.Fatalf("overwrite guard: %v", err)
	}
	if cr2.n > (1<<20)+(2<<20) { // 额度（抵扣后 1MB）+ 缓冲余量
		t.Fatalf("overwrite guard consumed %d", cr2.n)
	}
	bs, _ := s.BucketStat(ctx, "qta")
	if bs.Used != 900<<10 {
		t.Fatalf("usage must be untouched after aborted stream: %+v", bs)
	}
}

func TestStreamingQuotaGuardMultipart(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	quota := int64(1 << 20)
	s.CreateBucket(ctx, "qta", &quota, false)

	id, _ := s.MpBegin(ctx, "qta", "mp.bin", PutOptions{})
	cr := &countingReader{r: bytes.NewReader(randBytes(t, 32<<20))}
	_, _, err := s.MpPut(ctx, id, 1, cr)
	if !errors.Is(err, errs.ErrQuotaExceeded) {
		t.Fatalf("want QuotaExceeded, got %v", err)
	}
	if cr.n > quota+(2<<20) {
		t.Fatalf("part guard must abort early: consumed %d", cr.n)
	}
	bs, _ := s.BucketStat(ctx, "qta")
	if bs.Pending != 0 {
		t.Fatalf("pending must stay 0 after aborted part: %+v", bs)
	}
	// 会话仍可用：正常小 part 不受影响
	if _, _, err := s.MpPut(ctx, id, 1, bytes.NewReader(randBytes(t, 500<<10))); err != nil {
		t.Fatalf("normal part after guard: %v", err)
	}
}

func TestStreamingQuotaUnlimitedUnaffected(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	s.CreateBucket(ctx, "free", nil, false)                         // 不限桶：守卫关闭
	mustPut(t, s, "free", "big", randBytes(t, 5<<20), PutOptions{}) // 两块分块路径
}
