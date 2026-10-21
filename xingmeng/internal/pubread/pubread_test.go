package pubread

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"xingmeng/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
		KEK:            []byte("pubread-test-kek"), // 建桶即发钥（99-D025）
		GCGrace:        time.Hour,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestPublicRead 公共读面全态（99-D024）：匿名读放行、私有/不存在同构 404、
// Range/条件请求、无缓存头、serve 闸门外的只读面方法限制。
func TestPublicRead(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.CreateBucket(ctx, "pub", nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateBucket(ctx, "priv", nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "pub", "a/b.txt", strings.NewReader("hello-public"), store.PutOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "priv", "x.txt", strings.NewReader("secret-data"), store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(ctx, "pub", "d"); err != nil {
		t.Fatal(err)
	}
	h := New(s)
	srv := httptest.NewServer(h)
	defer srv.Close()

	get := func(path string, hdr map[string]string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	body := func(r *http.Response) string {
		t.Helper()
		var sb strings.Builder
		buf := make([]byte, 64)
		for {
			n, err := r.Body.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		return sb.String()
	}

	// 公共桶匿名读：内容、ETag、Content-Type、无缓存头
	r := get("/pub/a/b.txt", nil)
	if r.StatusCode != 200 || body(r) != "hello-public" {
		t.Fatalf("public read: %d %q", r.StatusCode, body(r))
	}
	if r.Header.Get("Content-Type") != "text/plain" || r.Header.Get("ETag") == "" {
		t.Fatalf("headers: %+v", r.Header)
	}
	if cc := r.Header.Get("Cache-Control"); cc != "" {
		t.Fatalf("must not emit cache headers (99-D024): %q", cc)
	}
	etag := r.Header.Get("ETag")

	// 同构 404：私有桶 / 桶不存在 / 对象不存在 / 目录 / 根路径
	for _, p := range []string{"/priv/x.txt", "/nobkt/x.txt", "/pub/missing.txt", "/pub/d/", "/", "/pub"} {
		if r := get(p, nil); r.StatusCode != 404 {
			t.Fatalf("%s must 404, got %d", p, r.StatusCode)
		}
	}

	// Range：单区间 + 后缀区间；越界 416
	r = get("/pub/a/b.txt", map[string]string{"Range": "bytes=6-"})
	if r.StatusCode != 206 || body(r) != "public" {
		t.Fatalf("range 6-: %d %q", r.StatusCode, body(r))
	}
	r = get("/pub/a/b.txt", map[string]string{"Range": "bytes=-6"})
	if r.StatusCode != 206 || body(r) != "public" {
		t.Fatalf("range -6: %d %q", r.StatusCode, body(r))
	}
	r = get("/pub/a/b.txt", map[string]string{"Range": "bytes=99-"})
	if r.StatusCode != 416 {
		t.Fatalf("unsatisfiable range must 416, got %d", r.StatusCode)
	}

	// 条件请求：If-None-Match 命中 304；If-Modified-Since 未来时间 304
	r = get("/pub/a/b.txt", map[string]string{"If-None-Match": etag})
	if r.StatusCode != 304 {
		t.Fatalf("if-none-match must 304, got %d", r.StatusCode)
	}
	r = get("/pub/a/b.txt", map[string]string{"If-Modified-Since": time.Now().UTC().Add(time.Hour).Format(http.TimeFormat)})
	if r.StatusCode != 304 {
		t.Fatalf("if-modified-since must 304, got %d", r.StatusCode)
	}

	// HEAD 同语义无 body；POST/PUT 拒绝 405
	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/pub/a/b.txt", nil)
	hr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	hr.Body.Close()
	if hr.StatusCode != 200 || hr.ContentLength != int64(len("hello-public")) {
		t.Fatalf("HEAD: %d len=%d", hr.StatusCode, hr.ContentLength)
	}
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/pub/a/b.txt", strings.NewReader("x"))
	pr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	pr.Body.Close()
	if pr.StatusCode != 405 {
		t.Fatalf("POST must 405, got %d", pr.StatusCode)
	}
}
