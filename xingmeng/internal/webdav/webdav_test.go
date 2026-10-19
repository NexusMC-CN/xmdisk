package webdav

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

const (
	testAK     = "DAVUSER01"
	testSK     = "dav-secret-0123456789"
	testBucket = "dav-bkt"
)

func newTestRouter(t *testing.T) (http.Handler, *store.Store) {
	t.Helper()
	st, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
		GCGrace:        time.Hour,
		KEK:            []byte("dav-test-kek"),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	ctx := context.Background()
	st.CreateBucket(ctx, testBucket, nil, false)
	st.CreateCred(ctx, testAK, testSK, testBucket)
	return Router(st), st
}

func randData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	crand.Read(b)
	return b
}

func doReq(t *testing.T, h http.Handler, method, path string, body []byte, auth bool) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if auth {
		req.SetBasicAuth(testAK, testSK)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebDAVLifecycle(t *testing.T) {
	h, st := newTestRouter(t)
	ctx := context.Background()

	// PUT / GET / 覆盖 / HEAD
	data := randData(t, 128<<10)
	if rec := doReq(t, h, "PUT", "/dav/"+testBucket+"/a.bin", data, true); rec.Code != 201 {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}
	if rec := doReq(t, h, "GET", "/dav/"+testBucket+"/a.bin", nil, true); rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("get: %d len=%d", rec.Code, rec.Body.Len())
	}
	if rec := doReq(t, h, "PUT", "/dav/"+testBucket+"/a.bin", []byte("small"), true); rec.Code != 201 {
		t.Fatalf("overwrite: %d", rec.Code)
	}
	// 大文件（分块路径）
	big := randData(t, 5<<20)
	if rec := doReq(t, h, "PUT", "/dav/"+testBucket+"/big.bin", big, true); rec.Code != 201 {
		t.Fatalf("put big: %d", rec.Code)
	}
	// Range GET（ServeContent 依赖 Seek）
	req := httptest.NewRequest("GET", "/dav/"+testBucket+"/big.bin", nil)
	req.SetBasicAuth(testAK, testSK)
	req.Header.Set("Range", "bytes=1000-1099")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 206 || !bytes.Equal(rec.Body.Bytes(), big[1000:1100]) {
		t.Fatalf("range get: %d", rec.Code)
	}

	// MKCOL + 嵌套 + PROPFIND
	if rec := doReq(t, h, "MKCOL", "/dav/"+testBucket+"/dir", nil, true); rec.Code != 201 {
		t.Fatalf("mkcol: %d %s", rec.Code, rec.Body)
	}
	if rec := doReq(t, h, "PUT", "/dav/"+testBucket+"/dir/nested.txt", []byte("x"), true); rec.Code != 201 {
		t.Fatalf("put nested: %d", rec.Code)
	}
	req = httptest.NewRequest("PROPFIND", "/dav/"+testBucket+"/", nil)
	req.SetBasicAuth(testAK, testSK)
	req.Header.Set("Depth", "1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 207 {
		t.Fatalf("propfind: %d %s", rec.Code, rec.Body)
	}
	for _, want := range []string{"a.bin", "big.bin", "dir/"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("propfind missing %s: %s", want, rec.Body)
		}
	}

	// MOVE 文件与集合
	if rec := doReq(t, h, "MOVE", "/dav/"+testBucket+"/a.bin", nil, true); rec.Code < 400 {
		// 缺 Destination 头，应失败
		t.Fatalf("move without destination: %d", rec.Code)
	}
	req = httptest.NewRequest("MOVE", "/dav/"+testBucket+"/a.bin", nil)
	req.SetBasicAuth(testAK, testSK)
	req.Header.Set("Destination", "/dav/"+testBucket+"/dir/a2.bin")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 && rec.Code != 204 {
		t.Fatalf("move file: %d %s", rec.Code, rec.Body)
	}
	req = httptest.NewRequest("MOVE", "/dav/"+testBucket+"/dir", nil)
	req.SetBasicAuth(testAK, testSK)
	req.Header.Set("Destination", "/dav/"+testBucket+"/dir2")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 && rec.Code != 204 {
		t.Fatalf("move dir: %d %s", rec.Code, rec.Body)
	}
	if _, err := st.Stat(ctx, testBucket, "dir2/nested.txt"); err != nil {
		t.Fatalf("moved nested content: %v", err)
	}

	// DELETE 文件与递归删集合
	if rec := doReq(t, h, "DELETE", "/dav/"+testBucket+"/dir2/a2.bin", nil, true); rec.Code != 204 {
		t.Fatalf("delete file: %d", rec.Code)
	}
	if rec := doReq(t, h, "DELETE", "/dav/"+testBucket+"/dir2", nil, true); rec.Code != 204 {
		t.Fatalf("delete dir: %d %s", rec.Code, rec.Body)
	}
	if _, err := st.Stat(ctx, testBucket, "dir2/nested.txt"); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("recursive delete must remove descendants: %v", err)
	}
}

func TestWebDAVAuthAndQuota(t *testing.T) {
	h, st := newTestRouter(t)
	ctx := context.Background()

	// 认证：无 / 错误密码 / 其他桶 → 401
	if rec := doReq(t, h, "GET", "/dav/"+testBucket+"/x", nil, false); rec.Code != 401 {
		t.Fatalf("no auth: %d", rec.Code)
	}
	req := httptest.NewRequest("GET", "/dav/"+testBucket+"/x", nil)
	req.SetBasicAuth(testAK, "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatalf("wrong password: %d", rec.Code)
	}
	// 别的桶（凭据不匹配）
	if rec := doReq(t, h, "GET", "/dav/other-bkt/x", nil, true); rec.Code != 401 {
		t.Fatalf("bucket mismatch: %d", rec.Code)
	}

	// 配额：带 Content-Length 的 PUT 超额 → 507（路由层预检，不读 body）
	quota := int64(1 << 20)
	st.CreateBucket(ctx, "tiny", &quota, false)
	st.CreateCred(ctx, "TINYAK1", "tiny-secret-0123", "tiny")
	req = httptest.NewRequest("PUT", "/dav/tiny/big.bin", bytes.NewReader(make([]byte, 32<<20)))
	req.SetBasicAuth("TINYAK1", "tiny-secret-0123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("quota pre-check: %d", rec.Code)
	}
	if _, err := st.Stat(ctx, "tiny", "big.bin"); err == nil {
		t.Fatalf("no entry expected")
	}
	// 无长度流式写入 → Write 即刻限流（x/net/webdav 固定映射 405，docs/06 已记偏离）
	req = httptest.NewRequest("PUT", "/dav/tiny/stream.bin", io.NopCloser(bytes.NewReader(make([]byte, 32<<20))))
	req.ContentLength = -1 // 强制未知长度
	req.SetBasicAuth("TINYAK1", "tiny-secret-0123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code < 400 {
		t.Fatalf("streaming quota must fail: %d", rec.Code)
	}
	if _, err := st.Stat(ctx, "tiny", "stream.bin"); err == nil {
		t.Fatalf("no entry expected after aborted stream")
	}
	// 正常额度内写入不受影响（512KB < 1MB）
	req = httptest.NewRequest("PUT", "/dav/tiny/ok.bin", bytes.NewReader(randData(t, 512<<10)))
	req.SetBasicAuth("TINYAK1", "tiny-secret-0123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("in-quota put: %d %s", rec.Code, rec.Body)
	}
	// 再写 600KB 超过总额度 → 507
	req = httptest.NewRequest("PUT", "/dav/tiny/ok2.bin", bytes.NewReader(randData(t, 600<<10)))
	req.SetBasicAuth("TINYAK1", "tiny-secret-0123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-quota with content-length: %d", rec.Code)
	}
}
