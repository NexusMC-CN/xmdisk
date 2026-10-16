package s3

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"xingmeng/internal/store"
)

const (
	testAK     = "TESTAK01"
	testSK     = "test-secret-0123456789"
	testKEK    = "unit-test-kek"
	testBucket = "tbkt"
)

func newTestHandler(t *testing.T) (*Handler, *store.Store) {
	t.Helper()
	s, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
		KEK:            []byte(testKEK),
		GCGrace:        time.Hour,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.CreateBucket(context.Background(), testBucket, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := s.CreateCred(context.Background(), testAK, testSK, testBucket); err != nil {
		t.Fatalf("create cred: %v", err)
	}
	return New(s), s
}

// signRequest 测试侧 SigV4 签名器（与被测验签器独立生成规范串，交叉验证实现）。
func signRequest(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format(amzTimeFormat)
	shortDate := now.Format(amzShortFormat)
	req.Header.Set("X-Amz-Date", amzDate)
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	names := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(names)
	creq := canonicalRequest(req, names, payloadHash, false)
	sts := stringToSign(amzDate, shortDate, "us-east-1", "s3", creq)
	sig := hmacHex(signingKey(testSK, shortDate, "us-east-1", "s3"), sts)
	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s/us-east-1/s3/aws4_request, SignedHeaders=%s, Signature=%s",
		algorithm, testAK, shortDate, strings.Join(names, ";"), sig))
}

func doSigned(t *testing.T, h *Handler, method, target string, body []byte, hdrs map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	signRequest(req, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func randData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	crand.Read(b)
	return b
}

func TestS3ObjectLifecycle(t *testing.T) {
	h, _ := newTestHandler(t)
	data := randData(t, 256<<10)

	// PUT
	rec := doSigned(t, h, http.MethodPut, "/"+testBucket+"/dir/obj.bin", data,
		map[string]string{"Content-Type": "application/x-bin", "x-amz-meta-owner": "test"})
	if rec.Code != 200 {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}
	etag := rec.Header().Get("ETag")

	// GET
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil, nil)
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("get: %d len=%d", rec.Code, rec.Body.Len())
	}
	if rec.Header().Get("ETag") != etag || rec.Header().Get("Content-Type") != "application/x-bin" {
		t.Fatalf("headers: %v", rec.Header())
	}
	if rec.Header().Get("x-amz-meta-owner") != "test" {
		t.Fatalf("user meta lost")
	}

	// HEAD
	rec = doSigned(t, h, http.MethodHead, "/"+testBucket+"/dir/obj.bin", nil, nil)
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatalf("head: %d", rec.Code)
	}

	// 条件请求
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil,
		map[string]string{"If-None-Match": etag})
	if rec.Code != 304 {
		t.Fatalf("if-none-match: %d", rec.Code)
	}
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil,
		map[string]string{"If-Match": `"bogus"`})
	if rec.Code != 412 {
		t.Fatalf("if-match: %d", rec.Code)
	}

	// Range
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil,
		map[string]string{"Range": fmt.Sprintf("bytes=100-%d", 199)})
	if rec.Code != 206 || rec.Body.Len() != 100 || !bytes.Equal(rec.Body.Bytes(), data[100:200]) {
		t.Fatalf("range: %d len=%d", rec.Code, rec.Body.Len())
	}
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil,
		map[string]string{"Range": "bytes=-64"})
	if rec.Code != 206 || !bytes.Equal(rec.Body.Bytes(), data[len(data)-64:]) {
		t.Fatalf("suffix range: %d", rec.Code)
	}

	// 覆盖写 + If-None-Match:*（create-only）
	rec = doSigned(t, h, http.MethodPut, "/"+testBucket+"/dir/obj.bin", []byte("new"), nil)
	if rec.Code != 200 {
		t.Fatalf("overwrite put: %d", rec.Code)
	}
	rec = doSigned(t, h, http.MethodPut, "/"+testBucket+"/new-obj", []byte("x"),
		map[string]string{"If-None-Match": "*"})
	if rec.Code != 200 {
		t.Fatalf("create-only put: %d", rec.Code)
	}
	rec = doSigned(t, h, http.MethodPut, "/"+testBucket+"/new-obj", []byte("y"),
		map[string]string{"If-None-Match": "*"})
	if rec.Code != 412 {
		t.Fatalf("create-only conflict: %d", rec.Code)
	}

	// DELETE
	rec = doSigned(t, h, http.MethodDelete, "/"+testBucket+"/dir/obj.bin", nil, nil)
	if rec.Code != 204 {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/dir/obj.bin", nil, nil)
	if rec.Code != 404 || !strings.Contains(rec.Body.String(), "NoSuchKey") {
		t.Fatalf("get deleted: %d %s", rec.Code, rec.Body)
	}
	// 幂等删除
	rec = doSigned(t, h, http.MethodDelete, "/"+testBucket+"/dir/obj.bin", nil, nil)
	if rec.Code != 204 {
		t.Fatalf("idempotent delete: %d", rec.Code)
	}
}

func TestS3AuthFailures(t *testing.T) {
	h, _ := newTestHandler(t)

	// 未签名
	req := httptest.NewRequest(http.MethodGet, "/"+testBucket+"/k", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("unsigned: %d %s", rec.Code, rec.Body)
	}

	// 错误 AK
	req = httptest.NewRequest(http.MethodGet, "/"+testBucket+"/k", nil)
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format(amzTimeFormat))
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	req.Header.Set("Authorization",
		fmt.Sprintf("%s Credential=WRONGAK/%s/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00",
			algorithm, time.Now().UTC().Format(amzShortFormat)))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "InvalidAccessKeyId") {
		t.Fatalf("bad ak: %d %s", rec.Code, rec.Body)
	}

	// 错误 SK（用真 AK 但篡改签名）
	req = httptest.NewRequest(http.MethodGet, "/"+testBucket+"/k", nil)
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format(amzTimeFormat))
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
		algorithm, testAK, time.Now().UTC().Format(amzShortFormat), strings.Repeat("0", 64)))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "SignatureDoesNotMatch") {
		t.Fatalf("bad sk: %d %s", rec.Code, rec.Body)
	}

	// 凭据不匹配的桶
	rec = doSigned(t, h, http.MethodPut, "/other-bkt/k", []byte("x"), nil)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("bucket mismatch: %d %s", rec.Code, rec.Body)
	}
}

func TestS3List(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, k := range []string{"a.txt", "dir/b.txt", "dir/c.txt", "top.bin"} {
		rec := doSigned(t, h, http.MethodPut, "/"+testBucket+"/"+k, []byte(k), nil)
		if rec.Code != 200 {
			t.Fatalf("put %s: %d", k, rec.Code)
		}
	}
	// v2 前缀+分隔符
	rec := doSigned(t, h, http.MethodGet, "/"+testBucket+"?list-type=2&delimiter=/", nil, nil)
	var lr listResultV2
	xml.Unmarshal(rec.Body.Bytes(), &lr)
	if len(lr.Contents) != 2 || len(lr.CommonPrefixes) != 1 || lr.CommonPrefixes[0].Prefix != "dir/" {
		t.Fatalf("v2 list: %+v", lr)
	}
	// 分页（max-keys=2 两轮）
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"?list-type=2&delimiter=/&max-keys=2", nil, nil)
	xml.Unmarshal(rec.Body.Bytes(), &lr)
	if !lr.IsTruncated || lr.NextContinuationToken == "" {
		t.Fatalf("page1: %+v", lr)
	}
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"?list-type=2&delimiter=/&max-keys=2&continuation-token="+
		url.QueryEscape(lr.NextContinuationToken), nil, nil)
	var lr2 listResultV2
	xml.Unmarshal(rec.Body.Bytes(), &lr2)
	if lr2.IsTruncated || len(lr2.Contents)+len(lr2.CommonPrefixes) != 1 {
		t.Fatalf("page2: %+v", lr2)
	}
	// v1 marker
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"?marker=a.txt", nil, nil)
	var lv1 listResultV1
	xml.Unmarshal(rec.Body.Bytes(), &lv1)
	if len(lv1.Contents) != 3 { // dir/b, dir/c, top.bin
		t.Fatalf("v1 marker list: %+v", lv1)
	}
}

func TestS3MultipartFlow(t *testing.T) {
	h, s := newTestHandler(t)
	p1, p2 := randData(t, 64<<10), randData(t, 32<<10)

	// initiate
	rec := doSigned(t, h, http.MethodPost, "/"+testBucket+"/mp.bin?uploads", nil,
		map[string]string{"Content-Type": "application/x-mp"})
	var init initiateMultipartXML
	xml.Unmarshal(rec.Body.Bytes(), &init)
	if init.UploadID == "" {
		t.Fatalf("initiate: %d %s", rec.Code, rec.Body)
	}
	// 上传 part（乱序）
	rec = doSigned(t, h, http.MethodPut, "/"+testBucket+"/mp.bin?partNumber=2&uploadId="+init.UploadID, p2, nil)
	etag2 := rec.Header().Get("ETag")
	if rec.Code != 200 || etag2 == "" {
		t.Fatalf("part2: %d", rec.Code)
	}
	rec = doSigned(t, h, http.MethodPut, "/"+testBucket+"/mp.bin?partNumber=1&uploadId="+init.UploadID, p1, nil)
	etag1 := rec.Header().Get("ETag")

	// pending 记账
	bs, _ := s.BucketStat(context.Background(), testBucket)
	if bs.Pending != int64(len(p1)+len(p2)) {
		t.Fatalf("pending: %+v", bs)
	}
	// ListParts
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/mp.bin?uploadId="+init.UploadID, nil, nil)
	var lp listPartsXML
	xml.Unmarshal(rec.Body.Bytes(), &lp)
	if len(lp.Part) != 2 || lp.Part[0].PartNumber != 1 {
		t.Fatalf("list parts: %+v", lp)
	}
	// complete
	body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag1 +
		`</ETag></Part><Part><PartNumber>2</PartNumber><ETag>` + etag2 + `</ETag></Part></CompleteMultipartUpload>`
	rec = doSigned(t, h, http.MethodPost, "/"+testBucket+"/mp.bin?uploadId="+init.UploadID, []byte(body), nil)
	if rec.Code != 200 {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}
	var cr completeMultipartXML
	xml.Unmarshal(rec.Body.Bytes(), &cr)
	if !strings.HasSuffix(strings.Trim(cr.ETag, `"`), "-2") {
		t.Fatalf("multipart etag: %s", cr.ETag)
	}
	// 内容 = part1||part2
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"/mp.bin", nil, nil)
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), append(append([]byte{}, p1...), p2...)) {
		t.Fatalf("mp content: %d len=%d", rec.Code, rec.Body.Len())
	}
	// abort 流程
	rec = doSigned(t, h, http.MethodPost, "/"+testBucket+"/mp2.bin?uploads", nil, nil)
	xml.Unmarshal(rec.Body.Bytes(), &init)
	doSigned(t, h, http.MethodPut, "/"+testBucket+"/mp2.bin?partNumber=1&uploadId="+init.UploadID, p1, nil)
	rec = doSigned(t, h, http.MethodDelete, "/"+testBucket+"/mp2.bin?uploadId="+init.UploadID, nil, nil)
	if rec.Code != 204 {
		t.Fatalf("abort: %d", rec.Code)
	}
	bs, _ = s.BucketStat(context.Background(), testBucket)
	if bs.Pending != 0 {
		t.Fatalf("pending after abort: %+v", bs)
	}
	// ListMultipartUploads 为空
	rec = doSigned(t, h, http.MethodGet, "/"+testBucket+"?uploads", nil, nil)
	var lu listUploadsXML
	xml.Unmarshal(rec.Body.Bytes(), &lu)
	if len(lu.Upload) != 0 {
		t.Fatalf("uploads should be empty: %+v", lu)
	}
}

func TestS3MultiDelete(t *testing.T) {
	h, s := newTestHandler(t)
	for _, k := range []string{"d1", "d2", "d3"} {
		doSigned(t, h, http.MethodPut, "/"+testBucket+"/"+k, []byte(k), nil)
	}
	body := `<Delete><Object><Key>d1</Key></Object><Object><Key>d2</Key></Object><Object><Key>missing</Key></Object></Delete>`
	rec := doSigned(t, h, http.MethodPost, "/"+testBucket+"?delete", []byte(body), nil)
	if rec.Code != 200 {
		t.Fatalf("multi delete: %d %s", rec.Code, rec.Body)
	}
	var dr deleteResponseXML
	xml.Unmarshal(rec.Body.Bytes(), &dr)
	if len(dr.Deleted) != 3 || len(dr.Error) != 0 {
		t.Fatalf("delete result: %+v", dr)
	}
	bs, _ := s.BucketStat(context.Background(), testBucket)
	if bs.Objects != 1 { // 只剩 d3
		t.Fatalf("objects after multi delete: %+v", bs)
	}
}

// presignURL 测试侧生成预签名 URL（与被测验签器独立走同一规范流程）。
func presignURL(t *testing.T, method, path string, ttl time.Duration, dateOffset time.Duration) string {
	t.Helper()
	now := time.Now().UTC().Add(dateOffset)
	shortDate := now.Format(amzShortFormat)
	q := url.Values{}
	q.Set("X-Amz-Algorithm", algorithm)
	q.Set("X-Amz-Credential", testAK+"/"+shortDate+"/us-east-1/s3/aws4_request")
	q.Set("X-Amz-Date", now.Format(amzTimeFormat))
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int(ttl.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")
	req := httptest.NewRequest(method, path+"?"+q.Encode(), nil)
	creq := canonicalRequest(req, []string{"host"}, unsignedPayload, true)
	sts := stringToSign(now.Format(amzTimeFormat), shortDate, "us-east-1", "s3", creq)
	sig := hmacHex(signingKey(testSK, shortDate, "us-east-1", "s3"), sts)
	return path + "?" + q.Encode() + "&X-Amz-Signature=" + sig
}

func doPresigned(t *testing.T, h *Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestS3Presigned(t *testing.T) {
	h, _ := newTestHandler(t)
	doSigned(t, h, http.MethodPut, "/"+testBucket+"/pre.txt", []byte("presigned-content"), nil)

	// 有效 presign
	rec := doPresigned(t, h, http.MethodGet, presignURL(t, http.MethodGet, "/"+testBucket+"/pre.txt", 5*time.Minute, 0))
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), []byte("presigned-content")) {
		t.Fatalf("presigned get: %d %s", rec.Code, rec.Body)
	}
	// 过期（签发于 1 小时前，5 分钟有效期）
	rec = doPresigned(t, h, http.MethodGet, presignURL(t, http.MethodGet, "/"+testBucket+"/pre.txt", 5*time.Minute, -time.Hour))
	if rec.Code != 403 {
		t.Fatalf("expired presign: %d", rec.Code)
	}
	// 篡改签名
	bad := strings.Replace(presignURL(t, http.MethodGet, "/"+testBucket+"/pre.txt", 5*time.Minute, 0),
		"X-Amz-Signature=", "X-Amz-Signature=0", 1)
	rec = doPresigned(t, h, http.MethodGet, bad)
	if rec.Code != 403 {
		t.Fatalf("tampered presign: %d", rec.Code)
	}
}

func TestS3AwsChunkedReader(t *testing.T) {
	// 帧格式：<hex-size>;chunk-signature=xx\r\n<data>\r\n ... 0;...\r\n\r\n
	frame := func(data []byte) string {
		return fmt.Sprintf("%x;chunk-signature=deadbeef\r\n%s\r\n", len(data), data)
	}
	payload := frame([]byte("hello ")) + frame([]byte("world")) + "0;chunk-signature=deadbeef\r\n\r\n"
	r := newAwsChunkedReader(strings.NewReader(payload))
	var out bytes.Buffer
	if _, err := out.ReadFrom(r); err != nil {
		t.Fatalf("chunked read: %v", err)
	}
	if out.String() != "hello world" {
		t.Fatalf("chunked payload = %q", out.String())
	}
}

func TestS3BucketOps(t *testing.T) {
	h, _ := newTestHandler(t)
	// HEAD 桶存在；外桶不可探（桶级凭据：不属于本凭据的桶一律 403，不泄露存在性）
	if rec := doSigned(t, h, http.MethodHead, "/"+testBucket, nil, nil); rec.Code != 200 {
		t.Fatalf("head bucket: %d", rec.Code)
	}
	if rec := doSigned(t, h, http.MethodHead, "/nope-bkt", nil, nil); rec.Code != 403 {
		t.Fatalf("head foreign bucket: %d", rec.Code)
	}
	// 非空桶删除被拒
	doSigned(t, h, http.MethodPut, "/"+testBucket+"/keep", []byte("x"), nil)
	if rec := doSigned(t, h, http.MethodDelete, "/"+testBucket, nil, nil); rec.Code != 409 {
		t.Fatalf("delete non-empty: %d", rec.Code)
	}
	// GetBucketLocation
	rec := doSigned(t, h, http.MethodGet, "/"+testBucket+"?location", nil, nil)
	if rec.Code != 200 {
		t.Fatalf("location: %d", rec.Code)
	}
}
