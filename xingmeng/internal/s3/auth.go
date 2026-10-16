// Package s3 是 S3 协议适配层（docs/01：v1 子集，path-style + SigV4）。
// 只调用 store 统一接口；协议语义（XML、签名、条件请求）到本层为止。
package s3

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

const (
	algorithm       = "AWS4-HMAC-SHA256"
	amzTimeFormat   = "20060102T150405Z"
	amzShortFormat  = "20060102"
	maxClockSkew    = 15 * time.Minute
	maxPresignTTL   = 7 * 24 * time.Hour
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

var (
	errBadAuth      = fmt.Errorf("%w: malformed authorization", errs.ErrPermissionDenied)
	errSigMismatch  = errors.New("signature does not match")
	errBadAccessKey = errors.New("invalid access key id")
	errExpired      = errors.New("request expired")
)

// authorize 校验 SigV4（头部或 presign 查询式）并核对桶授权。
// 失败原因对客户端归一为 403 族；桶不匹配返回 PermissionDenied。
func (h *Handler) authorize(r *http.Request, bucket string) (*store.Cred, error) {
	if r.URL.Query().Get("X-Amz-Algorithm") == algorithm {
		return h.authorizePresigned(r, bucket)
	}
	return h.authorizeHeader(r, bucket)
}

func (h *Handler) authorizeHeader(r *http.Request, bucket string) (*store.Cred, error) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, algorithm+" Credential=") {
		return nil, errBadAuth
	}
	credStr, headersStr, sigStr, ok := parseAuthHeader(auth)
	if !ok {
		return nil, errBadAuth
	}
	fields := strings.Split(credStr, "/")
	if len(fields) != 5 || fields[3] != "s3" || fields[4] != "aws4_request" {
		return nil, errBadAuth
	}
	cred, err := h.lookupAndCheck(r, fields[0], bucket)
	if err != nil {
		return nil, err
	}
	amzDate := r.Header.Get("X-Amz-Date")
	ts, err := time.Parse(amzTimeFormat, amzDate)
	if err != nil || skewTooLarge(time.Now().UTC(), ts) {
		return nil, fmt.Errorf("%w: bad or skewed x-amz-date", errs.ErrPermissionDenied)
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload // 宽容非标准客户端
	}
	if err := verify(r, amzDate, fields[1], fields[2], fields[3], headersStr, payloadHash, sigStr, cred.Secret); err != nil {
		return nil, err
	}
	return cred, nil
}

func (h *Handler) authorizePresigned(r *http.Request, bucket string) (*store.Cred, error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != algorithm {
		return nil, errBadAuth
	}
	credStr, err := url.QueryUnescape(q.Get("X-Amz-Credential"))
	if err != nil {
		return nil, errBadAuth
	}
	fields := strings.Split(credStr, "/")
	if len(fields) != 5 || fields[3] != "s3" || fields[4] != "aws4_request" {
		return nil, errBadAuth
	}
	cred, err := h.lookupAndCheck(r, fields[0], bucket)
	if err != nil {
		return nil, err
	}
	ts, err := time.Parse(amzTimeFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return nil, errBadAuth
	}
	expires, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || expires < 1 || time.Duration(expires)*time.Second > maxPresignTTL {
		return nil, fmt.Errorf("%w: bad X-Amz-Expires", errs.ErrPermissionDenied)
	}
	now := time.Now().UTC()
	if now.After(ts.Add(time.Duration(expires)*time.Second)) || skewTooLarge(now, ts) {
		return nil, fmt.Errorf("%w: %v", errs.ErrPermissionDenied, errExpired)
	}
	creq := canonicalRequest(r, strings.Split(q.Get("X-Amz-SignedHeaders"), ";"), unsignedPayload, true)
	sts := stringToSign(q.Get("X-Amz-Date"), fields[1], fields[2], fields[3], creq)
	sig := hmacHex(signingKey(cred.Secret, fields[1], fields[2], fields[3]), sts)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(q.Get("X-Amz-Signature"))) != 1 {
		return nil, errSigMismatch
	}
	return cred, nil
}

func (h *Handler) lookupAndCheck(r *http.Request, ak, bucket string) (*store.Cred, error) {
	cred, err := h.store.LookupCred(r.Context(), ak)
	if err != nil {
		return nil, errBadAccessKey
	}
	if cred.Bucket != bucket {
		return nil, fmt.Errorf("%w: credential not valid for bucket %q", errs.ErrPermissionDenied, bucket)
	}
	return cred, nil
}

func verify(r *http.Request, amzDate, date, region, service, signedHeaders, payloadHash, want, secret string) error {
	creq := canonicalRequest(r, strings.Split(signedHeaders, ";"), payloadHash, false)
	sts := stringToSign(amzDate, date, region, service, creq)
	sig := hmacHex(signingKey(secret, date, region, service), sts)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return errSigMismatch
	}
	return nil
}

func parseAuthHeader(auth string) (cred, headers, sig string, ok bool) {
	rest := strings.TrimPrefix(auth, algorithm+" ")
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		k, v, found := strings.Cut(part, "=")
		if !found {
			return "", "", "", false
		}
		switch k {
		case "Credential":
			cred = v
		case "SignedHeaders":
			headers = v
		case "Signature":
			sig = v
		}
	}
	return cred, headers, sig, cred != "" && headers != "" && sig != ""
}

// canonicalRequest 按 SigV4 规范拼接：方法、URI、查询、头、签名头清单、载荷哈希。
// presign 时查询串需剔除 X-Amz-Signature。
func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, presigned bool) string {
	headers := make([]string, len(signedHeaders))
	copy(headers, signedHeaders)
	sort.Strings(headers)
	var ch strings.Builder
	for _, name := range headers {
		vals := r.Header.Values(name) // host 不在 Header 里，特判
		joined := make([]string, 0, len(vals))
		if name == "host" {
			joined = append(joined, normalizeSpace(r.Host))
		} else {
			for _, v := range vals {
				joined = append(joined, normalizeSpace(v))
			}
		}
		ch.WriteString(name)
		ch.WriteString(":")
		ch.WriteString(strings.Join(joined, ","))
		ch.WriteString("\n")
	}
	return strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQuery(r.URL.RawQuery, presigned),
		ch.String(),
		strings.Join(headers, ";"),
		payloadHash,
	}, "\n")
}

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// canonicalURI 取原始路径逐段解码后按 AWS 规则重编码（'/' 保留）。
func canonicalURI(r *http.Request) string {
	raw := r.RequestURI
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	segs := strings.Split(raw, "/")
	for i, seg := range segs {
		if dec, err := url.PathUnescape(seg); err == nil {
			segs[i] = awsURIEncode(dec, true)
		}
	}
	return strings.Join(segs, "/")
}

// canonicalQuery 键值解码后按 AWS 规则重编码并排序；presign 剔除签名参数。
func canonicalQuery(rawQuery string, presigned bool) string {
	if rawQuery == "" {
		return ""
	}
	type kv struct{ k, v string }
	var list []kv
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		dk, err := url.QueryUnescape(k)
		if err != nil {
			dk = k
		}
		dv, err := url.QueryUnescape(v)
		if err != nil {
			dv = v
		}
		if presigned && dk == "X-Amz-Signature" {
			continue
		}
		list = append(list, kv{awsURIEncode(dk, false), awsURIEncode(dv, false)})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].k != list[j].k {
			return list[i].k < list[j].k
		}
		return list[i].v < list[j].v
	})
	parts := make([]string, len(list))
	for i, p := range list {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

// awsURIEncode：保留 unreserved（A-Za-z0-9-._~），isPath 时再保留 '/'，其余 %XX 大写。
func awsURIEncode(s string, isPath bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && isPath:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func stringToSign(amzDate, date, region, service, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		algorithm, amzDate,
		strings.Join([]string{date, region, service, "aws4_request"}, "/"),
		hex.EncodeToString(sum[:]),
	}, "\n")
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func hmacHex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func skewTooLarge(now, ts time.Time) bool {
	d := now.Sub(ts)
	return d > maxClockSkew || d < -maxClockSkew
}

// awsChunkedReader 解包 STREAMING-* 载荷帧（<hex-size>[;...]\r\n<data>\r\n），
// 忽略 chunk 签名与 trailer——完整性由 blob 哈希兜底。
type awsChunkedReader struct {
	r          *bufio.Reader
	remaining  int64
	sawFinal   bool
	expectCRLF bool // 上一块数据读完，待吞块终止 \r\n
}

func newAwsChunkedReader(rc io.Reader) *awsChunkedReader {
	return &awsChunkedReader{r: bufio.NewReader(rc)}
}

func (c *awsChunkedReader) Read(p []byte) (int, error) {
	for c.remaining == 0 {
		if c.sawFinal {
			return 0, io.EOF
		}
		if c.expectCRLF {
			if _, err := c.r.ReadString('\n'); err != nil { // 块终止 \r\n
				return 0, err
			}
			c.expectCRLF = false
			continue
		}
		line, err := c.r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		sizeField, _, _ := strings.Cut(line, ";")
		n, perr := strconv.ParseInt(sizeField, 16, 64)
		if perr != nil {
			return 0, fmt.Errorf("%w: bad aws-chunked frame", errs.ErrInvalidArgument)
		}
		if n == 0 {
			c.sawFinal = true
			// 吸掉 trailer 与结尾空行
			for {
				t, terr := c.r.ReadString('\n')
				if terr != nil || t == "\r\n" || t == "\n" {
					break
				}
			}
			return 0, io.EOF
		}
		c.remaining = n
	}
	n := int64(len(p))
	if n > c.remaining {
		n = c.remaining
	}
	read, err := c.r.Read(p[:n])
	c.remaining -= int64(read)
	if c.remaining == 0 {
		c.expectCRLF = true
	}
	return read, err
}
