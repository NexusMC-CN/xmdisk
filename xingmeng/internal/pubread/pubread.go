// Package pubread 是公共读面（99-D024）：独立只读端口（listeners.public，
// 默认 :9004），无鉴权、无签名、无写路径，只认 GET/HEAD /<桶>/<key>。
// 桶未开公共、桶不存在、对象不存在、目录 → 同构 404（不向探测者泄露
// 桶的存在性与公共性）；支持 Range 与 If-None-Match/If-Modified-Since
// （CDN 回源校验）；**不发任何缓存头**——缓存策略完全归用户的反代/web
// 服务器层（用户决策：不缓存最多慢一点，缓存错误容易出问题）。
// 域名绑定、URL 改写、错误页美化同样归 web 服务器。
package pubread

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

func New(s *store.Store) http.Handler { return &handler{store: s} }

type handler struct {
	store *store.Store
}

// internalErr 500 固定文案（S-06：不向匿名者泄露内部错误文本/路径），详情入日志。
func internalErr(w http.ResponseWriter, r *http.Request, where string, err error) {
	reqID := strconv.FormatInt(time.Now().UnixNano(), 36)
	slog.Error("pubread internal error", "where", where, "err", err.Error(), "request_id", reqID, "remote", r.RemoteAddr)
	http.Error(w, "internal error (request_id="+reqID+")", http.StatusInternalServerError)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// nosniff：对象 Content-Type 由上传者控制，禁 MIME 嗅探（S-07；与"不发
	// 缓存头"同立场——只做无副作用的加固，策略归反代）。
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 只认 /<bucket>/<key>；桶名后的全部路径为 key；尾斜杠（目录）即 404
	p := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(p, "/")
	if bucket == "" || key == "" || strings.HasSuffix(r.URL.Path, "/") {
		notFound(w)
		return
	}

	// 桶不存在 / 未开公共 / 对象不存在 → 同构 404（防枚举）
	b, err := h.store.BucketStat(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, errs.ErrBucketNotFound) {
			notFound(w)
			return
		}
		internalErr(w, r, "bucket_stat", err)
		return
	}
	if !b.Public {
		notFound(w)
		return
	}
	oi, err := h.store.Stat(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, errs.ErrKeyNotFound) || errors.Is(err, errs.ErrBucketNotFound) || errors.Is(err, errs.ErrIsDirectory) {
			notFound(w)
			return
		}
		internalErr(w, r, "stat", err)
		return
	}

	etag := `"` + oi.ETag + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", oi.MTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	ct := oi.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)

	// 条件请求（缓存类：304）；先决类不适用于公共只读面
	if notModified(r, etag, oi.MTime) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Range：单区间；语法非法按 RFC 退回全量 200；起点越界 416
	rng, unsatisfiable := parseRange(r.Header.Get("Range"), oi.Size)
	if unsatisfiable {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", oi.Size))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if rng != nil {
		rc, _, err := h.store.Get(r.Context(), bucket, key, rng)
		if err != nil {
			internalErr(w, r, "get_range", err)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Length", strconv.FormatInt(rng.Length, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.Start+rng.Length-1, oi.Size))
		w.WriteHeader(http.StatusPartialContent)
		if r.Method == http.MethodGet {
			io.Copy(w, rc)
		}
		return
	}
	if r.Method == http.MethodGet { // 先开流后发 200（S-08）：开流失败应答 5xx 而非 200+截断
		rc, _, err := h.store.Get(r.Context(), bucket, key, nil)
		if err != nil {
			internalErr(w, r, "get", err)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Length", strconv.FormatInt(oi.Size, 10))
		w.WriteHeader(http.StatusOK)
		io.Copy(w, rc)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(oi.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func notFound(w http.ResponseWriter) {
	http.Error(w, "not found", http.StatusNotFound)
}

// notModified：If-None-Match 优先（* 或 ETag 命中），否则 If-Modified-Since。
func notModified(r *http.Request, etag string, mtime time.Time) bool {
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		for _, cand := range strings.Split(inm, ",") {
			cand = strings.TrimSpace(cand)
			if cand == "*" || strings.Trim(cand, `"`) == strings.Trim(etag, `"`) {
				return true
			}
		}
		return false
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !mtime.After(t) {
			return true
		}
	}
	return false
}

// parseRange 解析单区间 "bytes=a-b"/"bytes=a-"/"bytes=-n"；语法非法返回
// (nil,false) 按全量 200；起点越界返回 (nil,true) → 416。
func parseRange(spec string, size int64) (rng *store.Range, unsatisfiable bool) {
	if !strings.HasPrefix(spec, "bytes=") {
		return nil, false
	}
	body := strings.TrimPrefix(spec, "bytes=")
	if strings.Contains(body, ",") {
		return nil, false // 多区间不支持，退回全量
	}
	a, b, ok := strings.Cut(body, "-")
	if !ok {
		return nil, false
	}
	if a == "" { // 后缀区间
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 {
			return nil, false
		}
		start := size - n
		if start < 0 {
			start = 0
		}
		return &store.Range{Start: start, Length: size - start}, false
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return nil, false
	}
	if start >= size {
		return nil, true
	}
	length := size - start
	if b != "" {
		end, err := strconv.ParseInt(b, 10, 64)
		if err != nil || end < start {
			return nil, false
		}
		if end >= size {
			end = size - 1
		}
		length = end - start + 1
	}
	return &store.Range{Start: start, Length: length}, false
}
