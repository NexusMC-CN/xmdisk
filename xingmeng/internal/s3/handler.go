package s3

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

// Handler 是 S3 适配层（path-style：/{bucket}/{key...}）。
type Handler struct {
	store *store.Store
}

func New(s *store.Store) *Handler { return &Handler{store: s} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := requestID()
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("Server", "xingmeng-s3")

	bucket, key := splitPath(r.URL.Path)
	cred, aerr := h.authorize(r, bucket)
	if aerr != nil {
		m := mapAuthError(aerr)
		writeXMLError(w, m.Status, m.Code, aerr.Error(), r.URL.Path, reqID)
		return
	}
	_ = cred
	// aws-chunked 载荷解包（STREAMING-* 签名载荷）
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		r.Body = io.NopCloser(newAwsChunkedReader(r.Body))
	}

	if bucket == "" {
		writeXMLError(w, 404, "NoSuchBucket", "empty bucket", r.URL.Path, reqID)
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			writeXMLError(w, 500, "InternalError", fmt.Sprint(rec), r.URL.Path, reqID)
		}
	}()
	if key == "" {
		h.bucketOp(w, r, bucket, reqID)
		return
	}
	h.objectOp(w, r, bucket, key, reqID)
}

func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", ""
	}
	b, k, _ := strings.Cut(p, "/")
	return b, k
}

func requestID() string {
	raw := make([]byte, 8)
	rand.Read(raw)
	return hex.EncodeToString(raw)
}

// ---- 桶级操作 ----

func (h *Handler) bucketOp(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if err := h.store.CreateBucket(r.Context(), bucket, nil); err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), bucket, reqID)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := h.store.DeleteBucket(r.Context(), bucket); err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), bucket, reqID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodHead:
		if _, err := h.store.BucketStat(r.Context(), bucket); err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPost:
		if _, ok := q["delete"]; ok {
			h.multiDelete(w, r, bucket, reqID)
			return
		}
		writeXMLError(w, http.StatusNotImplemented, "NotImplemented", "POST bucket only supports ?delete", bucket, reqID)
	case http.MethodGet:
		_, wantLocation := q["location"]
		_, wantUploads := q["uploads"]
		switch {
		case wantLocation:
			writeXML(w, http.StatusOK, struct {
				XMLName xml.Name `xml:"LocationConstraint"`
				Xmlns   string   `xml:"xmlns,attr"`
			}{Xmlns: xmlNs})
		case wantUploads:
			h.listUploads(w, r, bucket)
		default:
			h.listObjects(w, r, bucket)
		}
	default:
		writeXMLError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, bucket, reqID)
	}
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delim := q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxKeys = n
		}
	}
	isV2 := q.Get("list-type") == "2"
	after := ""
	if isV2 {
		if tok := q.Get("continuation-token"); tok != "" {
			raw, err := base64.StdEncoding.DecodeString(tok)
			if err != nil {
				writeXMLError(w, 400, "InvalidArgument", "bad continuation token", bucket, requestID())
				return
			}
			after = string(raw)
		} else {
			after = q.Get("start-after")
		}
	} else {
		after = q.Get("marker")
	}
	res, err := h.store.List(r.Context(), bucket, prefix, delim, after, maxKeys)
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), bucket, requestID())
		return
	}
	if isV2 {
		out := listResultV2{
			Xmlns: xmlNs, Name: bucket, Prefix: prefix, Delimiter: delim,
			MaxKeys: maxKeys, IsTruncated: res.Truncated,
			ContinuationToken: q.Get("continuation-token"), StartAfter: q.Get("start-after"),
		}
		out.Contents = toObjectXML(res)
		out.CommonPrefixes = toPrefixXML(res)
		out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
		if res.Truncated {
			out.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(res.NextAfter))
		}
		writeXML(w, http.StatusOK, out)
		return
	}
	out := listResultV1{
		Xmlns: xmlNs, Name: bucket, Prefix: prefix, Delimiter: delim,
		MaxKeys: maxKeys, IsTruncated: res.Truncated, Marker: q.Get("marker"),
	}
	out.Contents = toObjectXML(res)
	out.CommonPrefixes = toPrefixXML(res)
	if res.Truncated {
		out.NextMarker = res.NextAfter
	}
	writeXML(w, http.StatusOK, out)
}

func toObjectXML(res *store.ListResult) []objectXML {
	out := make([]objectXML, 0, len(res.Items))
	for _, it := range res.Items {
		out = append(out, objectXML{
			Key: it.Key, LastModified: it.MTime.UTC().Format(time.RFC3339),
			ETag: quoteETag(it.ETag), Size: it.Size, StorageClass: "STANDARD",
		})
	}
	return out
}

func toPrefixXML(res *store.ListResult) []commonPrefixXML {
	out := make([]commonPrefixXML, 0, len(res.CommonPrefixes))
	for _, p := range res.CommonPrefixes {
		out = append(out, commonPrefixXML{Prefix: p})
	}
	return out
}

func (h *Handler) multiDelete(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	var req deleteRequestXML
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeXMLError(w, 400, "MalformedXML", err.Error(), bucket, reqID)
		return
	}
	resp := deleteResponseXML{Xmlns: xmlNs}
	for _, o := range req.Objects {
		err := h.deleteObject(r.Context(), bucket, o.Key)
		if err != nil {
			m := mapError(err)
			resp.Error = append(resp.Error, struct {
				Key     string `xml:"Key"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}{o.Key, m.Code, err.Error()})
			continue
		}
		resp.Deleted = append(resp.Deleted, struct {
			Key string `xml:"Key"`
		}{o.Key})
	}
	writeXML(w, http.StatusOK, resp)
}

func (h *Handler) listUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	uploads, err := h.store.MpList(r.Context(), bucket)
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), bucket, requestID())
		return
	}
	out := listUploadsXML{Xmlns: xmlNs, Bucket: bucket}
	for _, u := range uploads {
		out.Upload = append(out.Upload, uploadXML{
			Key: u.Key, UploadID: u.UploadID,
			Initiated: time.Unix(0, u.CreatedNs).UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, out)
}

// ---- 对象级操作 ----

func (h *Handler) objectOp(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	if _, hasUploads := q["uploads"]; hasUploads && r.Method == http.MethodPost {
		h.initiateMultipart(w, r, bucket, key, reqID)
		return
	}
	if uploadID != "" {
		h.multipartOp(w, r, bucket, key, uploadID, reqID)
		return
	}
	switch r.Method {
	case http.MethodPut:
		h.putObject(w, r, bucket, key, reqID)
	case http.MethodGet:
		h.getObject(w, r, bucket, key, reqID, false)
	case http.MethodHead:
		h.getObject(w, r, bucket, key, reqID, true)
	case http.MethodDelete:
		if err := h.deleteObject(r.Context(), bucket, key); err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeXMLError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, r.URL.Path, reqID)
	}
}

// deleteObject 统一 S3 DELETE 语义：键不存在幂等成功；目录键（"foo/"）为
// 标记删除——删标记条目、子条目不动（非空目录仍以前缀呈现，递归清空是
// 客户端职责）。修复点：此前目录键回 404 NoSuchKey，客户端视作"已删除"
// 报成功而目录残留（用户实测 rclone/mc 删不掉 SFTP/WebDAV 建的目录）。
func (h *Handler) deleteObject(ctx context.Context, bucket, key string) error {
	err := h.store.Delete(ctx, bucket, key)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errs.ErrKeyNotFound):
		return nil // S3 幂等
	case errors.Is(err, errs.ErrIsDirectory):
		return h.store.DeleteDirMarker(ctx, bucket, key)
	default:
		return err
	}
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	if r.Header.Get("x-amz-copy-source") != "" {
		writeXMLError(w, http.StatusNotImplemented, "NotImplemented", "server-side copy not supported", r.URL.Path, reqID)
		return
	}
	opts := store.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
		Meta:        collectUserMeta(r),
	}
	if opts.ContentType == "" {
		opts.ContentType = "binary/octet-stream"
	}
	// PUT 条件写（rclone 等依赖 If-None-Match: * 做 create-only）
	if inm := r.Header.Get("If-None-Match"); inm == "*" {
		if _, err := h.store.Stat(r.Context(), bucket, key); err == nil {
			writeXMLError(w, 412, "PreconditionFailed", "object exists", r.URL.Path, reqID)
			return
		}
	}
	oi, err := h.store.Put(r.Context(), bucket, key, r.Body, opts)
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
		return
	}
	w.Header().Set("ETag", quoteETag(oi.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string, headOnly bool) {
	oi, err := h.store.Stat(r.Context(), bucket, key)
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
		return
	}
	// 条件请求（GET/HEAD）
	if code, ok := evaluateConditions(r, oi.ETag, oi.MTime); !ok {
		if code == http.StatusNotModified {
			w.Header().Set("ETag", quoteETag(oi.ETag))
			w.WriteHeader(http.StatusNotModified)
		} else {
			writeXMLError(w, code, "PreconditionFailed", "condition failed", r.URL.Path, reqID)
		}
		return
	}
	w.Header().Set("ETag", quoteETag(oi.ETag))
	w.Header().Set("Last-Modified", oi.MTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", contentTypeOf(oi))
	for k, v := range oi.UserMeta {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	rng := parseRange(r.Header.Get("Range"), oi.Size)
	if rng == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(oi.Size, 10))
		if headOnly {
			w.WriteHeader(http.StatusOK)
			return
		}
		rc, _, err := h.store.Get(r.Context(), bucket, key, nil)
		if err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		defer rc.Close()
		w.WriteHeader(http.StatusOK)
		io.Copy(w, rc)
		return
	}
	rc, _, err := h.store.Get(r.Context(), bucket, key, rng)
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Length", strconv.FormatInt(rng.Length, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.Start+rng.Length-1, oi.Size))
	w.WriteHeader(http.StatusPartialContent)
	if !headOnly {
		io.Copy(w, rc)
	}
}

func (h *Handler) initiateMultipart(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	id, err := h.store.MpBegin(r.Context(), bucket, key, store.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
		Meta:        collectUserMeta(r),
	})
	if err != nil {
		m := mapError(err)
		writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartXML{Xmlns: xmlNs, Bucket: bucket, Key: key, UploadID: id})
}

func (h *Handler) multipartOp(w http.ResponseWriter, r *http.Request, bucket, key, uploadID, reqID string) {
	switch r.Method {
	case http.MethodPut:
		partNo, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
		if err != nil {
			writeXMLError(w, 400, "InvalidArgument", "missing partNumber", r.URL.Path, reqID)
			return
		}
		etag, _, err := h.store.MpPut(r.Context(), uploadID, partNo, r.Body)
		if err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		w.Header().Set("ETag", quoteETag(etag))
		w.WriteHeader(http.StatusOK)
	case http.MethodPost:
		var req completeMultipartRequestXML
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			writeXMLError(w, 400, "MalformedXML", err.Error(), r.URL.Path, reqID)
			return
		}
		refs := make([]store.PartRef, len(req.Parts))
		for i, p := range req.Parts {
			refs[i] = store.PartRef{No: p.PartNumber, ETag: p.ETag}
		}
		oi, err := h.store.MpComplete(r.Context(), uploadID, refs)
		if err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		writeXML(w, http.StatusOK, completeMultipartXML{
			Xmlns: xmlNs, Location: fmt.Sprintf("/%s/%s", bucket, key),
			Bucket: bucket, Key: key, ETag: quoteETag(oi.ETag),
		})
	case http.MethodDelete:
		if err := h.store.MpAbort(r.Context(), uploadID); err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		parts, err := h.store.MpParts(r.Context(), uploadID)
		if err != nil {
			m := mapError(err)
			writeXMLError(w, m.Status, m.Code, err.Error(), r.URL.Path, reqID)
			return
		}
		out := listPartsXML{Xmlns: xmlNs, Bucket: bucket, Key: key, UploadID: uploadID}
		for _, p := range parts {
			out.Part = append(out.Part, partXML{
				PartNumber: p.No, ETag: quoteETag(p.ETag), Size: p.Size,
				LastModified: time.Now().UTC().Format(time.RFC3339),
			})
		}
		writeXML(w, http.StatusOK, out)
	default:
		writeXMLError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, r.URL.Path, reqID)
	}
}

// ---- 工具 ----

func collectUserMeta(r *http.Request) map[string]string {
	var meta map[string]string
	for name, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") && len(vals) > 0 {
			if meta == nil {
				meta = map[string]string{}
			}
			meta[strings.ToLower(strings.TrimPrefix(strings.ToLower(name), "x-amz-meta-"))] = vals[0]
		}
	}
	return meta
}

func contentTypeOf(oi *store.ObjInfo) string {
	if oi.ContentType != "" {
		return oi.ContentType
	}
	return "binary/octet-stream"
}

func quoteETag(e string) string { return `"` + e + `"` }

// evaluateConditions：先决失败类（If-Match / If-Unmodified-Since → 412）先行，
// 缓存类（If-None-Match / If-Modified-Since → 304）随后，If-None-Match 优先。
func evaluateConditions(r *http.Request, etag string, mtime time.Time) (int, bool) {
	if im := r.Header.Get("If-Match"); im != "" && !(im == "*" || etagInList(im, etag)) {
		return http.StatusPreconditionFailed, false
	}
	if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && mtime.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if inm == "*" || etagInList(inm, etag) {
			return http.StatusNotModified, false
		}
		return 0, true
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !mtime.After(t) {
			return http.StatusNotModified, false
		}
	}
	return 0, true
}

func etagInList(list, etag string) bool {
	for _, cand := range strings.Split(list, ",") {
		if strings.Trim(strings.TrimSpace(cand), `"`) == etag {
			return true
		}
	}
	return false
}

// parseRange 解析单区间 Range 头；语法非法返回 nil（按 200 全量响应，RFC 语义），
// 语义不可满足由 store 层判定返回 416。
func parseRange(spec string, size int64) *store.Range {
	if !strings.HasPrefix(spec, "bytes=") {
		return nil
	}
	body := strings.TrimPrefix(spec, "bytes=")
	if strings.Contains(body, ",") {
		return nil // 多区间不支持，退回全量
	}
	a, b, ok := strings.Cut(body, "-")
	if !ok {
		return nil
	}
	if a == "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 {
			return nil
		}
		start := size - n
		if start < 0 {
			start = 0
		}
		return &store.Range{Start: start, Length: size - start}
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return nil
	}
	if b == "" {
		return &store.Range{Start: start, Length: -1}
	}
	end, err := strconv.ParseInt(b, 10, 64)
	if err != nil || end < start {
		return nil
	}
	return &store.Range{Start: start, Length: end - start + 1}
}
