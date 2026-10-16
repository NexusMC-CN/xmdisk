package store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// ---- oplog payload（全量状态 op，99-D003）----

type bucketPayload struct {
	Name  string `json:"name"`
	Quota *int64 `json:"quota,omitempty"`
}

type entryPutPayload struct {
	Bucket      string    `json:"bucket"`
	Key         []byte    `json:"key"` // 二进制 key 走 base64，避免 JSON 有损
	IsDir       bool      `json:"is_dir"`
	Size        int64     `json:"size,omitempty"`
	ETag        string    `json:"etag,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
	UserMeta    string    `json:"user_meta,omitempty"`
	Blobs       []BlobRef `json:"blobs,omitempty"`
	UID         int64     `json:"uid,omitempty"`
	GID         int64     `json:"gid,omitempty"`
	Mode        uint32    `json:"mode,omitempty"`
	MTimeNs     int64     `json:"mtime_ns"`
}

type entryDeletePayload struct {
	Bucket string `json:"bucket"`
	Key    []byte `json:"key"`
}

func appendOp(t *meta.Tx, epoch uint32, typ string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = t.AppendOp(epoch, typ, string(b), time.Now().UnixNano())
	return err
}

// ---- 对象操作 ----

// Put 写入对象。执行序：流式落盘 blob（无锁）→ 事务（配额校验 + 条目 +
// 计数器 + oplog）→ 提交。事务失败时已落盘 blob 成为无主 blob，由 GC 回收。
func (s *Store) Put(ctx context.Context, bucket, key string, src io.Reader, opts PutOptions) (*ObjInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if _, err := s.meta.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	if strings.HasSuffix(key, "/") {
		return s.putDir(ctx, bucket, key, opts) // S3 目录标记约定：PUT "foo/" 即建目录
	}
	// 快速失败：同 key 已是目录（事务内还有权威校验）
	if e, err := s.meta.GetEntry(ctx, bucket, key); err == nil && e.IsDir {
		return nil, errs.ErrIsDirectory
	}
	// 流式配额守卫：预读计数快照给写入设上限，超限即刻中断——
	// 防"流完特大文件后事务才拒绝"把磁盘打爆（99-D019；权威校验仍在事务内）
	guarded, gerr := s.quotaGuard(ctx, src, bucket, key, 0)
	if gerr != nil {
		return nil, gerr
	}
	refs, total, etag, err := s.writeBlobs(guarded)
	if err != nil {
		return nil, err
	}
	now := opts.MTime
	if now.IsZero() {
		now = time.Now()
	}
	userMeta := "{}"
	if opts.Meta != nil {
		b, _ := json.Marshal(opts.Meta)
		userMeta = string(b)
	}
	blobsJSON, _ := json.Marshal(refs)

	var out *ObjInfo
	err = s.mutate(ctx, func(t *meta.Tx) error {
		b, err := t.GetBucket(bucket)
		if err != nil {
			return err
		}
		old, err := t.GetEntry(bucket, key)
		if err != nil && !errors.Is(err, errs.ErrKeyNotFound) {
			return err
		}
		if old != nil && old.IsDir {
			return errs.ErrIsDirectory
		}
		delta, dObj := total, int64(1)
		if old != nil {
			delta, dObj = total-old.Size, 0
		}
		// 配额口径含 pending：在途 multipart 占用的空间同样受限（99-D008）
		if b.Quota != nil && b.Used+b.Pending+delta > *b.Quota {
			return fmt.Errorf("%w: bucket %q used+pending %d + delta %d > quota %d",
				errs.ErrQuotaExceeded, bucket, b.Used+b.Pending, delta, *b.Quota)
		}
		e := &meta.Entry{
			Bucket: bucket, Key: key, Size: total, ETag: etag,
			ContentType: opts.ContentType, UserMeta: userMeta, Blobs: string(blobsJSON),
			Mode: 0o644, MTimeNs: now.UnixNano(),
		}
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		if err := t.AdjustUsage(bucket, delta, 0, dObj); err != nil {
			return err
		}
		pl, err := json.Marshal(entryPutPayload{
			Bucket: bucket, Key: []byte(key), Size: total, ETag: etag,
			ContentType: opts.ContentType, UserMeta: userMeta, Blobs: refs,
			Mode: 0o644, MTimeNs: now.UnixNano(),
		})
		if err != nil {
			return err
		}
		if err := appendOp(t, s.currentEpoch(), meta.OpEntryPut, json.RawMessage(pl)); err != nil {
			return err
		}
		out = &ObjInfo{
			Bucket: bucket, Key: key, Size: total, ETag: etag,
			ContentType: opts.ContentType, UserMeta: opts.Meta,
			Blobs: refs, MTime: now, Mode: 0o644,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Get 读取对象（可选区间）。返回的 ReadCloser 聚合跨 blob 的多个文件句柄，
// 调用方必须 Close。
func (s *Store) Get(ctx context.Context, bucket, key string, rng *Range) (io.ReadCloser, *ObjInfo, error) {
	e, err := s.meta.GetEntry(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	if e.IsDir {
		return nil, nil, errs.ErrIsDirectory
	}
	info, err := objInfo(e)
	if err != nil {
		return nil, nil, err
	}
	start, length := int64(0), e.Size
	if rng != nil {
		if rng.Start < 0 || rng.Start > e.Size {
			return nil, nil, fmt.Errorf("%w: start %d, size %d", errs.ErrRangeInvalid, rng.Start, e.Size)
		}
		start = rng.Start
		length = e.Size - start
		if rng.Length >= 0 && rng.Length < length {
			length = rng.Length
		}
	}
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), info, nil
	}
	// 跨 blob 偏移拼接（99-D002：分块布局上做区间读）
	rc := &multiCloser{}
	var rs []io.Reader
	pos, remaining := start, length
	var base int64
	for _, ref := range info.Blobs {
		if remaining == 0 {
			break
		}
		refStart, refEnd := base, base+ref.Size
		base = refEnd
		if refEnd <= pos {
			continue
		}
		skip := pos - refStart
		take := refEnd - pos
		if take > remaining {
			take = remaining
		}
		f, ferr := s.blobs.Open(ref.Sha256)
		if ferr != nil {
			rc.Close()
			return nil, nil, fmt.Errorf("%w: open blob %s: %v", errs.ErrInternal, ref.Sha256, ferr)
		}
		if _, serr := f.Seek(skip, io.SeekStart); serr != nil {
			f.Close()
			rc.Close()
			return nil, nil, fmt.Errorf("%w: seek blob %s: %v", errs.ErrInternal, ref.Sha256, serr)
		}
		rc.files = append(rc.files, f)
		rs = append(rs, io.LimitReader(f, take))
		pos += take
		remaining -= take
	}
	rc.Reader = io.MultiReader(rs...)
	return rc, info, nil
}

func (s *Store) Stat(ctx context.Context, bucket, key string) (*ObjInfo, error) {
	e, err := s.meta.GetEntry(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return objInfo(e)
}

// Delete 删除对象（条目层墓碑 = DELETE op；blob 由两阶段 GC 物理回收）。
func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		e, err := t.GetEntry(bucket, key)
		if err != nil {
			return err
		}
		if e.IsDir {
			return errs.ErrIsDirectory // 删目录走 Rmdir
		}
		if err := t.DeleteEntry(bucket, key); err != nil {
			return err
		}
		if err := t.AdjustUsage(bucket, -e.Size, 0, -1); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpEntryDelete, entryDeletePayload{Bucket: bucket, Key: []byte(key)})
	})
}

// ---- 目录操作（SFTP/WebDAV 树形语义；S3 通过 PUT "foo/" 触达）----

// Mkdir 创建目录；path 不带尾部斜杠，存储 key 为 path + "/"。
func (s *Store) Mkdir(ctx context.Context, bucket, path string) error {
	if err := validateDirPath(path); err != nil {
		return err
	}
	_, err := s.putDir(ctx, bucket, path+"/", PutOptions{})
	return err
}

// Rmdir 删除空目录。
func (s *Store) Rmdir(ctx context.Context, bucket, path string) error {
	if err := validateDirPath(path); err != nil {
		return err
	}
	key := path + "/"
	return s.mutate(ctx, func(t *meta.Tx) error {
		e, err := t.GetEntry(bucket, key)
		if err != nil {
			return err
		}
		if !e.IsDir {
			return errs.ErrIsFile
		}
		n, err := t.CountPrefix(bucket, key)
		if err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: %q has %d entries", errs.ErrDirNotEmpty, path, n)
		}
		if err := t.DeleteEntry(bucket, key); err != nil {
			return err
		}
		if err := t.AdjustUsage(bucket, 0, 0, -1); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpEntryDelete, entryDeletePayload{Bucket: bucket, Key: []byte(key)})
	})
}

func (s *Store) putDir(ctx context.Context, bucket, key string, opts PutOptions) (*ObjInfo, error) {
	now := opts.MTime
	if now.IsZero() {
		now = time.Now()
	}
	var out *ObjInfo
	err := s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(bucket); err != nil {
			return err
		}
		old, err := t.GetEntry(bucket, key)
		if err != nil && !errors.Is(err, errs.ErrKeyNotFound) {
			return err
		}
		if old != nil && !old.IsDir {
			return errs.ErrIsFile
		}
		dObj := int64(1)
		if old != nil {
			dObj = 0
		}
		e := &meta.Entry{Bucket: bucket, Key: key, IsDir: true, Mode: 0o755, MTimeNs: now.UnixNano()}
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		if err := t.AdjustUsage(bucket, 0, 0, dObj); err != nil {
			return err
		}
		if err := appendOp(t, s.currentEpoch(), meta.OpEntryPut, entryPutPayload{
			Bucket: bucket, Key: []byte(key), IsDir: true, Mode: 0o755, MTimeNs: now.UnixNano(),
		}); err != nil {
			return err
		}
		out = &ObjInfo{Bucket: bucket, Key: key, IsDir: true, MTime: now, Mode: 0o755}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- List ----

// List 前缀列举；delim 非空时聚合 CommonPrefixes（S3 ListV2 与树形列目录共用）。
func (s *Store) List(ctx context.Context, bucket, prefix, delim, after string, max int) (*ListResult, error) {
	if _, err := s.meta.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	if max <= 0 || max > 10000 {
		max = 10000
	}
	lo, hi := meta.PrefixRange([]byte(prefix))
	// after 为不透明续传 token：条目 token（即 key）追加 \x00 得排他下界；
	// CP token（key+\x00 形态）取字节后继以跳过该前缀的全部子键。
	var cur []byte
	if after != "" {
		if cp, ok := strings.CutSuffix(after, "\x00"); ok && cp != "" {
			cur = successor([]byte(cp))
		} else {
			cur = append([]byte(after), 0)
		}
		if bytes.Compare(cur, lo) < 0 {
			cur = lo
		}
	} else {
		cur = lo
	}
	res := &ListResult{}
	lastCP := ""
	lastEmitted := ""
	lastWasCP := false
	const batch = 1000
	for {
		entries, err := s.meta.ListEntries(ctx, bucket, cur, hi, batch)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			rest := e.Key[len(prefix):]
			if delim != "" {
				if idx := strings.Index(rest, delim); idx >= 0 {
					cp := prefix + rest[:idx+len(delim)]
					if cp == lastCP {
						continue // 归入已输出的前缀
					}
					if len(res.Items)+len(res.CommonPrefixes) >= max {
						res.Truncated = true
						res.NextAfter = emitToken(lastEmitted, lastWasCP)
						return res, nil
					}
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
					lastCP, lastEmitted, lastWasCP = cp, cp, true
					continue
				}
			}
			if len(res.Items)+len(res.CommonPrefixes) >= max {
				res.Truncated = true
				res.NextAfter = emitToken(lastEmitted, lastWasCP)
				return res, nil
			}
			oi, err := objInfo(&e)
			if err != nil {
				return nil, err
			}
			res.Items = append(res.Items, *oi)
			lastEmitted, lastWasCP = e.Key, false
		}
		if len(entries) < batch {
			return res, nil // 自然结束
		}
		cur = append([]byte(entries[len(entries)-1].Key), 0)
	}
}

// emitToken 生成续传 token：条目即 key；CP 追加 NUL 标记。
func emitToken(key string, isCP bool) string {
	if isCP {
		return key + "\x00"
	}
	return key
}

// ---- 内部工具 ----

// quotaReader 流式配额守卫的读端：消费一旦越过剩余额度立刻报错，
// 上层（blob.Put 的临时文件路径）随之清理，磁盘不落多余字节。
type quotaReader struct {
	r         io.Reader
	remaining int64
}

func (q *quotaReader) Read(p []byte) (int, error) {
	n, err := q.r.Read(p)
	q.remaining -= int64(n)
	if q.remaining < 0 {
		return n, fmt.Errorf("%w: streaming upload exceeds remaining quota allowance", errs.ErrQuotaExceeded)
	}
	return n, err
}

// quotaGuard 以当前桶计数快照计算写入额度并包装源流。credit 是可抵扣的
// 旧空间（覆盖写的旧对象 / 重传 part 的旧值）。快照非原子：并发下的超额
// 上界是各流起步时的额度之和，权威校验仍在提交事务内（99-D019）。
func (s *Store) quotaGuard(ctx context.Context, src io.Reader, bucket, key string, credit int64) (io.Reader, error) {
	b, err := s.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if b.Quota == nil {
		return src, nil // 不限桶：单次 PUT 上限由 MaxObjectSize 兜底
	}
	if key != "" {
		if e, eerr := s.meta.GetEntry(ctx, bucket, key); eerr == nil && !e.IsDir {
			credit += e.Size
		}
	}
	allowance := *b.Quota - b.Used - b.Pending + credit
	if allowance < 0 {
		allowance = 0
	}
	return &quotaReader{r: src, remaining: allowance}, nil
}

// writeBlobs 将流式内容写为 blob 引用列表：小文件整存、大文件定长分块
// （99-D002）。全程同步计算整流 MD5 作为默认 ETag（rclone 等工具依赖
// md5 形态的 etag；multipart 的合成 etag 由 S3 适配层在 Complete 时覆写）。
func (s *Store) writeBlobs(src io.Reader) (refs []BlobRef, total int64, etag string, err error) {
	md5all := md5.New()
	br := bufio.NewReaderSize(src, 1<<20)
	head := make([]byte, s.cfg.SmallThreshold+1)
	n, rerr := io.ReadFull(br, head)
	head = head[:n]
	if rerr != nil && !errors.Is(rerr, io.EOF) && !errors.Is(rerr, io.ErrUnexpectedEOF) {
		return nil, 0, "", rerr
	}
	if int64(len(head)) <= s.cfg.SmallThreshold {
		md5all.Write(head)
		h, sz, perr := s.blobs.Put(bytes.NewReader(head))
		if perr != nil {
			return nil, 0, "", perr
		}
		return []BlobRef{{Sha256: h, Size: sz}}, sz, hex.EncodeToString(md5all.Sum(nil)), nil
	}
	// 大文件：首块以 head 为前缀继续读满 ChunkSize，其后定长分块
	pending := head
	for {
		chunk, eof, ferr := fill(br, pending, s.cfg.ChunkSize)
		pending = nil
		if ferr != nil {
			return nil, 0, "", ferr
		}
		if len(chunk) > 0 {
			h, sz, perr := s.blobs.Put(io.TeeReader(bytes.NewReader(chunk), md5all))
			if perr != nil {
				return nil, 0, "", perr
			}
			refs = append(refs, BlobRef{Sha256: h, Size: sz})
			total += sz
			if total > s.cfg.MaxObjectSize {
				return nil, 0, "", fmt.Errorf("%w: exceeds %d bytes", errs.ErrEntityTooLarge, s.cfg.MaxObjectSize)
			}
		}
		if eof {
			break
		}
	}
	return refs, total, hex.EncodeToString(md5all.Sum(nil)), nil
}

// fill 从 br 读到 out 长度达到 n 或 EOF；init 只用于首块拼装。
func fill(br *bufio.Reader, init []byte, n int64) (out []byte, eof bool, err error) {
	out = append([]byte(nil), init...)
	buf := make([]byte, 1<<20)
	for int64(len(out)) < n {
		want := int64(len(buf))
		if r := n - int64(len(out)); r < want {
			want = r
		}
		rn, rerr := br.Read(buf[:want])
		if rn > 0 {
			out = append(out, buf[:rn]...)
		}
		if rerr == io.EOF {
			return out, true, nil
		}
		if rerr != nil {
			return out, false, rerr
		}
	}
	return out[:n], false, nil
}

// successor 返回字节后继（用于跳过一个 CommonPrefix 的全部子键）。
func successor(b []byte) []byte {
	out := append([]byte(nil), b...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out
		}
		out = out[:i]
	}
	return out // 全 0xFF：返回空表示无下界（配合前缀上界使用）
}

func objInfo(e *meta.Entry) (*ObjInfo, error) {
	oi := &ObjInfo{
		Bucket: e.Bucket, Key: e.Key, IsDir: e.IsDir, Size: e.Size,
		ETag: e.ETag, ContentType: e.ContentType,
		MTime: time.Unix(0, e.MTimeNs), Mode: e.Mode, UID: e.UID, GID: e.GID,
	}
	if e.Blobs != "" && e.Blobs != "[]" {
		if err := json.Unmarshal([]byte(e.Blobs), &oi.Blobs); err != nil {
			return nil, fmt.Errorf("%w: parse blobs for %q: %v", errs.ErrInternal, e.Key, err)
		}
	}
	if e.UserMeta != "" && e.UserMeta != "{}" && e.UserMeta != "null" {
		if err := json.Unmarshal([]byte(e.UserMeta), &oi.UserMeta); err != nil {
			return nil, fmt.Errorf("%w: parse user meta for %q: %v", errs.ErrInternal, e.Key, err)
		}
	}
	return oi, nil
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", errs.ErrInvalidArgument)
	}
	if len(key) > 1024 {
		return fmt.Errorf("%w: key longer than 1024 bytes", errs.ErrInvalidArgument)
	}
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("%w: NUL byte in key", errs.ErrInvalidArgument)
	}
	return nil
}

func validateDirPath(p string) error {
	if p == "" || strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") ||
		strings.Contains(p, "//") || strings.Contains(p, "\x00") {
		return fmt.Errorf("%w: invalid dir path %q", errs.ErrInvalidArgument, p)
	}
	return nil
}

type multiCloser struct {
	io.Reader
	files []*os.File
}

func (m *multiCloser) Close() error {
	var first error
	for _, f := range m.files {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
