package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// UploadInfo 进行中的 multipart 会话（ListMultipartUploads / admin 用）。
type UploadInfo struct {
	UploadID  string
	Key       string
	CreatedNs int64
}

// PartRef Complete 请求里的客户端 part 清单（S3 语义：对象只由列出的 part
// 组成；未列出的已传 part 被丢弃，其 pending 照常退还）。
type PartRef struct {
	No   int
	ETag string
}

// PartInfo 已上传 part 的视图（ListParts 用）。
type PartInfo struct {
	No   int
	ETag string
	Sha  string
	Size int64
}

// MpBegin 创建 multipart 会话。会话是本地状态，不复制（99-D010）：
// 主挂丢在途上传，客户端重试即可；parts 作为普通 blob，
// 从节点在 Complete 产生的 entry_put 时批量补拉。
func (s *Store) MpBegin(ctx context.Context, bucket, key string, opts PutOptions) (string, error) {
	if s.ro.Load() {
		return "", errs.ErrReadOnly
	}
	if err := validateKey(key); err != nil {
		return "", err
	}
	if strings.HasSuffix(key, "/") {
		return "", fmt.Errorf("%w: multipart to directory marker", errs.ErrInvalidArgument)
	}
	if _, err := s.meta.GetBucket(ctx, bucket); err != nil {
		return "", err
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw)
	sessJSON, _ := json.Marshal(sessionMeta{ContentType: opts.ContentType, UserMeta: opts.Meta})
	if err := s.meta.InsertUpload(ctx, id, bucket, key, string(sessJSON), time.Now().UnixNano()); err != nil {
		return "", err
	}
	return id, nil
}

type sessionMeta struct {
	ContentType string            `json:"content_type"`
	UserMeta    map[string]string `json:"user_meta"`
}

// MpPut 上传一个 part：整体一个 blob，边写边算 md5（S3 part ETag）与 sha256；
// 计入 pending（配额门控堵"配额内无限传 part"的洞，99-D008）。
func (s *Store) MpPut(ctx context.Context, uploadID string, partNo int, src io.Reader) (etag string, size int64, err error) {
	if s.ro.Load() {
		return "", 0, errs.ErrReadOnly
	}
	if partNo < 1 || partNo > 10000 {
		return "", 0, fmt.Errorf("%w: part number %d", errs.ErrInvalidArgument, partNo)
	}
	u, err := s.meta.GetUpload(ctx, uploadID)
	if err != nil {
		return "", 0, err
	}
	mr := md5.New()
	// 流式配额守卫（99-D019）：part 落盘前设上限，防特大 part 打爆磁盘；
	// 重传同一 part 时旧值空间可抵扣
	var credit int64
	if old, oerr := s.meta.GetUploadPart(ctx, uploadID, partNo); oerr == nil && old != nil {
		credit = old.Size
	}
	guarded, gerr := s.quotaGuard(ctx, src, u.Bucket, "", credit)
	if gerr != nil {
		return "", 0, gerr
	}
	sha, size, err := s.blobs.Put(io.TeeReader(guarded, mr))
	if err != nil {
		return "", 0, err
	}
	const maxPart = 5 << 30 // 对齐 S3 单 part 上限
	if size > maxPart {
		return "", 0, fmt.Errorf("%w: part %d is %d bytes", errs.ErrEntityTooLarge, partNo, size)
	}
	etag = hex.EncodeToString(mr.Sum(nil))
	err = s.meta.WithWriteTx(ctx, func(t *meta.Tx) error {
		b, err := t.GetBucket(u.Bucket)
		if err != nil {
			return err
		}
		var oldSize int64
		if old, gerr := t.GetUploadPart(uploadID, partNo); gerr != nil {
			return gerr
		} else if old != nil {
			oldSize = old.Size // 同 part 重传：旧 blob 交给 GC，pending 记差值
		}
		if b.Quota != nil && b.Used+b.Pending+size-oldSize > *b.Quota {
			return fmt.Errorf("%w: bucket %q used+pending %d + part delta %d > quota %d",
				errs.ErrQuotaExceeded, u.Bucket, b.Used+b.Pending, size-oldSize, *b.Quota)
		}
		if err := t.InsertUploadPart(uploadID, partNo, sha, size, etag); err != nil {
			return err
		}
		return t.AdjustUsage(u.Bucket, 0, size-oldSize, 0)
	})
	if err != nil {
		return "", 0, err // 已写 blob 成为无主 blob，GC 回收
	}
	return etag, size, nil
}

// MpComplete 合并会话为对象：单事务内配额校验、写条目、合成 ETag、
// 退还全部 pending、清理会话，并产生 entry_put（从节点此刻批量补拉 part blob）。
func (s *Store) MpComplete(ctx context.Context, uploadID string, clientParts []PartRef) (*ObjInfo, error) {
	u, err := s.meta.GetUpload(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	recorded, err := s.meta.UploadParts(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if len(recorded) == 0 {
		return nil, fmt.Errorf("%w: upload has no parts", errs.ErrInvalidArgument)
	}
	byNo := make(map[int]meta.PartRow, len(recorded))
	for _, p := range recorded {
		byNo[p.PartNo] = p
	}
	// 选中组成对象的 part：客户端清单（须与已传一致）或全部已传
	selected := make([]meta.PartRow, 0, len(recorded))
	if len(clientParts) > 0 {
		for _, cp := range clientParts {
			p, ok := byNo[cp.No]
			if !ok || trimETag(cp.ETag) != p.ETag {
				return nil, fmt.Errorf("%w: part %d not uploaded or etag mismatch", errs.ErrInvalidArgument, cp.No)
			}
			selected = append(selected, p)
		}
	} else {
		selected = append(selected, recorded...)
	}
	var total, pendingAll int64
	refs := make([]BlobRef, len(selected))
	for _, p := range recorded {
		pendingAll += p.Size
	}
	for i, p := range selected {
		total += p.Size
		refs[i] = BlobRef{Sha256: p.Sha256, Size: p.Size}
	}
	if total > s.cfg.MultipartMaxSize {
		return nil, fmt.Errorf("%w: object %d > multipart limit %d", errs.ErrEntityTooLarge, total, s.cfg.MultipartMaxSize)
	}
	// S3 multipart 合成 ETag：md5( 各 part md5 二进制依次拼接 )-N
	hasher := md5.New()
	for _, p := range selected {
		raw, derr := hex.DecodeString(p.ETag)
		if derr != nil {
			return nil, fmt.Errorf("%w: part %d etag", errs.ErrInternal, p.PartNo)
		}
		hasher.Write(raw)
	}
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(hasher.Sum(nil)), len(selected))

	var sess sessionMeta
	json.Unmarshal([]byte(u.MetaJSON), &sess)
	userMeta := "{}"
	if sess.UserMeta != nil {
		b, _ := json.Marshal(sess.UserMeta)
		userMeta = string(b)
	}
	blobsJSON, _ := json.Marshal(refs)

	var out *ObjInfo
	err = s.mutate(ctx, func(t *meta.Tx) error {
		b, err := t.GetBucket(u.Bucket)
		if err != nil {
			return err
		}
		old, err := t.GetEntry(u.Bucket, u.Key)
		if err != nil && !errors.Is(err, errs.ErrKeyNotFound) {
			return err
		}
		if old != nil && old.IsDir {
			return errs.ErrIsDirectory
		}
		dUsed, dObj := total, int64(1)
		if old != nil {
			dUsed, dObj = total-old.Size, 0
		}
		if b.Quota != nil && b.Used+dUsed > *b.Quota {
			return fmt.Errorf("%w: bucket %q used %d + %d > quota %d",
				errs.ErrQuotaExceeded, u.Bucket, b.Used, dUsed, *b.Quota)
		}
		e := &meta.Entry{
			Bucket: u.Bucket, Key: u.Key, Size: total, ETag: etag,
			ContentType: sess.ContentType, UserMeta: userMeta, Blobs: string(blobsJSON),
			Mode: 0o644, MTimeNs: time.Now().UnixNano(),
		}
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		if err := t.AdjustUsage(u.Bucket, dUsed, -pendingAll, dObj); err != nil {
			return err
		}
		if err := t.DeleteUpload(uploadID); err != nil {
			return err
		}
		if err := appendEntryPutOp(t, s.cfg.Epoch, e); err != nil {
			return err
		}
		out = &ObjInfo{
			Bucket: u.Bucket, Key: u.Key, Size: total, ETag: etag,
			ContentType: sess.ContentType, UserMeta: sess.UserMeta,
			Blobs: refs, MTime: time.Unix(0, e.MTimeNs), Mode: 0o644,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MpAbort 放弃会话：清表、退全部 pending；已写 part blob 由 GC 回收。
func (s *Store) MpAbort(ctx context.Context, uploadID string) error {
	u, err := s.meta.GetUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	parts, err := s.meta.UploadParts(ctx, uploadID)
	if err != nil {
		return err
	}
	var pending int64
	for _, p := range parts {
		pending += p.Size
	}
	return s.mutate(ctx, func(t *meta.Tx) error {
		if err := t.DeleteUpload(uploadID); err != nil {
			return err
		}
		if pending > 0 {
			return t.AdjustUsage(u.Bucket, 0, -pending, 0)
		}
		return nil
	})
}

// MpList 列出桶内进行中的会话。
func (s *Store) MpList(ctx context.Context, bucket string) ([]UploadInfo, error) {
	rows, err := s.meta.ListUploads(ctx, bucket)
	if err != nil {
		return nil, err
	}
	out := make([]UploadInfo, 0, len(rows))
	for _, u := range rows {
		out = append(out, UploadInfo{UploadID: u.UploadID, Key: u.Key, CreatedNs: u.CreatedNs})
	}
	return out, nil
}

// MpParts 列出会话已上传的 part（ListParts / 校验用）；会话不存在返回 ErrUploadNotFound。
func (s *Store) MpParts(ctx context.Context, uploadID string) ([]PartInfo, error) {
	if _, err := s.meta.GetUpload(ctx, uploadID); err != nil {
		return nil, err
	}
	parts, err := s.meta.UploadParts(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	out := make([]PartInfo, 0, len(parts))
	for _, p := range parts {
		out = append(out, PartInfo{No: p.PartNo, ETag: p.ETag, Sha: p.Sha256, Size: p.Size})
	}
	return out, nil
}

func trimETag(e string) string {
	return strings.Trim(strings.TrimSpace(e), `"`)
}
