package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// AttrPatch 修改条目属性（不动内容与大小；SFTP chmod/utimes、WebDAV PROPPATCH 用）。
type AttrPatch struct {
	Mode        *uint32
	UID, GID    *int64
	MTime       *time.Time
	ContentType *string
	Meta        map[string]string // 非 nil 时整体覆盖 user meta
}

// SetAttrs 应用属性补丁并产生 entry_put（全量状态 op，不重写内容，99-D003）。
func (s *Store) SetAttrs(ctx context.Context, bucket, key string, patch AttrPatch) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(bucket); err != nil {
			return err
		}
		e, err := t.GetEntry(bucket, key)
		if err != nil {
			return err
		}
		if patch.Mode != nil {
			e.Mode = *patch.Mode
		}
		if patch.UID != nil {
			e.UID = *patch.UID
		}
		if patch.GID != nil {
			e.GID = *patch.GID
		}
		if patch.MTime != nil {
			e.MTimeNs = patch.MTime.UnixNano()
		}
		if patch.ContentType != nil {
			e.ContentType = *patch.ContentType
		}
		if patch.Meta != nil {
			b, err := json.Marshal(patch.Meta)
			if err != nil {
				return err
			}
			e.UserMeta = string(b)
		}
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		return appendEntryPutOp(t, s.currentEpoch(), e)
	})
}

type treeMovePayload struct {
	Bucket string `json:"bucket"`
	Src    []byte `json:"src"` // 含尾斜杠的前缀
	Dst    []byte `json:"dst"`
}

// Move 同桶内重命名。文件产生相邻的 entry_put + entry_delete 两条 op；
// 目录（key 以 "/" 结尾）产生单条 tree_move（99-D011）：主从都在同一
// 事务内重写前缀下全部 key，blob 引用原样搬移，零内容 I/O。
func (s *Store) Move(ctx context.Context, bucket, src, dst string) error {
	if err := validateKey(src); err != nil {
		return err
	}
	if err := validateKey(dst); err != nil {
		return err
	}
	if src == dst {
		return fmt.Errorf("%w: move onto itself", errs.ErrInvalidArgument)
	}
	srcDir, dstDir := strings.HasSuffix(src, "/"), strings.HasSuffix(dst, "/")
	if srcDir != dstDir {
		return fmt.Errorf("%w: dir move requires both keys with trailing slash", errs.ErrInvalidArgument)
	}
	if srcDir {
		if strings.HasPrefix(dst, src) {
			return fmt.Errorf("%w: move directory into itself", errs.ErrInvalidArgument)
		}
		return s.mutate(ctx, func(t *meta.Tx) error {
			e, err := t.GetEntry(bucket, src)
			if err != nil {
				return err
			}
			if !e.IsDir {
				return errs.ErrIsFile
			}
			dUsed, dObj, err := movePrefixTx(ctx, t, bucket, src, dst)
			if err != nil {
				return err
			}
			if dUsed != 0 || dObj != 0 {
				if err := t.AdjustUsage(bucket, dUsed, 0, dObj); err != nil {
					return err
				}
			}
			return appendOp(t, s.currentEpoch(), meta.OpTreeMove,
				treeMovePayload{Bucket: bucket, Src: []byte(src), Dst: []byte(dst)})
		})
	}
	return s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(bucket); err != nil {
			return err
		}
		e, err := t.GetEntry(bucket, src)
		if err != nil {
			return err
		}
		if e.IsDir {
			return errs.ErrIsDirectory // 目录移动必须走带尾斜杠的 tree_move
		}
		old, err := t.GetEntry(bucket, dst)
		if err != nil && !errors.Is(err, errs.ErrKeyNotFound) {
			return err
		}
		if old != nil && old.IsDir {
			return errs.ErrIsDirectory
		}
		e.Key = dst
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		if err := t.DeleteEntry(bucket, src); err != nil {
			return err
		}
		// 条目搬运不改变用量；覆盖已有目标条目时，被覆盖者的用量出账
		var dUsed, dObj int64
		if old != nil {
			dUsed = -old.Size
			dObj = -1
		}
		if dUsed != 0 || dObj != 0 {
			if err := t.AdjustUsage(bucket, dUsed, 0, dObj); err != nil {
				return err
			}
		}
		if err := appendEntryPutOp(t, s.currentEpoch(), e); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpEntryDelete, entryDeletePayload{Bucket: bucket, Key: []byte(src)})
	})
}

// movePrefixTx 在事务内把 src 前缀下全部条目（含目录条目自身）重写为 dst
// 前缀，返回用量与计数变化。主节点 Move 与从节点 tree_move 回放共用同一实现。
func movePrefixTx(ctx context.Context, t *meta.Tx, bucket, src, dst string) (dUsed, dObj int64, err error) {
	lo, hi := meta.PrefixRange([]byte(src))
	type moved struct {
		from, to string
		e        meta.Entry
	}
	var moves []moved
	var cursor []byte = lo
	const batch = 1000
	for {
		entries, lerr := t.ListEntries(ctx, bucket, cursor, hi, batch)
		if lerr != nil {
			return 0, 0, lerr
		}
		for _, e := range entries {
			to := dst + e.Key[len(src):]
			moves = append(moves, moved{from: e.Key, to: to, e: e})
		}
		if len(entries) < batch {
			break
		}
		cursor = append([]byte(entries[len(entries)-1].Key), 0)
	}
	for _, m := range moves {
		old, gerr := t.GetEntry(bucket, m.to)
		if gerr != nil && !errors.Is(gerr, errs.ErrKeyNotFound) {
			return 0, 0, gerr
		}
		ne := m.e
		ne.Key = m.to
		if uerr := t.UpsertEntry(&ne); uerr != nil {
			return 0, 0, uerr
		}
		if derr := t.DeleteEntry(bucket, m.from); derr != nil {
			return 0, 0, derr
		}
		// 搬运不改变用量；覆盖已存在目标条目时，被覆盖者出账
		if old != nil {
			dUsed -= old.Size
			dObj--
		}
	}
	return dUsed, dObj, nil
}

// appendEntryPutOp 由条目构造全量状态 entry_put op（Move/SetAttrs 与 Put 共用形态）。
func appendEntryPutOp(t *meta.Tx, epoch uint32, e *meta.Entry) error {
	var refs []BlobRef
	if e.Blobs != "" && e.Blobs != "[]" {
		if err := json.Unmarshal([]byte(e.Blobs), &refs); err != nil {
			return fmt.Errorf("%w: corrupt blobs json in entry %q", errs.ErrInternal, e.Key)
		}
	}
	return appendOp(t, epoch, meta.OpEntryPut, entryPutPayload{
		Bucket: e.Bucket, Key: []byte(e.Key), IsDir: e.IsDir, Size: e.Size,
		ETag: e.ETag, ContentType: e.ContentType, UserMeta: e.UserMeta, Blobs: refs,
		UID: e.UID, GID: e.GID, Mode: e.Mode, MTimeNs: e.MTimeNs,
	})
}
