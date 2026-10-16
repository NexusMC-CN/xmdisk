package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// ApplyOp 从节点按序回放一条 op：以原 seq/epoch 写入本地 oplog 并施加
// 状态与计数器变更，同一事务（docs/03 复制协议）。幂等与安全规则：
//   - op.Seq <= 本地 LastSeq 视为已应用，直接跳过；
//   - entry_put 覆盖为目标状态；entry_delete 目标不存在视为成功；
//   - op.Epoch 小于本地日志最大 epoch 时拒绝（防旧主复活，99-D005）。
func (s *Store) ApplyOp(ctx context.Context, op meta.Op) error {
	last, err := s.meta.LastSeq(ctx)
	if err != nil {
		return err
	}
	if op.Seq <= last {
		return nil
	}
	maxEpoch, err := s.meta.MaxEpoch(ctx)
	if err != nil {
		return err
	}
	if op.Epoch < maxEpoch {
		return fmt.Errorf("%w: op %d epoch %d < local max %d", errs.ErrStaleEpoch, op.Seq, op.Epoch, maxEpoch)
	}
	return s.meta.WithWriteTx(ctx, func(t *meta.Tx) error {
		if err := applyOpTx(ctx, t, op); err != nil {
			return err
		}
		return t.InsertOp(op)
	})
}

func applyOpTx(ctx context.Context, t *meta.Tx, op meta.Op) error {
	switch op.Type {
	case meta.OpBucketCreate:
		var p bucketPayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		if err := t.CreateBucket(p.Name, p.Quota, op.TsNs); err != nil &&
			!errors.Is(err, errs.ErrBucketExists) {
			return err
		}
		return nil

	case meta.OpBucketDelete:
		var p bucketPayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		return t.DeleteBucket(p.Name)

	case meta.OpQuotaUpdate:
		var p bucketPayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		return t.SetQuota(p.Name, p.Quota)

	case meta.OpEntryPut:
		e, err := parseEntryPut(op.Payload)
		if err != nil {
			return badPayload(op, err)
		}
		old, err := t.GetEntry(e.Bucket, e.Key)
		if err != nil && !errors.Is(err, errs.ErrKeyNotFound) {
			return err
		}
		if err := t.UpsertEntry(e); err != nil {
			return err
		}
		dUsed, dObj := e.Size, int64(1)
		if old != nil {
			dUsed, dObj = e.Size-old.Size, 0
		}
		if dUsed != 0 || dObj != 0 {
			return t.AdjustUsage(e.Bucket, dUsed, 0, dObj)
		}
		return nil

	case meta.OpEntryDelete:
		var p entryDeletePayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		key := string(p.Key)
		old, err := t.GetEntry(p.Bucket, key)
		if errors.Is(err, errs.ErrKeyNotFound) {
			return nil // 幂等
		}
		if err != nil {
			return err
		}
		if err := t.DeleteEntry(p.Bucket, key); err != nil {
			return err
		}
		return t.AdjustUsage(p.Bucket, -old.Size, 0, -1)

	case meta.OpTreeMove:
		var p treeMovePayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		src, dst := string(p.Src), string(p.Dst)
		dUsed, dObj, err := movePrefixTx(ctx, t, p.Bucket, src, dst)
		if err != nil {
			return err
		}
		if dUsed != 0 || dObj != 0 {
			return t.AdjustUsage(p.Bucket, dUsed, 0, dObj)
		}
		return nil

	case meta.OpCredPut:
		var p credPutPayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		enc, err := base64.StdEncoding.DecodeString(p.SecretEnc)
		if err != nil {
			return badPayload(op, err)
		}
		return t.InsertCred(p.AccessKey, enc, p.Bucket, op.TsNs)

	case meta.OpCredDelete:
		var p credDeletePayload
		if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
			return badPayload(op, err)
		}
		return t.DeleteCred(p.AccessKey)

	default:
		return fmt.Errorf("%w: unknown op type %q (newer master?)", errs.ErrInternal, op.Type)
	}
}

func badPayload(op meta.Op, err error) error {
	return fmt.Errorf("%w: op %d (%s) payload: %v", errs.ErrInternal, op.Seq, op.Type, err)
}

func parseEntryPut(payload string) (*meta.Entry, error) {
	var p entryPutPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return nil, err
	}
	blobs := "[]"
	if p.Blobs != nil {
		b, err := json.Marshal(p.Blobs)
		if err != nil {
			return nil, err
		}
		blobs = string(b)
	}
	userMeta := "{}"
	if p.UserMeta != "" {
		userMeta = p.UserMeta
	}
	e := &meta.Entry{
		Bucket: p.Bucket, Key: string(p.Key), IsDir: p.IsDir,
		Size: p.Size, ETag: p.ETag, ContentType: p.ContentType,
		UserMeta: userMeta, Blobs: blobs,
		UID: p.UID, GID: p.GID, Mode: p.Mode, MTimeNs: p.MTimeNs, Seq: 0,
	}
	return e, nil
}
