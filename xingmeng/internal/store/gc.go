package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// GCResult 是一轮标记-清扫的统计（admin /gc 与日志共用）。
type GCResult struct {
	EntriesScanned int64
	UploadsAborted int
	Trashed        int64
	Swept          int64
	Resurrected    int64
	GateOpen       bool // 从节点 ack 门控是否放行清扫
	MarkSeq        int64
}

// RunGCOnce 执行一轮完整的标记-清扫（99-D006）：
//
//  1. 清理超时 multipart 会话（其在途 part 随之失去引用）；
//  2. 标记：扫描全部条目与在途 parts 构建存活 blob 集合，
//     blobs/ 中不在集合内者移入 trash（幂等，带时间戳）；
//  3. 清扫：门控（所有已注册从节点 ack 越过上次标记 seq）放行后，
//     对超过宽限期的 trash 条目逐个重验存活——仍被引用者复活，
//     其余物理删除。重验不信标记，自愈优先。
func (s *Store) RunGCOnce(ctx context.Context) (*GCResult, error) {
	res := &GCResult{}

	n, err := s.abortStaleUploads(ctx)
	if err != nil {
		return res, fmt.Errorf("abort stale uploads: %w", err)
	}
	res.UploadsAborted = n

	live, scanned, err := s.scanLiveBlobs(ctx)
	if err != nil {
		return res, fmt.Errorf("mark: %w", err)
	}
	res.EntriesScanned = scanned

	physical, err := s.blobs.List()
	if err != nil {
		return res, fmt.Errorf("list blobs: %w", err)
	}
	for _, h := range physical {
		if live[h] {
			continue
		}
		if err := s.blobs.Trash(h); err != nil {
			return res, fmt.Errorf("trash %s: %w", h, err)
		}
		res.Trashed++
	}

	prevMark, err := s.meta.GCMarkSeq(ctx)
	if err != nil {
		return res, fmt.Errorf("read gc mark seq: %w", err)
	}
	seq, err := s.meta.LastSeq(ctx)
	if err != nil {
		return res, err
	}
	if err := s.meta.SetGCMarkSeq(ctx, seq); err != nil {
		return res, fmt.Errorf("write gc mark seq: %w", err)
	}
	res.MarkSeq = seq

	minAck, hasSlaves, err := s.meta.MinSlaveAck(ctx)
	if err != nil {
		return res, err
	}
	res.GateOpen = !hasSlaves || minAck >= prevMark
	if !res.GateOpen {
		return res, nil
	}
	swept, resurrected, err := s.blobs.SweepTrash(s.cfg.GCGrace, func(h string) bool { return live[h] })
	if err != nil {
		return res, fmt.Errorf("sweep: %w", err)
	}
	res.Swept, res.Resurrected = swept, resurrected
	return res, nil
}

// StartGC 启动后台标记-清扫循环，ctx 取消即停（main 接管生命周期）。
func (s *Store) StartGC(ctx context.Context) {
	go func() {
		t := time.NewTicker(s.cfg.GCInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				res, err := s.RunGCOnce(ctx)
				if err != nil {
					slog.Error("gc run failed", "err", err)
					continue
				}
				slog.Info("gc run",
					"scanned", res.EntriesScanned,
					"uploadsAborted", res.UploadsAborted,
					"trashed", res.Trashed,
					"swept", res.Swept,
					"resurrected", res.Resurrected,
					"gateOpen", res.GateOpen,
					"markSeq", res.MarkSeq,
				)
			}
		}
	}()
}

// scanLiveBlobs 构建存活 blob 集合：全部条目的 blob 引用 + 在途上传 parts。
// 条目 blobs 解析失败视为致命错误（宁可中止 GC 也不冒误删风险）。
func (s *Store) scanLiveBlobs(ctx context.Context) (map[string]bool, int64, error) {
	live := map[string]bool{}
	var scanned int64
	buckets, err := s.meta.ListBuckets(ctx)
	if err != nil {
		return nil, 0, err
	}
	const batch = 1000
	for _, b := range buckets {
		var cursor []byte
		for {
			entries, err := s.meta.ListEntries(ctx, b, cursor, nil, batch)
			if err != nil {
				return nil, 0, err
			}
			for i := range entries {
				e := &entries[i]
				scanned++
				if e.Blobs == "" || e.Blobs == "[]" {
					continue
				}
				var refs []BlobRef
				if err := json.Unmarshal([]byte(e.Blobs), &refs); err != nil {
					return nil, 0, fmt.Errorf("%w: entry %q has corrupt blobs json", errs.ErrInternal, e.Key)
				}
				for _, r := range refs {
					live[r.Sha256] = true
				}
			}
			if len(entries) < batch {
				break
			}
			cursor = append([]byte(entries[len(entries)-1].Key), 0)
		}
	}
	partHashes, err := s.meta.UploadPartHashes(ctx)
	if err != nil {
		return nil, 0, err
	}
	for _, h := range partHashes {
		live[h] = true
	}
	return live, scanned, nil
}

// abortStaleUploads 清理超过 MultipartAbortAfter 的 multipart 会话并退还 pending 计数。
func (s *Store) abortStaleUploads(ctx context.Context) (int, error) {
	cut := time.Now().Add(-s.cfg.MultipartAbortAfter).UnixNano()
	stale, err := s.meta.ListStaleUploads(ctx, cut)
	if err != nil {
		return 0, err
	}
	for _, u := range stale {
		parts, err := s.meta.UploadParts(ctx, u.UploadID)
		if err != nil {
			return 0, err
		}
		var pending int64
		for _, p := range parts {
			pending += p.Size
		}
		err = s.meta.WithWriteTx(ctx, func(t *meta.Tx) error {
			if err := t.DeleteUpload(u.UploadID); err != nil {
				return err
			}
			if pending > 0 {
				return t.AdjustUsage(u.Bucket, 0, -pending, 0)
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("abort upload %s: %w", u.UploadID, err)
		}
	}
	return len(stale), nil
}
