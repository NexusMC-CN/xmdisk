package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"xingmeng/internal/errs"
)

// VerifyResult 抽样校验结果（admin /verify）。
type VerifyResult struct {
	EntriesChecked int64
	BlobsChecked   int64
	Failures       []VerifyFailure
}

type VerifyFailure struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Sha256 string `json:"sha256"`
	Reason string `json:"reason"`
}

// VerifySample 抽样校验：按字典序扫描至多 limit 个条目，逐个 blob 全量
// 读取并对账 sha256（99-D002 的内容寻址对账——磁盘位翻转/静默损坏的兜底）。
// limit<=0 时默认 100。
func (s *Store) VerifySample(ctx context.Context, limit int) (*VerifyResult, error) {
	if limit <= 0 {
		limit = 100
	}
	res := &VerifyResult{}
	buckets, err := s.meta.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	const batch = 100
	for _, b := range buckets {
		var cursor []byte
		for res.EntriesChecked < int64(limit) {
			entries, err := s.meta.ListEntries(ctx, b, cursor, nil, batch)
			if err != nil {
				return nil, err
			}
			for i := range entries {
				if res.EntriesChecked >= int64(limit) {
					break
				}
				e := &entries[i]
				res.EntriesChecked++
				if e.Blobs == "" || e.Blobs == "[]" {
					continue
				}
				var refs []BlobRef
				if err := json.Unmarshal([]byte(e.Blobs), &refs); err != nil {
					res.Failures = append(res.Failures, VerifyFailure{b, e.Key, "", "corrupt blobs json"})
					continue
				}
				for _, ref := range refs {
					if seen[ref.Sha256] {
						continue
					}
					seen[ref.Sha256] = true
					res.BlobsChecked++
					if err := s.verifyBlob(ref.Sha256); err != nil {
						res.Failures = append(res.Failures, VerifyFailure{b, e.Key, ref.Sha256, err.Error()})
					}
				}
			}
			if len(entries) < batch {
				break
			}
			cursor = append([]byte(entries[len(entries)-1].Key), 0)
		}
	}
	return res, nil
}

func (s *Store) verifyBlob(sha string) error {
	f, err := s.blobs.Open(sha)
	if err != nil {
		return fmt.Errorf("%w: open: %v", errs.ErrInternal, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("%w: read: %v", errs.ErrInternal, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sha {
		return fmt.Errorf("hash mismatch: on-disk %s", got)
	}
	return nil
}
