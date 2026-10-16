package store

import (
	"context"
	"encoding/json"
	"fmt"

	"xingmeng/internal/errs"
)

// EachBlobRef 遍历当前全部条目与在途上传 parts 引用的 blob 哈希（已去重）。
// 从节点 Bootstrap 后用它补齐缺失 blob——快照只带元数据，内容按需补拉（99-D004）。
func (s *Store) EachBlobRef(ctx context.Context, fn func(sha string) error) error {
	buckets, err := s.meta.ListBuckets(ctx)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	emit := func(sha string) error {
		if sha == "" || seen[sha] {
			return nil
		}
		seen[sha] = true
		return fn(sha)
	}
	const batch = 1000
	for _, b := range buckets {
		var cursor []byte
		for {
			entries, err := s.meta.ListEntries(ctx, b, cursor, nil, batch)
			if err != nil {
				return err
			}
			for i := range entries {
				e := &entries[i]
				if e.Blobs == "" || e.Blobs == "[]" {
					continue
				}
				var refs []BlobRef
				if err := json.Unmarshal([]byte(e.Blobs), &refs); err != nil {
					return fmt.Errorf("%w: entry %q has corrupt blobs json", errs.ErrInternal, e.Key)
				}
				for _, r := range refs {
					if err := emit(r.Sha256); err != nil {
						return err
					}
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
		return err
	}
	for _, h := range partHashes {
		if err := emit(h); err != nil {
			return err
		}
	}
	return nil
}
