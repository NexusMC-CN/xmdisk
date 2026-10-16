package meta

import (
	"context"
	"database/sql"
	"fmt"

	"xingmeng/internal/errs"
)

// Bucket 桶状态与配额计数（同事务维护，99-D008）。
type Bucket struct {
	Name      string
	Quota     *int64 // nil = 不限
	Used      int64  // 逻辑字节（计费/配额口径）
	Pending   int64  // multipart 在途
	Objects   int64
	CreatedNs int64
}

func getBucket(q rowQuerier, name string) (*Bucket, error) {
	row := q.QueryRow(
		`SELECT name, quota_bytes, used_bytes, pending_bytes, object_count, created_at FROM buckets WHERE name = ?`,
		name,
	)
	var b Bucket
	var quota sql.NullInt64
	if err := row.Scan(&b.Name, &quota, &b.Used, &b.Pending, &b.Objects, &b.CreatedNs); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: bucket %q", errs.ErrBucketNotFound, name)
		}
		return nil, err
	}
	if quota.Valid {
		v := quota.Int64
		b.Quota = &v
	}
	return &b, nil
}

func (d *DB) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	return getBucket(d.db, name)
}

func (t *Tx) GetBucket(name string) (*Bucket, error) {
	return getBucket(t.tx, name)
}

func (t *Tx) CreateBucket(name string, quota *int64, nowNs int64) error {
	var q any
	if quota != nil {
		q = *quota
	}
	_, err := t.tx.Exec(`INSERT INTO buckets(name, quota_bytes, created_at) VALUES(?, ?, ?)`, name, q, nowNs)
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: bucket %q", errs.ErrBucketExists, name)
	}
	return err
}

func (t *Tx) DeleteBucket(name string) error {
	_, err := t.tx.Exec(`DELETE FROM buckets WHERE name = ?`, name)
	return err
}

// AdjustUsage 增减桶计数器；必须与条目变更同事务调用（99-D008）。
func (t *Tx) AdjustUsage(name string, dUsed, dPending, dObjects int64) error {
	_, err := t.tx.Exec(
		`UPDATE buckets SET used_bytes = used_bytes + ?, pending_bytes = pending_bytes + ?, object_count = object_count + ? WHERE name = ?`,
		dUsed, dPending, dObjects, name,
	)
	return err
}

func (t *Tx) SetQuota(name string, quota *int64) error {
	var q any
	if quota != nil {
		q = *quota
	}
	_, err := t.tx.Exec(`UPDATE buckets SET quota_bytes = ? WHERE name = ?`, q, name)
	return err
}
