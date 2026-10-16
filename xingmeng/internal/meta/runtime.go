package meta

import (
	"context"
)

// ---- 桶列举（GC 标记用）----

func (d *DB) ListBuckets(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT name FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ---- 从节点进度（repl API 写入；GC 门控与日志截断读取）----

// MinSlaveAck 返回已注册从节点的最小 ack seq；exists 为 false 表示当前无从节点。
func (d *DB) MinSlaveAck(ctx context.Context) (min int64, exists bool, err error) {
	var n int
	err = d.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MIN(last_acked), 0) FROM slaves`).Scan(&n, &min)
	return min, n > 0, err
}

// SetSlaveAck upsert 从节点进度（ack 心跳，repl 面调用）。
func (d *DB) SetSlaveAck(ctx context.Context, id string, acked int64, nowNs int64) error {
	_, err := d.db.ExecContext(ctx, `INSERT INTO slaves(id, last_acked, last_seen_ns) VALUES(?, ?, ?)
ON CONFLICT(id) DO UPDATE SET last_acked = excluded.last_acked, last_seen_ns = excluded.last_seen_ns`,
		id, acked, nowNs)
	return err
}

// RegisterSlave 登记从节点但不回退已有进度（进度以随后的 ack 为准）。
func (d *DB) RegisterSlave(ctx context.Context, id string, nowNs int64) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO slaves(id, last_acked, epoch, last_seen_ns) VALUES(?, 0, 0, ?)`, id, nowNs)
	return err
}

// ---- GC 状态（schema v2）----

// GCMarkSeq 返回上次完成标记时的 oplog seq。
func (d *DB) GCMarkSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := d.db.QueryRowContext(ctx, `SELECT last_mark_seq FROM gc_state WHERE id = 1`).Scan(&seq)
	return seq, err
}

func (d *DB) SetGCMarkSeq(ctx context.Context, seq int64) error {
	_, err := d.db.ExecContext(ctx, `UPDATE gc_state SET last_mark_seq = ? WHERE id = 1`, seq)
	return err
}

// ---- multipart 会话（本地表，不复制，99-D010；GC 清理超时会话）----

type UploadRow struct {
	UploadID  string
	Bucket    string
	Key       string
	MetaJSON  string
	CreatedNs int64
}

type PartRow struct {
	PartNo int
	Sha256 string
	Size   int64
	ETag   string
}

func (d *DB) InsertUpload(ctx context.Context, uploadID, bucket, key, metaJSON string, nowNs int64) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO uploads(upload_id, bucket, key, meta_json, created_at) VALUES(?, ?, ?, ?, ?)`,
		uploadID, bucket, []byte(key), metaJSON, nowNs)
	return err
}

func (d *DB) InsertUploadPart(ctx context.Context, uploadID string, partNo int, sha string, size int64, etag string) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO upload_parts(upload_id, part_no, sha256, size, etag) VALUES(?, ?, ?, ?, ?)
ON CONFLICT(upload_id, part_no) DO UPDATE SET sha256 = excluded.sha256, size = excluded.size, etag = excluded.etag`,
		uploadID, partNo, sha, size, etag)
	return err
}

// ListStaleUploads 返回创建时间早于 olderThanNs 的会话。
func (d *DB) ListStaleUploads(ctx context.Context, olderThanNs int64) ([]UploadRow, error) {
	rows, err := d.db.QueryContext(ctx,
		`SELECT upload_id, bucket, key, meta_json, created_at FROM uploads WHERE created_at < ?`, olderThanNs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UploadRow
	for rows.Next() {
		var u UploadRow
		var key []byte
		if err := rows.Scan(&u.UploadID, &u.Bucket, &key, &u.MetaJSON, &u.CreatedNs); err != nil {
			return nil, err
		}
		u.Key = string(key)
		out = append(out, u)
	}
	return out, rows.Err()
}

func (d *DB) UploadParts(ctx context.Context, uploadID string) ([]PartRow, error) {
	rows, err := d.db.QueryContext(ctx,
		`SELECT part_no, sha256, size, etag FROM upload_parts WHERE upload_id = ? ORDER BY part_no`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartRow
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.PartNo, &p.Sha256, &p.Size, &p.ETag); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeleteUpload 连带删除会话与其 parts 行（须在写事务内调用）。
func (t *Tx) DeleteUpload(uploadID string) error {
	if _, err := t.tx.Exec(`DELETE FROM upload_parts WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}
	_, err := t.tx.Exec(`DELETE FROM uploads WHERE upload_id = ?`, uploadID)
	return err
}

// UploadPartHashes 返回全部在途 part 引用的 blob 哈希（标记阶段保活在途上传）。
func (d *DB) UploadPartHashes(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT DISTINCT sha256 FROM upload_parts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
