package meta

import (
	"context"
	"database/sql"
	"fmt"

	"xingmeng/internal/errs"
)

// 条目类型。
const (
	TypeFile = 1
	TypeDir  = 2
)

// Entry 是命名空间中的一条元数据（file 或 dir）。
type Entry struct {
	Bucket      string
	Key         string // 原始字节（可能非 UTF-8），存库时转 BLOB 保序
	IsDir       bool
	Size        int64
	ETag        string
	ContentType string
	UserMeta    string // JSON 原文
	Blobs       string // JSON 原文 [{sha256,size}]
	UID         int64
	GID         int64
	Mode        uint32
	MTimeNs     int64
	Seq         int64
}

const entryCols = `bucket, key, type, size, etag, content_type, user_meta, blobs, uid, gid, mode, mtime_ns, seq`

func scanEntry(scan func(...any) error) (*Entry, error) {
	var e Entry
	var key []byte
	var typ int
	if err := scan(&e.Bucket, &key, &typ, &e.Size, &e.ETag, &e.ContentType, &e.UserMeta, &e.Blobs, &e.UID, &e.GID, &e.Mode, &e.MTimeNs, &e.Seq); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: entry", errs.ErrKeyNotFound)
		}
		return nil, err
	}
	e.Key = string(key)
	e.IsDir = typ == TypeDir
	return &e, nil
}

func entryArgs(e *Entry) []any {
	typ := TypeFile
	if e.IsDir {
		typ = TypeDir
	}
	return []any{e.Bucket, []byte(e.Key), typ, e.Size, e.ETag, e.ContentType, e.UserMeta, e.Blobs, e.UID, e.GID, e.Mode, e.MTimeNs, e.Seq}
}

func getEntry(q rowQuerier, bucket, key string) (*Entry, error) {
	return scanEntry(q.QueryRow(`SELECT `+entryCols+` FROM entries WHERE bucket = ? AND key = ?`, bucket, []byte(key)).Scan)
}

func (d *DB) GetEntry(ctx context.Context, bucket, key string) (*Entry, error) {
	return getEntry(d.db, bucket, key)
}

func (t *Tx) GetEntry(bucket, key string) (*Entry, error) {
	return getEntry(t.tx, bucket, key)
}

func (t *Tx) UpsertEntry(e *Entry) error {
	_, err := t.tx.Exec(
		`INSERT INTO entries(`+entryCols+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(bucket, key) DO UPDATE SET
  type = excluded.type, size = excluded.size, etag = excluded.etag,
  content_type = excluded.content_type, user_meta = excluded.user_meta,
  blobs = excluded.blobs, uid = excluded.uid, gid = excluded.gid,
  mode = excluded.mode, mtime_ns = excluded.mtime_ns, seq = excluded.seq`,
		entryArgs(e)...,
	)
	return err
}

func (t *Tx) DeleteEntry(bucket, key string) error {
	_, err := t.tx.Exec(`DELETE FROM entries WHERE bucket = ? AND key = ?`, bucket, []byte(key))
	return err
}

// PrefixRange 计算前缀扫描界：lo 含、hi 不含；hi 为 nil 表示无上界。
func PrefixRange(prefix []byte) (lo, hi []byte) {
	hi = append([]byte(nil), prefix...)
	for i := len(hi) - 1; i >= 0; i-- {
		if hi[i] != 0xFF {
			hi[i]++
			return prefix, hi
		}
	}
	return prefix, nil // 前缀全 0xFF：无有限上界
}

// CountPrefix 统计前缀下条目数（rmdir 判空用；不含前缀条目自身）。
func (t *Tx) CountPrefix(bucket, prefix string) (int64, error) {
	lo, hi := PrefixRange([]byte(prefix))
	q := `SELECT COUNT(*) FROM entries WHERE bucket = ? AND key >= ? AND key != ?`
	args := []any{bucket, lo, []byte(prefix)}
	if hi != nil {
		q += ` AND key < ?`
		args = append(args, hi)
	}
	var n int64
	err := t.tx.QueryRow(q, args...).Scan(&n)
	return n, err
}

// queryer 让条目扫描同时服务 *sql.DB（读路径）与 *sql.Tx（事务内，tree_move 重写用）。
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func listEntries(q queryer, ctx context.Context, bucket string, lo, hi []byte, limit int) ([]Entry, error) {
	sqlStr := `SELECT ` + entryCols + ` FROM entries WHERE bucket = ?`
	args := []any{bucket}
	if lo != nil {
		sqlStr += ` AND key >= ?`
		args = append(args, lo)
	}
	if hi != nil {
		sqlStr += ` AND key < ?`
		args = append(args, hi)
	}
	sqlStr += ` ORDER BY key LIMIT ?`
	args = append(args, limit)
	rows, err := q.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListEntries 按字典序返回 [lo, hi) 内的条目（lo 为 nil 表示不设下界）。
func (d *DB) ListEntries(ctx context.Context, bucket string, lo, hi []byte, limit int) ([]Entry, error) {
	return listEntries(d.db, ctx, bucket, lo, hi, limit)
}

// ListEntries 事务内版本（tree_move 重写共用同一扫描语义）。
func (t *Tx) ListEntries(ctx context.Context, bucket string, lo, hi []byte, limit int) ([]Entry, error) {
	return listEntries(t.tx, ctx, bucket, lo, hi, limit)
}
