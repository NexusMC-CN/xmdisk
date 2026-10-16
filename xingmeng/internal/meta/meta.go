// Package meta 封装元数据面（SQLite，99-D001）：条目、桶、oplog、凭据、
// multipart 会话、从节点进度共库。本包只提供存储原语；业务一致性
// （配额计数与 oplog 的原子性）由 store 包在 WithWriteTx 的同一事务内组织。
package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// oplog 的 op 类型（99-D003：全量状态 op，幂等回放）。
const (
	OpBucketCreate = "bucket_create"
	OpBucketDelete = "bucket_delete"
	OpQuotaUpdate  = "quota_update"
	OpEntryPut     = "entry_put"
	OpEntryDelete  = "entry_delete"
	OpTreeMove     = "tree_move"
	OpCredPut      = "cred_put"
	OpCredDelete   = "cred_delete"
)

type DB struct{ db *sql.DB }

// Open 打开（或初始化）元数据库。_txlock=immediate 使写事务串行，
// check-and-increment 类操作天然原子（99-D008）。
func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)",
		strings.ReplaceAll(path, "\\", "/"),
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	d := &DB{db: db}
	if err := d.migrate(); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error { return d.db.Close() }

func (d *DB) migrate() error {
	if _, err := d.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("create schema_version: %w", err)
	}
	var v int
	if err := d.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&v); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if v > len(schemas) {
		return fmt.Errorf("metadata schema v%d is newer than this binary supports", v)
	}
	for ; v < len(schemas); v++ {
		if _, err := d.db.Exec(schemas[v]); err != nil {
			return fmt.Errorf("apply schema v%d: %w", v+1, err)
		}
		if _, err := d.db.Exec(`INSERT INTO schema_version(version) VALUES (?)`, v+1); err != nil {
			return fmt.Errorf("stamp schema v%d: %w", v+1, err)
		}
	}
	return nil
}

// Tx 是写事务句柄，其中所有操作与提交同生共死。
type Tx struct{ tx *sql.Tx }

// WithWriteTx 在单个写事务内执行 fn，任一步出错整体回滚。
func (d *DB) WithWriteTx(ctx context.Context, fn func(*Tx) error) error {
	stx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&Tx{stx}); err != nil {
		_ = stx.Rollback()
		return err
	}
	return stx.Commit()
}

// rowQuerier 让同一查询实现同时服务 *sql.DB（读路径）与 *sql.Tx（事务内）。
type rowQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// Snapshot 用 VACUUM INTO 生成一致性快照文件（docs/03；读事务实现，不阻塞写入）。
func (d *DB) Snapshot(dst string) error {
	norm := strings.ReplaceAll(dst, "\\", "/")
	esc := strings.ReplaceAll(norm, "'", "''")
	_, err := d.db.Exec(fmt.Sprintf("VACUUM INTO '%s'", esc))
	return err
}
