package meta

import (
	"context"
	"database/sql"
	"fmt"

	"xingmeng/internal/errs"
)

// CredRow 凭据行；SecretEnc 是 AES-GCM 密文（99-D009）。
type CredRow struct {
	AccessKey string
	SecretEnc []byte
	Bucket    string
	Enabled   bool
	CreatedNs int64
}

// InsertCred 插入或更新凭据（主节点 CreateCred 与从节点 cred_put 回放共用）。
func (t *Tx) InsertCred(ak string, secretEnc []byte, bucket string, nowNs int64) error {
	_, err := t.tx.Exec(`INSERT INTO creds(access_key, secret_enc, bucket, enabled, created_at) VALUES(?, ?, ?, 1, ?)
ON CONFLICT(access_key) DO UPDATE SET secret_enc = excluded.secret_enc, bucket = excluded.bucket, enabled = 1`,
		ak, secretEnc, bucket, nowNs)
	return err
}

func (t *Tx) DeleteCred(ak string) error {
	_, err := t.tx.Exec(`DELETE FROM creds WHERE access_key = ?`, ak)
	return err
}

// GetCred 未命中返回 ErrKeyNotFound（调用方归一为鉴权失败）。
func (d *DB) GetCred(ctx context.Context, ak string) (*CredRow, error) {
	row := d.db.QueryRowContext(ctx,
		`SELECT access_key, secret_enc, bucket, enabled, created_at FROM creds WHERE access_key = ?`, ak)
	var c CredRow
	var enabled int
	if err := row.Scan(&c.AccessKey, &c.SecretEnc, &c.Bucket, &enabled, &c.CreatedNs); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: access key", errs.ErrKeyNotFound)
		}
		return nil, err
	}
	c.Enabled = enabled != 0
	return &c, nil
}

// ListCreds 列出凭据（admin 用；不含明文 secret）。
func (d *DB) ListCreds(ctx context.Context) ([]CredRow, error) {
	rows, err := d.db.QueryContext(ctx,
		`SELECT access_key, secret_enc, bucket, enabled, created_at FROM creds ORDER BY access_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CredRow
	for rows.Next() {
		var c CredRow
		var enabled int
		if err := rows.Scan(&c.AccessKey, &c.SecretEnc, &c.Bucket, &enabled, &c.CreatedNs); err != nil {
			return nil, err
		}
		c.Enabled = enabled != 0
		out = append(out, c)
	}
	return out, rows.Err()
}
