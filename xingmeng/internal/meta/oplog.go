package meta

import (
	"context"
)

// Op 是 oplog 中的一条变更事件（复制与恢复的依据，99-D003/D004）。
type Op struct {
	Seq     int64
	Epoch   uint32
	TsNs    int64
	Type    string
	Payload string
}

// AppendOp 追加一条 op 并分配单调 seq（写事务内 max+1；写串行下无缝）。
func (t *Tx) AppendOp(epoch uint32, typ, payload string, tsNs int64) (int64, error) {
	var seq int64
	if err := t.tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) + 1 FROM oplog`).Scan(&seq); err != nil {
		return 0, err
	}
	_, err := t.tx.Exec(
		`INSERT INTO oplog(seq, epoch, ts_ns, type, payload) VALUES(?, ?, ?, ?, ?)`,
		seq, epoch, tsNs, typ, payload,
	)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// InsertOp 以原有 seq/epoch 写入 op（从节点回放用：保持与主一致的日志历史，
// 提升后其他从节点可无缝续传，99-D013）。
func (t *Tx) InsertOp(op Op) error {
	_, err := t.tx.Exec(
		`INSERT INTO oplog(seq, epoch, ts_ns, type, payload) VALUES(?, ?, ?, ?, ?)`,
		op.Seq, op.Epoch, op.TsNs, op.Type, op.Payload,
	)
	return err
}

// MinSeq 返回现存最老 op 的 seq；hasAny 为 false 表示日志为空（从节点应转快照重建）。
func (d *DB) MinSeq(ctx context.Context) (min int64, hasAny bool, err error) {
	var n int
	err = d.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(seq), 0) FROM oplog`).Scan(&n, &min)
	return min, n > 0, err
}

// MaxEpoch 返回日志中的最大 epoch（fencing 基线，99-D005）。
func (d *DB) MaxEpoch(ctx context.Context) (uint32, error) {
	var e int64
	err := d.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(epoch), 0) FROM oplog`).Scan(&e)
	return uint32(e), err
}

// OpsAfter 返回 seq 大于 after 的 op（从节点追赶用）。
func (d *DB) OpsAfter(ctx context.Context, after int64, limit int) ([]Op, error) {
	rows, err := d.db.QueryContext(
		ctx,
		`SELECT seq, epoch, ts_ns, type, payload FROM oplog WHERE seq > ? ORDER BY seq LIMIT ?`,
		after, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Op
	for rows.Next() {
		var op Op
		if err := rows.Scan(&op.Seq, &op.Epoch, &op.TsNs, &op.Type, &op.Payload); err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (d *DB) LastSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := d.db.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM oplog`).Scan(&seq)
	return seq, err
}

// TruncateOplog 删除 seq 小于 before 的 op，返回删除条数（日志截断，03 文档）。
func (d *DB) TruncateOplog(ctx context.Context, before int64) (int64, error) {
	res, err := d.db.ExecContext(ctx, `DELETE FROM oplog WHERE seq < ?`, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
