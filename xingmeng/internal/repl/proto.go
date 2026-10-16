// Package repl 实现复制面（docs/03）：Master 暴露 oplog 拉取、快照下载、
// blob 下载（墓碑感知）与 ack/register；Slave 拉取-回放-补 blob-上报。
// 挂载在独立 repl 监听（99-D012），与数据面/管理面隔离。
package repl

import (
	"encoding/json"

	"xingmeng/internal/meta"
)

// wireOp 是复制面 JSON 线格式（蛇形命名，curl 可直接排障）。
type wireOp struct {
	Seq     int64           `json:"seq"`
	Epoch   uint32          `json:"epoch"`
	TsNs    int64           `json:"ts_ns"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func toWire(op meta.Op) wireOp {
	return wireOp{Seq: op.Seq, Epoch: op.Epoch, TsNs: op.TsNs, Type: op.Type, Payload: json.RawMessage(op.Payload)}
}

func fromWire(w wireOp) meta.Op {
	return meta.Op{Seq: w.Seq, Epoch: w.Epoch, TsNs: w.TsNs, Type: w.Type, Payload: string(w.Payload)}
}

type oplogResp struct {
	Ops []wireOp `json:"ops"`
}

type ackReq struct {
	SlaveID string `json:"slave_id"`
	Epoch   uint32 `json:"epoch"`
	LastSeq int64  `json:"last_seq"`
}

type registerReq struct {
	SlaveID string `json:"slave_id"`
}

type registerResp struct {
	Epoch   uint32 `json:"epoch"`
	LastSeq int64  `json:"last_seq"`
}

type errorResp struct {
	Error      string `json:"error"`
	Resnapshot bool   `json:"resnapshot,omitempty"`
	Tombstone  bool   `json:"tombstone,omitempty"`
}
