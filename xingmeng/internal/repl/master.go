package repl

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"xingmeng/internal/store"
)

// Master 是复制面的主节点端点集合（docs/03 复制协议一节）。
type Master struct {
	store *store.Store
	token string // 静态凭证；空 = 不鉴权（开发形态）
}

func NewMaster(s *store.Store, token string) *Master {
	return &Master{store: s, token: token}
}

func (m *Master) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /oplog", m.handleOplog)
	mux.HandleFunc("GET /snapshot", m.handleSnapshot)
	mux.HandleFunc("GET /blob/{sha}", m.handleBlob)
	mux.HandleFunc("POST /ack", m.handleAck)
	mux.HandleFunc("POST /register", m.handleRegister)
	return m.auth(mux)
}

func (m *Master) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 空 token 的"不鉴权"仅供包内测试直连形态；节点层保证空 token 不监听
		// 复制面（99-D023③），线上不存在无凭证可达的复制面。
		if m.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Xm-Token")), []byte(m.token)) != 1 {
			writeErr(w, http.StatusUnauthorized, "bad or missing token", false)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeErr(w http.ResponseWriter, status int, msg string, resnapshot bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResp{Error: msg, Resnapshot: resnapshot})
}

// writeInternal 500 固定文案（内部细节只进日志，不回对端）。
func writeInternal(w http.ResponseWriter, where string, err error) {
	slog.Error("repl internal error", "where", where, "err", err.Error())
	writeErr(w, http.StatusInternalServerError, "internal error", false)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// handleOplog 返回 seq > after 的 op。从节点落后超出日志保留期时返回
// 409 + resnapshot，从节点转全量重建，不死磕（docs/03）。
func (m *Master) handleOplog(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = 500
	}
	if limit > 5000 {
		limit = 5000
	}
	ctx := r.Context()
	minSeq, hasAny, err := m.store.MinSeq(ctx)
	if err != nil {
		writeInternal(w, "oplog/min_seq", err)
		return
	}
	if hasAny && after+1 < minSeq {
		writeErr(w, http.StatusConflict,
			fmt.Sprintf("requested after=%d is below retained log (min=%d)", after, minSeq), true)
		return
	}
	ops, err := m.store.Ops(ctx, after, limit)
	if err != nil {
		writeInternal(w, "oplog/fetch", err)
		return
	}
	out := make([]wireOp, len(ops))
	for i, op := range ops {
		out[i] = toWire(op)
	}
	writeJSON(w, http.StatusOK, oplogResp{Ops: out})
}

// handleSnapshot 生成快照（VACUUM INTO）并以下载提供；X-Xm-Seq 头为快照截断点。
func (m *Master) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	path, seq, err := m.store.Snapshot(r.Context())
	if err != nil {
		writeInternal(w, "snapshot/create", err)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeInternal(w, "snapshot/open", err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Xm-Seq", strconv.FormatInt(seq, 10))
	io.Copy(w, f)
}

// handleBlob 提供 blob 内容；处于待删除区时返回 410 墓碑（从节点跳过，
// 消除"删除与同步赛跑"的重试风暴，99-D006）。
func (m *Master) handleBlob(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha")
	if m.store.BlobTombstoned(sha) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(errorResp{Error: "blob tombstoned", Tombstone: true})
		return
	}
	f, size, err := m.store.GetBlob(sha)
	if err != nil {
		writeErr(w, http.StatusNotFound, "blob not found: "+sha, false)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	io.Copy(w, f)
}

func (m *Master) handleAck(w http.ResponseWriter, r *http.Request) {
	var req ackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SlaveID == "" {
		writeErr(w, http.StatusBadRequest, "bad ack body", false)
		return
	}
	if err := m.store.AckSlave(r.Context(), req.SlaveID, req.LastSeq, time.Now().UnixNano()); err != nil {
		writeInternal(w, "ack", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleRegister 登记从节点并回主节点当前 epoch / last_seq（epoch 握手）。
// 进度以从节点随后的 ack 为准，注册本身不回退已有进度。
func (m *Master) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SlaveID == "" {
		writeErr(w, http.StatusBadRequest, "bad register body", false)
		return
	}
	ctx := r.Context()
	if err := m.store.RegisterSlave(ctx, req.SlaveID, time.Now().UnixNano()); err != nil {
		writeInternal(w, "register", err)
		return
	}
	epoch, err := m.store.MaxEpoch(ctx)
	if err != nil {
		writeInternal(w, "register/epoch", err)
		return
	}
	last, err := m.store.LastSeq(ctx)
	if err != nil {
		writeInternal(w, "register/last_seq", err)
		return
	}
	writeJSON(w, http.StatusOK, registerResp{Epoch: epoch, LastSeq: last})
}
