// Package node 是节点运行时（docs/03 角色模型）：role / serve / sync 三开关
// 状态机（99-D014），promote 护栏（99-D005），从节点开机自同步。
package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"xingmeng/internal/config"
	"xingmeng/internal/errs"
	"xingmeng/internal/repl"
	"xingmeng/internal/s3"
	xsftp "xingmeng/internal/sftp"
	"xingmeng/internal/store"
	xwebdav "xingmeng/internal/webdav"
)

const (
	ServeOff = "off"
	ServeRO  = "ro"
	ServeRW  = "rw"
)

type Node struct {
	cfg   *config.Config
	kek   []byte
	mu    sync.Mutex
	role  string // master | slave
	serve string // off | ro | rw（slave 恒 off）
	epoch uint32

	store    *store.Store
	gcCancel context.CancelFunc

	sftpSrv *xsftp.Server // SSH/SFTP 服务端（serve≠off 时接受连接）

	syncCancel context.CancelFunc
	syncDone   chan struct{}
	syncAddr   string

	master *repl.Master // role==master 时的复制面端点
}

// Start 组装节点：配置初值 → 运行态覆盖（99-D016）→ 打开存储 → 起后台任务。
// 从节点配置了 master_addr 即自动开始同步（重启即恢复常态）。
func Start(ctx context.Context, cfg *config.Config, kek []byte) (*Node, error) {
	state, err := config.LoadState(cfg.Node.DataDir)
	if err != nil {
		return nil, err
	}
	role, serve, epoch := cfg.Node.Role, ServeOff, uint32(1)
	if cfg.Node.Role == "master" {
		serve = ServeRW
	}
	if state != nil { // 运行态优先
		if state.Role == "master" || state.Role == "slave" {
			role = state.Role
		}
		if state.Epoch > epoch {
			epoch = state.Epoch
		}
		if state.Serve == ServeOff || state.Serve == ServeRO || state.Serve == ServeRW {
			serve = state.Serve
		}
	}
	if role == "slave" {
		serve = ServeOff // 从节点不对外服务（docs/03 不变量）
	}

	st, err := store.Open(store.Config{
		DataDir:             cfg.Node.DataDir,
		Epoch:               epoch,
		SmallThreshold:      int64(cfg.Storage.BlobSmallThreshold),
		ChunkSize:           int64(cfg.Storage.ChunkSize),
		MaxObjectSize:       int64(cfg.Storage.MaxObjectSize),
		MultipartMaxSize:    int64(cfg.Storage.MultipartMaxSize),
		GCInterval:          cfg.Storage.GCInterval,
		GCGrace:             cfg.Storage.GCGrace,
		MultipartAbortAfter: cfg.Storage.MultipartAbortAfter,
		KEK:                 kek,
	})
	if err != nil {
		return nil, err
	}
	st.SetReadOnly(serve == ServeRO)

	n := &Node{cfg: cfg, kek: kek, role: role, serve: serve, epoch: epoch, store: st}
	if role == "master" {
		n.master = repl.NewMaster(st, cfg.Replication.Token)
	}
	if cfg.Listeners.DataSSH != "" {
		if sf, err := xsftp.NewServer(st, cfg.Node.DataDir); err == nil {
			n.sftpSrv = sf
		} else {
			slog.Warn("sftp server init failed; data_ssh disabled", "err", err)
		}
	}
	n.saveState()

	gcCtx, gcCancel := context.WithCancel(ctx)
	n.gcCancel = gcCancel
	st.StartGC(gcCtx) // 主从都跑：从节点做本地副本回收（缺失即回源拉取兜底）

	if role == "slave" && cfg.Replication.MasterAddr != "" {
		if err := n.SyncStart(""); err != nil {
			slog.Warn("slave auto-sync failed to start", "err", err)
		}
	}
	slog.Info("node started", "role", role, "serve", serve, "epoch", epoch, "dataDir", cfg.Node.DataDir)
	return n, nil
}

func (n *Node) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.syncCancel != nil {
		n.syncCancel()
		if n.syncDone != nil {
			select {
			case <-n.syncDone:
			case <-time.After(5 * time.Second):
			}
		}
	}
	if n.gcCancel != nil {
		n.gcCancel()
	}
	n.store.Close()
}

func (n *Node) Store() *store.Store { return n.store }

func (n *Node) saveState() {
	_ = config.SaveState(n.cfg.Node.DataDir, &config.State{
		Role: n.role, Epoch: n.epoch, Serve: n.serve, UpdatedAt: time.Now().Unix(),
	})
}

// ---- ReplHandler：复制面（master 服务，slave 拒绝）----

func (n *Node) ReplHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		m := n.master
		n.mu.Unlock()
		if m == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"node is not master"}`))
			return
		}
		m.Handler().ServeHTTP(w, r)
	})
}

// ---- DataHandler：数据面 HTTP（/dav/ → WebDAV，其余 → S3；serve=off 全量 503）----

func (n *Node) DataHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/dav/", xwebdav.Router(n.store))
	mux.Handle("/", n.S3Handler())
	return n.serveGate(mux)
}

func (n *Node) serveGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !n.serving() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"data plane is off (serve=off)"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (n *Node) serving() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.serve != ServeOff
}

// ---- S3Handler：S3 子路由（serve=off 时全量 503，含未鉴权请求）----

func (n *Node) S3Handler() http.Handler {
	inner := s3.New(n.store)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		off := n.serve == ServeOff
		n.mu.Unlock()
		if off {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"data plane is off (serve=off)"}`))
			return
		}
		inner.ServeHTTP(w, r)
	})
}

// ---- SFTPConn：SSH 数据面入口（serve=off 时直接断开）----

func (n *Node) SFTPConn(conn net.Conn) {
	n.mu.Lock()
	off := n.serve == ServeOff
	srv := n.sftpSrv
	n.mu.Unlock()
	if off || srv == nil {
		conn.Close()
		return
	}
	go srv.ServeConn(conn)
}

// ---- serve 开关 ----

// SetServe 调整数据面模式（仅 master；slave 不对外服务，docs/03 不变量）。
func (n *Node) SetServe(mode string) error {
	if mode != ServeOff && mode != ServeRO && mode != ServeRW {
		return fmt.Errorf("%w: serve mode must be off|ro|rw", errs.ErrInvalidArgument)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != "master" {
		return fmt.Errorf("%w: slave never serves; promote first", errs.ErrInvalidArgument)
	}
	n.serve = mode
	n.store.SetReadOnly(mode == ServeRO)
	n.saveState()
	slog.Info("serve mode changed", "mode", mode)
	return nil
}

// ---- sync 开关 ----

// SyncStart 启动从节点同步循环；addrOverride 为空用配置里的 master_addr。
func (n *Node) SyncStart(addrOverride string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != "slave" {
		return fmt.Errorf("%w: master is the sync source, not a consumer", errs.ErrInvalidArgument)
	}
	if n.syncCancel != nil {
		return fmt.Errorf("%w: sync already running", errs.ErrInvalidArgument)
	}
	addr := addrOverride
	if addr == "" {
		addr = n.cfg.Replication.MasterAddr
	}
	if addr == "" {
		return fmt.Errorf("%w: master_addr not configured", errs.ErrInvalidArgument)
	}
	slave, err := repl.NewSlave(repl.SlaveConfig{
		ID: n.cfg.NodeID(), MasterURL: addr, Token: n.cfg.Replication.Token,
	}, n.store)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	n.syncCancel, n.syncDone, n.syncAddr = cancel, done, addr
	go func() {
		defer close(done)
		slave.Run(ctx)
	}()
	slog.Info("sync started", "master", addr)
	return nil
}

// SyncStop 停止同步并等待循环退出（promote 护栏依赖"确实已停"）。
func (n *Node) SyncStop() error {
	n.mu.Lock()
	cancel, done := n.syncCancel, n.syncDone
	n.syncCancel, n.syncDone, n.syncAddr = nil, nil, ""
	n.mu.Unlock()
	if cancel == nil {
		return fmt.Errorf("%w: sync not running", errs.ErrInvalidArgument)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("%w: sync loop did not stop in 10s", errs.ErrInternal)
	}
	slog.Info("sync stopped")
	return nil
}

func (n *Node) SyncRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.syncCancel != nil
}

// ---- promote（灾备 runbook A3，99-D005/D014）----

type PromoteResult struct {
	Role  string `json:"role"`
	Epoch uint32 `json:"epoch"`
	Serve string `json:"serve"`
}

// Promote 把本节点提升为主：护栏（role=slave、sync 已停）→ epoch+1 →
// serve=ro（先只读冒烟，rw 需再显式开启）→ 挂复制面。自动化只做校验与
// 执行，决策在人（docs/03）。
func (n *Node) Promote() (*PromoteResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != "slave" {
		return nil, fmt.Errorf("%w: node is already master (runbook A3 precondition: role=slave)", errs.ErrInvalidArgument)
	}
	if n.syncCancel != nil {
		return nil, fmt.Errorf("%w: stop sync first (runbook A2)", errs.ErrInvalidArgument)
	}
	maxE, err := n.store.MaxEpoch(context.Background())
	if err != nil {
		return nil, err
	}
	newEpoch := maxE + 1
	if newEpoch <= n.epoch {
		newEpoch = n.epoch + 1
	}
	n.role = "master"
	n.epoch = newEpoch
	n.serve = ServeRO
	n.store.SetEpoch(newEpoch)
	n.store.SetReadOnly(true)
	n.master = repl.NewMaster(n.store, n.cfg.Replication.Token)
	n.saveState()
	slog.Warn("node PROMOTED", "epoch", newEpoch, "serve", ServeRO)
	return &PromoteResult{Role: "master", Epoch: newEpoch, Serve: ServeRO}, nil
}

// ---- 状态 ----

type Status struct {
	Role          string `json:"role"`
	Serve         string `json:"serve"`
	Epoch         uint32 `json:"epoch"`
	LastSeq       int64  `json:"last_seq"`
	MaxEpoch      uint32 `json:"max_epoch_in_log"`
	SyncRunning   bool   `json:"sync_running"`
	SyncAddr      string `json:"sync_addr,omitempty"`
	KEKConfigured bool   `json:"kek_configured"`
}

func (n *Node) Status(ctx context.Context) (*Status, error) {
	n.mu.Lock()
	st := &Status{
		Role: n.role, Serve: n.serve, Epoch: n.epoch,
		SyncRunning: n.syncCancel != nil, SyncAddr: n.syncAddr,
		KEKConfigured: len(n.kek) > 0,
	}
	n.mu.Unlock()
	var err error
	if st.LastSeq, err = n.store.LastSeq(ctx); err != nil {
		return nil, err
	}
	if st.MaxEpoch, err = n.store.MaxEpoch(ctx); err != nil {
		return nil, err
	}
	return st, nil
}
