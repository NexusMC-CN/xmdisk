// Package node 是节点运行时（docs/03 角色模型）：role / serve / sync 三开关
// 状态机（99-D014），promote 护栏（99-D005），从节点开机自同步。
package node

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"xingmeng/internal/config"
	"xingmeng/internal/errs"
	"xingmeng/internal/pubread"
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
	syncAddr   string // 最近生效的同步地址（state 持久化，重启恢复，99-D023）

	replSealed string       // 运行时 repl token（base64(AES-GCM(KEK))；空 = 回落 env/YAML 初值）
	replToken  string       // 生效中的 repl token（Start 解封/SetReplToken 更新）
	replLn     net.Listener // 复制面监听：master 且生效 token 非空才存在（99-D023③）
	replSrv    *http.Server

	master *repl.Master // role==master 且 repl token 非空时的复制面端点
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
	// 角色不一致软提醒（99-D023④）：state 优先意味着改 env 无法翻转已有数据的
	// 节点角色（主降从机制上不存在，D013）；promote 后的节点重启不误降级。
	if state != nil && (state.Role == "master" || state.Role == "slave") && state.Role != cfg.Node.Role {
		slog.Warn("role mismatch: runtime state wins (env/yaml role is birth-only)",
			"state_role", state.Role, "configured_role", cfg.Node.Role)
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
		OplogRetention:      cfg.Replication.OplogRetention,
		SlaveActiveWindow:   cfg.Replication.SlaveActiveWindow,
		KEK:                 kek,
	})
	if err != nil {
		return nil, err
	}
	st.SetReadOnly(serve == ServeRO)

	n := &Node{cfg: cfg, kek: kek, role: role, serve: serve, epoch: epoch, store: st}

	// repl token 生效值（99-D023③）：运行时设置（state 封存）> env/YAML 初值。
	n.replToken = cfg.Replication.Token
	if state != nil {
		n.syncAddr = state.SyncAddr
		if state.ReplTokenSealed != "" {
			if raw, err := base64.StdEncoding.DecodeString(state.ReplTokenSealed); err != nil {
				slog.Error("runtime repl token: bad base64 in state; falling back to initial", "err", err)
			} else if tok, err := st.OpenSecret(raw); err != nil {
				slog.Error("runtime repl token: unseal failed (KEK changed?); falling back to initial", "err", err)
			} else {
				n.replSealed, n.replToken = state.ReplTokenSealed, tok
			}
		}
	}

	// 复制面：master 且 token 非空才构造端点并监听；空 token = 不监听（99-D023③）。
	if role == "master" && n.replToken != "" {
		n.master = repl.NewMaster(st, n.replToken)
		n.startReplListenerLocked()
	} else if role == "master" {
		slog.Info("replication not configured; repl face disabled (set XM_REPL_TOKEN or POST /repl/token)")
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

	// 从节点自动恢复同步：运行态地址 > 配置初值（99-D023；重启不回落旧主）。
	if role == "slave" && n.effectiveMasterAddr() != "" {
		if err := n.SyncStart(""); err != nil {
			slog.Warn("slave auto-sync failed to start", "err", err)
		}
	}
	slog.Info("node started", "role", role, "serve", serve, "epoch", epoch, "dataDir", cfg.Node.DataDir,
		"replTokenConfigured", n.replToken != "")
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
	n.stopReplListenerLocked()
	if n.gcCancel != nil {
		n.gcCancel()
	}
	n.store.Close()
}

func (n *Node) Store() *store.Store { return n.store }

func (n *Node) saveState() {
	_ = config.SaveState(n.cfg.Node.DataDir, &config.State{
		Role: n.role, Epoch: n.epoch, Serve: n.serve,
		SyncAddr: n.syncAddr, ReplTokenSealed: n.replSealed,
		UpdatedAt: time.Now().Unix(),
	})
}

// ---- ReplHandler：复制面（master 服务，slave 拒绝/未配置不可用）----

func (n *Node) ReplHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		m := n.master
		n.mu.Unlock()
		if m == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"node is not master or replication disabled (no repl token)"}`))
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

// PublicHandler 公共读面（99-D024）：独立只读端口，无鉴权只认桶公共标志；
// 与数据面同闸门（serve=off 503、ro 照常读）。
func (n *Node) PublicHandler() http.Handler {
	return n.serveGate(pubread.New(n.store))
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

// effectiveMasterAddr 同步目标生效值：运行态地址 > 配置初值（99-D023）。
func (n *Node) effectiveMasterAddr() string {
	if n.syncAddr != "" {
		return n.syncAddr
	}
	return n.cfg.Replication.MasterAddr
}

// SyncStart 启动从节点同步循环；addrOverride 为空依次用运行态地址、配置初值。
// 生效地址持久化进运行态：容灾重指后重启不回落旧主（99-D023）。
func (n *Node) SyncStart(addrOverride string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != "slave" {
		return fmt.Errorf("%w: master is the sync source, not a consumer", errs.ErrInvalidArgument)
	}
	if n.syncCancel != nil {
		return fmt.Errorf("%w: sync already running", errs.ErrInvalidArgument)
	}
	if n.replToken == "" {
		return fmt.Errorf("%w: repl token not configured (set XM_REPL_TOKEN or POST /repl/token)", errs.ErrInvalidArgument)
	}
	addr := addrOverride
	if addr == "" {
		addr = n.effectiveMasterAddr()
	}
	if addr == "" {
		return fmt.Errorf("%w: master_addr not configured", errs.ErrInvalidArgument)
	}
	slave, err := repl.NewSlave(repl.SlaveConfig{
		ID: n.cfg.NodeID(), MasterURL: addr, Token: n.replToken,
	}, n.store)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	n.syncCancel, n.syncDone, n.syncAddr = cancel, done, addr
	n.saveState() // 地址进运行态
	go func() {
		defer close(done)
		slave.Run(ctx)
	}()
	slog.Info("sync started", "master", addr)
	return nil
}

// SyncStop 停止同步并等待循环退出（promote 护栏依赖"确实已停"）。
// syncAddr 不清：运行态地址在 stop/start 循环与重启间都保持粘性（99-D023
// "不回落旧配置"；实测曾因清空导致 start 不带地址回落 env 旧主并覆盖重指
// 结果，集群实测 2026-10-23）。需换主地址时显式传 master_addr 覆盖。
func (n *Node) SyncStop() error {
	n.mu.Lock()
	cancel, done := n.syncCancel, n.syncDone
	n.syncCancel, n.syncDone = nil, nil
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
	if n.replToken != "" { // 升主即接管复制面（99-D023：token 生效值）
		n.master = repl.NewMaster(n.store, n.replToken)
		n.startReplListenerLocked()
	}
	n.saveState()
	slog.Warn("node PROMOTED", "epoch", newEpoch, "serve", ServeRO, "replTokenConfigured", n.replToken != "")
	return &PromoteResult{Role: "master", Epoch: newEpoch, Serve: ServeRO}, nil
}

// ---- repl token 运行时设置（99-D023③）----

// SetReplToken 设置/清除运行时 repl token 并持久化（KEK 封装存 node-state.json）。
// 空串 = 清除运行时设置，回落 env/YAML 初值。master 热替换复制面凭证并联动
// 监听起停；slave 的生效值供下次 SyncStart 使用（运行中的循环持旧 token，
// 需 stop/start 生效——提示由 API 层返回）。
func (n *Node) SetReplToken(token string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if token != "" {
		if len(n.kek) == 0 {
			return fmt.Errorf("%w: KEK required to seal repl token", errs.ErrInvalidArgument)
		}
		if len(token) < 8 {
			return fmt.Errorf("%w: repl token too short (min 8)", errs.ErrInvalidArgument)
		}
		enc, err := n.store.SealSecret(token)
		if err != nil {
			return err
		}
		n.replSealed, n.replToken = base64.StdEncoding.EncodeToString(enc), token
	} else {
		n.replSealed, n.replToken = "", n.cfg.Replication.Token
	}
	if n.role == "master" {
		if n.replToken != "" {
			n.master = repl.NewMaster(n.store, n.replToken)
			n.startReplListenerLocked()
		} else {
			n.master = nil
			n.stopReplListenerLocked()
		}
	}
	n.saveState()
	slog.Info("repl token updated via admin API", "runtime_set", token != "", "effective_configured", n.replToken != "")
	return nil
}

// startReplListenerLocked / stopReplListenerLocked 管理复制面监听生命周期
// （调用方持有 n.mu）。首次运行时设置 token 即动态起监听——节点里唯一
// 运行期管理的监听。
func (n *Node) startReplListenerLocked() {
	if n.replLn != nil {
		return
	}
	if n.cfg.Listeners.Repl == "" {
		slog.Warn("repl token set but listeners.repl empty; repl face disabled")
		return
	}
	ln, err := net.Listen("tcp", n.cfg.Listeners.Repl)
	if err != nil {
		slog.Error("repl listen failed; repl face unavailable until next change", "err", err)
		return
	}
	n.replLn = ln
	n.replSrv = &http.Server{Handler: n.ReplHandler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = n.replSrv.Serve(ln) }()
	slog.Info("listening", "face", "repl", "addr", ln.Addr().String())
}

func (n *Node) stopReplListenerLocked() {
	if n.replSrv != nil {
		_ = n.replSrv.Close()
	}
	if n.replLn != nil {
		slog.Info("repl listener stopped")
	}
	n.replLn, n.replSrv = nil, nil
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

	ConfiguredRole string `json:"configured_role"`       // env/YAML 初值（不一致软提醒，99-D023）
	RoleMismatch   bool   `json:"role_mismatch"`         // 运行态与初值不一致（promote 后重建容器会出现）
	ReplTokenSet   bool   `json:"repl_token_configured"` // 生效 repl token 非空
	ReplTokenRT    bool   `json:"repl_token_runtime"`    // 生效值来自运行时设置（非 env/YAML）
	ReplListening  bool   `json:"repl_listening"`        // 复制面监听中（master 且 token 非空）
	PublicEndpoint string `json:"public_endpoint,omitempty"`
}

func (n *Node) Status(ctx context.Context) (*Status, error) {
	n.mu.Lock()
	st := &Status{
		Role: n.role, Serve: n.serve, Epoch: n.epoch,
		SyncRunning: n.syncCancel != nil, SyncAddr: n.syncAddr,
		KEKConfigured: len(n.kek) > 0,

		ConfiguredRole: n.cfg.Node.Role,
		RoleMismatch:   n.cfg.Node.Role == "master" || n.cfg.Node.Role == "slave",
		ReplTokenSet:   n.replToken != "",
		ReplTokenRT:    n.replSealed != "",
		ReplListening:  n.replLn != nil,
		PublicEndpoint: n.cfg.PublicEndpoint,
	}
	st.RoleMismatch = st.RoleMismatch && st.ConfiguredRole != st.Role
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
