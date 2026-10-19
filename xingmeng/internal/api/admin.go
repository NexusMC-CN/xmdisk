// Package api 是 HTTP 路由组挂载点（docs/04 监听面；用户路由习惯：
// main 挂路由组，路由组挂鉴权+业务）。admin 面鉴权为双 token（99-D023⑤），
// admin token 必填（空 = 路由整体 503，main 层拒启兜底）。
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"xingmeng/internal/errs"
	"xingmeng/internal/node"
	"xingmeng/internal/webui"
)

// RouterConfig admin 面路由配置。
type RouterConfig struct {
	AdminToken string // 必填；缺失 = 路由整体 503（99-D023②）
	WebUI      bool   // 是否挂内嵌管理页（XM_WEBUI，默认开）
}

// AdminRouter 返回 admin 面路由（docs/04 admin API 一节）。
func AdminRouter(n *node.Node, rc RouterConfig) http.Handler {
	if rc.AdminToken == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "admin token not configured; set XM_ADMIN_TOKEN (required, 99-D023)"})
		})
	}
	sess := newSessionStore()
	mux := http.NewServeMux()
	// 会话面（仅 admin token 可签发/全清；登出为会话自吊销）
	mux.HandleFunc("POST /auth/session", adminAuth(rc.AdminToken, handleIssueSession(sess)))
	mux.HandleFunc("DELETE /auth/session", handleLogout(sess))
	mux.HandleFunc("DELETE /auth/sessions", adminAuth(rc.AdminToken, handleRevokeAll(sess)))
	mux.HandleFunc("GET /status", handleStatus(n))
	mux.HandleFunc("GET /health", handleHealth(n))
	mux.HandleFunc("GET /metrics", handleMetrics(n))
	mux.HandleFunc("POST /serve", handleServe(n))
	mux.HandleFunc("POST /sync", handleSync(n))
	mux.HandleFunc("POST /promote", handlePromote(n))
	mux.HandleFunc("POST /snapshot", masterOnly(n, handleSnapshot(n)))
	mux.HandleFunc("POST /gc", handleGC(n))
	mux.HandleFunc("POST /verify", handleVerify(n))
	mux.HandleFunc("GET /buckets", handleListBuckets(n))
	mux.HandleFunc("POST /buckets", handleCreateBucket(n))
	mux.HandleFunc("DELETE /buckets/{name}", handleDeleteBucket(n))
	mux.HandleFunc("PATCH /buckets/{name}", handlePatchBucket(n))
	mux.HandleFunc("GET /creds", handleListCreds(n))
	mux.HandleFunc("POST /creds", handleCreateCred(n))
	mux.HandleFunc("POST /creds/{ak}/rotate", handleRotateCred(n))
	mux.HandleFunc("DELETE /creds/{ak}", handleDeleteCred(n))
	// 集群配置（99-D023③：token 运行时设置，地址经 /sync）
	mux.HandleFunc("POST /repl/token", handleSetReplToken(n))
	mux.HandleFunc("DELETE /repl/token", handleSetReplToken(n))

	// 管理页在鉴权之外（登录页必须先可见才能换会话；静态资源无秘密，99-D023⑥），
	// 其余全部走双 token 鉴权。
	root := http.NewServeMux()
	if rc.WebUI {
		root.Handle("GET /{$}", webui.Handler())
	}
	root.Handle("/", auth(rc.AdminToken, sess, mux))
	return root
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return fmt.Errorf("%w: %v", errs.ErrInvalidArgument, err)
	}
	return nil
}

// fail：可预期的业务错误（sentinel 族）统一 400，其余 500。
func fail(w http.ResponseWriter, err error) {
	status := 500
	for _, s := range []error{
		errs.ErrInvalidArgument, errs.ErrKeyNotFound, errs.ErrBucketNotFound,
		errs.ErrBucketExists, errs.ErrQuotaExceeded, errs.ErrReadOnly, errs.ErrDirNotEmpty,
	} {
		if errors.Is(err, s) {
			status = 400
			break
		}
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func masterOnly(n *node.Node, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if st, _ := n.Status(r.Context()); st == nil || st.Role != "master" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "node is not master"})
			return
		}
		next(w, r)
	}
}

// ---- 状态 ----

func handleStatus(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := n.Status(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		buckets, err := n.Store().ListBucketStats(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"node": st, "buckets": buckets})
	}
}

func handleHealth(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := n.Status(r.Context())
		if err != nil {
			writeJSON(w, 503, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "role": st.Role, "serve": st.Serve, "kek_configured": st.KEKConfigured})
	}
}

// handleMetrics 输出 Prometheus 文本格式（gauge 集；scrape 即时计算，无缓存）。
func handleMetrics(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := n.Status(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		roleGauge := 0
		if st.Role == "master" {
			roleGauge = 1
		}
		serveGauge := map[string]int{"off": 0, "ro": 1, "rw": 2}[st.Serve]
		out := fmt.Sprintf(`# HELP xm_node_role 1=master 0=slave
# TYPE xm_node_role gauge
xm_node_role %d
# HELP xm_node_serve 0=off 1=ro 2=rw
# TYPE xm_node_serve gauge
xm_node_serve %d
# HELP xm_node_epoch Current op generation epoch
# TYPE xm_node_epoch gauge
xm_node_epoch %d
# HELP xm_oplog_last_seq Last replicated oplog seq
# TYPE xm_oplog_last_seq gauge
xm_oplog_last_seq %d
# HELP xm_sync_running 1 if slave sync loop running
# TYPE xm_sync_running gauge
xm_sync_running %d
# HELP xm_repl_listening 1 if repl face listening (master with repl token)
# TYPE xm_repl_listening gauge
xm_repl_listening %d
`, roleGauge, serveGauge, st.Epoch, st.LastSeq, boolToInt(st.SyncRunning), boolToInt(st.ReplListening))
		buckets, err := n.Store().ListBucketStats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for _, b := range buckets {
			out += fmt.Sprintf("xm_bucket_used_bytes{bucket=%q} %d\nxm_bucket_objects{bucket=%q} %d\nxm_bucket_pending_bytes{bucket=%q} %d\nxm_bucket_public{bucket=%q} %d\n",
				b.Name, b.Used, b.Name, b.Objects, b.Name, b.Pending, b.Name, boolToInt(b.Public))
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Write([]byte(out))
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---- 开关与灾备 ----

func handleServe(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Mode string `json:"mode"`
		}
		if err := readJSON(r, &req); err != nil {
			fail(w, err)
			return
		}
		if err := n.SetServe(req.Mode); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"serve": req.Mode})
	}
}

func handleSync(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Action     string `json:"action"` // start | stop
			MasterAddr string `json:"master_addr"`
		}
		if err := readJSON(r, &req); err != nil {
			fail(w, err)
			return
		}
		var err error
		switch req.Action {
		case "start":
			err = n.SyncStart(req.MasterAddr)
		case "stop":
			err = n.SyncStop()
		default:
			err = fmt.Errorf("%w: action must be start|stop", errs.ErrInvalidArgument)
		}
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"sync": req.Action})
	}
}

func handlePromote(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := n.Promote()
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, res)
	}
}

func handleSnapshot(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path, seq, err := n.Store().Snapshot(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"path": path, "seq": seq})
	}
}

func handleGC(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := n.Store().RunGCOnce(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, res)
	}
}

func handleVerify(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Limit int `json:"limit"`
		}
		if r.ContentLength > 0 {
			if err := readJSON(r, &req); err != nil {
				fail(w, err)
				return
			}
		}
		res, err := n.Store().VerifySample(r.Context(), req.Limit)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, res)
	}
}

// ---- 桶 ----

func handleListBuckets(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := n.Store().ListBucketStats(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, stats)
	}
}

func handleCreateBucket(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name   string `json:"name"`
			Quota  *int64 `json:"quota"`
			Public *bool  `json:"public"`
		}
		if err := readJSON(r, &req); err != nil {
			fail(w, err)
			return
		}
		public := req.Public != nil && *req.Public
		if err := n.Store().CreateBucket(r.Context(), req.Name, req.Quota, public); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"name": req.Name, "quota": req.Quota, "public": public})
	}
}

func handleDeleteBucket(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := n.Store().DeleteBucket(r.Context(), r.PathValue("name")); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// handlePatchBucket 桶配置更新（配额/公共读）。字段**存在性**区分语义：
// 缺席 = 不动；quota 显式 null = 解除限制；public 为布尔。两者皆缺席 → 400。
func handlePatchBucket(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var raw map[string]json.RawMessage
		if err := readJSON(r, &raw); err != nil {
			fail(w, err)
			return
		}
		if _, ok := raw["quota"]; !ok {
			if _, ok := raw["public"]; !ok {
				fail(w, fmt.Errorf("%w: nothing to update (quota or public required)", errs.ErrInvalidArgument))
				return
			}
		}
		if rq, ok := raw["quota"]; ok {
			var quota *int64
			if string(rq) != "null" {
				var v int64
				if err := json.Unmarshal(rq, &v); err != nil || v <= 0 {
					fail(w, fmt.Errorf("%w: quota must be a positive integer or null", errs.ErrInvalidArgument))
					return
				}
				quota = &v
			}
			if err := n.Store().SetQuota(r.Context(), name, quota); err != nil {
				fail(w, err)
				return
			}
		}
		if rp, ok := raw["public"]; ok {
			var public bool
			if err := json.Unmarshal(rp, &public); err != nil {
				fail(w, fmt.Errorf("%w: public must be a boolean", errs.ErrInvalidArgument))
				return
			}
			if err := n.Store().SetBucketPublic(r.Context(), name, public); err != nil {
				fail(w, err)
				return
			}
		}
		st, err := n.Store().BucketStat(r.Context(), name)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, st)
	}
}

// ---- 凭据 ----

func handleListCreds(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		creds, err := n.Store().ListCreds(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, creds)
	}
}

// handleCreateCred 创建或覆盖凭据；secret 省略时服务端生成并在响应中
// 返回一次（批量重置 KEK 丢失场景的原子单位，docs/02）。
func handleCreateCred(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AccessKey string `json:"access_key"`
			Secret    string `json:"secret"`
			Bucket    string `json:"bucket"`
		}
		if err := readJSON(r, &req); err != nil {
			fail(w, err)
			return
		}
		generated := false
		if req.Secret == "" {
			raw := make([]byte, 24)
			rand.Read(raw)
			req.Secret = hex.EncodeToString(raw)
			generated = true
		}
		if err := n.Store().CreateCred(r.Context(), req.AccessKey, req.Secret, req.Bucket); err != nil {
			fail(w, err)
			return
		}
		resp := map[string]any{"access_key": req.AccessKey, "bucket": req.Bucket}
		if generated {
			resp["secret"] = req.Secret // 仅此一次返回
		}
		writeJSON(w, 200, resp)
	}
}

func handleRotateCred(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ak := r.PathValue("ak")
		creds, err := n.Store().ListCreds(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		var bucket string
		for _, c := range creds {
			if c.AccessKey == ak {
				bucket = c.Bucket
			}
		}
		if bucket == "" {
			fail(w, fmt.Errorf("%w: access key", errs.ErrKeyNotFound))
			return
		}
		raw := make([]byte, 24)
		rand.Read(raw)
		secret := hex.EncodeToString(raw)
		if err := n.Store().CreateCred(r.Context(), ak, secret, bucket); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"access_key": ak, "secret": secret}) // 仅此一次返回
	}
}

func handleDeleteCred(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := n.Store().DeleteCred(r.Context(), r.PathValue("ak")); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ---- 集群配置（99-D023③）----

// handleSetReplToken POST 设置运行时 repl token；DELETE 清除（回落 env/YAML 初值）。
// 运行中的同步循环持旧 token，需 stop/start 生效——响应带提示。
func handleSetReplToken(n *node.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := ""
		if r.Method == http.MethodPost {
			var req struct {
				Token string `json:"token"`
			}
			if err := readJSON(r, &req); err != nil {
				fail(w, err)
				return
			}
			token = req.Token
		}
		if err := n.SetReplToken(token); err != nil {
			fail(w, err)
			return
		}
		resp := map[string]any{"ok": true}
		if n.SyncRunning() {
			resp["hint"] = "sync is running; stop/start sync to apply the new token"
		}
		writeJSON(w, 200, resp)
	}
}
