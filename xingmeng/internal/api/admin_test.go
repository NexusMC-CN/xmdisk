package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"xingmeng/internal/config"
	"xingmeng/internal/errs"
	"xingmeng/internal/node"
	"xingmeng/internal/store"
)

// admTok 是测试用 admin token（99-D023②：admin 面必鉴权）。
const admTok = "adm-tok-0123456789"

func testConfig(t *testing.T, role, dataDir, masterAddr string) *config.Config {
	t.Helper()
	c := &config.Config{}
	c.Node.Role = role
	c.Node.DataDir = dataDir
	c.Node.ID = "node-" + role
	c.Listeners.DataHTTP = "127.0.0.1:0"
	c.Listeners.Admin = "127.0.0.1:0"
	c.Listeners.Repl = "127.0.0.1:0"
	c.Storage.BlobSmallThreshold = 1 << 20
	c.Storage.ChunkSize = 4 << 20
	c.Storage.GCInterval = time.Hour
	c.Storage.GCGrace = time.Hour
	c.Replication.MasterAddr = masterAddr
	c.Replication.Token = "repl-tok-0123456789" // 99-D023③：空 token = 不监听复制面
	return c
}

func adminCall(t *testing.T, base, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, base+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Xm-Token", admTok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// waitFor 轮询直到条件成立或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// TestFailoverRunbook 灾备 runbook 的机制级回归（docs/03 场景 A 核心）：
// 从节点自同步 → 护栏拒绝 → 停同步 → promote（epoch+1、serve=ro）→
// 显式开 rw → 新写入携带新 epoch → 旧 epoch 被 fence → 重启保持运行态。
func TestFailoverRunbook(t *testing.T) {
	ctx := context.Background()
	kek := []byte("runbook-kek")

	// 主节点 + admin/repl 监听
	master, err := node.Start(ctx, testConfig(t, "master", t.TempDir()+"/m", ""), kek)
	if err != nil {
		t.Fatalf("start master: %v", err)
	}
	defer master.Close()
	replSrv := httptest.NewServer(master.ReplHandler())
	defer replSrv.Close()
	adminSrv := httptest.NewServer(AdminRouter(master, RouterConfig{AdminToken: admTok}))
	defer adminSrv.Close()

	// 经 admin API 造数据（桶 + 凭据 + 对象）
	if code, out := adminCall(t, adminSrv.URL, "POST", "/buckets", map[string]any{"name": "bkt"}); code != 200 {
		t.Fatalf("create bucket: %d %v", code, out)
	}
	if code, out := adminCall(t, adminSrv.URL, "POST", "/creds", map[string]any{
		"access_key": "OPAK01", "secret": "op-secret-0123456789", "bucket": "bkt",
	}); code != 200 {
		t.Fatalf("create cred: %d %v", code, out)
	}
	for _, k := range []string{"a", "b"} {
		if _, err := master.Store().Put(ctx, "bkt", k, strings.NewReader("data-"+k), store.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// 从节点：开机自同步
	slave, err := node.Start(ctx, testConfig(t, "slave", t.TempDir()+"/s", replSrv.URL), kek)
	if err != nil {
		t.Fatalf("start slave: %v", err)
	}
	defer slave.Close()
	slaveAdmin := httptest.NewServer(AdminRouter(slave, RouterConfig{AdminToken: admTok}))
	defer slaveAdmin.Close()

	mLast, _ := master.Store().LastSeq(ctx)
	waitFor(t, 10*time.Second, func() bool {
		l, _ := slave.Store().LastSeq(ctx)
		return l >= mLast
	}, "slave catch up")
	if _, err := slave.Store().Stat(ctx, "bkt", "a"); err != nil {
		t.Fatalf("slave should have bkt/a: %v", err)
	}

	// 护栏：同步未停禁止 promote；slave 禁止 serve
	if code, out := adminCall(t, slaveAdmin.URL, "POST", "/promote", map[string]any{}); code != 400 || !strings.Contains(out["error"].(string), "stop sync") {
		t.Fatalf("promote with sync running should be rejected: %d %v", code, out)
	}
	if code, _ := adminCall(t, slaveAdmin.URL, "POST", "/serve", map[string]any{"mode": "rw"}); code != 400 {
		t.Fatalf("slave serve should be rejected")
	}
	// 主节点禁止作为同步消费者
	if code, _ := adminCall(t, adminSrv.URL, "POST", "/sync", map[string]any{"action": "start"}); code != 400 {
		t.Fatalf("master sync start should be rejected")
	}

	// A2 停同步 → A3 promote
	if code, out := adminCall(t, slaveAdmin.URL, "POST", "/sync", map[string]any{"action": "stop"}); code != 200 {
		t.Fatalf("sync stop: %d %v", code, out)
	}
	code, out := adminCall(t, slaveAdmin.URL, "POST", "/promote", map[string]any{})
	if code != 200 {
		t.Fatalf("promote: %d %v", code, out)
	}
	if out["role"] != "master" || out["epoch"].(float64) != 2 || out["serve"] != "ro" {
		t.Fatalf("promote result: %v", out)
	}
	// ro 态拒绝写入
	if _, err := slave.Store().Put(ctx, "bkt", "c", strings.NewReader("x"), store.PutOptions{}); !errors.Is(err, errs.ErrReadOnly) {
		t.Fatalf("serve=ro must reject writes: %v", err)
	}
	// A6 显式开 rw，新写入携带新 epoch
	if code, out := adminCall(t, slaveAdmin.URL, "POST", "/serve", map[string]any{"mode": "rw"}); code != 200 {
		t.Fatalf("serve rw: %d %v", code, out)
	}
	if _, err := slave.Store().Put(ctx, "bkt", "c", strings.NewReader("new-era"), store.PutOptions{}); err != nil {
		t.Fatalf("put after promote: %v", err)
	}
	ops, _ := slave.Store().Ops(ctx, 0, 1000)
	lastOp := ops[len(ops)-1]
	if lastOp.Epoch != 2 {
		t.Fatalf("new ops must carry epoch 2, got %d", lastOp.Epoch)
	}

	// 旧主复活 fencing：epoch 1 的 op 被拒（99-D005）
	stale := lastOp
	stale.Epoch = 1
	stale.Seq = lastOp.Seq + 1
	if err := slave.Store().ApplyOp(ctx, stale); !errors.Is(err, errs.ErrStaleEpoch) {
		t.Fatalf("stale epoch must be fenced: %v", err)
	}

	// 提升后的节点挂复制面（新从可接入；本轮不做第二轮同步，机制已在 repl 测试覆盖）
	newMasterRepl := httptest.NewServer(slave.ReplHandler())
	defer newMasterRepl.Close()
}

// TestStateSurvivesRestart 运行态跨重启（D016：node-state.json 优先于配置初值）。
func TestStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir() + "/n"
	kek := []byte("kek")

	n1, err := node.Start(ctx, testConfig(t, "master", dir, ""), kek)
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.SetServe("ro"); err != nil {
		t.Fatal(err)
	}
	n1.Close()

	n2, err := node.Start(ctx, testConfig(t, "master", dir, ""), kek)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	st, _ := n2.Status(ctx)
	if st.Serve != "ro" {
		t.Fatalf("serve must persist across restart, got %s", st.Serve)
	}
	// 配置里 role=slave 但运行态 master → 运行态优先（99-D016）
	n2.Close()
	n3, err := node.Start(ctx, testConfig(t, "slave", dir, "http://unused:9003"), kek)
	if err != nil {
		t.Fatal(err)
	}
	defer n3.Close()
	st3, _ := n3.Status(ctx)
	if st3.Role != "master" {
		t.Fatalf("state role must win over config: %+v", st3)
	}
	// 软提醒字段（99-D023④）：初值 slave / 运行态 master → 不一致可见
	if st3.ConfiguredRole != "slave" || !st3.RoleMismatch {
		t.Fatalf("role mismatch fields: %+v", st3)
	}
}

// call 是带自定义头的原始调用（会话/admin 两类凭证的测试用）。
func call(t *testing.T, base, method, path string, body any, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, base+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestSessionFlow 双 token 会话全流程（99-D023⑤）。
func TestSessionFlow(t *testing.T) {
	ctx := context.Background()
	n, err := node.Start(ctx, testConfig(t, "master", t.TempDir()+"/m", ""), []byte("sess-kek"))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	srv := httptest.NewServer(AdminRouter(n, RouterConfig{AdminToken: admTok}))
	defer srv.Close()

	// 无凭证 / 错 admin token → 401
	if code, _ := call(t, srv.URL, "GET", "/status", nil, nil); code != 401 {
		t.Fatalf("no cred must 401, got %d", code)
	}
	if code, _ := call(t, srv.URL, "GET", "/status", nil, map[string]string{"X-Xm-Token": "wrong"}); code != 401 {
		t.Fatalf("bad admin token must 401, got %d", code)
	}
	// 签发校验：错 admin token 401；ttl 越界 400
	if code, _ := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 8}, map[string]string{"X-Xm-Token": "wrong"}); code != 401 {
		t.Fatalf("issue with bad token must 401, got %d", code)
	}
	if code, _ := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 0}, map[string]string{"X-Xm-Token": admTok}); code != 400 {
		t.Fatalf("ttl 0 must 400, got %d", code)
	}
	if code, _ := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 25}, map[string]string{"X-Xm-Token": admTok}); code != 400 {
		t.Fatalf("ttl 25 must 400, got %d", code)
	}
	// 正常签发 → 会话 token 可用
	code, out := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 1}, map[string]string{"X-Xm-Token": admTok})
	if code != 200 || out["token"] == "" || out["expires_at"] == nil {
		t.Fatalf("issue: %d %v", code, out)
	}
	sess := out["token"].(string)
	if code, _ := call(t, srv.URL, "GET", "/status", nil, map[string]string{"X-Xm-Session": sess}); code != 200 {
		t.Fatalf("session token must work, got %d", code)
	}
	// 会话 token 不得签发/全清（仅 admin token）
	if code, _ := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 1}, map[string]string{"X-Xm-Session": sess}); code != 401 {
		t.Fatalf("issue with session must 401, got %d", code)
	}
	// 登出（自吊销）→ 会话失效
	if code, _ := call(t, srv.URL, "DELETE", "/auth/session", nil, map[string]string{"X-Xm-Session": sess}); code != 204 {
		t.Fatalf("logout must 204")
	}
	if code, _ := call(t, srv.URL, "GET", "/status", nil, map[string]string{"X-Xm-Session": sess}); code != 401 {
		t.Fatalf("logged-out session must 401, got %d", code)
	}
	// admin 全清：再签两个会话 → DELETE /auth/sessions → 全部失效
	var toks []string
	for i := 0; i < 2; i++ {
		_, o := call(t, srv.URL, "POST", "/auth/session", map[string]any{"ttl_hours": 2}, map[string]string{"X-Xm-Token": admTok})
		toks = append(toks, o["token"].(string))
	}
	if code, o := call(t, srv.URL, "DELETE", "/auth/sessions", nil, map[string]string{"X-Xm-Token": admTok}); code != 200 || o["revoked"].(float64) < 2 {
		t.Fatalf("revoke all: %d %v", code, o)
	}
	for _, tok := range toks {
		if code, _ := call(t, srv.URL, "GET", "/health", nil, map[string]string{"X-Xm-Session": tok}); code != 401 {
			t.Fatalf("revoked session must 401, got %d", code)
		}
	}
}

// TestReplTokenLifecycle repl token 运行时设置/清除/持久化与复制面联动（99-D023③）。
func TestReplTokenLifecycle(t *testing.T) {
	ctx := context.Background()
	kek := []byte("repl-kek")
	dir := t.TempDir() + "/m"
	cfg := testConfig(t, "master", dir, "")
	cfg.Replication.Token = "" // 出生无 token：不监听复制面

	n, err := node.Start(ctx, cfg, kek)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(AdminRouter(n, RouterConfig{AdminToken: admTok}))
	defer srv.Close()

	st, _ := n.Status(ctx)
	if st.ReplTokenSet || st.ReplListening || st.ReplTokenRT {
		t.Fatalf("tokenless master must not serve repl: %+v", st)
	}
	// repl 面未配置：ReplHandler 503
	rh := httptest.NewServer(n.ReplHandler())
	defer rh.Close()
	if code, _ := call(t, rh.URL, "GET", "/oplog", nil, nil); code != 503 {
		t.Fatalf("repl without token must 503, got %d", code)
	}

	// 运行时设置 → 监听起、新 token 鉴权生效
	if code, out := adminCall(t, srv.URL, "POST", "/repl/token", map[string]any{"token": "runtime-tok-0123456789"}); code != 200 || out["ok"] != true {
		t.Fatalf("set repl token: %d %v", code, out)
	}
	st, _ = n.Status(ctx)
	if !st.ReplTokenSet || !st.ReplTokenRT || !st.ReplListening {
		t.Fatalf("runtime token must enable repl: %+v", st)
	}
	if code, _ := call(t, rh.URL, "GET", "/oplog", nil, nil); code != 401 {
		t.Fatalf("repl must require token now, got %d", code)
	}
	if code, _ := call(t, rh.URL, "GET", "/oplog", nil, map[string]string{"X-Xm-Token": "runtime-tok-0123456789"}); code != 200 {
		t.Fatalf("repl with runtime token must 200, got %d", code)
	}

	// 重启：运行时 token 持久化（KEK 封装存 state）且自动恢复监听
	n.Close()
	n2, err := node.Start(ctx, cfg, kek)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	st2, _ := n2.Status(ctx)
	if !st2.ReplTokenSet || !st2.ReplTokenRT || !st2.ReplListening {
		t.Fatalf("runtime repl token must survive restart: %+v", st2)
	}

	// 清除 → 回落初值（空）→ 停止监听
	n2.Close()
	n3, err := node.Start(ctx, cfg, kek)
	if err != nil {
		t.Fatal(err)
	}
	defer n3.Close()
	srv3 := httptest.NewServer(AdminRouter(n3, RouterConfig{AdminToken: admTok}))
	defer srv3.Close()
	if code, _ := adminCall(t, srv3.URL, "DELETE", "/repl/token", nil); code != 200 {
		t.Fatalf("clear repl token failed")
	}
	st3, _ := n3.Status(ctx)
	if st3.ReplTokenSet || st3.ReplTokenRT || st3.ReplListening {
		t.Fatalf("clear must fall back to empty initial: %+v", st3)
	}
}

// TestSyncAddrRuntimePersist 同步地址运行态持久化：API 指定的主地址跨重启保留，
// 不回落配置初值（99-D023）。
func TestSyncAddrRuntimePersist(t *testing.T) {
	ctx := context.Background()
	kek := []byte("sync-kek")
	master, err := node.Start(ctx, testConfig(t, "master", t.TempDir()+"/m", ""), kek)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	replSrv := httptest.NewServer(master.ReplHandler())
	defer replSrv.Close()

	sc := testConfig(t, "slave", t.TempDir()+"/s", "")
	sc.Replication.MasterAddr = "" // 出生不带地址：空转待指
	s1, err := node.Start(ctx, sc, kek)
	if err != nil {
		t.Fatal(err)
	}
	st1, _ := s1.Status(ctx)
	if st1.SyncRunning {
		t.Fatalf("slave without addr must not auto-sync")
	}
	s1.Close()

	// 经 API 指定地址启动（走 admin /sync）
	s2, err := node.Start(ctx, sc, kek)
	if err != nil {
		t.Fatal(err)
	}
	admin2 := httptest.NewServer(AdminRouter(s2, RouterConfig{AdminToken: admTok}))
	defer admin2.Close()
	if code, out := adminCall(t, admin2.URL, "POST", "/sync", map[string]any{"action": "start", "master_addr": replSrv.URL}); code != 200 {
		t.Fatalf("sync start via api: %d %v", code, out)
	}
	s2.Close()

	// 重启（无地址参数）→ 自动恢复到 API 指定的地址
	s3, err := node.Start(ctx, sc, kek)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	st3, _ := s3.Status(ctx)
	if !st3.SyncRunning || st3.SyncAddr != replSrv.URL {
		t.Fatalf("sync addr must persist and auto-resume: %+v", st3)
	}
}

// TestWebUIMount 内嵌管理页挂载开关（99-D023⑥）。
func TestWebUIMount(t *testing.T) {
	ctx := context.Background()
	n, err := node.Start(ctx, testConfig(t, "master", t.TempDir()+"/m", ""), []byte("ui-kek"))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	hdr := map[string]string{"X-Xm-Token": admTok}

	on := httptest.NewServer(AdminRouter(n, RouterConfig{AdminToken: admTok, WebUI: true}))
	defer on.Close()
	if code, _ := call(t, on.URL, "GET", "/", nil, hdr); code != 200 {
		t.Fatalf("webui must be served, got %d", code)
	}
	resp, _ := http.Get(on.URL + "/") // 无凭证也可见登录页（静态资源无秘密）
	if resp.StatusCode != 200 {
		t.Fatalf("login page must be reachable without auth, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "星梦存储") {
		t.Fatalf("login page content unexpected")
	}

	off := httptest.NewServer(AdminRouter(n, RouterConfig{AdminToken: admTok, WebUI: false}))
	defer off.Close()
	if code, _ := call(t, off.URL, "GET", "/", nil, hdr); code != 404 {
		t.Fatalf("webui off must 404, got %d", code)
	}
	// 空凭证路由整体 503（99-D023②）
	bad := httptest.NewServer(AdminRouter(n, RouterConfig{}))
	defer bad.Close()
	if code, _ := call(t, bad.URL, "GET", "/health", nil, map[string]string{"X-Xm-Token": "x"}); code != 503 {
		t.Fatalf("empty admin token router must 503, got %d", code)
	}
}
