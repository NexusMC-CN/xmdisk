package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	adminSrv := httptest.NewServer(AdminRouter(master, ""))
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
	slaveAdmin := httptest.NewServer(AdminRouter(slave, ""))
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
}
