package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "xmd.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadFriendlyUnits(t *testing.T) {
	p := writeCfg(t, `
node:
  role: slave
  data_dir: /var/lib/xingmeng
replication:
  master_addr: http://10.0.0.2:9003
  token: tok
storage:
  blob_small_threshold: 4MB
  chunk_size: 8MiB
  gc_grace: 48h
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Storage.BlobSmallThreshold != 4<<20 || c.Storage.ChunkSize != 8<<20 {
		t.Fatalf("size parse: %d %d", c.Storage.BlobSmallThreshold, c.Storage.ChunkSize)
	}
	if c.Storage.GCGrace != 48*time.Hour {
		t.Fatalf("duration parse: %v", c.Storage.GCGrace)
	}
	if c.Node.Role != "slave" || c.Replication.MasterAddr == "" {
		t.Fatalf("fields: %+v", c)
	}
}

func TestLoadDefaultsAndValidation(t *testing.T) {
	p := writeCfg(t, "node:\n  data_dir: /tmp/x\n")
	c, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Node.Role != "master" || c.Listeners.Admin != "0.0.0.0:9002" {
		t.Fatalf("defaults: %+v", c)
	}
	// slave 无 master_addr 允许启动（99-D023：空转安全，地址可后经 admin API 设置）
	p2 := writeCfg(t, "node:\n  role: slave\n  data_dir: /tmp/x\n")
	if _, err := Load(p2); err != nil {
		t.Fatalf("slave without master_addr must boot: %v", err)
	}
	// 非法角色
	p3 := writeCfg(t, "node:\n  role: replica\n  data_dir: /tmp/x\n")
	if _, err := Load(p3); err == nil {
		t.Fatalf("invalid role must fail")
	}
}

func TestApplyEnv(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	c.Node.Role = "master"
	c.Replication.MasterAddr = "http://yaml:9003"

	t.Setenv("XM_ROLE", "slave")
	t.Setenv("XM_DATA_DIR", "/var/lib/xmd")
	t.Setenv("XM_ADMIN_TOKEN", "adm-tok")
	t.Setenv("XM_REPL_TOKEN", "repl-tok")
	t.Setenv("XM_MASTER_ADDR", "http://env:9003")
	t.Setenv("XM_PUBLIC_ENDPOINT", "http://storage.example.com")
	t.Setenv("XM_WEBUI", "off")

	applied, err := ApplyEnv(c)
	if err != nil {
		t.Fatalf("apply env: %v", err)
	}
	if len(applied) != 7 {
		t.Fatalf("applied keys: %v", applied)
	}
	if c.Node.Role != "slave" || c.Node.DataDir != "/var/lib/xmd" || c.AdminToken != "adm-tok" ||
		c.Replication.Token != "repl-tok" || c.Replication.MasterAddr != "http://env:9003" ||
		c.PublicEndpoint != "http://storage.example.com" || c.WebUIEnabled() {
		t.Fatalf("env overlay result: %+v webui=%v", c, c.WebUIEnabled())
	}

	t.Setenv("XM_ROLE", "replica")
	if _, err := ApplyEnv(&Config{}); err == nil {
		t.Fatalf("invalid XM_ROLE must fail")
	}
	t.Setenv("XM_ROLE", "")
	t.Setenv("XM_WEBUI", "maybe")
	if _, err := ApplyEnv(&Config{}); err == nil {
		t.Fatalf("invalid XM_WEBUI must fail")
	}
}

func TestStateRoundtrip(t *testing.T) {
	dir := t.TempDir()
	if st, err := LoadState(dir); err != nil || st != nil {
		t.Fatalf("missing state: %v %v", st, err)
	}
	if err := SaveState(dir, &State{Role: "master", Epoch: 7, Serve: "ro"}); err != nil {
		t.Fatal(err)
	}
	st, err := LoadState(dir)
	if err != nil || st.Role != "master" || st.Epoch != 7 || st.Serve != "ro" {
		t.Fatalf("roundtrip: %v %+v", err, st)
	}
}
