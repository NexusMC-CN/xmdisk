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
	if c.Node.Role != "master" || c.Listeners.Admin != "127.0.0.1:9002" {
		t.Fatalf("defaults: %+v", c)
	}
	// slave 必须有 master_addr
	p2 := writeCfg(t, "node:\n  role: slave\n  data_dir: /tmp/x\n")
	if _, err := Load(p2); err == nil {
		t.Fatalf("slave without master_addr must fail")
	}
	// 非法角色
	p3 := writeCfg(t, "node:\n  role: replica\n  data_dir: /tmp/x\n")
	if _, err := Load(p3); err == nil {
		t.Fatalf("invalid role must fail")
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
