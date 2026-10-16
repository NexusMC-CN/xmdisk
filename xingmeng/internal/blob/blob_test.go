package blob

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"xingmeng/internal/errs"
)

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestPutOpenSize(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := randBytes(t, 3<<20)
	h, size, err := s.Put(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	want := sha256.Sum256(data)
	if h != hex.EncodeToString(want[:]) {
		t.Fatalf("hash mismatch")
	}
	if size != int64(len(data)) {
		t.Fatalf("size = %d", size)
	}
	sz, err := s.Size(h)
	if err != nil || sz != int64(len(data)) {
		t.Fatalf("stat: %v %d", err, sz)
	}
	f, err := s.Open(h)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch")
	}

	// 重复写同内容：去重路径
	h2, _, err := s.Put(bytes.NewReader(data))
	if err != nil || h2 != h {
		t.Fatalf("dedup put: %v %s", err, h2)
	}

	// 空内容
	he, se, err := s.Put(bytes.NewReader(nil))
	if err != nil || se != 0 || !s.Exists(he) {
		t.Fatalf("empty put: %v %d", err, se)
	}
}

func TestInvalidHash(t *testing.T) {
	s, _ := Open(t.TempDir())
	if _, err := s.Open("../etc/passwd"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if _, err := s.Open("ZZ00"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if s.Exists("") {
		t.Fatalf("empty hash should not exist")
	}
}

func TestTrashAndSweep(t *testing.T) {
	s, _ := Open(t.TempDir())
	h, _, err := s.Put(bytes.NewReader(randBytes(t, 64<<10)))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.Trash(h); err != nil {
		t.Fatalf("trash: %v", err)
	}
	if s.Exists(h) {
		t.Fatalf("blob should be out of blobs/ after trash")
	}

	// 宽限期内不清扫
	if swept, resurr, err := s.SweepTrash(1*time.Hour, func(string) bool { return false }); err != nil || swept != 0 || resurr != 0 {
		t.Fatalf("sweep in grace: %v %d %d", err, swept, resurr)
	}

	// keep=true：复活回 blobs/
	if swept, resurr, err := s.SweepTrash(0, func(string) bool { return true }); err != nil || swept != 0 || resurr != 1 {
		t.Fatalf("sweep resurrect: %v %d %d", err, swept, resurr)
	}
	if !s.Exists(h) {
		t.Fatalf("blob should be resurrected")
	}

	// keep=false：物理删除
	s.Trash(h)
	if swept, _, err := s.SweepTrash(0, func(string) bool { return false }); err != nil || swept != 1 {
		t.Fatalf("sweep delete: %v %d", err, swept)
	}
	if s.Exists(h) {
		t.Fatalf("blob should be gone")
	}
}

// TestSymlinkHardening 防跳板链接（99-D018，源自用户备份还原项目的真实事故）：
// 存储目录内被植入指向外部的符号链接时，绝不跟随读写、绝不在清扫时复活。
func TestSymlinkHardening(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := randBytes(t, 16<<10)
	h, _, err := s.Put(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	p, err := s.path(h)
	if err != nil {
		t.Fatal(err)
	}
	// 存储目录外的诱饵文件
	secret := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(secret, []byte("TOPSECRET"), 0o600); err != nil {
		t.Fatal(err)
	}

	plant := func(where string) bool {
		os.Remove(where)
		if err := os.Symlink(secret, where); err != nil {
			t.Skipf("平台不允许创建符号链接（Windows 需开发者模式），留待 Linux 容器覆盖: %v", err)
			return false
		}
		return true
	}

	// blobs 位被链接占位：读路径全部拒绝
	if !plant(p) {
		return
	}
	if s.Exists(h) {
		t.Fatalf("Exists must not follow symlink")
	}
	if _, err := s.Open(h); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("Open must reject symlink: %v", err)
	}
	if _, err := s.Size(h); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("Size must reject symlink: %v", err)
	}
	// List 过滤链接
	if got, _ := s.List(); len(got) != 0 {
		t.Fatalf("List must skip symlink: %v", got)
	}

	// 同内容重写：铲除链接占位，落真实文件（自愈）
	h2, _, err := s.Put(bytes.NewReader(data))
	if err != nil || h2 != h {
		t.Fatalf("put over planted symlink: %v %s", err, h2)
	}
	f, err := s.Open(h)
	if err != nil {
		t.Fatalf("open after heal: %v", err)
	}
	body, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(body, data) {
		t.Fatalf("healed content mismatch")
	}

	// trash 中的链接：清扫只删链接本身，不复活、不动目标
	os.Remove(p)
	if !plant(filepath.Join(s.root, "trash", h)) {
		return
	}
	swept, resurr, err := s.SweepTrash(0, func(string) bool { return true }) // keep=true 也不得复活
	if err != nil || resurr != 0 || swept != 1 {
		t.Fatalf("link must be removed not resurrected: %v swept=%d resurr=%d", err, swept, resurr)
	}
	if s.Exists(h) {
		t.Fatalf("link must not be resurrected into blobs/")
	}
	got, _ := os.ReadFile(secret)
	if string(got) != "TOPSECRET" {
		t.Fatalf("link target must be untouched")
	}
}
