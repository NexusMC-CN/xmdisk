package blob

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
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
