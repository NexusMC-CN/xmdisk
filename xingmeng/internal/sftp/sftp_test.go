package sftp

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"

	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

const (
	testAK     = "SFTPUSER1"
	testSK     = "sftp-secret-0123456789"
	testBucket = "sf-bkt"
)

func startTestServer(t *testing.T) (*Server, net.Addr, *store.Store) {
	t.Helper()
	st, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Epoch:          1,
		SmallThreshold: 1 << 20,
		ChunkSize:      4 << 20,
		GCGrace:        time.Hour,
		KEK:            []byte("sftp-test-kek"),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.CreateBucket(context.Background(), testBucket, nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := st.CreateCred(context.Background(), testAK, testSK, testBucket); err != nil {
		t.Fatalf("create cred: %v", err)
	}
	srv, err := NewServer(st, t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return srv, ln.Addr(), st
}

func dialSFTP(t *testing.T, addr net.Addr, user, pass string) *sftp.Client {
	t.Helper()
	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.Password(pass)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}
	conn, err := ssh.Dial("tcp", addr.String(), cfg)
	if err != nil {
		t.Fatalf("ssh dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	sc, err := sftp.NewClient(conn)
	if err != nil {
		t.Fatalf("sftp client: %v", err)
	}
	t.Cleanup(func() { sc.Close() })
	return sc
}

func randData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	crand.Read(b)
	return b
}

func putVia(t *testing.T, sc *sftp.Client, path string, data []byte) {
	t.Helper()
	f, err := sc.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := f.Close(); err != nil { // Close 触发提交
		t.Fatalf("close %s: %v", path, err)
	}
}

func TestSFTPRoundtrip(t *testing.T) {
	_, addr, st := startTestServer(t)
	sc := dialSFTP(t, addr, testAK, testSK)
	ctx := context.Background()

	// 小文件 + 目录 + 嵌套
	if err := sc.Mkdir("/dir"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	small := randData(t, 100<<10)
	putVia(t, sc, "/dir/small.bin", small)
	// 大文件（分块路径）
	big := randData(t, 5<<20)
	putVia(t, sc, "/dir/big.bin", big)

	// 读回（含随机偏移读，sshfs 语义）
	f, err := sc.Open("/dir/small.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got := make([]byte, len(small))
	if _, err := io.ReadFull(f, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	f.Close()
	if !bytes.Equal(got, small) {
		t.Fatalf("small roundtrip mismatch")
	}
	f, _ = sc.Open("/dir/big.bin")
	buf := make([]byte, 64*1024)
	if _, err := f.ReadAt(buf, 3<<20); err != nil { // 跨块随机读
		t.Fatalf("readat: %v", err)
	}
	f.Close()
	if !bytes.Equal(buf, big[3<<20:3<<20+len(buf)]) {
		t.Fatalf("random read mismatch")
	}

	// 存储侧可见（S3/复制面共享同一命名空间）
	oi, err := st.Stat(ctx, testBucket, "dir/big.bin")
	if err != nil || oi.Size != int64(len(big)) {
		t.Fatalf("store stat: %v %+v", err, oi)
	}
	if !st.HasBlob(oi.Blobs[0].Sha256) {
		t.Fatal("blob missing")
	}

	// 列目录
	entries, err := sc.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "dir" || !entries[0].IsDir() {
		t.Fatalf("root entries: %+v", entries)
	}
	entries, err = sc.ReadDir("/dir")
	if err != nil || len(entries) != 2 {
		t.Fatalf("dir entries: %v %+v", err, entries)
	}

	// Stat
	fi, err := sc.Stat("/dir/big.bin")
	if err != nil || fi.Size() != int64(len(big)) || fi.IsDir() {
		t.Fatalf("stat: %v %+v", err, fi)
	}

	// Rename 文件与目录
	if err := sc.Rename("/dir/small.bin", "/dir/small2.bin"); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	if err := sc.Rename("/dir", "/dir2"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	if _, err := sc.Stat("/dir2/big.bin"); err != nil {
		t.Fatalf("moved content: %v", err)
	}
	if _, err := st.Stat(ctx, testBucket, "dir/"); err == nil {
		t.Fatal("old prefix must be gone")
	}

	// Setstat（mtime 保持，rsync -a / WinSCP 依赖）
	if err := sc.Chtimes("/dir2/big.bin", time.Now(), time.Unix(1600000000, 0)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	fi, _ = sc.Stat("/dir2/big.bin")
	if fi.ModTime().Unix() != 1600000000 {
		t.Fatalf("mtime not preserved: %v", fi.ModTime())
	}

	// 删除：文件、非空目录拒绝、清空后目录可删
	if err := sc.Remove("/dir2/small2.bin"); err != nil {
		t.Fatalf("remove file: %v", err)
	}
	if err := sc.Remove("/dir2"); err == nil {
		t.Fatal("non-empty dir remove must fail")
	}
	if err := sc.Remove("/dir2/big.bin"); err != nil {
		t.Fatalf("remove big: %v", err)
	}
	if err := sc.Remove("/dir2"); err != nil {
		t.Fatalf("rmdir: %v", err)
	}
	if _, err := st.Stat(ctx, testBucket, "dir2/"); !errors.Is(err, errs.ErrKeyNotFound) && err != nil {
		t.Fatalf("dir gone: %v", err)
	}
}

func TestSFTPAuthAndErrors(t *testing.T) {
	_, addr, st := startTestServer(t)

	// 密码错误：SSH 层拒绝
	cfg := &ssh.ClientConfig{
		User: testAK, Auth: []ssh.AuthMethod{ssh.Password("wrong")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 5 * time.Second,
	}
	if c, err := ssh.Dial("tcp", addr.String(), cfg); err == nil {
		c.Close()
		t.Fatalf("wrong password must be rejected")
	}

	sc := dialSFTP(t, addr, testAK, testSK)
	// 读不存在
	if _, err := sc.Open("/nope"); err == nil {
		t.Fatalf("missing file open must fail")
	}
	// symlink 拒绝（99-D018）
	if err := sc.Symlink("/a", "/b"); err == nil {
		t.Fatalf("symlink must be rejected")
	}
	// 写到配额满的桶：OS 层权限错误（映射自 QuotaExceeded）
	ctx := context.Background()
	quota := int64(1 << 20)
	st.CreateBucket(ctx, "tiny", &quota)
	st.CreateCred(ctx, "TINYAK1", "tiny-secret-0123", "tiny")
	tiny := dialSFTP(t, addr, "TINYAK1", "tiny-secret-0123")
	f, err := tiny.Create("/big.bin")
	if err != nil {
		t.Fatalf("tiny create: %v", err)
	}
	// 32MB 写入应被流式守卫中断（99-D019）
	_, werr := f.Write(randData(t, 32<<20))
	if werr == nil {
		f.Close()
		t.Fatalf("quota-exceeding write must fail")
	}
	f.Close()
	// 桶隔离：tiny 凭据看不到别的桶内容
	if _, err := tiny.Stat("/dir"); err == nil {
		t.Fatalf("bucket isolation violated")
	}
}
