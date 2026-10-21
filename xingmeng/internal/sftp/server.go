package sftp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"xingmeng/internal/store"
)

// Server 是 SSH/SFTP 服务端：密码认证走凭据表（AK/SK），认证成功即把
// 该凭据绑定的桶注入连接权限，SFTP 会话以此为根目录（99-D021）。
type Server struct {
	store *store.Store
	cfg   *ssh.ServerConfig
}

// NewServer 构造 SSH 服务端；主机密钥首次启动生成并持久化于数据目录。
func NewServer(st *store.Store, dataDir string) (*Server, error) {
	signer, err := loadOrCreateHostKey(filepath.Join(dataDir, "ssh_host_ed25519"))
	if err != nil {
		return nil, err
	}
	cfg := &ssh.ServerConfig{
		MaxAuthTries: 3,
		PasswordCallback: func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			cred, err := st.LookupCred(context.Background(), meta.User())
			if err != nil ||
				subtle.ConstantTimeCompare([]byte(cred.Secret), password) != 1 {
				return nil, errors.New("sftp auth failed")
			}
			return &ssh.Permissions{Extensions: map[string]string{"bucket": cred.Bucket}}, nil
		},
	}
	cfg.AddHostKey(signer)
	return &Server{store: st, cfg: cfg}, nil
}

// ServeConn 处理一条 SSH 连接：session 通道 + sftp 子系统。
func (s *Server) ServeConn(nc net.Conn) {
	sconn, chans, reqs, err := ssh.NewServerConn(nc, s.cfg)
	if err != nil {
		nc.Close()
		return
	}
	defer sconn.Close()
	go ssh.DiscardRequests(reqs)
	bucket := sconn.Permissions.Extensions["bucket"]
	backend := NewBackend(s.store, bucket)
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		channel, chReqs, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go s.handleSession(channel, chReqs, backend)
	}
}

type subsystemPayload struct{ Name string }

func (s *Server) handleSession(channel ssh.Channel, chReqs <-chan *ssh.Request, backend *Backend) {
	defer channel.Close()
	for req := range chReqs {
		if req.Type != "subsystem" {
			req.Reply(false, nil)
			continue
		}
		var payload subsystemPayload
		if err := ssh.Unmarshal(req.Payload, &payload); err != nil || payload.Name != "sftp" {
			req.Reply(false, nil)
			continue
		}
		req.Reply(true, nil)
		rs := sftp.NewRequestServer(channel, sftp.Handlers{
			FileGet: backend, FilePut: backend, FileCmd: backend, FileList: backend,
		})
		if err := rs.Serve(); err != nil {
			slog.Debug("sftp session ended", "err", err)
		}
		return
	}
}

// loadOrCreateHostKey 加载或生成 ed25519 主机密钥（PKCS#8 PEM）。
// 仅在文件不存在时生成（S-11）：其他读取错误（权限/损坏）直接失败，绝不
// 静默换键——主机键无端变化会动摇客户端 TOFU 信任链并掩盖磁盘问题。
func loadOrCreateHostKey(path string) (ssh.Signer, error) {
	if raw, err := os.ReadFile(path); err == nil {
		return ssh.ParsePrivateKey(raw)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read host key %s: %w", path, err)
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	// O_EXCL：并发启动或路径已被占位时不覆盖既有文件
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("persist host key: %w", err)
	}
	if _, err := f.Write(pemBytes); err != nil {
		f.Close()
		return nil, fmt.Errorf("persist host key: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("persist host key: %w", err)
	}
	return ssh.NewSignerFromKey(priv)
}
