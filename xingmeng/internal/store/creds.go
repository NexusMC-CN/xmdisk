package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"regexp"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// 凭据面（99-D009）：SigV4 要求持明文 secret 重算 HMAC，故 secret 以
// AES-GCM 可逆加密落库，KEK 由配置层从环境变量注入（开发期 .env）。
// oplog 复制的是密文——KEK 是部署级共享（所有节点同值）。

type credPutPayload struct {
	AccessKey string `json:"access_key"`
	SecretEnc string `json:"secret_enc"` // base64(AES-GCM 密文)
	Bucket    string `json:"bucket"`
}

type credDeletePayload struct {
	AccessKey string `json:"access_key"`
}

// Cred 是解密后的凭据视图（仅鉴权路径内部使用）。
type Cred struct {
	AccessKey string
	Secret    string
	Bucket    string
}

// CredInfo 是不暴露明文的列表视图（admin 用）。
type CredInfo struct {
	AccessKey string
	Bucket    string
	Enabled   bool
}

var accessKeyRe = regexp.MustCompile(`^[A-Za-z0-9]{4,64}$`)

// CreateCred 创建桶级凭据并复制（oplog 记密文；主密钥丢失走批量重置，docs/02）。
func (s *Store) CreateCred(ctx context.Context, accessKey, secret, bucket string) error {
	if len(s.cfg.KEK) == 0 {
		return fmt.Errorf("%w: KEK not configured", errs.ErrInvalidArgument)
	}
	if !accessKeyRe.MatchString(accessKey) {
		return fmt.Errorf("%w: access key must be 4-64 alphanumerics", errs.ErrInvalidArgument)
	}
	if len(secret) < 8 {
		return fmt.Errorf("%w: secret too short", errs.ErrInvalidArgument)
	}
	enc, err := s.seal(secret)
	if err != nil {
		return err
	}
	return s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(bucket); err != nil {
			return err
		}
		if err := t.InsertCred(accessKey, enc, bucket, time.Now().UnixNano()); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpCredPut, credPutPayload{
			AccessKey: accessKey, SecretEnc: base64.StdEncoding.EncodeToString(enc),
			Bucket: bucket,
		})
	})
}

// LookupCred 供 S3 鉴权：未配置 KEK / 无此凭据 / 已停用 / 解密失败
// 一律归一为 PermissionDenied（不向客户端泄露差异）。
func (s *Store) LookupCred(ctx context.Context, accessKey string) (*Cred, error) {
	if len(s.cfg.KEK) == 0 {
		return nil, errs.ErrPermissionDenied
	}
	row, err := s.meta.GetCred(ctx, accessKey)
	if err != nil {
		return nil, errs.ErrPermissionDenied
	}
	if !row.Enabled {
		return nil, errs.ErrPermissionDenied
	}
	secret, err := s.open(row.SecretEnc)
	if err != nil {
		return nil, errs.ErrPermissionDenied
	}
	return &Cred{AccessKey: accessKey, Secret: secret, Bucket: row.Bucket}, nil
}

// DeleteCred 停用并删除凭据（幂等）。
func (s *Store) DeleteCred(ctx context.Context, accessKey string) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		if err := t.DeleteCred(accessKey); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpCredDelete, credDeletePayload{AccessKey: accessKey})
	})
}

// ListCreds 列出凭据（不含明文 secret）。
func (s *Store) ListCreds(ctx context.Context) ([]CredInfo, error) {
	rows, err := s.meta.ListCreds(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]CredInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, CredInfo{AccessKey: r.AccessKey, Bucket: r.Bucket, Enabled: r.Enabled})
	}
	return out, nil
}

// seal/open：AES-256-GCM，key = SHA-256(KEK)，nonce 前置存储。
func (s *Store) seal(plaintext string) ([]byte, error) {
	block, err := aes.NewCipher(sha256Sum(s.cfg.KEK))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, []byte(plaintext), nil), nil
}

func (s *Store) open(ciphertext []byte) (string, error) {
	block, err := aes.NewCipher(sha256Sum(s.cfg.KEK))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return "", fmt.Errorf("%w: ciphertext too short", errs.ErrInternal)
	}
	nonce, ct := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt cred: %w", err)
	}
	return string(pt), nil
}

func sha256Sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}
