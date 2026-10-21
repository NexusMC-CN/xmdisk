package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
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

// genAccessKey / genSecret 服务端生成强凭据（99-D025：AK/SK 均不由使用方提供）。
// AK 带 xm 前缀便于日志排障识别；80 位随机熵，碰撞由唯一约束兜底重试。
func genAccessKey() string {
	raw := make([]byte, 10)
	rand.Read(raw)
	return "xm" + hex.EncodeToString(raw)
}

func genSecret() string {
	raw := make([]byte, 24)
	rand.Read(raw)
	return hex.EncodeToString(raw)
}

// CreateCred 插入或覆盖凭据（测试与内部原语；admin 面已不暴露——凭据随桶
// 生灭，99-D025）。一桶一密由 idx_creds_bucket 唯一索引兜底：同桶第二把
// 不同 AK 被拒；同 AK 覆盖（KEK 丢失重置路径）不受影响。
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
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return fmt.Errorf("%w: %q already has a credential (one per bucket, 99-D025)", errs.ErrInvalidArgument, bucket)
			}
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpCredPut, credPutPayload{
			AccessKey: accessKey, SecretEnc: base64.StdEncoding.EncodeToString(enc),
			Bucket: bucket,
		})
	})
}

// RotateBucketCred 重置桶凭据：保留 AK、换新 SK（保持 AK 的唯一随机性即可，
// SK 碰撞无意义）。无钥桶（旧二进制所建）自愈补发 AK+SK（99-D025）。
// "冻结租户" = rotate 到不外发的新 SK（可逆：再 rotate 发给租户）。
func (s *Store) RotateBucketCred(ctx context.Context, bucket string) (*Cred, error) {
	if len(s.cfg.KEK) == 0 {
		return nil, fmt.Errorf("%w: KEK not configured", errs.ErrInvalidArgument)
	}
	sk := genSecret()
	var ak string
	err := s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(bucket); err != nil {
			return err
		}
		row, gerr := t.GetCredByBucket(bucket)
		if gerr != nil && !errors.Is(gerr, errs.ErrKeyNotFound) {
			return gerr
		}
		if row != nil {
			ak = row.AccessKey
		} else {
			ak = genAccessKey() // 无钥桶自愈
		}
		enc, serr := s.seal(sk)
		if serr != nil {
			return serr
		}
		if err := t.InsertCred(ak, enc, bucket, time.Now().UnixNano()); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpCredPut, credPutPayload{
			AccessKey: ak, SecretEnc: base64.StdEncoding.EncodeToString(enc), Bucket: bucket,
		})
	})
	if err != nil {
		return nil, err
	}
	return &Cred{AccessKey: ak, Secret: sk, Bucket: bucket}, nil
}

// SealedSecretsStatus 密封凭据健康自检（docs/02：KEK 更换后定位待轮换桶）。
// unreadable = 当前 KEK 解不开的条数（对应桶需逐个 rotate 恢复）。
func (s *Store) SealedSecretsStatus(ctx context.Context) (total, unreadable int, err error) {
	if len(s.cfg.KEK) == 0 {
		return 0, 0, nil
	}
	rows, err := s.meta.ListCreds(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, r := range rows {
		total++
		if _, oerr := s.open(r.SecretEnc); oerr != nil {
			unreadable++
		}
	}
	return total, unreadable, nil
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

// SealSecret / OpenSecret 导出 AES-GCM 封装：凭据与运行时 repl token 共用
// 同一管线（99-D023；token 存 node-state.json，不进 meta.db）。
func (s *Store) SealSecret(plaintext string) ([]byte, error) { return s.seal(plaintext) }

func (s *Store) OpenSecret(ciphertext []byte) (string, error) { return s.open(ciphertext) }

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
