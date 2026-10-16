// Package store 是统一存储接口的实现（docs/04）：组合元数据面（meta）
// 与数据面（blob），协议适配层只允许调用本包。
//
// 写入路径固定顺序（四原则）：流式落盘 blob（无锁、可并行）→
// 毫秒级事务（条目 + 配额计数器 + oplog）→ 提交。
package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"

	"xingmeng/internal/blob"
	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
)

// BlobRef 是内容寻址引用（条目 blobs 与 oplog payload 共用此形态）。
type BlobRef struct {
	Sha256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

const (
	DefaultSmallThreshold = 4 << 20 // 小文件整存阈值
	DefaultChunkSize      = 8 << 20 // 大文件定长分块
	DefaultMaxObjectSize  = 5 << 30 // 对齐 S3 单次 PUT 上限
)

type Config struct {
	DataDir             string
	Epoch               uint32 // fencing（99-D005）；promote 时由 admin 面更新
	SmallThreshold      int64
	ChunkSize           int64
	MaxObjectSize       int64
	MultipartMaxSize    int64         // multipart 对象上限（默认 1TB，docs/01）
	GCInterval          time.Duration // 标记-清扫周期（默认 1h）
	GCGrace             time.Duration // trash 宽限期（默认 48h，docs/05 gc_grace）
	MultipartAbortAfter time.Duration // multipart 会话超时清理（默认 168h）
	KEK                 []byte        // 凭据主密钥（配置层从环境变量注入；空 = 凭据面不可用，99-D009）
}

type Store struct {
	cfg   Config
	meta  *meta.DB
	blobs *blob.Store
	ro    atomic.Bool // serve=ro（99-D014；admin 面接管开关）
}

func Open(cfg Config) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("%w: DataDir is required", errs.ErrInvalidArgument)
	}
	if cfg.SmallThreshold <= 0 {
		cfg.SmallThreshold = DefaultSmallThreshold
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = DefaultChunkSize
	}
	if cfg.MaxObjectSize <= 0 {
		cfg.MaxObjectSize = DefaultMaxObjectSize
	}
	if cfg.ChunkSize < cfg.SmallThreshold {
		return nil, fmt.Errorf("%w: ChunkSize < SmallThreshold", errs.ErrInvalidArgument)
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = time.Hour
	}
	if cfg.GCGrace <= 0 {
		cfg.GCGrace = 48 * time.Hour
	}
	if cfg.MultipartAbortAfter <= 0 {
		cfg.MultipartAbortAfter = 168 * time.Hour
	}
	if cfg.MultipartMaxSize <= 0 {
		cfg.MultipartMaxSize = 1 << 40 // 1TB（docs/01 设计上限）
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	m, err := meta.Open(filepath.Join(cfg.DataDir, "meta.db"))
	if err != nil {
		return nil, fmt.Errorf("open meta: %w", err)
	}
	b, err := blob.Open(cfg.DataDir)
	if err != nil {
		m.Close()
		return nil, fmt.Errorf("open blob store: %w", err)
	}
	return &Store{cfg: cfg, meta: m, blobs: b}, nil
}

func (s *Store) Close() error { return s.meta.Close() }

// SetReadOnly 对应 serve 开关的 ro 态；为 true 时一切变更操作返回 ErrReadOnly。
func (s *Store) SetReadOnly(v bool) { s.ro.Store(v) }

// ObjInfo 是适配层可见的对象视图。
type ObjInfo struct {
	Bucket      string
	Key         string
	IsDir       bool
	Size        int64
	ETag        string
	ContentType string
	UserMeta    map[string]string
	Blobs       []BlobRef
	MTime       time.Time
	Mode        uint32
	UID, GID    int64
}

// BucketStat 是桶用量视图（计费/配额口径 = 逻辑字节，99-D008）。
type BucketStat struct {
	Name    string
	Quota   *int64
	Used    int64
	Pending int64
	Objects int64
}

type PutOptions struct {
	ContentType string
	Meta        map[string]string
	MTime       time.Time // 零值 = 当前时间
}

// Range 是读取区间：Start 含，Length 为负表示读到结尾。
type Range struct {
	Start  int64
	Length int64
}

// ListResult 同时服务 S3 ListV2 与树形列目录（接口原则 3）。
// NextAfter 是不透明续传 token，原样回传给下一轮 List 即可：
// 条目 token 即 key 本身；CommonPrefix 的 token 追加 \x00（key 禁含 NUL，
// 二者无歧义——CP 需按字节后继跳过全部子键，条目只需排他下界）。
type ListResult struct {
	Items          []ObjInfo
	CommonPrefixes []string
	NextAfter      string
	Truncated      bool
}

var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// CreateBucket 创建桶；quota 为 nil 表示不限。
func (s *Store) CreateBucket(ctx context.Context, name string, quota *int64) error {
	if !bucketNameRe.MatchString(name) {
		return fmt.Errorf("%w: invalid bucket name %q", errs.ErrInvalidArgument, name)
	}
	return s.mutate(ctx, func(t *meta.Tx) error {
		if err := t.CreateBucket(name, quota, time.Now().UnixNano()); err != nil {
			return err
		}
		return appendOp(t, s.cfg.Epoch, meta.OpBucketCreate, bucketPayload{Name: name, Quota: quota})
	})
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		b, err := t.GetBucket(name)
		if err != nil {
			return err
		}
		if b.Objects > 0 {
			return fmt.Errorf("%w: bucket %q has %d objects", errs.ErrDirNotEmpty, name, b.Objects)
		}
		if err := t.DeleteBucket(name); err != nil {
			return err
		}
		return appendOp(t, s.cfg.Epoch, meta.OpBucketDelete, bucketPayload{Name: name})
	})
}

// SetQuota 调整桶配额；nil 表示解除限制。
func (s *Store) SetQuota(ctx context.Context, name string, quota *int64) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(name); err != nil {
			return err
		}
		if err := t.SetQuota(name, quota); err != nil {
			return err
		}
		return appendOp(t, s.cfg.Epoch, meta.OpQuotaUpdate, bucketPayload{Name: name, Quota: quota})
	})
}

func (s *Store) BucketStat(ctx context.Context, name string) (*BucketStat, error) {
	b, err := s.meta.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}
	return &BucketStat{Name: b.Name, Quota: b.Quota, Used: b.Used, Pending: b.Pending, Objects: b.Objects}, nil
}

// LastSeq 返回 oplog 当前位点（admin/status 与从节点初始进度用）。
func (s *Store) LastSeq(ctx context.Context) (int64, error) { return s.meta.LastSeq(ctx) }

// MaxEpoch 返回日志中的最大 epoch（fencing 基线）。
func (s *Store) MaxEpoch(ctx context.Context) (uint32, error) { return s.meta.MaxEpoch(ctx) }

// MinSeq 返回现存最老 op 的 seq；hasAny 为 false 表示日志为空（从节点应转快照重建）。
func (s *Store) MinSeq(ctx context.Context) (int64, bool, error) { return s.meta.MinSeq(ctx) }

// AckSlave 记录从节点进度（日志截断与 GC 门控的共同数据源）。
func (s *Store) AckSlave(ctx context.Context, id string, acked int64, nowNs int64) error {
	return s.meta.SetSlaveAck(ctx, id, acked, nowNs)
}

// RegisterSlave 登记从节点（不回退已有进度）。
func (s *Store) RegisterSlave(ctx context.Context, id string, nowNs int64) error {
	return s.meta.RegisterSlave(ctx, id, nowNs)
}

// Ops 返回 seq 大于 after 的 op（admin 排障与测试用）。
func (s *Store) Ops(ctx context.Context, after int64, limit int) ([]meta.Op, error) {
	return s.meta.OpsAfter(ctx, after, limit)
}

// TruncateOplog 删除 seq 小于 before 的 op（日志截断任务用）。
func (s *Store) TruncateOplog(ctx context.Context, before int64) (int64, error) {
	return s.meta.TruncateOplog(ctx, before)
}

// Snapshot 生成一致性快照文件（VACUUM INTO，repl 面下载），返回路径与截断点 seq。
func (s *Store) Snapshot(ctx context.Context) (path string, seq int64, err error) {
	dir := filepath.Join(s.cfg.DataDir, "snapshots")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	p := filepath.Join(dir, fmt.Sprintf("snap-%d.db", time.Now().UnixNano()))
	if err := s.meta.Snapshot(p); err != nil {
		return "", 0, fmt.Errorf("vacuum into: %w", err)
	}
	seq, err = s.meta.LastSeq(ctx)
	if err != nil {
		return "", 0, err
	}
	return p, seq, nil
}

// ReplaceMeta 用快照文件替换本地元数据库并重开（从节点 Bootstrap 专用；
// 调用方须保证此时无并发访问——同步循环已停、节点不对外服务）。
func (s *Store) ReplaceMeta(snapPath string) error {
	metaPath := filepath.Join(s.cfg.DataDir, "meta.db")
	if err := s.meta.Close(); err != nil {
		return err
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(metaPath + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.Rename(snapPath, metaPath); err != nil {
		return err
	}
	m, err := meta.Open(metaPath)
	if err != nil {
		return fmt.Errorf("reopen meta after bootstrap: %w", err)
	}
	s.meta = m
	return nil
}

// DataDir 返回数据目录（从节点 bootstrap 落快照暂存用）。
func (s *Store) DataDir() string { return s.cfg.DataDir }

// HasBlob 报告 blob 是否已在本地数据面（从节点补 blob 前探测）。
func (s *Store) HasBlob(sha string) bool { return s.blobs.Exists(sha) }

// GetBlob 打开 blob 供读取（repl 主端下载用；调用方负责 Close）。
func (s *Store) GetBlob(sha string) (*os.File, int64, error) {
	f, err := s.blobs.Open(sha)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

// BlobTombstoned 报告 blob 是否处于待删除区（repl 墓碑感知）。
func (s *Store) BlobTombstoned(sha string) bool { return s.blobs.TrashExists(sha) }

// WriteBlob 把流写入本地数据面并返回内容哈希与大小（从节点拉取 blob 用；
// 返回哈希与请求哈希比对即完整性校验）。
func (s *Store) WriteBlob(src io.Reader) (hash string, size int64, err error) {
	return s.blobs.Put(src)
}

// mutate 是变更操作的统一入口：只读检查 + 写事务（四原则 1 的锁纪律由此保证）。
func (s *Store) mutate(ctx context.Context, fn func(t *meta.Tx) error) error {
	if s.ro.Load() {
		return errs.ErrReadOnly
	}
	return s.meta.WithWriteTx(ctx, fn)
}
