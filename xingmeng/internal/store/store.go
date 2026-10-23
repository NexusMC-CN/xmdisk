// Package store 是统一存储接口的实现（docs/04）：组合元数据面（meta）
// 与数据面（blob），协议适配层只允许调用本包。
//
// 写入路径固定顺序（四原则）：流式落盘 blob（无锁、可并行）→
// 毫秒级事务（条目 + 配额计数器 + oplog）→ 提交。
package store

import (
	"context"
	"encoding/base64"
	"errors"
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
	OplogRetention      time.Duration // oplog 截断保留期（默认 24h；仅活跃从节点全部 ack 过的部分可删，99-D026）
	SlaveActiveWindow   time.Duration // 从节点活跃判定窗口（默认 7d；窗口外注册不阻塞清扫/截断，99-D026）
	KEK                 []byte        // 凭据主密钥（配置层从环境变量注入；空 = 凭据面不可用，99-D009）
}

type Store struct {
	cfg   Config
	meta  *meta.DB
	blobs *blob.Store
	ro    atomic.Bool   // serve=ro（99-D014；admin 面接管开关）
	epoch atomic.Uint32 // 当前 op 生成 epoch（promote 时更新，99-D005）
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
	if cfg.OplogRetention <= 0 {
		cfg.OplogRetention = 24 * time.Hour
	}
	if cfg.SlaveActiveWindow <= 0 {
		cfg.SlaveActiveWindow = 7 * 24 * time.Hour
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	m, err := meta.Open(filepath.Join(cfg.DataDir, "meta.db"))
	if err != nil {
		return nil, fmt.Errorf("open meta: %w", err)
	}
	// 密封凭据密文与全部元数据都在 meta.db 里：收紧到 0600（含 -wal/-shm，尽力而为——
	// 数据目录属主即服务账户，docs/05 安全基线）。
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Chmod(filepath.Join(cfg.DataDir, "meta.db"+suffix), 0o600)
	}
	b, err := blob.Open(cfg.DataDir)
	if err != nil {
		m.Close()
		return nil, fmt.Errorf("open blob store: %w", err)
	}
	st := &Store{cfg: cfg, meta: m, blobs: b}
	st.epoch.Store(cfg.Epoch)
	return st, nil
}

func (s *Store) Close() error { return s.meta.Close() }

// SetReadOnly 对应 serve 开关的 ro 态；为 true 时一切变更操作返回 ErrReadOnly。
func (s *Store) SetReadOnly(v bool) { s.ro.Store(v) }

// SetEpoch 更新 op 生成 epoch（promote 时调用；新 op 立即携带新 epoch）。
func (s *Store) SetEpoch(e uint32) { s.epoch.Store(e) }

func (s *Store) currentEpoch() uint32 { return s.epoch.Load() }

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
// AccessKey/SecretOK 来自绑定凭据（一桶一密，99-D025）：无钥桶（旧二进制
// 所建）AccessKey 为空；SecretOK=false = 当前 KEK 解不开（需 rotate 恢复）。
type BucketStat struct {
	Name      string
	Quota     *int64
	Used      int64
	Pending   int64
	Objects   int64
	Public    bool   // 公共读开关（99-D024）
	AccessKey string // 绑定凭据 AK（不含 secret）
	SecretOK  bool   // 密文在当前 KEK 下可解
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

// CreateBucket 创建桶并同事务签发凭据（桶=租户=一把钥匙，99-D025）：
// AK/SK 均服务端生成，Secret 仅此一次随返回值交出（服务端只存密文）。
// 同事务落 bucket_create + cred_put 双 op，从节点按序回放即收敛。
func (s *Store) CreateBucket(ctx context.Context, name string, quota *int64, public bool) (*Cred, error) {
	if !bucketNameRe.MatchString(name) {
		return nil, fmt.Errorf("%w: invalid bucket name %q", errs.ErrInvalidArgument, name)
	}
	if len(s.cfg.KEK) == 0 {
		return nil, fmt.Errorf("%w: KEK not configured", errs.ErrInvalidArgument)
	}
	var cred *Cred
	err := s.mutate(ctx, func(t *meta.Tx) error {
		if err := t.CreateBucket(name, quota, time.Now().UnixNano()); err != nil {
			return err
		}
		if public {
			if err := t.SetBucketPublic(name, true); err != nil {
				return err
			}
		}
		ak, sk := genAccessKey(), genSecret()
		enc, serr := s.seal(sk)
		if serr != nil {
			return serr
		}
		if err := t.InsertCred(ak, enc, name, time.Now().UnixNano()); err != nil {
			return err
		}
		p := bucketPayload{Name: name, Quota: quota}
		if public {
			p.Public = &public
		}
		if err := appendOp(t, s.currentEpoch(), meta.OpBucketCreate, p); err != nil {
			return err
		}
		if err := appendOp(t, s.currentEpoch(), meta.OpCredPut, credPutPayload{
			AccessKey: ak, SecretEnc: base64.StdEncoding.EncodeToString(enc), Bucket: name,
		}); err != nil {
			return err
		}
		cred = &Cred{AccessKey: ak, Secret: sk, Bucket: name}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cred, nil
}

// DeleteBucket 删除空桶并同事务级联删除其绑定凭据（99-D025）：桶在=钥在，
// 桶亡=钥亡——已删桶的旧凭据即刻失效，不存在"数据面复活已删桶"的窗口。
// 同事务落 bucket_delete + cred_delete 双 op（复用现有 op 类型，复制协议零变更）。
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		b, err := t.GetBucket(name)
		if err != nil {
			return err
		}
		if b.Objects > 0 {
			return fmt.Errorf("%w: bucket %q has %d objects", errs.ErrDirNotEmpty, name, b.Objects)
		}
		if n, uerr := t.CountUploads(name); uerr != nil {
			return uerr
		} else if n > 0 {
			return fmt.Errorf("%w: bucket %q has %d in-flight multipart uploads", errs.ErrDirNotEmpty, name, n)
		}
		if err := t.DeleteBucket(name); err != nil {
			return err
		}
		row, gerr := t.GetCredByBucket(name)
		if gerr != nil && !errors.Is(gerr, errs.ErrKeyNotFound) {
			return gerr
		}
		if row != nil {
			if err := t.DeleteCredByBucket(name); err != nil {
				return err
			}
			if err := appendOp(t, s.currentEpoch(), meta.OpCredDelete, credDeletePayload{AccessKey: row.AccessKey}); err != nil {
				return err
			}
		}
		return appendOp(t, s.currentEpoch(), meta.OpBucketDelete, bucketPayload{Name: name})
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
		return appendOp(t, s.currentEpoch(), meta.OpQuotaUpdate, bucketPayload{Name: name, Quota: quota})
	})
}

// SetBucketPublic 切换桶公共读标志（99-D024；复制面收敛，从节点回放生效）。
func (s *Store) SetBucketPublic(ctx context.Context, name string, public bool) error {
	return s.mutate(ctx, func(t *meta.Tx) error {
		if _, err := t.GetBucket(name); err != nil {
			return err
		}
		if err := t.SetBucketPublic(name, public); err != nil {
			return err
		}
		return appendOp(t, s.currentEpoch(), meta.OpBucketPublic, bucketPayload{Name: name, Public: &public})
	})
}

func (s *Store) BucketStat(ctx context.Context, name string) (*BucketStat, error) {
	b, err := s.meta.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}
	st := &BucketStat{Name: b.Name, Quota: b.Quota, Used: b.Used, Pending: b.Pending, Objects: b.Objects, Public: b.Public, SecretOK: true}
	if row, cerr := s.meta.GetCredByBucket(ctx, name); cerr == nil {
		st.AccessKey = row.AccessKey
		if _, oerr := s.open(row.SecretEnc); oerr != nil {
			st.SecretOK = false // 当前 KEK 解不开：需 rotate 恢复（docs/02）
		}
	} else if !errors.Is(cerr, errs.ErrKeyNotFound) {
		return nil, cerr
	}
	return st, nil
}

// ListBucketStats 返回全部桶用量（admin status 与 metrics 用；含绑定 AK 与密文可读性）。
func (s *Store) ListBucketStats(ctx context.Context) ([]BucketStat, error) {
	names, err := s.meta.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	creds, err := s.meta.ListCreds(ctx)
	if err != nil {
		return nil, err
	}
	byBucket := make(map[string]meta.CredRow, len(creds))
	for _, r := range creds {
		byBucket[r.Bucket] = r
	}
	out := make([]BucketStat, 0, len(names))
	for _, name := range names {
		b, err := s.meta.GetBucket(ctx, name)
		if err != nil {
			return nil, err
		}
		st := BucketStat{Name: b.Name, Quota: b.Quota, Used: b.Used, Pending: b.Pending, Objects: b.Objects, Public: b.Public, SecretOK: true}
		if row, ok := byBucket[name]; ok {
			st.AccessKey = row.AccessKey
			if _, oerr := s.open(row.SecretEnc); oerr != nil {
				st.SecretOK = false
			}
		}
		out = append(out, st)
	}
	return out, nil
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

// Slave 一个已注册从节点的观察视图（GET /slaves 与 xm_slave_* 指标）。
type Slave struct {
	ID         string
	LastAcked  int64  // 从节点已应用的 oplog seq
	Lag        int64  // 本节点 last_seq - LastAcked（≥0）
	Epoch      uint32 // 从节点最近 ack 携带的 epoch
	LastSeenNs int64  // 最近一次注册/ack 时间（活跃窗口判定源）
	Active     bool   // last_seen 在活跃窗口内（窗口外=僵尸注册，不计门控）
}

// Slaves 返回注册从节点观察视图（主节点面；从节点本地表无意义）。
func (s *Store) Slaves(ctx context.Context) ([]Slave, error) {
	infos, err := s.meta.ListSlaves(ctx)
	if err != nil {
		return nil, err
	}
	last, err := s.LastSeq(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-s.cfg.SlaveActiveWindow).UnixNano()
	out := make([]Slave, 0, len(infos))
	for _, in := range infos {
		lag := last - in.LastAcked
		if lag < 0 {
			lag = 0
		}
		out = append(out, Slave{
			ID: in.ID, LastAcked: in.LastAcked, Lag: lag,
			Epoch: in.Epoch, LastSeenNs: in.LastSeenNs, Active: in.LastSeenNs >= cutoff,
		})
	}
	return out, nil
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
// 安全序（S-14）：先打开校验快照可读 → 旧库改名 .bak 暂存（不直接删）→
// 原子改名接管 → 重开失败则回滚 .bak，任意时刻磁盘上都有完整元数据文件。
func (s *Store) ReplaceMeta(snapPath string) error {
	// 1. 预校验：快照必须是可打开、可查询的 SQLite 库（拦截传输损坏/截断）
	if chk, err := meta.Open(snapPath); err != nil {
		return fmt.Errorf("validate snapshot: %w", err)
	} else {
		if _, lerr := chk.LastSeq(context.Background()); lerr != nil {
			chk.Close()
			return fmt.Errorf("validate snapshot: %w", lerr)
		}
		chk.Close()
		for _, suffix := range []string{"-wal", "-shm"} { // 校验打开可能产生的伴生文件
			os.Remove(snapPath + suffix)
		}
	}
	metaPath := filepath.Join(s.cfg.DataDir, "meta.db")
	backup := metaPath + ".bak"
	os.Remove(backup)
	if err := s.meta.Close(); err != nil {
		return err
	}
	// 2. 旧库暂存（含伴生文件）；失败窗口内 .bak 兜底
	for _, suffix := range []string{"-wal", "-shm"} {
		os.Remove(metaPath + suffix)
	}
	if err := os.Rename(metaPath, backup); err != nil && !os.IsNotExist(err) {
		return err
	}
	// 3. 快照原子接管；重开失败回滚 .bak
	if err := os.Rename(snapPath, metaPath); err != nil {
		os.Rename(backup, metaPath)
		return err
	}
	m, err := meta.Open(metaPath)
	if err != nil {
		os.Remove(metaPath)
		os.Rename(backup, metaPath)
		return fmt.Errorf("reopen meta after bootstrap: %w", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Chmod(metaPath+suffix, 0o600)
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
