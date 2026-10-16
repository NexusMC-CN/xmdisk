// Package sftp 是 SFTP 适配层（docs/01 v1 范围：读写/列目录/重命名/建删目录；
// 拒绝 symlink/hardlink/xattr/锁）。认证复用凭据表：用户名=AK、密码=SK，
// 登录即以该凭据绑定的桶为根目录（天然租户隔离，99-D021）。
package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

// Backend 实现 pkg/sftp 的四个处理器接口，命名空间为单个桶。
type Backend struct {
	store  *store.Store
	bucket string
}

func NewBackend(st *store.Store, bucket string) *Backend {
	return &Backend{store: st, bucket: bucket}
}

// ---- 读：ReaderAt，每次区间读走 store.Get 的跨 blob 偏移拼接 ----

type readerAt struct {
	st          *store.Store
	bucket, key string
	size        int64
}

func (r *readerAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	rc, _, err := r.st.Get(context.Background(), r.bucket, r.key, &store.Range{Start: off, Length: int64(len(p))})
	if err != nil {
		return 0, mapErr(err)
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

// Fileread 打开对象读取。
func (b *Backend) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	key, oi, err := b.resolve(r.Filepath)
	if err != nil {
		return nil, mapErr(err)
	}
	if oi.IsDir {
		return nil, os.ErrInvalid
	}
	return &readerAt{st: b.store, bucket: b.bucket, key: key, size: oi.Size}, nil
}

// ---- 写：临时文件 + Close 提交（内容寻址事务语义不变：先内容后元数据）。
// WriteAt 即刻限流（99-D019）：SFTP 路径不能等 Close 才发现超额——
// 否则临时文件先吃满磁盘，流式守卫被绕过。

type tempWriter struct {
	st          *store.Store
	bucket, key string
	tmp         *os.File
	name        string
	limit       int64 // -1 = 不限
	highWater   int64
	failed      bool
}

func (w *tempWriter) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > w.highWater {
		w.highWater = end
	}
	if w.limit >= 0 && w.highWater > w.limit {
		w.failed = true
		return 0, os.ErrPermission // 映射自 QuotaExceeded（docs/04）
	}
	return w.tmp.WriteAt(p, off)
}

// Close 由 request server 在句柄关闭时调用：提交对象；失败即向客户端暴露错误。
func (w *tempWriter) Close() error {
	defer os.Remove(w.name)
	defer w.tmp.Close()
	if w.failed {
		return os.ErrPermission
	}
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := w.st.Put(context.Background(), w.bucket, w.key, w.tmp, store.PutOptions{})
	if err != nil {
		return mapErr(err)
	}
	return nil
}

// Filewrite 打开对象写入（覆盖语义；句柄关闭时提交）。
func (b *Backend) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	key := normPath(r.Filepath)
	if key == "" || strings.HasSuffix(key, "/") {
		return nil, os.ErrInvalid
	}
	if _, err := b.store.Stat(context.Background(), b.bucket, key+"/"); err == nil {
		return nil, os.ErrInvalid // 目录占位，拒绝写同名文件
	}
	limit := int64(-1)
	if allowance, limited, err := b.store.QuotaAllowance(context.Background(), b.bucket, key); err != nil {
		return nil, mapErr(err)
	} else if limited {
		limit = allowance
	}
	tmp, err := os.CreateTemp("", "xmd-sftp-*")
	if err != nil {
		return nil, err
	}
	return &tempWriter{st: b.store, bucket: b.bucket, key: key, tmp: tmp, name: tmp.Name(), limit: limit}, nil
}

// ---- 命令：重命名/建删目录/删除/Setstat；symlink/hardlink 拒绝 ----

func (b *Backend) Filecmd(r *sftp.Request) error {
	ctx := context.Background()
	src := normPath(r.Filepath)
	dst := normPath(r.Target)
	switch r.Method {
	case "Rename":
		key, oi, err := b.resolve(r.Filepath)
		if err != nil {
			return mapErr(err)
		}
		if oi.IsDir {
			return mapErr(b.store.Move(ctx, b.bucket, key, dst+"/"))
		}
		return mapErr(b.store.Move(ctx, b.bucket, key, dst))
	case "Rmdir":
		return mapErr(b.store.Rmdir(ctx, b.bucket, src))
	case "Mkdir":
		return mapErr(b.store.Mkdir(ctx, b.bucket, src))
	case "Remove":
		key, oi, err := b.resolve(r.Filepath)
		if err != nil {
			return mapErr(err)
		}
		if oi.IsDir {
			return mapErr(b.store.Rmdir(ctx, b.bucket, src))
		}
		return mapErr(b.store.Delete(ctx, b.bucket, key))
	case "Setstat":
		attrs := r.Attributes()
		if attrs == nil {
			return nil
		}
		key, _, err := b.resolve(r.Filepath)
		if err != nil {
			return mapErr(err)
		}
		patch := store.AttrPatch{}
		if attrs.Mtime > 0 {
			mt := attrs.ModTime()
			patch.MTime = &mt
		}
		if attrs.Mode != 0 {
			m := uint32(attrs.FileMode().Perm())
			patch.Mode = &m
		}
		if patch.MTime == nil && patch.Mode == nil {
			return nil
		}
		return mapErr(b.store.SetAttrs(ctx, b.bucket, key, patch))
	case "Link", "Symlink":
		return sftp.ErrSSHFxOpUnsupported // 99-D018：无链接语义，拒绝
	default:
		return sftp.ErrSSHFxOpUnsupported
	}
}

// ---- 列举/属性 ----

type fileInfo struct {
	name string
	size int64
	mode os.FileMode
	mod  time.Time
}

func (f *fileInfo) Name() string       { return f.name }
func (f *fileInfo) Size() int64        { return f.size }
func (f *fileInfo) Mode() os.FileMode  { return f.mode }
func (f *fileInfo) ModTime() time.Time { return f.mod }
func (f *fileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f *fileInfo) Sys() any           { return nil }

func infoOf(key string, oi *store.ObjInfo) *fileInfo {
	name := path.Base(strings.TrimSuffix(key, "/"))
	mode := os.FileMode(oi.Mode)
	if oi.IsDir {
		mode = os.ModeDir | 0o755
	}
	if mode.Perm() == 0 {
		mode = 0o644
	}
	return &fileInfo{name: name, size: oi.Size, mode: mode, mod: oi.MTime}
}

func rootInfo() *fileInfo {
	return &fileInfo{name: "/", mode: os.ModeDir | 0o755, mod: time.Now()}
}

// listerAt 把 []os.FileInfo 适配为 pkg/sftp 的分页列举接口。
type listerAt []os.FileInfo

func (l listerAt) ListAt(f []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(f, l[offset:])
	if n < len(f) {
		return n, io.EOF
	}
	return n, nil
}

// Filelist 处理 List（列目录）与 Stat（单条属性）。
func (b *Backend) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	ctx := context.Background()
	switch r.Method {
	case "List":
		p := normPath(r.Filepath)
		if p == "" { // 根目录
			return b.listDir(ctx, "")
		}
		key, oi, err := b.resolve(r.Filepath)
		if err != nil {
			return nil, mapErr(err)
		}
		if !oi.IsDir {
			return listerAt{infoOf(key, oi)}, nil
		}
		return b.listDir(ctx, key) // key 形如 "a/"
	case "Stat":
		if p := normPath(r.Filepath); p == "" {
			return listerAt{rootInfo()}, nil
		}
		key, oi, err := b.resolve(r.Filepath)
		if err != nil {
			return nil, mapErr(err)
		}
		return listerAt{infoOf(key, oi)}, nil
	default:
		return nil, sftp.ErrSSHFxOpUnsupported
	}
}

// listDir 列目录：跨页拉全，跳过目录标记自身（docs/04 List 语义）。
func (b *Backend) listDir(ctx context.Context, prefix string) (listerAt, error) {
	var out []os.FileInfo
	after := ""
	for {
		res, err := b.store.List(ctx, b.bucket, prefix, "/", after, 10000)
		if err != nil {
			return nil, mapErr(err)
		}
		for i := range res.Items {
			if res.Items[i].Key == prefix { // 目录标记自身
				continue
			}
			out = append(out, infoOf(res.Items[i].Key, &res.Items[i]))
		}
		for _, cp := range res.CommonPrefixes {
			out = append(out, infoOf(cp, &store.ObjInfo{IsDir: true, MTime: time.Now()}))
		}
		if !res.Truncated {
			return listerAt(out), nil
		}
		after = res.NextAfter
	}
}

// resolve 把客户端路径解析为存储 key：先按文件查，再按目录标记（"a/"）查。
func (b *Backend) resolve(p string) (key string, oi *store.ObjInfo, err error) {
	norm := normPath(p)
	if norm == "" {
		return "", nil, errs.ErrKeyNotFound
	}
	if oi, err = b.store.Stat(context.Background(), b.bucket, norm); err == nil {
		return norm, oi, nil
	}
	if oi, err = b.store.Stat(context.Background(), b.bucket, norm+"/"); err == nil {
		return norm + "/", oi, nil
	}
	return "", nil, err
}

// normPath 规范化客户端路径：去前导 "/"，消化 "." 与 ".."（纯命名空间归一，
// 无文件系统穿越面——key 不映射路径，99-D018）。
func normPath(p string) string {
	p = path.Clean("/" + strings.ReplaceAll(p, "\\", "/"))
	p = strings.TrimPrefix(p, "/")
	if p == "." {
		return ""
	}
	return p
}

// mapErr：store sentinel → os 语义错误（pkg/sftp 转换为对应 SSH_FX 码，
// 对应 docs/04 错误映射表的 SFTP 列）。
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, errs.ErrKeyNotFound), errors.Is(err, errs.ErrBucketNotFound):
		return os.ErrNotExist
	case errors.Is(err, errs.ErrBucketExists):
		return os.ErrExist
	case errors.Is(err, errs.ErrQuotaExceeded), errors.Is(err, errs.ErrPermissionDenied),
		errors.Is(err, errs.ErrReadOnly):
		return os.ErrPermission
	case errors.Is(err, errs.ErrIsDirectory), errors.Is(err, errs.ErrIsFile),
		errors.Is(err, errs.ErrInvalidArgument):
		return os.ErrInvalid
	default:
		return fmt.Errorf("sftp backend: %w", err)
	}
}
