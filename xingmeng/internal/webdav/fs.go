// Package webdav 是 WebDAV 适配层（docs/01 v1：best effort）。
// 认证与 SFTP 同源：Basic 用户名=AK、密码=SK，挂载点 /dav/<bucket>（docs/04）。
// PROPPATCH（死属性）暂缓；锁用进程内 MemLS（Office 类客户端需要）。
package webdav

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"time"

	xwebdav "golang.org/x/net/webdav"

	"xingmeng/internal/errs"
	"xingmeng/internal/store"
)

// FS 实现 x/net/webdav.FileSystem，命名空间为单个桶。
type FS struct {
	store  *store.Store
	bucket string
}

func NewFS(st *store.Store, bucket string) *FS { return &FS{store: st, bucket: bucket} }

// ---- 基础操作 ----

func (f *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return mapErr(f.store.Mkdir(ctx, f.bucket, normPath(name)))
}

func (f *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	if normPath(name) == "" {
		return rootInfo(), nil
	}
	key, oi, err := f.resolve(name)
	if err != nil {
		return nil, mapErr(err)
	}
	return infoOf(key, oi), nil
}

// Rename 解析文件/目录后走 store.Move（目录 = 单条 tree_move，99-D011）。
func (f *FS) Rename(ctx context.Context, oldName, newName string) error {
	key, oi, err := f.resolve(oldName)
	if err != nil {
		return mapErr(err)
	}
	if oi.IsDir {
		return mapErr(f.store.Move(ctx, f.bucket, key, normPath(newName)+"/"))
	}
	return mapErr(f.store.Move(ctx, f.bucket, key, normPath(newName)))
}

// RemoveAll 递归删除（DELETE 集合语义）：文件单删；目录逐条删除全部后代
// 与目录标记。逐条产生 oplog op——大目录删除是重操作，运维应有预期。
func (f *FS) RemoveAll(ctx context.Context, name string) error {
	norm := normPath(name)
	if norm == "" {
		return os.ErrInvalid // 拒绝删除根
	}
	key, oi, err := f.resolve(name)
	if err != nil {
		if errors.Is(err, errs.ErrKeyNotFound) {
			return nil // 幂等
		}
		return mapErr(err)
	}
	if !oi.IsDir {
		return mapErr(f.store.Delete(ctx, f.bucket, key))
	}
	prefix := key // 形如 "a/"
	const batch = 1000
	for {
		res, err := f.store.List(ctx, f.bucket, prefix, "", "", batch)
		if err != nil {
			return mapErr(err)
		}
		for i := range res.Items {
			if res.Items[i].Key == prefix {
				continue // 目录标记最后删
			}
			if err := f.store.Delete(ctx, f.bucket, res.Items[i].Key); err != nil {
				return mapErr(err)
			}
		}
		if !res.Truncated {
			break
		}
	}
	// 后代已清空，用 Rmdir 移除目录标记（Delete 对目录条目会拒绝）
	return mapErr(f.store.Rmdir(ctx, f.bucket, norm))
}

// ---- OpenFile：读（含目录列举）与写（临时文件 + Close 提交 + 即刻限流）----

func (f *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (xwebdav.File, error) {
	norm := normPath(name)
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		if norm == "" {
			return nil, os.ErrInvalid
		}
		if _, err := f.store.Stat(ctx, f.bucket, norm+"/"); err == nil {
			return nil, os.ErrInvalid // 目录占位，拒绝写同名文件
		}
		limit := int64(-1)
		if allowance, limited, err := f.store.QuotaAllowance(ctx, f.bucket, norm); err != nil {
			return nil, mapErr(err)
		} else if limited {
			limit = allowance // 99-D019：SFTP 同款教训——写入中限流
		}
		tmp, err := os.CreateTemp("", "xmd-dav-*")
		if err != nil {
			return nil, err
		}
		return &writeFile{fs: f, key: norm, tmp: tmp, name: tmp.Name(), limit: limit}, nil
	}
	// 读路径
	if norm == "" {
		return &dirFile{fs: f, prefix: ""}, nil
	}
	key, oi, err := f.resolve(name)
	if err != nil {
		return nil, mapErr(err)
	}
	if oi.IsDir {
		return &dirFile{fs: f, prefix: key}, nil
	}
	return &readFile{fs: f, key: key, size: oi.Size, info: infoOf(key, oi)}, nil
}

// readFile：顺序读 + Seek（重开区间流）；http.ServeContent 依赖 Seek。
type readFile struct {
	fs     *FS
	key    string
	size   int64
	info   os.FileInfo
	cur    io.ReadCloser
	pos    int64
	closed bool
}

func (r *readFile) Read(p []byte) (int, error) {
	if r.cur == nil {
		rc, _, err := r.fs.store.Get(context.Background(), r.fs.bucket, r.key,
			&store.Range{Start: r.pos, Length: -1})
		if err != nil {
			return 0, mapErr(err)
		}
		r.cur = rc
	}
	n, err := r.cur.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *readFile) Seek(offset int64, whence int) (int64, error) {
	var np int64
	switch whence {
	case io.SeekStart:
		np = offset
	case io.SeekCurrent:
		np = r.pos + offset
	case io.SeekEnd:
		np = r.size + offset
	default:
		return 0, os.ErrInvalid
	}
	if np < 0 {
		return 0, os.ErrInvalid
	}
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
	r.pos = np
	return np, nil
}

func (r *readFile) Stat() (os.FileInfo, error) { return r.info, nil }
func (r *readFile) Close() error {
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
	r.closed = true
	return nil
}
func (r *readFile) Readdir(count int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (r *readFile) Write(p []byte) (int, error)              { return 0, os.ErrInvalid }
func (r *readFile) Name() string                             { return r.info.Name() }

// writeFile：临时文件承接客户端流，Close 提交（PUT 语义）。
type writeFile struct {
	fs        *FS
	key       string
	tmp       *os.File
	name      string
	limit     int64 // -1 = 不限
	highWater int64
	failed    bool
}

func (w *writeFile) Write(p []byte) (int, error) {
	w.highWater += int64(len(p))
	if w.limit >= 0 && w.highWater > w.limit {
		w.failed = true
		return 0, os.ErrPermission // 99-D019：写入中限流
	}
	return w.tmp.Write(p)
}

// Stat 供 x/net/webdav 在 PUT 后计算 ETag（modtime+size 形态）。
func (w *writeFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: path.Base(w.key), size: w.highWater, mode: 0o644, mod: time.Now()}, nil
}

func (w *writeFile) Close() error {
	defer os.Remove(w.name)
	defer w.tmp.Close()
	if w.failed {
		return os.ErrPermission
	}
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := w.fs.store.Put(context.Background(), w.fs.bucket, w.key, w.tmp, store.PutOptions{})
	if err != nil {
		return mapErr(err)
	}
	return nil
}
func (w *writeFile) Read(p []byte) (int, error)               { return 0, os.ErrInvalid }
func (w *writeFile) Seek(int64, int) (int64, error)           { return 0, os.ErrInvalid }
func (w *writeFile) Readdir(count int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (w *writeFile) Name() string                             { return path.Base(w.key) }

// dirFile：PROPFIND/GET 的目录列举（跨页拉全，跳过目录标记自身）。
type dirFile struct {
	fs      *FS
	prefix  string
	entries []os.FileInfo
	loaded  bool
	pos     int
}

func (d *dirFile) load() error {
	if d.loaded {
		return nil
	}
	after := ""
	for {
		res, err := d.fs.store.List(context.Background(), d.fs.bucket, d.prefix, "/", after, 10000)
		if err != nil {
			return mapErr(err)
		}
		for i := range res.Items {
			if res.Items[i].Key == d.prefix {
				continue
			}
			d.entries = append(d.entries, infoOf(res.Items[i].Key, &res.Items[i]))
		}
		for _, cp := range res.CommonPrefixes {
			d.entries = append(d.entries, infoOf(cp, &store.ObjInfo{IsDir: true, MTime: time.Now()}))
		}
		if !res.Truncated {
			break
		}
		after = res.NextAfter
	}
	d.loaded = true
	return nil
}

func (d *dirFile) Readdir(count int) ([]os.FileInfo, error) {
	if err := d.load(); err != nil {
		return nil, err
	}
	if d.pos >= len(d.entries) {
		if count <= 0 {
			return nil, nil
		}
		return nil, io.EOF
	}
	end := len(d.entries)
	if count > 0 && d.pos+count < end {
		end = d.pos + count
	}
	out := d.entries[d.pos:end]
	d.pos = end
	return out, nil
}

func (d *dirFile) Stat() (os.FileInfo, error)     { return rootInfo(), nil }
func (d *dirFile) Close() error                   { return nil }
func (d *dirFile) Read(p []byte) (int, error)     { return 0, os.ErrInvalid }
func (d *dirFile) Write(p []byte) (int, error)    { return 0, os.ErrInvalid }
func (d *dirFile) Seek(int64, int) (int64, error) { return 0, os.ErrInvalid }
func (d *dirFile) Name() string {
	if d.prefix == "" {
		return "/"
	}
	return path.Base(strings.TrimSuffix(d.prefix, "/"))
}

// ---- 工具 ----

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

// resolve 客户端路径 → 存储 key（先文件后目录标记）。
func (f *FS) resolve(p string) (key string, oi *store.ObjInfo, err error) {
	ctx := context.Background()
	norm := normPath(p)
	if norm == "" {
		return "", nil, errs.ErrKeyNotFound
	}
	if oi, err = f.store.Stat(ctx, f.bucket, norm); err == nil {
		return norm, oi, nil
	}
	if oi, err = f.store.Stat(ctx, f.bucket, norm+"/"); err == nil {
		return norm + "/", oi, nil
	}
	return "", nil, err
}

func normPath(p string) string {
	p = path.Clean("/" + p)
	p = strings.TrimPrefix(p, "/")
	if p == "." {
		return ""
	}
	return p
}

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
		return err
	}
}

// 编译期接口合规
var _ xwebdav.FileSystem = (*FS)(nil)
