// Package blob 实现内容寻址数据面（99-D002）：sha256 定位、写满 fsync 后
// 原子改名入库、同哈希即同文件（去重天然成立）、trash 支撑两阶段 GC（99-D006）。
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"xingmeng/internal/errs"
)

var hashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Store struct{ root string }

// Open 初始化数据目录（blobs/tmp/trash）。
func Open(root string) (*Store, error) {
	s := &Store{root: root}
	for _, d := range []string{"blobs", "tmp", "trash"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// path 哈希必须严格校验后才能拼路径（防路径穿越）。
func (s *Store) path(h string) (string, error) {
	if !hashRe.MatchString(h) {
		return "", fmt.Errorf("%w: invalid blob hash %q", errs.ErrInvalidArgument, h)
	}
	return filepath.Join(s.root, "blobs", h[:2], h[2:4], h), nil
}

// Put 流式写入一个 blob：临时文件 → fsync → 原子改名；目标已存在则直接去重返回。
// 调用方保证"写完才引用"：本函数返回成功后内容即已持久。
func (s *Store) Put(src io.Reader) (hash string, size int64, err error) {
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "put-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := tmp.Write(buf[:n]); werr != nil {
				err = werr
				return
			}
			h.Write(buf[:n])
			size += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			err = rerr
			return
		}
	}
	if err = tmp.Sync(); err != nil {
		return
	}
	if err = tmp.Close(); err != nil {
		return
	}
	hash = hex.EncodeToString(h.Sum(nil))
	dst, perr := s.path(hash)
	if perr != nil {
		err = perr
		return
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return
	}
	if err = os.Rename(tmpName, dst); err != nil {
		// 目标已存在（并发/重复写同内容）：内容按哈希一致，复用即可。
		if _, serr := os.Stat(dst); serr == nil {
			os.Remove(tmpName)
			err = nil
		}
	}
	return
}

// Open 打开 blob 用于读取（调用方负责 Close；配合 Seek/LimitReader 做区间读）。
func (s *Store) Open(h string) (*os.File, error) {
	p, err := s.path(h)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (s *Store) Size(h string) (int64, error) {
	p, err := s.path(h)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *Store) Exists(h string) bool {
	p, err := s.path(h)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

// TrashExists 报告 blob 是否处于待删除区（repl 墓碑感知，99-D006）。
func (s *Store) TrashExists(h string) bool {
	if !hashRe.MatchString(h) {
		return false
	}
	_, err := os.Stat(filepath.Join(s.root, "trash", h))
	return err == nil
}

// Trash 将 blob 移入待删除区，文件 mtime 即标记时间戳（标记阶段，99-D006）。
func (s *Store) Trash(h string) error {
	p, err := s.path(h)
	if err != nil {
		return err
	}
	tp := filepath.Join(s.root, "trash", h)
	if err := os.MkdirAll(filepath.Dir(tp), 0o755); err != nil {
		return err
	}
	if err := os.Rename(p, tp); err != nil {
		if os.IsNotExist(err) {
			return nil // 已不在 blobs（重复标记或已清理），幂等
		}
		// trash 已有同哈希：源与目标内容一致，直接删源。
		if _, serr := os.Stat(tp); serr == nil {
			return os.Remove(p)
		}
		return err
	}
	return nil
}

// SweepTrash 清扫 olderThan 之前标记的 blob：keep 回调逐个复核存活——
// true 复活（blobs/ 已有同哈希则只清 trash 副本），false 物理删除。
// 清扫前重验，不信标记（自愈，99-D006）。
func (s *Store) SweepTrash(olderThan time.Duration, keep func(hash string) bool) (swept, resurrected int64, err error) {
	dir := filepath.Join(s.root, "trash")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	cut := time.Now().Add(-olderThan)
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			return swept, resurrected, ierr
		}
		if info.ModTime().After(cut) {
			continue
		}
		h := e.Name()
		tp := filepath.Join(dir, h)
		if !hashRe.MatchString(h) {
			if rerr := os.Remove(tp); rerr != nil {
				return swept, resurrected, rerr
			}
			continue // 非法文件名：垃圾直接清
		}
		if keep(h) {
			p, perr := s.path(h)
			if perr != nil {
				return swept, resurrected, perr
			}
			if _, serr := os.Stat(p); serr == nil {
				// blobs/ 已有同哈希（删除后重写）：清 trash 副本即可
				if rerr := os.Remove(tp); rerr != nil {
					return swept, resurrected, rerr
				}
			} else {
				if merr := os.MkdirAll(filepath.Dir(p), 0o755); merr != nil {
					return swept, resurrected, merr
				}
				if rerr := os.Rename(tp, p); rerr != nil {
					return swept, resurrected, rerr
				}
			}
			resurrected++
			continue
		}
		if rerr := os.Remove(tp); rerr != nil {
			return swept, resurrected, rerr
		}
		swept++
	}
	return swept, resurrected, nil
}

// List 返回 blobs/ 下全部 blob 哈希（GC 标记阶段用）。
func (s *Store) List() ([]string, error) {
	root := filepath.Join(s.root, "blobs")
	lvl1, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, d1 := range lvl1 {
		if !d1.IsDir() {
			continue
		}
		lvl2, err := os.ReadDir(filepath.Join(root, d1.Name()))
		if err != nil {
			return nil, err
		}
		for _, d2 := range lvl2 {
			if !d2.IsDir() {
				continue
			}
			files, err := os.ReadDir(filepath.Join(root, d1.Name(), d2.Name()))
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				if !f.IsDir() && hashRe.MatchString(f.Name()) {
					out = append(out, f.Name())
				}
			}
		}
	}
	return out, nil
}
