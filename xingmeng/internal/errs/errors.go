// Package errs 定义存储内核的唯一错误枚举（docs/04 错误语义映射、99-D017）。
// 协议适配层各自维护穷举映射表，禁止私自引入新的错误语义。
package errs

import "errors"

var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrKeyNotFound        = errors.New("key not found")
	ErrUploadNotFound     = errors.New("upload not found")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrIsDirectory        = errors.New("target is a directory")
	ErrIsFile             = errors.New("target is a file")
	ErrDirNotEmpty        = errors.New("directory not empty")
	ErrQuotaExceeded      = errors.New("quota exceeded")
	ErrReadOnly           = errors.New("node is read-only")
	ErrEntityTooLarge     = errors.New("entity too large")
	ErrRangeInvalid       = errors.New("range not satisfiable")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrNotModified        = errors.New("not modified")
	ErrPermissionDenied   = errors.New("permission denied")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrStaleEpoch         = errors.New("stale epoch (fenced, likely old master resurrected)")
	ErrInternal           = errors.New("internal error")
)
