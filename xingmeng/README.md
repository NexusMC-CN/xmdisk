# 星梦存储 · 服务端

Go 实现的兼容 S3/SFTP/WebDAV 的自建存储服务。设计文档见 [`../docs/`](../docs/README.md)——
那是开工宪法：代码与文档冲突时，先改文档达成共识，再改代码。

## 分包结构（分包 + 解耦 + 函数连接）

| 包 | 职责 | 文档 |
|---|---|---|
| `internal/errs` | 内部错误枚举（三协议映射的单一来源） | 04 |
| `internal/meta` | 元数据面：SQLite（条目/桶/oplog/凭据/从节点） | 02 |
| `internal/blob` | 数据面：内容寻址 blob 仓库（写/trash/清扫） | 02 |
| `internal/store` | 统一存储接口：组合 meta+blob，配额与 oplog 同事务 | 04 |
| `internal/repl` | 复制（规划中） | 03 |
| `internal/api` | 路由组：admin / s3 / webdav（规划中） | 04 |
| `internal/sftp` | SFTP 适配（规划中） | 01 |

依赖方向单向：`协议层 → store → meta/blob → errs`。
底层包永不 import 协议层；升级鉴权或协议兼容只动上层包。

## 开发

```sh
go vet ./...
go test ./...
```

## 当前状态

存储内核：Put/Get/Stat/Delete/Mkdir/Rmdir/Move/SetAttrs/List、Multipart 五操作、
桶与配额（含 pending 门控）、oplog（同事务）、内容寻址分块去重、readOnly、
GC（标记-清扫 + 从节点 ack 门控 + 超时会话清理）。
复制（repl）：主端 oplog/快照/blob（墓碑感知）/ack/register；从端拉取-回放-补
blob-上报，落后超限自动快照重建（含 blob 全量补齐）；凭据密文复制。
S3 适配层：SigV4（头部 + presign + aws-chunked）、对象全操作、条件请求、Range、
ListV2/v1、multipart 全端点、多对象删除、桶级凭据鉴权、D017 错误映射。
下一步：cmd/main + admin API（serve/sync/promote，灾备 runbook 执行面）→ SFTP → WebDAV → 鉴权补全。
