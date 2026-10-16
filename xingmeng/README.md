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
| `internal/repl` | 复制：主端端点 + 从端同步引擎 | 03 |
| `internal/config` | YAML 配置（友好单位）+ node-state 运行态 | 05 |
| `internal/node` | 节点运行时：role/serve/sync 状态机、promote 护栏 | 03 |
| `internal/api` | admin 路由组（状态/开关/灾备/桶/凭据/metrics） | 04 |
| `internal/s3` | S3 适配层（SigV4、全端点） | 01 |
| `internal/sftp` | SFTP 适配层（AK/SK 认证绑桶、临时文件+限流） | 01 |
| `internal/webdav` | WebDAV 适配层（/dav/<bucket>、MemLS 锁、递归删除） | 01 |
| `cmd/xmd` | 入口：配置 → 节点 → 监听面组装 | 05 |

依赖方向单向：`协议层 → store → meta/blob → errs`。
底层包永不 import 协议层；升级鉴权或协议兼容只动上层包。

## 开发

```sh
go vet ./...
go test ./...
```

## 当前状态

存储内核 + GC + 复制 + 凭据 + S3 适配层（见 git log 与 docs/06）。
节点运行时与 admin API：三开关状态机（role/serve/sync）、promote 护栏、
从节点开机自同步、本地 GC、status/health/metrics/verify/snapshot、
桶与凭据管理（含 rotate）、配置（YAML 友好单位）与运行态分离。
入口 `cmd/xmd`：三监听面（data_http/admin/repl）+ 优雅退出。
灾备 runbook 已有机制级自动回归（internal/api TestFailoverRunbook）。
三协议（S3/SFTP/WebDAV）全部就绪。下一步：Docker 二进制级 e2e（-race、aws-cli/rclone/真实客户端冒烟、符号链接测试真跑、runbook 演练）。
