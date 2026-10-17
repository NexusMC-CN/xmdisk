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
| `internal/config` | YAML 配置（友好单位）+ env 初值覆盖 + node-state 运行态 | 05 |
| `internal/node` | 节点运行时：role/serve/sync 状态机、promote 护栏、repl 监听生命周期 | 03 |
| `internal/api` | admin 路由组（双 token 会话/状态/开关/灾备/桶/凭据/集群配置/metrics） | 04 |
| `internal/webui` | 内嵌管理页（go:embed 单页，admin 端口同源下发） | 04 |
| `internal/s3` | S3 适配层（SigV4、全端点） | 01 |
| `internal/sftp` | SFTP 适配层（AK/SK 认证绑桶、临时文件+限流） | 01 |
| `internal/webdav` | WebDAV 适配层（/dav/<bucket>、MemLS 锁、递归删除） | 01 |
| `cmd/xmd` | 入口：配置（可选文件 + XM_* env）→ 节点 → 监听面组装 | 05 |

依赖方向单向：`协议层 → store → meta/blob → errs`。
底层包永不 import 协议层；升级鉴权或协议兼容只动上层包。

## 开发

```sh
go vet ./...
go test ./...
```

## 运行（docker，99-D023 发行形态）

```sh
# 单节点：两个必填 env，浏览器打开 http://127.0.0.1:9002 即管理页
docker run -d -p 9000:9000 -p 9001:9001 -p 127.0.0.1:9002:9002 \
  -v xmd-data:/var/lib/xmd \
  -e XM_KEK=<长随机串> -e XM_ADMIN_TOKEN=<长随机串> xmd:latest
```

集群、env 清单与安全基线见 docs/05。镜像构建（先交叉编译再打镜像）：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/xmd ./cmd/xmd
docker build -t xmd:latest .
```

## 当前状态

存储内核 + GC + 复制 + 凭据 + 三协议适配层（S3/SFTP/WebDAV）全部就绪；
v1 里程碑（含 Docker 二进制级 e2e：-race、真实客户端冒烟、灾备演练）已完成。
v2 管理与发行面（99-D023）：env 出生初值 + 运行态优先、双 token 会话、
repl token 运行时设置（空 = 不监听复制面）、内嵌 WebUI、docker 拉起即用。
已知缺口见 docs/06。

