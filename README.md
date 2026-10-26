# 星梦存储（xmd）

Go 实现的单二进制自建存储服务：单进程同时提供 **S3 / SFTP / WebDAV** 三种协议、内置管理 WebUI，并支持主从异步复制与灾备切换。面向需要自有对象存储的业务平台与个人；不做多租户平台，定位于一块可靠、可审计的存储基础设施。

| 监听面 | 默认端口 | 协议 / 内容 | 鉴权 |
|---|---|---|---|
| 数据面 HTTP | :9000 | S3（path-style）+ WebDAV（`/dav/<bucket>`） | SigV4 / Basic（凭据绑桶） |
| 数据面 SSH | :9001 | SFTP | SSH 密码 = AK/SK |
| 管理面 | :9002 | admin API + WebUI + Prometheus | admin token / 短时会话 |
| 复制面 | :9003 | repl API（主从） | repl token（空 = 不监听） |
| 公共读面 | :9004 | 匿名只读直链（仅公共桶） | 无（只认桶公共标志） |

---

## 使用者入口

适用对象：部署并使用本服务，不参与开发。

### 1. 获取镜像

```bash
docker pull defeatedperson/xmdisk:latest
# 或指定版本：docker pull defeatedperson/xmdisk:v0.1.2
```

镜像仅提供 `linux/amd64`。运行需 Docker 20.10+。

### 2. 启动容器（单节点）

```bash
docker run -d --name xmd \
  -p 9000:9000 -p 9001:9001 -p 127.0.0.1:9002:9002 \
  -v xmd-data:/var/lib/xmd \
  -e XM_KEK=<长度≥16的高熵随机串> \
  -e XM_ADMIN_TOKEN=<长度≥16的高熵随机串> \
  defeatedperson/xmdisk:latest
```

`XM_KEK` 与 `XM_ADMIN_TOKEN` 为必填项，长度不达标时容器拒绝启动。生成建议：`openssl rand -hex 24`。

**端口映射（`-p`）**

| 容器端口 | 用途 | 对公网发布的建议 |
|---|---|---|
| 9000 | S3 + WebDAV 数据面 | 经 TLS 反代后可发布 |
| 9001 | SFTP 数据面 | 建议经隧道/堡垒机访问 |
| 9002 | 管理面（WebUI + admin API） | 不发布；绑定 `127.0.0.1` 或仅内网可达 |
| 9003 | 复制面（主从同步） | 不发布；仅集群节点间内网可达 |
| 9004 | 公共读面（匿名只读直链） | 按需；仅对开启公共读的桶生效 |

> 9001（SFTP）由镜像内置配置启用；使用裸二进制运行时默认不监听，需在 YAML 中设置 `listeners.data_ssh`。

**环境变量（`-e`）**

| 变量 | 必填 | 说明 |
|---|---|---|
| `XM_KEK` | 是 | 凭据密封主密钥。≥16 字符；集群各节点必须同值；丢失后既有凭据密文不可解密，恢复方式为逐桶 rotate（[docs/02](docs/02-存储层设计.md)） |
| `XM_ADMIN_TOKEN` | 是 | 管理面长期凭证。≥16 字符；用于登录 WebUI 与调用 admin API |
| `XM_REPL_TOKEN` | 否 | 复制面凭证，≥8 字符；集群各节点同值；**留空则不监听 9003**（单节点建议留空） |
| `XM_ROLE` | 否 | `master`（默认）/ `slave`；仅作为初始角色，promote 后以运行态为准 |
| `XM_MASTER_ADDR` | 否 | 从节点的初始主地址，如 `http://<主节点内网IP>:9003` |
| `XM_DATA_DIR` | 否 | 数据目录，容器内默认 `/var/lib/xmd` |
| `XM_PUBLIC_ENDPOINT` | 否 | 对外展示的数据面地址（WebUI 连接信息显示用） |
| `XM_WEBUI` | 否 | `on`（默认）/ `off`，内嵌管理页开关 |

**数据卷**：`/var/lib/xmd` 为数据目录，包含元数据库（`meta.db`）与内容寻址 blob 存储。该目录自包含，**即完整的备份单元**，必须持久化（命名卷或绑定挂载）。

自定义监听地址/端口等高级配置：挂载 YAML 后以命令 `xmd -config <路径>` 启动。字段与默认值见 [docs/05](docs/05-部署与配置.md)。

### 3. 初始化

1. 浏览器访问 `http://127.0.0.1:9002`，使用 `XM_ADMIN_TOKEN` 登录（换取 1–24 小时短时会话）；
2. 创建存储桶：服务端在同一事务内创建桶并生成该桶唯一的 AK/SK；**Secret 仅在创建响应中展示一次**，须立即保存；
3. 客户端接入数据面（:9000）：

```bash
mc alias set xm http://<主机>:9000 <AK> <SK>
mc cp ./file.bin xm/<bucket>/
```

SFTP 使用同一 AK/SK（端口 9001，登录后以对应桶为根目录）；WebDAV 路径为 `http://<主机>:9000/dav/<bucket>`（Basic 认证，用户名/密码 = AK/SK）。

### 4. 集群部署（可选）

从节点在单节点参数基础上增加角色与主地址：

```bash
docker run -d --name xmd-slave \
  -p 9000:9000 -p 127.0.0.1:9002:9002 -p 9003:9003 \
  -v xmd-data-slave:/var/lib/xmd \
  -e XM_KEK=<与主节点一致> \
  -e XM_ADMIN_TOKEN=<本节点管理口令> \
  -e XM_ROLE=slave \
  -e XM_REPL_TOKEN=<主从同值> \
  -e XM_MASTER_ADDR=http://<主节点内网地址>:9003 \
  defeatedperson/xmdisk:latest
```

从节点启动后自动完成快照引导并持续追赶主节点。主从切换遵循手动灾备流程（[docs/03](docs/03-复制与灾备.md)），全部管理命令的调用方式见 [docs/08 管理API接入指南](docs/08-管理API接入指南.md)。

### 安全须知

- 本服务不内置 TLS：数据面与公共读面对外发布时必须经由 TLS 反代或隧道（WebDAV Basic 认证在明文信道下会暴露 SK）。
- 管理面（9002）与复制面（9003）不应直接发布至公网。
- 完整安全模型见下文[安全模型](#安全模型)与 [docs/05 安全基线](docs/05-部署与配置.md)。

---

## 开发者入口

适用对象：参与开发、自行构建或审计本服务。

- 分包结构与本地开发：[xingmeng/README.md](xingmeng/README.md)（`go vet ./...` / `go test ./...`）
- 构建官方镜像：`scripts/build-image.ps1`（交叉编译 + alpine 打包，含版本记录；`-Push` 发布至 Docker Hub）
- 端到端演练（真容器 + mc/rclone 三协议 + 灾备切换）：`xingmeng/e2e/run.sh`
- 管理 API 调用手册：[docs/08-管理API接入指南.md](docs/08-管理API接入指南.md)

---

## 特性

- **三协议共享一个命名空间**：S3（SigV4 签名、presigned URL、multipart、ListV2）为主接口；SFTP 服务人工备份场景；WebDAV 提供 `http://<主机>:9000/dav/<bucket>` 访问。mc / aws-cli / rclone / WinSCP / rsync 均可直接使用。
- **桶 = 租户 = 一把钥匙**：建桶即由服务端生成 AK/SK（Secret 仅展示一次，AES-GCM 密封落库）；删桶级联删除凭据；一个桶至多一把钥匙；重置/冻结走桶级 rotate。
- **数据可靠性优先的存储内核**：内容寻址（SHA-256）数据面，fsync 后原子入库；SQLite（WAL + synchronous=FULL）元数据、配额与 oplog 同事务维护；两阶段 GC 在清扫前重验存活；流式配额守卫；抽样对账命令可随时核验磁盘数据。
- **主从异步复制 + 手动灾备**：oplog 全量状态 op 与 epoch fencing 防旧主复活；从节点经快照 + 追赶完成引导；promote 带前置护栏，切换流程 runbook 化（先 ro 冒烟再开 rw）。
- **管理面**：单页暗色 WebUI（状态 / 桶与凭据 / serve 开关 / 同步 / promote / GC / 对账 / 连接信息）+ admin API（双 token 会话、审计日志）+ Prometheus 指标。
- **可选公共读面**：按桶开关的独立只读端口（:9004），同构 404 防枚举，配合反向代理提供文件分发；写与列举操作始终需要凭据。

## 管理页速览

| 概览 | 建桶即发钥（Secret 仅展示一次） |
|---|---|
| ![overview](docs/images/webui-overview.png) | ![secret](docs/images/webui-secret-modal.png) |

| 桶与凭据同页管理 | 运维（serve / 同步 / promote / GC / 对账） |
|---|---|
| ![buckets](docs/images/webui-buckets.png) | ![ops](docs/images/webui-ops.png) |

登录页以 admin token 换取 1–24 小时短时会话，浏览器不长期持有长期凭证：

![login](docs/images/webui-login.png)

## 安全模型

- **传输安全由部署层提供**：本服务不内置 TLS（[ADR D028](docs/99-决策记录.md)）；数据面/公共读面对外须经 TLS 反代或隧道；admin/repl 面不进公网。
- 凭据 Secret 以 AES-256-GCM（KEK 来自环境变量）密封落库，服务端不保存明文；KEK 丢失的恢复路径为逐桶 rotate。
- 授权模型即桶级凭据（有意不提供 IAM/ACL/policy，保持简单可审计）；每桶可设置逻辑字节配额。
- 数据目录自包含（`meta.db` + 内容寻址 blob），整目录即备份单元。

## 文档

设计即文档——本仓库的开发流程以 `docs/` 为开工宪法（代码与文档冲突时先改文档）：

- [01 产品定义与边界](docs/01-产品定义与边界.md) · [02 存储层设计](docs/02-存储层设计.md) · [03 复制与灾备](docs/03-复制与灾备.md)
- [04 接口定义](docs/04-接口定义.md) · [05 部署与配置](docs/05-部署与配置.md) · [06 测试清单](docs/06-测试清单.md)
- [08 管理API接入指南](docs/08-管理API接入指南.md) —— admin 面实操手册
- [99 决策记录（ADR）](docs/99-决策记录.md) —— 每条设计决策的理由与被否方案
- [xingmeng/README.md](xingmeng/README.md) —— 分包结构与开发指南

## License

[Apache-2.0](LICENSE)
