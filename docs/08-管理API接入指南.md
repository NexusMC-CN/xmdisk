# 08 · 管理 API 接入指南

> 面向使用者/运维的实操手册：如何接入 admin 面（:9002），完成鉴权、巡检、桶与凭据管理、集群运维与灾备切换。
> 接口语义权威定义见 [04-接口定义](04-接口定义.md)；本文是"怎么用"的展开，示例均来自真实集群（三节点实测 2026-10-23）。

## 1. 前置与安全边界

- admin 面默认监听 `0.0.0.0:9002`（yaml `listeners.admin`；裸机建议改 `127.0.0.1:9002` + 反代）。
- **长期凭证**：`XM_ADMIN_TOKEN` 环境变量（≥16 字符，不达标拒绝启动，99-D027）。全部接口要求鉴权（除 WebUI 登录页与静态资源）。
- **本服务不内置 TLS**（99-D028）：跨网络调用 admin 面请套反代或隧道；同机/内网可直连。
- admin 面的定位是**基础设施运维面**：桶生命周期、凭据轮换、角色切换都在这里；数据读写走 S3/SFTP/WebDAV 三个数据面（见 01/04）。

以下用 `$ADM` 代表 admin token，`$H` 代表 `http://<节点>:9002`。

## 2. 鉴权：长期 token 与会话 token

**方式一：长期 token 直用**（脚本/巡检）——所有接口带头：

```bash
curl -s -H "X-Xm-Token: $ADM" $H/status
```

**方式二：会话 token**（人机交互，WebUI 即此流程）——用长期 token 换 1–24 小时会话，之后只带会话（长期 token 不再出网络）：

```bash
# 签发（ttl_hours ∈ [1,24]）
curl -s -H "X-Xm-Token: $ADM" -XPOST $H/auth/session -d '{"ttl_hours":8}'
# → {"expires_at":1792723253,"expires_iso":"2026-10-23T22:00:53+08:00","token":"6f2a…hex…"}
#   （expires_at 为 Unix 秒；expires_iso 为 RFC3339 可读格式）

# 使用（全部业务接口同 X-Xm-Session）
curl -s -H "X-Xm-Session: 6f2a…hex…" $H/status

# 登出（自吊销）/ 全清（仅长期 token 可调，吊销全部会话）
curl -s -H "X-Xm-Session: 6f2a…" -XDELETE $H/auth/session
curl -s -H "X-Xm-Token: $ADM"    -XDELETE $H/auth/sessions
```

会话仅存于节点内存，重启即全部失效。鉴权失败固定延迟 250ms（抗爆破，S-20）。

## 3. 快速巡检（三条命令）

```bash
curl -sf -H "X-Xm-Token: $ADM" $H/health   # 存活 + KEK/密文健康
curl -s  -H "X-Xm-Token: $ADM" $H/status   # 角色/serve/epoch/seq/桶用量
curl -s  -H "X-Xm-Token: $ADM" $H/metrics  # Prometheus 文本
```

`/health` 关键字段：`sealed_secrets`（凭据密文总数）、`sealed_secrets_unreadable`（KEK 变更后待轮换数，>0 时逐桶 rotate 即恢复，99-D009）。

## 4. 端点手册

### 4.1 节点状态

| 端点 | 说明 |
|---|---|
| `GET /status` | 节点运行态 + 全桶用量；`role_mismatch` 为软提醒（env 初值≠运行角色，promote 后属正常） |
| `GET /health` | 存活探测（无凭据语义负载） |
| `GET /metrics` | Prometheus 指标（§6） |

```jsonc
// GET /status（主节点示例）
{"buckets":[{"Name":"wanbkt","Quota":null,"Used":39976976,"Pending":0,"Objects":11,
             "Public":false,"AccessKey":"xm988d5691790c773dcfbc","SecretOK":true}],
 "node":{"role":"master","serve":"rw","epoch":2,"last_seq":18,
         "sync_running":false,"repl_listening":true,
         "kek_configured":true,"role_mismatch":true,"public_endpoint":"http://…:9000"}}
// 从节点另有 "sync_addr"（当前同步主地址，空值省略）与 "configured_role"；
// 主节点另有 "repl_token_configured"/"repl_token_runtime"/"max_epoch_in_log" 等字段。
```

### 4.2 数据面开关

```bash
curl -s -H "X-Xm-Token: $ADM" -XPOST $H/serve -d '{"mode":"rw"}'   # off | ro | rw
# → {"serve":"rw"}
```

slave 恒为 off；promote 后默认 ro，业务冒烟通过后显式开 rw。

### 4.3 桶与凭据（桶=租户=一把钥匙，99-D025）

| 端点 | 说明 |
|---|---|
| `GET /buckets` | 全桶列表（含 AK、用量、公共读标志、`SecretOK`；false=密文不可读需轮换） |
| `POST /buckets` | 建桶**即发钥**：AK/SK 服务端生成，Secret 仅此一次返回 |
| `DELETE /buckets/{name}` | 删桶（须空；同事务级联删凭据）；204 |
| `PATCH /buckets/{name}` | 改配置：字段**存在性**区分语义——`quota` 缺席不动/显式 `null` 解除/正整数生效；`public` 布尔；两者皆缺席 400 |
| `POST /buckets/{name}/rotate` | 重置密钥：保 AK 换 SK（无钥桶自愈补发 AK+SK） |

```bash
curl -s -H "X-Xm-Token: $ADM" -XPOST $H/buckets -d '{"name":"wanbkt","public":false,"quota":null}'
# → {"access_key":"xm988d5691790c773dcfbc","name":"wanbkt","public":false,
#    "quota":null,"secret":"c56c45daf72a…"}        # ← secret 只出现这一次

curl -s -H "X-Xm-Token: $ADM" -XPOST $H/buckets/wanbkt/rotate
# → {"access_key":"xm988d5691790c773dcfbc","bucket":"wanbkt","secret":"<新SK>"}

curl -s -H "X-Xm-Token: $ADM" -XPATCH $H/buckets/wanbkt -d '{"quota":107374182400}'
```

桶增删只在 admin 面——数据面（S3/WebDAV/SFTP）对桶操作返回 405。凭据冻结=rotate 到不外发的随机值。配额按逻辑字节计（含 multipart 在途）。

### 4.4 同步与灾备（从节点侧）

| 端点 | 说明 |
|---|---|
| `POST /sync` | `{"action":"stop"}` 或 `{"action":"start","master_addr":"http://主:9003"}`；start 的地址持久化，**stop/start 循环与重启都不回落旧配置**（换主必须显式传地址） |
| `POST /promote` | 前置校验：role=slave、sync 已停。执行：epoch+1、接管复制面、serve=ro |

```bash
curl -s -H "X-Xm-Token: $ADM" -XPOST $H/sync -d '{"action":"start","master_addr":"http://172.16.42.209:9003"}'
# → {"sync":"start"}
curl -s -H "X-Xm-Token: $ADM" -XPOST $H/promote
# → {"role":"master","epoch":2,"serve":"ro"}
```

### 4.5 集群观察与维护（主节点侧）

| 端点 | 说明 |
|---|---|
| `GET /slaves` | 注册从节点视图：`ID/LastAcked/Lag/Epoch/LastSeenNs/Active`。runbook A4/B2 的 lag→0 验证入口。`Active=false`（活跃窗口外）= 僵尸注册，不阻塞 GC/截断 |
| `POST /snapshot` | 强制一轮元数据快照（返回 path/seq） |
| `POST /gc` | 手动一轮标记-清扫，**联动 oplog 截断**（`TruncatedOps` 字段；与后台循环同策略：须全部活跃从已 ack 且过保留期；最新一条 op 永远保留作 seq 水位线） |
| `POST /verify` | 抽样校验 `{"limit":N}`：条目+blob 哈希比对，返回失败清单 |

```jsonc
// GET /slaves（主节点示例；一从追平、一从滞后）
{"slaves":[
  {"ID":"instance-jk8z6f8S","LastAcked":22,"Lag":0,"Epoch":2,"LastSeenNs":1792735252…,"Active":true},
  {"ID":"instance-uoUmRgFV","LastAcked":18,"Lag":4,"Epoch":1,"LastSeenNs":1792735045…,"Active":true}]}

// POST /gc
{"EntriesScanned":10,"UploadsAborted":0,"Trashed":0,"Swept":0,"Resurrected":0,
 "GateOpen":true,"MarkSeq":22,"TruncatedOps":20}
// GateOpen=false = 有活跃从滞后，清扫暂缓（等待追平）；Trashed/Swept 为本轮垃圾桶计数

// POST /verify {"limit":100}
{"EntriesChecked":10,"BlobsChecked":11,"Failures":null}
```

### 4.6 集群配置

| 端点 | 说明 |
|---|---|
| `POST /repl/token` | `{"token":"…"}`（≥8 字符）运行时设置复制面凭证；KEK 封存持久化，优先于 env。同步运行中会附 `hint` 提示先停后启 |
| `DELETE /repl/token` | 清除运行时值（回落 env 初值） |

## 5. 运维剧本（curl 序列）

### 灾备切换（主 M 宕机，场景 A；决策永远由人做）

```bash
# A1 在各从节点逐台看状态，选 last_seq 最大者 C：
curl -s -H "X-Xm-Token: $ADM" http://C:9002/status | grep last_seq
# A2 停 C 的同步；A3 提升；A4 其余从逐台重指 C（内部地址 9003）：
curl -s -H "X-Xm-Token: $ADM" -XPOST http://C:9002/sync    -d '{"action":"stop"}'
curl -s -H "X-Xm-Token: $ADM" -XPOST http://C:9002/promote
curl -s -H "X-Xm-Token: $ADM" -XPOST http://D:9002/sync -d '{"action":"start","master_addr":"http://C内网:9003"}'
# A4 验证（在 C 上）：逐台出现且 lag→0
curl -s -H "X-Xm-Token: $ADM" http://C:9002/slaves
# A6 业务冒烟通过后恢复读写：
curl -s -H "X-Xm-Token: $ADM" -XPOST http://C:9002/serve -d '{"mode":"rw"}'
```

### 新从节点接入（扩容/重建）

节点以 `XM_ROLE=slave` + `XM_MASTER_ADDR=http://主:9003` 出生即自动快照引导；blob 目录保留时内容寻址只补缺失部分。接入后在主节点 `GET /slaves` 看 lag 归零。

### 日常巡检

`/health`（密文健康）→ `/slaves`（复制滞后）→ `/metrics`（趋势告警）→ 定期 `POST /verify {"limit":100}`。

## 6. 指标清单（GET /metrics）

| 指标 | 说明 |
|---|---|
| `xm_node_role` | 1=master 0=slave |
| `xm_node_serve` | 0=off 1=ro 2=rw |
| `xm_node_epoch` | 当前 op 代 |
| `xm_oplog_last_seq` / `xm_oplog_first_seq` | 日志尾/首序号——**两者之差即保留窗口**，first 持续抬高=截断在工作。first_seq 仅在 oplog 非空时输出 |
| `xm_sync_running` | 从节点同步循环是否运行 |
| `xm_repl_listening` | 复制面是否监听 |
| `xm_slave_acked{slave}` / `xm_slave_lag{slave}` / `xm_slave_active{slave}` | 主节点侧各从复制进度/滞后/活跃。无注册从节点时不输出 |
| `xm_bucket_used_bytes / pending_bytes / objects / public{bucket}` | 桶用量（配额口径） |

## 7. 错误语义

- 业务错误统一 `{"error":"<errs 枚举文案>…"}`；参数/状态类（not found、already exists、quota、readonly、dir not empty、invalid argument）为 **400**，其余 500（固定文案，不泄露内部路径）。
- 前置校验类：非 master 调主节点专属端点 → 503 `node is not master`；sync 未停调 promote → 400（文案说明原因）；已运行再 start → 400 `sync already running`。
- 完整错误枚举与各协议映射见 [04-接口定义](04-接口定义.md)。

## 8. 变更历史

- 2026-10-23 初版（集群实测轮）：新增 `GET /slaves`、`POST /gc` 联动截断（`TruncatedOps`）、`xm_oplog_first_seq`/`xm_slave_*` 指标；`/sync` stop/start 地址粘性语义明确。

- 2026-10-26 终检校准（Docker Hub v0.1.2 实测）：rotate 响应补 `bucket`；会话响应补 `expires_iso`；/status 主节点示例去掉空 `sync_addr`（omitempty）；指标输出条件注记。
