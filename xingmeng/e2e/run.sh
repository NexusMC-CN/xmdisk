#!/usr/bin/env bash
# 星梦存储 e2e 演练：真容器 1主1从 + rclone/mc 三协议冒烟 + 灾备切换。
# 前置：xmd:e2e 镜像已构建（docker build -f e2e/Dockerfile -t xmd:e2e .）
# D023 形态：集群参数全 env（XM_*），yaml 仅保留无 env 对应面的监听口；
# admin 面必鉴权（X-Xm-Token），repl 面凭证 XM_REPL_TOKEN。
set -u
export MSYS_NO_PATHCONV=1
NET=xmnet; M=xmd-master; S=xmd-slave
AK=E2EAK001; SK=e2e-secret-0123456789; BKT=e2ebkt
KEK=e2e-kek-0123456789abcdef
ADM=e2e-adm-tok-0123456789
RTOK=e2e-repl-tok-0123456789

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\n[FAIL] %s\n' "$*"; CLEANUP; exit 1; }
CLEANUP() { docker rm -f $M $S >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }

# 仅监听口走 yaml（端口属配置文件面）；角色/凭证/数据目录全 env（99-D023）
LISTENERS_YAML='printf "listeners:\n  data_ssh: \"0.0.0.0:9001\"\n" > /xmd.yaml && exec xmd -config /xmd.yaml'

CLEANUP
docker network create $NET >/dev/null

say "1. 起主节点（env 出生初值：role=master + 双必填 token + repl token）"
docker run -d --name $M --network $NET \
  -e XM_KEK=$KEK -e XM_ADMIN_TOKEN=$ADM -e XM_REPL_TOKEN=$RTOK \
  -e XM_DATA_DIR=/var/lib/xingmeng \
  --entrypoint sh xmd:e2e -c "$LISTENERS_YAML" >/dev/null || fail "master start"
for i in $(seq 1 30); do docker exec $M curl -sf -H "X-Xm-Token: $ADM" localhost:9002/health >/dev/null 2>&1 && break; sleep 1; done
docker exec $M curl -sf -H "X-Xm-Token: $ADM" localhost:9002/health >/dev/null || fail "master health"
docker exec $M curl -s -H "X-Xm-Token: $ADM" localhost:9002/status | grep -q '"repl_listening":true' || fail "repl listening"
# 管理页可达（登录页免凭证可见）
docker exec $M curl -sf localhost:9002/ | grep -q 星梦存储 || fail "webui served"

say "2. 建桶建凭据（admin API，会话 token 亦可——这里直用 admin token）"
docker exec $M curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/buckets -d "{\"name\":\"$BKT\"}" | grep -q "$BKT" || fail "create bucket"
docker exec $M curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/creds -d "{\"access_key\":\"$AK\",\"secret\":\"$SK\",\"bucket\":\"$BKT\"}" | grep -q "$AK" || fail "create cred"
# 双 token：admin token 换 1 小时会话，会话调用 /status 成功
docker exec $M sh -c "curl -s -H 'X-Xm-Token: $ADM' -XPOST localhost:9002/auth/session -d '{\"ttl_hours\":1}' | grep -o '\"token\":\"[a-f0-9]*' | cut -d'\"' -f4 > /tmp/sess" || fail "issue session"
docker exec $M sh -c "curl -sf -H \"X-Xm-Session: \$(cat /tmp/sess)\" localhost:9002/status >/dev/null" || fail "session works"

say "3. mc 冒烟（真 S3 客户端：小文件 + 100MB multipart + 内容比对）"
docker run --rm --network $NET -v mcdata:/data --entrypoint sh minio/mc -c "
  mc alias set xm http://xmd-master:9000 $AK $SK >/dev/null &&
  dd if=/dev/urandom of=/data/small.bin bs=1k count=64 2>/dev/null &&
  dd if=/dev/urandom of=/data/big.bin bs=1M count=100 2>/dev/null &&
  mc cp /data/small.bin xm/$BKT/ &&
  mc cp /data/big.bin xm/$BKT/ &&
  mc stat xm/$BKT/big.bin >/dev/null &&
  A=\$(md5sum < /data/small.bin | cut -d\  -f1) &&
  B=\$(mc cat xm/$BKT/small.bin | md5sum | cut -d\  -f1) &&
  [ \"\$A\" = \"\$B\" ]" || fail "mc smoke"

say "4. rclone 三协议冒烟（S3/WebDAV/SFTP，含 ETag 对账）"
OBSC=$(docker run --rm rclone/rclone obscure "$SK")
docker run --rm --network $NET -v mcdata:/data --entrypoint sh rclone/rclone -c "
  export RCLONE_CONFIG_S3T_TYPE=s3 RCLONE_CONFIG_S3T_ENDPOINT=http://xmd-master:9000          RCLONE_CONFIG_S3T_ACCESS_KEY_ID=$AK RCLONE_CONFIG_S3T_SECRET_ACCESS_KEY=$SK          RCLONE_CONFIG_DAV_TYPE=webdav RCLONE_CONFIG_DAV_URL=http://xmd-master:9000/dav/$BKT          RCLONE_CONFIG_DAV_VENDOR=other RCLONE_CONFIG_DAV_USER=$AK RCLONE_CONFIG_DAV_PASS='$OBSC'          RCLONE_CONFIG_SF_TYPE=sftp RCLONE_CONFIG_SF_HOST=xmd-master RCLONE_CONFIG_SF_PORT=9001          RCLONE_CONFIG_SF_USER=$AK RCLONE_CONFIG_SF_PASS='$OBSC' RCLONE_CONFIG_SF_KNOWN_HOSTS_FILE=none   && mkdir -p /src && cp /data/small.bin /data/big.bin /src/   && rclone copy /src s3t:$BKT/rclone-dir/ && rclone check /src s3t:$BKT/rclone-dir/ --one-way   && rclone copy /src dav:rclone-dir/ && rclone check /src dav:rclone-dir/ --one-way   && rclone copy /src sf:/rclone-dir/ && rclone check /src sf:/rclone-dir/ --one-way   && rclone mkdir sf:/mark-del" || fail "rclone smoke"

say "4b. 目录标记删除回归（SFTP 建目录 → S3 协议删 → 确认消失；修复：DELETE 目录键曾回 404 被当成功）"
docker run --rm --network $NET --entrypoint sh minio/mc -c "
  mc alias set xm http://xmd-master:9000 $AK $SK >/dev/null
  mc stat xm/$BKT/mark-del/ >/dev/null &&
  mc rm xm/$BKT/mark-del/ >/dev/null &&
  ! mc stat xm/$BKT/mark-del/ >/dev/null 2>&1" || fail "dir marker delete"

say "5. 起从节点（env：XM_ROLE=slave + XM_MASTER_ADDR；repl token 同值）并等待追平"
docker run -d --name $S --network $NET \
  -e XM_KEK=$KEK -e XM_ADMIN_TOKEN=$ADM -e XM_REPL_TOKEN=$RTOK \
  -e XM_ROLE=slave -e XM_MASTER_ADDR=http://xmd-master:9003 \
  -e XM_DATA_DIR=/var/lib/xingmeng \
  --entrypoint sh xmd:e2e -c "$LISTENERS_YAML" >/dev/null || fail "slave start"
for i in $(seq 1 60); do
  ML=$(docker exec $M curl -s -H "X-Xm-Token: $ADM" localhost:9002/status | grep -o '"last_seq":[0-9]*' | cut -d: -f2)
  SL=$(docker exec $S curl -s -H "X-Xm-Token: $ADM" localhost:9002/status | grep -o '"last_seq":[0-9]*' | cut -d: -f2)
  [ -n "$ML" ] && [ "$ML" = "$SL" ] && [ "$ML" != "0" ] && break; sleep 1
done
[ "$ML" = "$SL" ] && [ "$ML" != "0" ] || fail "slave catch up (master=$ML slave=$SL)"
docker exec $S curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/verify -d '{"limit":100}' | grep -q '"Failures":null\|"failures":null\|"failures":\[\]' || fail "slave verify"

say "6. 灾备切换（runbook A0-A6）：主离线 → 停同步 → promote → 开 rw"
docker stop $M >/dev/null || fail "kill master"
docker exec $S curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/sync -d '{"action":"stop"}' | grep -q stop || fail "sync stop"
docker exec $S curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/promote | grep -q '"epoch":2' || fail "promote epoch=2"
docker exec $S curl -s -H "X-Xm-Token: $ADM" -XPOST localhost:9002/serve -d '{"mode":"rw"}' | grep -q rw || fail "serve rw"
# 升主后接管复制面（promote 联动 repl 监听）
docker exec $S curl -s -H "X-Xm-Token: $ADM" localhost:9002/status | grep -q '"repl_listening":true' || fail "promoted repl listening"

say "7. 新主读写（客户端切换回源，ETag 对账）"
docker run --rm --network $NET -v mcdata:/data --entrypoint sh minio/mc -c "
  mc alias set xm2 http://xmd-slave:9000 $AK $SK >/dev/null   && mc stat xm2/$BKT/big.bin >/dev/null   && mc cp /data/small.bin xm2/$BKT/post-promote.bin >/dev/null" || fail "mc post-promote"
docker run --rm --network $NET -v mcdata:/data --entrypoint sh rclone/rclone -c "
  export RCLONE_CONFIG_NM_TYPE=s3 RCLONE_CONFIG_NM_ENDPOINT=http://xmd-slave:9000          RCLONE_CONFIG_NM_ACCESS_KEY_ID=$AK RCLONE_CONFIG_NM_SECRET_ACCESS_KEY=$SK   && mkdir -p /src && cp /data/small.bin /data/big.bin /src/   && rclone check /src nm:$BKT/rclone-dir/ --one-way   && rclone copy nm:$BKT/big.bin /data/check-new/   && rclone check /src nm:$BKT/rclone-dir/ --one-way" || fail "rclone post-promote"

say "8. 收尾"
docker exec $S curl -s -H "X-Xm-Token: $ADM" localhost:9002/status | grep -q '"role":"master"' || fail "final role"
CLEANUP
printf '\n[PASS] e2e 演练全部通过：-race + 三协议真实客户端 + 灾备切换 + D023 新语义（env 出生/repl token/双 token/升主接管复制面）\n'
