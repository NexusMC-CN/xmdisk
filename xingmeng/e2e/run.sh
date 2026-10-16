#!/usr/bin/env bash
# 星梦存储 e2e 演练：真容器 1主1从 + rclone/mc 三协议冒烟 + 灾备切换。
# 前置：xmd:e2e 镜像已构建（docker build -f e2e/Dockerfile -t xmd:e2e .）
set -u
export MSYS_NO_PATHCONV=1
NET=xmnet; M=xmd-master; S=xmd-slave
AK=E2EAK001; SK=e2e-secret-0123456789; BKT=e2ebkt
KEK=e2e-kek-0123456789abcdef

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\n[FAIL] %s\n' "$*"; CLEANUP; exit 1; }
CLEANUP() { docker rm -f $M $S >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }

MASTER_CMD='printf "node:\n  role: master\n  data_dir: /var/lib/xingmeng\nlisteners:\n  data_ssh: \"0.0.0.0:9001\"\n" > /xmd.yaml && exec xmd -config /xmd.yaml'
SLAVE_CMD='printf "node:\n  role: slave\n  data_dir: /var/lib/xingmeng\nlisteners:\n  data_ssh: \"0.0.0.0:9001\"\nreplication:\n  master_addr: http://xmd-master:9003\n" > /xmd.yaml && exec xmd -config /xmd.yaml'

CLEANUP
docker network create $NET >/dev/null

say "1. 起主节点"
docker run -d --name $M --network $NET -e XM_KEK=$KEK --entrypoint sh xmd:e2e -c "$MASTER_CMD" >/dev/null || fail "master start"
for i in $(seq 1 30); do docker exec $M curl -sf localhost:9002/health >/dev/null 2>&1 && break; sleep 1; done
docker exec $M curl -sf localhost:9002/health >/dev/null || fail "master health"

say "2. 建桶建凭据（admin API）"
docker exec $M curl -s -XPOST localhost:9002/buckets -d "{\"name\":\"$BKT\"}" | grep -q "$BKT" || fail "create bucket"
docker exec $M curl -s -XPOST localhost:9002/creds -d "{\"access_key\":\"$AK\",\"secret\":\"$SK\",\"bucket\":\"$BKT\"}" | grep -q "$AK" || fail "create cred"

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
  [ "\$A" = "\$B" ]" || fail "mc smoke"

say "4. rclone 三协议冒烟（S3/WebDAV/SFTP，含 ETag 对账）"
OBSC=$(docker run --rm rclone/rclone obscure "$SK")
docker run --rm --network $NET -v mcdata:/data --entrypoint sh rclone/rclone -c "
  export RCLONE_CONFIG_S3T_TYPE=s3 RCLONE_CONFIG_S3T_ENDPOINT=http://xmd-master:9000          RCLONE_CONFIG_S3T_ACCESS_KEY_ID=$AK RCLONE_CONFIG_S3T_SECRET_ACCESS_KEY=$SK          RCLONE_CONFIG_DAV_TYPE=webdav RCLONE_CONFIG_DAV_URL=http://xmd-master:9000/dav/$BKT          RCLONE_CONFIG_DAV_VENDOR=other RCLONE_CONFIG_DAV_USER=$AK RCLONE_CONFIG_DAV_PASS='$OBSC'          RCLONE_CONFIG_SF_TYPE=sftp RCLONE_CONFIG_SF_HOST=xmd-master RCLONE_CONFIG_SF_PORT=9001          RCLONE_CONFIG_SF_USER=$AK RCLONE_CONFIG_SF_PASS='$OBSC' RCLONE_CONFIG_SF_KNOWN_HOSTS_FILE=none   && mkdir -p /src && cp /data/small.bin /data/big.bin /src/   && rclone copy /src s3t:$BKT/rclone-dir/ && rclone check /src s3t:$BKT/rclone-dir/ --one-way   && rclone copy /src dav:rclone-dir/ && rclone check /src dav:rclone-dir/ --one-way   && rclone copy /src sf:/rclone-dir/ && rclone check /src sf:/rclone-dir/ --one-way" || fail "rclone smoke"

say "5. 起从节点并等待追平"
docker run -d --name $S --network $NET -e XM_KEK=$KEK --entrypoint sh xmd:e2e -c "$SLAVE_CMD" >/dev/null || fail "slave start"
for i in $(seq 1 60); do
  ML=$(docker exec $M curl -s localhost:9002/status | grep -o '"last_seq":[0-9]*' | cut -d: -f2)
  SL=$(docker exec $S curl -s localhost:9002/status | grep -o '"last_seq":[0-9]*' | cut -d: -f2)
  [ -n "$ML" ] && [ "$ML" = "$SL" ] && [ "$ML" != "0" ] && break; sleep 1
done
[ "$ML" = "$SL" ] && [ "$ML" != "0" ] || fail "slave catch up (master=$ML slave=$SL)"
docker exec $S curl -s -XPOST localhost:9002/verify -d '{"limit":100}' | grep -q '"Failures":null\|"failures":null\|"failures":\[\]' || fail "slave verify"

say "6. 灾备切换（runbook A0-A6）：主离线 → 停同步 → promote → 开 rw"
docker stop $M >/dev/null || fail "kill master"
docker exec $S curl -s -XPOST localhost:9002/sync -d '{"action":"stop"}' | grep -q stop || fail "sync stop"
docker exec $S curl -s -XPOST localhost:9002/promote | grep -q '"epoch":2' || fail "promote epoch=2"
docker exec $S curl -s -XPOST localhost:9002/serve -d '{"mode":"rw"}' | grep -q rw || fail "serve rw"

say "7. 新主读写（客户端切换回源，ETag 对账）"
docker run --rm --network $NET -v mcdata:/data --entrypoint sh minio/mc -c "
  mc alias set xm2 http://xmd-slave:9000 $AK $SK >/dev/null   && mc stat xm2/$BKT/big.bin >/dev/null   && mc cp /data/small.bin xm2/$BKT/post-promote.bin >/dev/null" || fail "mc post-promote"
docker run --rm --network $NET -v mcdata:/data --entrypoint sh rclone/rclone -c "
  export RCLONE_CONFIG_NM_TYPE=s3 RCLONE_CONFIG_NM_ENDPOINT=http://xmd-slave:9000          RCLONE_CONFIG_NM_ACCESS_KEY_ID=$AK RCLONE_CONFIG_NM_SECRET_ACCESS_KEY=$SK   && mkdir -p /src && cp /data/small.bin /data/big.bin /src/   && rclone check /src nm:$BKT/rclone-dir/ --one-way   && rclone copy nm:$BKT/big.bin /data/check-new/   && rclone check /src nm:$BKT/rclone-dir/ --one-way" || fail "rclone post-promote"

say "8. 收尾"
docker exec $S curl -s localhost:9002/status | grep -q '"role":"master"' || fail "final role"
CLEANUP
printf '\n[PASS] e2e 演练全部通过：-race + 符号链接真跑（构建期）+ 三协议真实客户端 + 灾备切换全链路\n'
