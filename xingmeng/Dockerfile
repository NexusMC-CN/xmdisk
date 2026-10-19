# 星梦存储生产镜像（99-D023 docker 发行面）：预编译静态二进制 + alpine。
# 无网络构建步骤——先在宿主机交叉编译，再以本目录为上下文构建镜像：
#   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/xmd ./cmd/xmd
#   docker build -t xmd:latest .
# 运行（单节点最小形态，必填两项）：
#   docker run -d -p 9000:9000 -p 9001:9001 -p 127.0.0.1:9002:9002 \
#     -v xmd-data:/var/lib/xmd \
#     -e XM_KEK=<长随机串> -e XM_ADMIN_TOKEN=<长随机串> xmd:latest
# 集群从节点：加 -e XM_ROLE=slave -e XM_REPL_TOKEN=<主从同值> \
#     -e XM_MASTER_ADDR=http://<主>:9003 -p 9003:9003（主）
# 公共读面（99-D024）：-p 9004:9004，仅对开公共读的桶匿名可读；反代公共域名指向它
# 默认监听 0.0.0.0 的 9000(S3/WebDAV) 9001(SFTP) 9002(admin/WebUI) 9003(repl) 9004(公共读)；
# admin/repl 面不建议直接发布到公网（docs/05 安全基线）。
FROM alpine:3.20
ENV XM_DATA_DIR=/var/lib/xmd
VOLUME /var/lib/xmd
COPY dist/xmd /usr/local/bin/xmd
COPY xmd.docker.yaml /xmd.yaml
EXPOSE 9000 9001 9002 9003 9004
# busybox wget 即可；admin 面全端点鉴权（99-D023），健康检查须带凭证
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD wget -q -O /dev/null --header "X-Xm-Token: ${XM_ADMIN_TOKEN}" http://127.0.0.1:9002/health || exit 1
ENTRYPOINT ["xmd"]
