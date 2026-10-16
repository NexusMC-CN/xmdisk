# 星梦存储 e2e：test 阶段跑 -race（容器内有 CGO），build 阶段产静态二进制。
# 构建：docker build -f e2e/Dockerfile --target test -t xmd-test .
#       docker build -f e2e/Dockerfile -t xmd:e2e .   （默认=runtime）
FROM golang:1.26-alpine AS test
ENV GOPROXY=https://goproxy.cn,direct CGO_ENABLED=1
RUN apk add --no-cache build-base
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go vet ./...
RUN go test -race -count=1 ./...

FROM golang:1.26-alpine AS build
ENV GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /xmd ./cmd/xmd

FROM alpine:3.20 AS runtime
RUN apk add --no-cache curl ca-certificates
COPY --from=build /xmd /usr/local/bin/xmd
ENTRYPOINT ["xmd"]
