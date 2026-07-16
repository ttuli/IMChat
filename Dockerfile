# syntax=docker/dockerfile:1
#
# IM2 全服务共用构建文件（由 docker-bake.hcl 驱动）:
#   builder 阶段一次性编译全部服务，整个依赖图只编译一次；
#   bake 并行构建各服务镜像时，builder 阶段在所有目标间自动去重。

FROM golang:1.24.4-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git
ENV GO111MODULE=on GOPROXY=https://goproxy.io,direct \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64

# 依赖单独成层: go.mod/go.sum 不变时整层命中缓存，跳过下载
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# 顺序编译共享 Go 构建缓存，公共依赖只编译一次；产物名 = 服务名 = 镜像名
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/idgen-rpc   ./cmd/Idgen/rpc   && \
    go build -ldflags="-s -w" -o /out/user-rpc    ./cmd/User/rpc    && \
    go build -ldflags="-s -w" -o /out/auth-rpc    ./cmd/Auth/rpc    && \
    go build -ldflags="-s -w" -o /out/group-rpc   ./cmd/Group/rpc   && \
    go build -ldflags="-s -w" -o /out/message-rpc ./cmd/Message/rpc && \
    go build -ldflags="-s -w" -o /out/llm-rpc     ./cmd/Llm/rpc     && \
    go build -ldflags="-s -w" -o /out/user-api    ./cmd/User/api    && \
    go build -ldflags="-s -w" -o /out/auth-api    ./cmd/Auth/api    && \
    go build -ldflags="-s -w" -o /out/group-api   ./cmd/Group/api   && \
    go build -ldflags="-s -w" -o /out/message-api ./cmd/Message/api && \
    go build -ldflags="-s -w" -o /out/file-api    ./cmd/File/api    && \
    go build -ldflags="-s -w" -o /out/llm-api     ./cmd/Llm/api     && \
    go build -ldflags="-s -w" -o /out/websocket   ./cmd/websocket

FROM alpine:latest AS runtime
RUN apk add --no-cache tzdata ca-certificates
WORKDIR /app
# SERVICE: 服务名(=二进制名)；ETC_DIR: 配置目录（相对仓库根），均由 bake 传入
ARG SERVICE
ARG ETC_DIR
COPY --from=builder /out/${SERVICE} /app/server
COPY --from=builder /app/${ETC_DIR} /app/etc

ENTRYPOINT ["/app/server", "-f", "/app/etc/config.yaml"]
