FROM golang:1.24.4-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git tzdata gcc musl-dev
ENV GO111MODULE=on GOPROXY=https://goproxy.io,direct
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /app/bin/server ./cmd/Group/rpc

FROM alpine:latest
RUN apk update --no-cache && apk add --no-cache tzdata ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/server /app/server
COPY --from=builder /app/internal/apps/Group/rpc/etc /app/etc
# Group RPC port

ENTRYPOINT ["/app/server", "-f", "/app/etc/config.yaml"]
