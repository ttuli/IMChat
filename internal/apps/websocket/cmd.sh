#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="websocket"

echo "WebSocket 服务不需要代码生成，使用 go build 编译即可"
echo "编译命令: cd ${SCRIPT_DIR}/../../.. && go build -o bin/${SERVICE_NAME} ./cmd/${SERVICE_NAME}"
