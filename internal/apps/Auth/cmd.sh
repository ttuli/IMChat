#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="auth"

if [ "$1" == "-api" ]; then
    echo "生成 API 代码..."
    goctl api go -api ${SCRIPT_DIR}/api/${SERVICE_NAME}.api -dir ${SCRIPT_DIR}/api/ -style gozero
elif [ "$1" == "-rpc" ]; then
    echo "生成 RPC 代码..."
    goctl rpc protoc ${SCRIPT_DIR}/rpc/${SERVICE_NAME}.proto -I ${SCRIPT_DIR} --go_out=${SCRIPT_DIR}/rpc --go-grpc_out=${SCRIPT_DIR}/rpc --zrpc_out=${SCRIPT_DIR}/rpc -m
else
    echo "未指定参数，默认执行 API 和 RPC 生成..."
    goctl api go -api ${SCRIPT_DIR}/api/${SERVICE_NAME}.api -dir ${SCRIPT_DIR}/api/ -style gozero
    goctl rpc protoc ${SCRIPT_DIR}/rpc/${SERVICE_NAME}.proto -I ${SCRIPT_DIR} --go_out=${SCRIPT_DIR}/rpc --go-grpc_out=${SCRIPT_DIR}/rpc --zrpc_out=${SCRIPT_DIR}/rpc -m
fi