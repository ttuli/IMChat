#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <service_name> [-api|-rpc]"
    echo "Example: $0 message -api"
    exit 1
fi

SERVICE_NAME="$1"
GEN_TYPE="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 提取首字母并大写，拼接剩余部分得到目录名
SERVICE_DIR_NAME="$(echo ${SERVICE_NAME:0:1} | tr '[:lower:]' '[:upper:]')${SERVICE_NAME:1}"
TARGET_DIR="${PROJECT_ROOT}/internal/apps/${SERVICE_DIR_NAME}"

if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: Directory $TARGET_DIR does not exist."
    exit 1
fi

# function to fix api structure
fix_api() {
    cd "${TARGET_DIR}/api" || return
    
    # 清理不应覆盖的生成物 (config, svc)
    [ -d "internal/config" ] && rm -rf internal/config
    [ -d "internal/svc" ] && rm -rf internal/svc
    
    # 覆盖 types
    if [ -d "internal/types" ]; then
        cp -r internal/types/* types/
        rm -rf internal/types
    fi
    
    # 合并 handler
    if [ -d "internal/handler" ]; then
        # routes.go 总是由 goctl 全新生成，应该覆盖
        [ -f "internal/handler/routes.go" ] && cp internal/handler/routes.go handler/routes.go
        
        # 对于新生成的 handler，如果原先不存在则拷贝
        find internal/handler -type f | while read -r file; do
            dest="handler/${file#internal/handler/}"
            mkdir -p "$(dirname "$dest")"
            if [ ! -f "$dest" ]; then
                cp "$file" "$dest"
            fi
        done
        rm -rf internal/handler
    fi
    
    # 替换 import 路径中的 internal/ (适配 handler, logic 等)
    if [ "$(uname)" == "Darwin" ]; then
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/config"|/config"|g' {} +
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/handler|/handler|g' {} +
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/svc"|/svc"|g' {} +
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/types"|/types"|g' {} +
    else
        find . -name "*.go" -type f -exec sed -i 's|/internal/config"|/config"|g' {} +
        find . -name "*.go" -type f -exec sed -i 's|/internal/handler|/handler|g' {} +
        find . -name "*.go" -type f -exec sed -i 's|/internal/svc"|/svc"|g' {} +
        find . -name "*.go" -type f -exec sed -i 's|/internal/types"|/types"|g' {} +
    fi
}

# function to fix rpc structure
fix_rpc() {
    cd "${TARGET_DIR}/rpc" || return
    
    # 清理不应覆盖的生成物 (config, svc)
    [ -d "internal/config" ] && rm -rf internal/config
    [ -d "internal/svc" ] && rm -rf internal/svc
    [ -f "${SERVICE_NAME}.go" ] && rm -rf ${SERVICE_NAME}.go
    
    # 合并 server (覆盖，因为里面只有 xxxserver.go，由 goctl 维护)
    if [ -d "internal/server" ]; then
        cp -r internal/server/* server/
        rm -rf internal/server
    fi

    rm "${SERVICE_NAME}rpc.go"
    
    # logic 中的新文件默认由 goctl 产生（goctl rpc 增量生成 logic 时，不覆盖已有文件）
    
    # 替换 import 路径中的 internal/ (适配 server, logic 等)
    if [ "$(uname)" == "Darwin" ]; then
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/server|/server|g' {} +
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/config"|/config"|g' {} +
        find . -name "*.go" -type f -exec sed -i '' 's|/internal/svc"|/svc"|g' {} +
    else
        find . -name "*.go" -type f -exec sed -i 's|/internal/server|/server|g' {} +
        find . -name "*.go" -type f -exec sed -i 's|/internal/config"|/config"|g' {} +
        find . -name "*.go" -type f -exec sed -i 's|/internal/svc"|/svc"|g' {} +
    fi
}

if [ "$GEN_TYPE" == "-api" ]; then
    echo "生成 API 代码 for ${SERVICE_NAME}..."
    goctl api go -api ${TARGET_DIR}/api/${SERVICE_NAME}.api -dir ${TARGET_DIR}/api/ -style gozero
    fix_api
elif [ "$GEN_TYPE" == "-rpc" ]; then
    echo "生成 RPC 代码 for ${SERVICE_NAME}..."
    goctl rpc protoc ${TARGET_DIR}/rpc/${SERVICE_NAME}.proto -I ${TARGET_DIR} --go_out=${TARGET_DIR}/rpc --go-grpc_out=${TARGET_DIR}/rpc --zrpc_out=${TARGET_DIR}/rpc -m
    fix_rpc
else
    echo "未指定参数，默认执行 API 和 RPC 生成 for ${SERVICE_NAME}..."
    goctl api go -api ${TARGET_DIR}/api/${SERVICE_NAME}.api -dir ${TARGET_DIR}/api/ -style gozero
    fix_api
    goctl rpc protoc ${TARGET_DIR}/rpc/${SERVICE_NAME}.proto -I ${TARGET_DIR} --go_out=${TARGET_DIR}/rpc --go-grpc_out=${TARGET_DIR}/rpc --zrpc_out=${TARGET_DIR}/rpc -m
    fix_rpc
fi
