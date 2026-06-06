#!/bin/bash
set -e

# 确保 Go 工具链的 bin 目录在 PATH 中（protoc-go-inject-tag 安装在此处）
export PATH="$PATH:$(go env GOPATH)/bin"

# 获取脚本所在目录及项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Scanning for proto files..."

# 切换到项目根目录（这一步非常关键！）
cd "${PROJECT_ROOT}"

# 查找并在当前相对路径下遍历所有 .proto 文件
# 仅限定在每个 api 服务下（internal/apps/*/api）、pkg 下搜索
PROTO_FILES=$(find internal/apps/*/api pkg -type f -name "*.proto" 2>/dev/null)

for file in $PROTO_FILES; do
    echo "Processing proto file: $file"
    # 全部基于根目录 "." 相对执行
    protoc -I="." \
        --go_out=paths=source_relative:"." \
        "$file"
done

echo "Injecting tags..."
# 同样限定在 api、pkg 和 internal/types 目录里注入 Tags
PB_FILES=$(find internal/apps/*/api pkg -type f -name "*.pb.go" 2>/dev/null || true)
for pb in $PB_FILES; do
    echo "  Injecting tags: $pb"
    protoc-go-inject-tag -input="$pb"
done

echo "Proto generation completed successfully!"