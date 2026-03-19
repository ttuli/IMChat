#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

protoc -I ${SCRIPT_DIR} --go_out=${SCRIPT_DIR} ${SCRIPT_DIR}/common.proto

for types_dir in ${SCRIPT_DIR}/apps/*/api/types; do
    if [ -d "$types_dir" ]; then
        if ls "$types_dir"/*.proto >/dev/null 2>&1; then
            protoc -I ${SCRIPT_DIR} -I "$types_dir" --go_out="$types_dir" "$types_dir"/*.proto
            protoc-go-inject-tag -input="$types_dir"/*.pb.go
        fi
    fi
done