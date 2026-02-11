#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

goctl rpc protoc ${SCRIPT_DIR}/rpc/idgen.proto -I ${SCRIPT_DIR} --go_out=${SCRIPT_DIR}/rpc --go-grpc_out=${SCRIPT_DIR}/rpc --zrpc_out=${SCRIPT_DIR}/rpc

