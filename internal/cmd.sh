#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

protoc -I ${SCRIPT_DIR} --go_out=${SCRIPT_DIR} ${SCRIPT_DIR}/common.proto