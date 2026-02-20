#!/bin/bash
# 确保安装了 protoc-gen-go
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc --proto_path=. --go_out=. --go_opt=paths=source_relative message_api.proto
protoc-go-inject-tag -input=./message_api.pb.go
