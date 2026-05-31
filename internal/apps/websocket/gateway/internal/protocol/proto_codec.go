package protocol

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec Protobuf 二进制编解码器
type ProtoCodec struct{}

// NewProtoCodec 创建 Protobuf 编解码器
func NewProtoCodec() *ProtoCodec {
	return &ProtoCodec{}
}

// Encode 编码 protobuf 消息为二进制
func (c *ProtoCodec) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("ProtoCodec Encode: expected proto.Message, got %T", v)
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Decode 解码 protobuf 二进制为目标消息
func (c *ProtoCodec) Decode(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("ProtoCodec Decode: expected proto.Message, got %T", v)
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}
	return nil
}
