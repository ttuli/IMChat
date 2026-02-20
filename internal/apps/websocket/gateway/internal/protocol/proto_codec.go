package protocol

import (
	"IM2/internal/common"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec Protobuf 二进制编解码器
type ProtoCodec struct{}

// NewProtoCodec 创建 Protobuf 编解码器
func NewProtoCodec() *ProtoCodec {
	return &ProtoCodec{}
}

// Encode 编码 WSMessage 为 protobuf 二进制
func (c *ProtoCodec) Encode(msg *common.WSMessage) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Decode 解码 protobuf 二进制为 WSMessage
func (c *ProtoCodec) Decode(data []byte) (*common.WSMessage, error) {
	msg := &common.WSMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// EncodeInternal 编码内部消息
func (c *ProtoCodec) EncodeInternal(msg *common.InternalMessage) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// DecodeInternal 解码内部消息
func (c *ProtoCodec) DecodeInternal(data []byte) (*common.InternalMessage, error) {
	msg := &common.InternalMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
