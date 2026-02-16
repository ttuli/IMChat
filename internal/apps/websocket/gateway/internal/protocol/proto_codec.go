package protocol

import (
	"IM2/internal/apps/websocket/gateway/types"
	"IM2/pkg/xerr"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec Protobuf 二进制编解码器
type ProtoCodec struct{}

// NewProtoCodec 创建 Protobuf 编解码器
func NewProtoCodec() *ProtoCodec {
	return &ProtoCodec{}
}

// Encode 编码 WSMessage 为 protobuf 二进制
func (c *ProtoCodec) Encode(msg *types.WSMessage) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "proto encode WSMessage failed")
	}
	return data, nil
}

// Decode 解码 protobuf 二进制为 WSMessage
func (c *ProtoCodec) Decode(data []byte) (*types.WSMessage, error) {
	msg := &types.WSMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "proto decode WSMessage failed")
	}
	return msg, nil
}

// EncodeInternal 编码内部消息
func (c *ProtoCodec) EncodeInternal(msg *types.InternalMessage) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode internal message failed")
	}
	return data, nil
}

// DecodeInternal 解码内部消息
func (c *ProtoCodec) DecodeInternal(data []byte) (*types.InternalMessage, error) {
	msg := &types.InternalMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode internal message failed")
	}
	return msg, nil
}
