package protocol

import (
	"IM2/internal/apps/websocket/gateway/types"
	"IM2/pkg/xerr"

	"google.golang.org/protobuf/encoding/protojson"
)

// JSONCodec JSON 编解码器 (使用 protojson)
type JSONCodec struct {
	marshaler   protojson.MarshalOptions
	unmarshaler protojson.UnmarshalOptions
}

// NewJSONCodec 创建 JSON 编解码器
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{
		marshaler: protojson.MarshalOptions{
			UseEnumNumbers: true, // 枚举用数字，前端交互更方便
		},
		unmarshaler: protojson.UnmarshalOptions{
			DiscardUnknown: true,
		},
	}
}

// Encode 编码 WSMessage 为 JSON
func (c *JSONCodec) Encode(msg *types.WSMessage) ([]byte, error) {
	data, err := c.marshaler.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode WSMessage failed")
	}
	return data, nil
}

// Decode 解码 JSON 为 WSMessage
func (c *JSONCodec) Decode(data []byte) (*types.WSMessage, error) {
	msg := &types.WSMessage{}
	if err := c.unmarshaler.Unmarshal(data, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode WSMessage failed")
	}
	return msg, nil
}

// EncodeInternal 编码内部消息
func (c *JSONCodec) EncodeInternal(msg *types.InternalMessage) ([]byte, error) {
	data, err := c.marshaler.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode internal message failed")
	}
	return data, nil
}

// DecodeInternal 解码内部消息
func (c *JSONCodec) DecodeInternal(data []byte) (*types.InternalMessage, error) {
	msg := &types.InternalMessage{}
	if err := c.unmarshaler.Unmarshal(data, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode internal message failed")
	}
	return msg, nil
}
