package protocol

import (
	"IM2/internal/common"
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
func (c *JSONCodec) Encode(msg *common.WSMessage) ([]byte, error) {
	data, err := c.marshaler.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode WSMessage failed")
	}
	return data, nil
}

// Decode 解码 JSON 为 WSMessage
func (c *JSONCodec) Decode(data []byte) (*common.WSMessage, error) {
	msg := &common.WSMessage{}
	if err := c.unmarshaler.Unmarshal(data, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode WSMessage failed")
	}
	return msg, nil
}
