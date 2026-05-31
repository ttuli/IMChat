package protocol

import (
	"fmt"

	"IM2/pkg/xerr"

	"google.golang.org/protobuf/proto"

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

// Encode 编码消息为 JSON
func (c *JSONCodec) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("JSONCodec Encode: expected proto.Message, got %T", v)
	}
	data, err := c.marshaler.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode failed")
	}
	return data, nil
}

// Decode 解码 JSON 为目标消息
func (c *JSONCodec) Decode(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("JSONCodec Decode: expected proto.Message, got %T", v)
	}
	if err := c.unmarshaler.Unmarshal(data, msg); err != nil {
		return xerr.Wrap(err, xerr.ErrDecoding, "decode failed")
	}
	return nil
}
