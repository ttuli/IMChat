package protocol

import (
	"encoding/json"

	"IM2/pkg/xerr"
)

// JSONCodec JSON 编解码器
type JSONCodec struct{}

// NewJSONCodec 创建 JSON 编解码器
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode 编码消息
func (c *JSONCodec) Encode(msg *Message) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode message failed")
	}
	return data, nil
}

// Decode 解码消息
func (c *JSONCodec) Decode(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode message failed")
	}
	return &msg, nil
}

// EncodeInternal 编码内部消息
func (c *JSONCodec) EncodeInternal(msg *InternalMessage) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode internal message failed")
	}
	return data, nil
}

// DecodeInternal 解码内部消息
func (c *JSONCodec) DecodeInternal(data []byte) (*InternalMessage, error) {
	var msg InternalMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode internal message failed")
	}
	return &msg, nil
}
