package protocol

import (
	"encoding/json"

	"IM2/pkg/xerr"
)

// DecodeData 解码消息数据到指定结构
func DecodeData[T any](msg *Message) (*T, error) {
	if msg.Data == nil {
		return nil, xerr.New(xerr.ErrInvalidParams, "message data is nil")
	}
	var data T
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode message data failed")
	}
	return &data, nil
}

// EncodeData 编码数据到消息
func EncodeData(data any) (json.RawMessage, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrEncoding, "encode data failed")
	}
	return bytes, nil
}

// NewMessage 创建新消息
func NewMessage(msgType MessageType, data any) (*Message, error) {
	msg := &Message{
		Type: msgType,
	}
	if data != nil {
		encoded, err := EncodeData(data)
		if err != nil {
			return nil, err
		}
		msg.Data = encoded
	}
	return msg, nil
}

// NewErrorMessage 创建错误消息
func NewErrorMessage(code int32, message string) *Message {
	data, _ := EncodeData(&ErrorData{
		Code:    code,
		Message: message,
	})
	return &Message{
		Type: MessageTypeError,
		Data: data,
	}
}

// NewHeartbeatAck 创建心跳响应消息
func NewHeartbeatAck() *Message {
	return &Message{
		Type: MessageTypeHeartbeatAck,
	}
}
