package protocol

import (
	"errors"
	"time"

	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/message"

	"google.golang.org/protobuf/proto"
)

// NewWSMessage 创建新的 WSMessage
func NewWSMessage(msgType transport.MessageType, payload proto.Message) (*transport.WSMessage, error) {
	msg := &transport.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      msgType,
	}
	if payload != nil {
		data, err := proto.Marshal(payload)
		if err != nil {
			return nil, err
		}
		msg.Payload = data
	}
	return msg, nil
}

// NewErrorWSMessage 创建错误消息
func NewErrorWSMessage(code int32, message string) *transport.WSMessage {
	errMsg := &transport.ErrorMessage{
		ErrorCode: code,
		ErrorMsg:  message,
	}
	data, _ := proto.Marshal(errMsg)
	return &transport.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      transport.MessageType_ERROR,
		Payload:   data,
	}
}

func NewAckMessage(base *message.BaseMessage, st message.AckStatus) *transport.WSMessage {
	wm := &transport.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      transport.MessageType_MSG_ACK,
	}
	ack := &message.MessageAck{
		MsgId:     base.MsgId,
		ClientId:  base.ClientId,
		SessionId: base.SessionId,
		Status:    st,
	}
	data, _ := proto.Marshal(ack)
	wm.Payload = data
	return wm
}

// DecodePayload 从 WSMessage 中解码 payload 到指定 proto message
func DecodePayload[T proto.Message](msg *transport.WSMessage, target T) error {
	if len(msg.Payload) == 0 {
		return errors.New("empty payload")
	}
	if err := proto.Unmarshal(msg.Payload, target); err != nil {
		return err
	}
	return nil
}
