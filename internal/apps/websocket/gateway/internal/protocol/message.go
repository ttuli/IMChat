package protocol

import (
	"errors"
	"time"

	"IM2/internal/common"

	"google.golang.org/protobuf/proto"
)

// NewWSMessage 创建新的 WSMessage
func NewWSMessage(msgType common.MessageType, payload proto.Message) (*common.WSMessage, error) {
	msg := &common.WSMessage{
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
func NewErrorWSMessage(code int32, message string) *common.WSMessage {
	errMsg := &common.ErrorMessage{
		ErrorCode: code,
		ErrorMsg:  message,
	}
	data, _ := proto.Marshal(errMsg)
	return &common.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      common.MessageType_ERROR,
		Payload:   data,
	}
}

func NewAckMessage(base *common.BaseMessage, st common.AckStatus) *common.WSMessage {
	wm := &common.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      common.MessageType_MSG_ACK,
	}
	ack := &common.MessageAck{
		MsgId:     base.MsgId,
		ClientId:  base.ClientId,
		SessionId: base.SessionId,
		Seq:       base.MsgSeq,
		Status:    st,
	}
	data, _ := proto.Marshal(ack)
	wm.Payload = data
	return wm
}

// DecodePayload 从 WSMessage 中解码 payload 到指定 proto message
func DecodePayload[T proto.Message](msg *common.WSMessage, target T) error {
	if len(msg.Payload) == 0 {
		return errors.New("empty payload")
	}
	if err := proto.Unmarshal(msg.Payload, target); err != nil {
		return err
	}
	return nil
}
