package protocol

import (
	"errors"
	"time"

	"IM2/internal/apps/websocket/gateway/types"

	"google.golang.org/protobuf/proto"
)

// NewWSMessage 创建新的 WSMessage
func NewWSMessage(msgType types.MessageType, payload proto.Message) (*types.WSMessage, error) {
	msg := &types.WSMessage{
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
func NewErrorWSMessage(code int32, message string) *types.WSMessage {
	errMsg := &types.ErrorMessage{
		ErrorCode: code,
		ErrorMsg:  message,
	}
	data, _ := proto.Marshal(errMsg)
	return &types.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      types.MessageType_ERROR,
		Payload:   data,
	}
}

func NewAckMessage(base *types.BaseMessage, st types.AckStatus) *types.WSMessage {
	wm := &types.WSMessage{
		Timestamp: time.Now().UnixMilli(),
		Type:      types.MessageType_MSG_ACK,
	}
	ack := &types.MessageAck{
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
func DecodePayload[T proto.Message](msg *types.WSMessage, target T) error {
	if len(msg.Payload) == 0 {
		return errors.New("empty payload")
	}
	if err := proto.Unmarshal(msg.Payload, target); err != nil {
		return err
	}
	return nil
}

// IsChatMessage 判断是否为单聊消息类型 (100-199)
func IsChatMessage(t types.MessageType) bool {
	return t >= 100 && t < 200
}

// IsGroupMessage 判断是否为群聊消息类型 (200-299)
func IsGroupMessage(t types.MessageType) bool {
	return t >= 200 && t < 300
}
