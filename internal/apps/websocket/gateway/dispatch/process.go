package dispatch

import (
	"fmt"
	"time"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/svc"
	"IM2/pkg/proto/transport"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// processMessage 提取 base、填充服务端字段、通过 repack 重新打包为完整 WSMessage 后发送到 NATS
func (h *Dispatcher) processMessage(msg *transport.WSMessage) error {
	msg.SenderId = h.conn.UserID
	base, preview, repack, err := transport.ParseMessage(msg)
	if err != nil {
		logger.Errorf("[Dispatcher] prepare message failed: %v", err)
		return err
	}

	// 验证目标
	if base.Target == 0 {
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return fmt.Errorf("target is empty")
	}

	// 网关层完全无状态化：不再生成 MsgId
	// 实际的全局唯一 MsgId 由 Message 服务在消费 MQ 后用其本地 SnowflakeNode 生成
	// 网关层仅使用客户端提供的 ClientId 进行 ACK 回包
	base.FromUserId = h.conn.UserID
	base.MsgId = base.ClientId // 临时占位，实际 MsgId 由 Message 服务复写
	base.SendTime = time.Now().UnixMilli()
	msg.Timestamp = base.SendTime
	base.Status = message.MessageStatus_MESSAGE_STATUS_SENDING

	// 将服务端填充的字段写回内层消息，重新打包为完整 WSMessage
	// 这样下游消费者可获取完整消息体（含图片尺寸、视频时长等），无需依赖 MessageSend 的固定格式
	newMsg, err := repack()
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return err
	}

	msgSend := &svc.MessageSend{
		ClientId:   base.ClientId,
		SessionId:  base.SessionId,
		SessionKey: base.SessionKey,
		Sender:     base.FromUserId,
		Target:     base.Target,
		MsgType:    int64(msg.Type),
		Timestamp:  base.SendTime,
		Preview:    preview,
		Payload:    newMsg.Payload,
	}
	newMsgBytes, err := proto.Marshal(msgSend)
	if err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return err
	}

	dedupKey := fmt.Sprintf("%d:%s", base.FromUserId, base.ClientId)
	if _, err := h.svcCtx.JetStream.Publish(
		h.svcCtx.Config.Nats.DBSubject,
		newMsgBytes,
		nats.MsgId(dedupKey),
	); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_FAILED))
		return err
	}

	h.conn.Send(protocol.NewAckMessage(base, message.AckStatus_ACK_STATUS_SUCCESS))
	return nil
}
