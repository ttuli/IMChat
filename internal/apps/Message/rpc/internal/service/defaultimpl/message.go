package defaultimpl

import (
	"context"
	"time"

	"IM2/internal/model"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/logger"
	"IM2/pkg/proto/svc"
	"IM2/pkg/xerr"

	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/protobuf/proto"
)

// GetHistory 获取历史消息（基于 Seq 区间分页）
// startSeq/endSeq 负数表示无界，limit 兜底最大 100。
func (s *messageService) GetHistory(ctx context.Context, conversationID string, startSeq, endSeq int64, limit int) ([]*model.Message, error) {
	const maxLimit = 100
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}
	messages, err := s.messageDAO.FindByConversation(ctx, conversationID, startSeq, endSeq, limit)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询历史消息失败")
	}
	return messages, nil
}

// SendMessage 消费NATS消息、生成序号、广播事件、同步落库
// TODO 改为在消息消费后发送一个ack消息到nats，再由websocket消费发送
func (s *messageService) SendMessage(ctx context.Context, msg *svc.MessageSend) (*model.Message, error) {
	// 1. 生成或递增 Seq
	seq, err := s.conversationDAO.IncrSeq(ctx, msg.ConversationId)
	if err != nil {
		logger.Errorf("Failed to incr seq for conversation %s: %v", msg.ConversationId, err)
		return nil, err
	}
	status := int32(message.MessageStatus_MESSAGE_STATUS_SENT)

	// 3. 将完整会话状态推送到 SeqSyncer
	s.conversationDAO.PushSeqUpdate(
		msg.ConversationId, seq,
		msg.Content, msg.Sender, msg.CreateTime,
	)

	// 4. 构建 db model 并落库
	dbMsg := &model.Message{
		MsgID:          msg.MsgId,
		ClientID:       msg.ClientId,
		ConversationID: msg.ConversationId,
		FromUserID:     msg.Sender,
		MsgType:        int16(msg.MsgType),
		Seq:            seq,
		Content:        msg.Content,
		MediaURL:       msg.MediaUrl,
		Status:         int8(status),
		CreateTime:     time.UnixMilli(msg.CreateTime),
	}

	if err := s.messageDAO.InsertMessages(ctx, []*model.Message{dbMsg}); err != nil {
		logger.Errorf("Failed to persist message %s: %v", msg.MsgId, err)
		return nil, err
	}

	return dbMsg, nil
}

// RecallMessage 撤回消息
// 1. 校验消息是否存在
// 2. 校验是否为发送者本人
// 3. 校验是否在 2 分钟内
// 4. 更新消息状态为已撤回
func (s *messageService) RecallMessage(ctx context.Context, userID uint64, msgID string, sessionID string) error {
	const recallWindowSeconds = 120 // 撤回时间窗口：2分钟

	// 1. 查询消息
	msg, err := s.messageDAO.FindByMsgID(ctx, msgID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return xerr.Wrap(err, xerr.ErrNotFound, "消息不存在")
		}
		return xerr.Wrap(err, xerr.ErrDatabase, "查询消息失败")
	}

	// 2. 校验发送者
	if msg.FromUserID != userID {
		return xerr.Wrap(nil, xerr.ErrForbidden, "只能撤回自己发送的消息")
	}

	// 3. 校验撤回时间窗口
	if time.Since(msg.CreateTime).Seconds() > recallWindowSeconds {
		return xerr.Wrap(nil, xerr.ErrForbidden, "超过撤回时间限制")
	}

	// 4. 校验消息状态（避免重复撤回）
	if msg.Status != 0 {
		return xerr.Wrap(nil, xerr.ErrForbidden, "该消息已被撤回或删除")
	}

	// 5. 更新消息状态为已撤回 (status=1)
	if err := s.messageDAO.UpdateMessageStatus(ctx, msgID, 1); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "撤回消息失败")
	}

	ws, err := util.NewMessageOperationMsg(transport.MessageType_MSG_OP_RECALL, userID, msg)
	if err != nil {
		logger.Errorf("Failed to create WSMessage: %v", err)
		return nil
	}
	payload, err := proto.Marshal(ws)
	if err != nil {
		logger.Errorf("Failed to marshal WSMessage: %v", err)
		return nil
	}
	if _, err := s.js.Publish(s.Config.Listener.BroadcastSubject, payload); err != nil {
		logger.Errorf("Failed to publish NATS message: %v", err)
	}

	return nil
}

// BulkPersistMessages 批量持久化消息，由 NATS Listener 消费后调用。
func (s *messageService) BulkPersistMessages(ctx context.Context, msgs []*model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := s.messageDAO.InsertMessages(ctx, msgs); err != nil {
		logger.Errorf("[MessageService] BulkPersistMessages failed (batch=%d): %v", len(msgs), err)
		return err
	}
	logger.Infof("[MessageService] BulkPersistMessages ok: %d messages persisted", len(msgs))
	return nil
}
