package defaultimpl

import (
	"context"
	"time"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/xerr"

	"github.com/google/uuid"
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

// SendMessage 发送消息、生成序号、广播事件、异步落库
func (s *messageService) SendMessage(ctx context.Context, msg *message.Message) (*message.Message, error) {
	// 0. 幂等性校验：检查是否已经有相同的 from_user_id 和 client_id 的消息
	if msg.ClientId != "" {
		existingMsg, err := s.messageDAO.FindBySenderAndClient(ctx, msg.FromUserId, msg.ClientId)
		if err == nil && existingMsg != nil {
			logger.Infof("Idempotent check hit: message already exists for client_id %s, from_user_id %d", msg.ClientId, msg.FromUserId)
			// 直接返回已存在的那条消息的完整信息
			msg.MsgId = existingMsg.MsgID
			msg.Seq = existingMsg.Seq
			msg.CreateTime = existingMsg.CreateTime.UnixMilli()
			msg.Status = int32(existingMsg.Status)
			return msg, nil
		}
	}

	if msg.MsgId == "" {
		msg.MsgId = uuid.New().String()
	}
	if msg.CreateTime == 0 {
		msg.CreateTime = time.Now().UnixMilli()
	}

	// 1. 生成或递增 Seq
	seq, err := s.conversationDAO.IncrSeq(ctx, msg.ConversationId)
	if err != nil {
		logger.Errorf("Failed to incr seq for conversation %s: %v", msg.ConversationId, err)
		return nil, err
	}
	msg.Seq = seq
	msg.Status = int32(common.MessageStatus_MESSAGE_STATUS_SENT)

	// 2. 构建 content preview（用于 last_content 和 UpdateSession 广播）
	contentPreview := contentPreviewOf(msg.MsgType, msg.Content)

	// 3. 将完整会话状态推送到 SeqSyncer：批量刷 MySQL + 广播 UpdateSession
	s.conversationDAO.PushSeqUpdate(
		msg.ConversationId, msg.Seq,
		contentPreview, msg.FromUserId, msg.CreateTime,
	)

	// 4. 异步将消息发布到 DBSubject，由 NATS Consumer 批量写入 MongoDB
	msgData, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("Failed to marshal message for DBSubject: %v", err)
	} else {
		if _, err := s.js.Publish(s.Config.Listener.DBSubject, msgData); err != nil {
			logger.Errorf("Failed to publish message to DBSubject: %v", err)
		}
	}

	return msg, nil
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

	ws, err := common.NewMessageOperationMsg(common.MessageType_MSG_OP_RECALL, userID, msg)
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
