package defaultimpl

import (
	"context"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/xerr"

	"github.com/gogo/protobuf/proto"
	"go.mongodb.org/mongo-driver/mongo"
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
