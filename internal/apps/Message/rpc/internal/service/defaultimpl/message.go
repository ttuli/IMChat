package defaultimpl

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/model"
	"IM2/pkg/xerr"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gorm.io/gorm"
)

// SendMessage 发送消息
func (s *messageService) SendMessage(ctx context.Context,
	msgID, conversationID string, fromUserID uint64,
	msgType int16, content, mediaURL string, extra map[string]any,
) (*service.SendMessageResult, error) {
	// 1. 幂等校验：检查 msgID 是否已存在
	existing, err := s.messageDAO.FindByMsgID(ctx, msgID)
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询消息失败")
	}
	if existing != nil {
		// 消息已存在，直接返回（幂等）
		return &service.SendMessageResult{
			Seq:        existing.Seq,
			CreateTime: existing.CreateTime,
		}, nil
	}

	// 2. 生成 Seq
	seq, err := s.messageDAO.GetNextSeq(ctx, conversationID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "生成消息序号失败")
	}

	// 3. 构造消息并插入 MongoDB
	msg := &model.Message{
		ID:             primitive.NewObjectID(),
		MsgID:          msgID,
		ConversationID: conversationID,
		FromUserID:     fromUserID,
		MsgType:        msgType,
		Seq:            seq,
		Content:        content,
		MediaURL:       mediaURL,
		Extra:          extra,
		Status:         model.MsgStatusNormal,
	}
	if err := s.messageDAO.InsertMessage(ctx, msg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "插入消息失败")
	}

	// 4. 更新会话最后消息（MySQL）
	if err := s.messageDAO.UpdateConversationLastMsg(ctx, conversationID, 0, msg.CreateTime, seq); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "更新会话失败")
	}

	return &service.SendMessageResult{
		Seq:        seq,
		CreateTime: msg.CreateTime,
	}, nil
}

// GetHistory 获取历史消息
func (s *messageService) GetHistory(ctx context.Context, conversationID string, cursorSeq uint64, limit int) ([]*model.Message, error) {
	messages, err := s.messageDAO.FindByConversation(ctx, conversationID, cursorSeq, limit)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询历史消息失败")
	}
	return messages, nil
}
