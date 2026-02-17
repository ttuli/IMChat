package defaultimpl

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/xerr"
)

// GetHistory 获取历史消息
func (s *messageService) GetHistory(ctx context.Context, conversationID string, cursorSeq uint64, limit int) ([]*model.Message, error) {
	messages, err := s.messageDAO.FindByConversation(ctx, conversationID, cursorSeq, limit)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询历史消息失败")
	}
	return messages, nil
}
