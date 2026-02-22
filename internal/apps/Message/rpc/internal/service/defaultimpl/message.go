package defaultimpl

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/xerr"
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
