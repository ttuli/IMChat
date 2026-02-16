package defaultimpl

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/xerr"
)

// GetConversationList 获取用户会话列表
func (s *messageService) GetConversationList(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	convs, err := s.messageDAO.FindUserConversations(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询会话列表失败")
	}
	return convs, nil
}

// ReadMessage 消息已读上报
func (s *messageService) ReadMessage(ctx context.Context, userID uint64, conversationID string, seq uint64) error {
	// 清零未读并更新已读游标
	if err := s.messageDAO.ClearUnread(ctx, userID, conversationID, 0, seq); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新已读状态失败")
	}
	return nil
}

// UpdateConversation 更新会话设置
// isTop/isDisturb/isMute: 0-不变更 1-开启 2-关闭
func (s *messageService) UpdateConversation(ctx context.Context, userID uint64, conversationID string, isTop, isDisturb, isMute int32) error {
	updates := make(map[string]any)

	if isTop == 1 {
		updates["is_top"] = true
	} else if isTop == 2 {
		updates["is_top"] = false
	}

	if isDisturb == 1 {
		updates["is_disturb"] = true
	} else if isDisturb == 2 {
		updates["is_disturb"] = false
	}

	if isMute == 1 {
		updates["is_mute"] = true
	} else if isMute == 2 {
		updates["is_mute"] = false
	}

	if len(updates) == 0 {
		return nil
	}

	if err := s.messageDAO.UpdateUserConversation(ctx, userID, conversationID, updates); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新会话设置失败")
	}
	return nil
}
