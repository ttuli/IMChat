package defaultimpl

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/xerr"
)

// GetConversationList 获取用户会话列表
func (s *messageService) GetConversationList(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	convs, err := s.conversationDAO.FindUserConversations(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询会话列表失败")
	}
	return convs, nil
}

// ReadMessage 消息已读上报
func (s *messageService) ReadMessage(ctx context.Context, userID uint64, conversationID string, seq uint64) error {
	// 清零未读并更新已读游标
	if err := s.conversationDAO.ClearUnread(ctx, userID, conversationID, 0, seq); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新已读状态失败")
	}
	return nil
}

// UpdateConversation 更新会话设置
// isTop/isDisturb/isMute: 0-不变更 1-开启 2-关闭
func (s *messageService) UpdateConversation(ctx context.Context, userID uint64, conversationID string, isTop, isDisturb, isMute int32) error {
	updates := make(map[string]any)

	switch isTop {
	case 1:
		updates["is_disturb"] = true
	case 2:
		updates["is_disturb"] = false
	}

	switch isMute {
	case 1:
		updates["is_mute"] = true
	case 2:
		updates["is_mute"] = false
	}

	if len(updates) == 0 {
		return nil
	}

	if err := s.conversationDAO.UpdateUserConversation(ctx, userID, conversationID, updates); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新会话设置失败")
	}
	return nil
}

// GetConversation 批量获取会话详情
func (s *messageService) GetConversation(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	convs, err := s.conversationDAO.FindConversationsByIDs(ctx, conversationIDs)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "批量查询会话失败")
	}
	return convs, nil
}

// GetUserConversations 获取用户所有会话
func (s *messageService) GetUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	convs, err := s.conversationDAO.FindUserConversations(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询用户会话失败")
	}
	return convs, nil
}

// GetUserActiveConversations 获取用户活跃的会话列表，基于时间戳增量获取
func (s *messageService) GetUserActiveConversations(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Conversation, error) {
	// 1. 从 Redis ZSet 获取活跃会话 IDs (score > sinceTimestamp)
	activeIDs, err := s.conversationDAO.GetActiveConversationIDs(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "获取活跃会话列表失败")
	}

	if len(activeIDs) == 0 {
		return []*model.Conversation{}, nil
	}

	// 2. 批量查询会话详情
	convs, err := s.GetConversation(ctx, activeIDs)
	if err != nil {
		return nil, err
	}

	// 此时 MySQL IN 查询出来的结果可能顺序乱了，但我们在 DAO 层提取出的 activeIDs 是按时间倒（或者我们想要的正序）
	// 为保持时间线特征，我们可以按 activeIDs 的顺序对结果进行排序，但协议端可能不在乎，客户端会根据 message 的拉取自行处理排序。
	// 这里直接返回查询到的结果即可。
	return convs, nil
}
