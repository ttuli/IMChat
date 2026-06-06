package service

import (
	"context"

	model "IM2/internal/Entity"
	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"
)

// GetConversationList 获取用户会话列表
func (s *MessageService) GetConversationList(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	convs, err := s.conversationDAO.FindUserConversations(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询会话列表失败")
	}
	return convs, nil
}

// ReadMessage 消息已读上报
func (s *MessageService) ReadMessage(ctx context.Context, userID uint64, conversationID string, seq uint64) error {
	// 清零未读并更新已读游标
	if err := s.conversationDAO.ClearUnread(ctx, userID, conversationID, 0, seq); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新已读状态失败")
	}
	return nil
}

// UpdateConversation 更新会话设置
// isTop/isDisturb: 0-不变更 1-开启 2-关闭
func (s *MessageService) UpdateConversation(ctx context.Context, userID uint64, conversationID string, isTop, isDisturb int32) error {
	updates := make(map[string]any)

	if isDisturb != 0 {
		updates["is_disturb"] = isDisturb
	}
	if isTop != 0 {
		updates["is_top"] = isTop
	}

	if len(updates) == 0 {
		return nil
	}

	if err := s.conversationDAO.UpdateUserConversation(ctx, userID, conversationID, updates); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新会话设置失败")
	}
	return nil
}

// GetConversation 批量获取会话详情
func (s *MessageService) GetConversation(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	convs, err := s.conversationDAO.FindConversationsByIDs(ctx, conversationIDs)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "批量查询会话失败")
	}
	return convs, nil
}

// GetUserConversations 获取用户所有会话
func (s *MessageService) GetUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error) {
	convs, err := s.conversationDAO.FindUserConversations(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户会话失败")
	}
	return convs, nil
}

// GetUserActiveConversations 获取用户活跃的会话列表，基于时间戳增量获取
func (s *MessageService) GetUserActiveConversations(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Conversation, error) {
	// 1. 从 Redis ZSet 获取活跃会话 IDs (score > sinceTimestamp)
	activeIDs, err := s.conversationDAO.GetActiveConversationIDs(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "获取活跃会话列表失败")
	}

	if len(activeIDs) == 0 {
		return []*model.Conversation{}, nil
	}

	// 2. 批量查询会话详情
	convs, err := s.GetConversation(ctx, activeIDs)
	if err != nil {
		return nil, err
	}

	return convs, nil
}
