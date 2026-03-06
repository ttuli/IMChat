package service

import (
	"context"

	"IM2/internal/model"
)

// MessageService 消息服务接口
type MessageService interface {
	// ========== 消息操作 ==========

	// GetHistory 获取历史消息（基于 Seq 区间分页）
	// startSeq/endSeq 负数表示无界；limit≤0 时服务内部兜底 100。
	GetHistory(ctx context.Context, conversationID string, startSeq, endSeq int64, limit int) ([]*model.Message, error)

	// ========== 会话操作 ==========

	// GetConversationList 获取用户会话列表
	GetConversationList(ctx context.Context, userID uint64) ([]*model.UserConversation, error)
	// ReadMessage 消息已读上报（清零未读、更新LastReadSeq）
	ReadMessage(ctx context.Context, userID uint64, conversationID string, seq uint64) error
	// UpdateConversation 更新会话设置（置顶/免打扰/静音）
	UpdateConversation(ctx context.Context, userID uint64, conversationID string, isTop, isDisturb, isMute int32) error
	// GetConversation 批量获取会话详情
	GetConversation(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error)
	// GetUserConversations 获取用户所有会话（含未读数等用户维度信息）
	GetUserConversations(ctx context.Context, userID uint64) ([]*model.UserConversation, error)
	// GetUserActiveConversations 获取用户活跃的会话列表，基于时间戳增量获取
	GetUserActiveConversations(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Conversation, error)

	// RecallMessage 撤回消息（校验发送者 + 2分钟时间窗口）
	RecallMessage(ctx context.Context, userID uint64, msgID string, sessionID string) error
}
