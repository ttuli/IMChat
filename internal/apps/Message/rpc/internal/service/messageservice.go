package service

import (
	"context"
	"time"

	"IM2/internal/model"
)

// SendMessageResult 发送消息的返回结果
type SendMessageResult struct {
	Seq        uint64
	CreateTime time.Time
}

// MessageService 消息服务接口
type MessageService interface {
	// ========== 消息操作 ==========

	// SendMessage 发送消息（生成Seq、存MongoDB、更新会话）
	SendMessage(ctx context.Context, msgID, conversationID string, fromUserID uint64, msgType int16, content, mediaURL string, extra map[string]any) (*SendMessageResult, error)
	// GetHistory 获取历史消息（基于Seq游标分页）
	GetHistory(ctx context.Context, conversationID string, cursorSeq uint64, limit int) ([]*model.Message, error)

	// ========== 会话操作 ==========

	// GetConversationList 获取用户会话列表
	GetConversationList(ctx context.Context, userID uint64) ([]*model.UserConversation, error)
	// ReadMessage 消息已读上报（清零未读、更新LastReadSeq）
	ReadMessage(ctx context.Context, userID uint64, conversationID string, seq uint64) error
	// UpdateConversation 更新会话设置（置顶/免打扰/静音）
	UpdateConversation(ctx context.Context, userID uint64, conversationID string, isTop, isDisturb, isMute int32) error
}
