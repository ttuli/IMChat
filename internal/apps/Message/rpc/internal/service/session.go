package service

import (
	"context"

	model "IM2/internal/model"
	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"
)

// GetUserSessions 获取用户所有会话
func (s *MessageService) GetUserSessions(ctx context.Context, userID uint64) ([]*model.UserSession, error) {
	sessions, err := s.svcCtx.SessionDAO.FindUserSessions(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户会话失败")
	}
	return sessions, nil
}

// GetUserActiveSessions 获取用户活跃的会话列表，基于时间戳增量获取
func (s *MessageService) GetUserActiveSessions(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Session, error) {
	// 1. 从 Redis ZSet 获取活跃会话 IDs (score > sinceTimestamp)
	activeIDs, err := s.svcCtx.SessionDAO.GetActiveSessionIDs(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "获取活跃会话列表失败")
	}

	if len(activeIDs) == 0 {
		return []*model.Session{}, nil
	}

	// 2. 批量查询会话详情
	sessions, err := s.GetSession(ctx, activeIDs)
	if err != nil {
		return nil, err
	}

	return sessions, nil
}

// GetSession 批量获取会话详情
func (s *MessageService) GetSession(ctx context.Context, sessionIDs []string) ([]*model.Session, error) {
	sessions, err := s.svcCtx.SessionDAO.FindSessionsByIDs(ctx, sessionIDs)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "批量查询会话失败")
	}
	return sessions, nil
}

// UpdateSession 更新会话设置
// isTop/isDisturb: 0-不变更 1-开启 2-关闭
func (s *MessageService) UpdateSession(ctx context.Context, userID uint64, sessionID string, isTop, isDisturb int32) error {
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

	if err := s.svcCtx.SessionDAO.UpdateUserSession(ctx, userID, sessionID, updates); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新会话设置失败")
	}
	return nil
}

// GetOrCreateSession 按 session_id 或 session_key 查询会话。
// 若按 session_key 且不存在，则创建新会话后返回，created=true。
func (s *MessageService) GetOrCreateSession(ctx context.Context, sessionID string, sessionKey string, sessionType int8) (*model.Session, bool, error) {
	if sessionID != "" {
		session, err := s.svcCtx.SessionDAO.FindBySessionID(ctx, sessionID)
		if err != nil {
			return nil, false, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询会话失败")
		}
		return session, false, nil
	}
	if sessionKey != "" {
		newSessionID := s.svcCtx.SnowflakeNode.Generate().String()
		session, created, err := s.svcCtx.SessionDAO.FindOrCreateBySessionKey(ctx, newSessionID, sessionKey, sessionType)
		if err != nil {
			return nil, false, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询或创建会话失败")
		}
		return session, created, nil
	}
	return nil, false, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "session_id 和 session_key 至少提供一个")
}
