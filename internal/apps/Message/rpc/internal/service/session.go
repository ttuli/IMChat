package service

import (
	"context"
	"errors"

	"IM2/internal/apps/Message/rpc/internal/dao"
	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
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

// GetUserActiveSessions 获取用户活跃的会话列表，基于时间戳增量获取。
// DAO 内部：Redis 时间线取活跃 ID → 完整快照批量命中，未命中批量查 MySQL。
func (s *MessageService) GetUserActiveSessions(ctx context.Context, userID uint64, sinceTimestamp int64) ([]*model.Session, error) {
	sessions, err := s.svcCtx.SessionDAO.GetActiveSessions(ctx, userID, sinceTimestamp)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "获取活跃会话列表失败")
	}
	return sessions, nil
}

// UpdateSession 更新会话设置
// isTop/isDisturb: 0-不变更 1-开启 2-关闭
func (s *MessageService) UpdateSession(ctx context.Context, userID uint64, sessionID, sessionKey string, isTop, isDisturb int32) error {
	if sessionID == "" {
		if sessionKey == "" {
			return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "更新失败")
		}
		sessionType := model.SessionTypeSingle
		if util.IsGroupSession(sessionKey) {
			sessionType = model.SessionTypeGroup
		}
		resolvedID, err := s.ResolveSessionID(ctx, sessionKey, sessionType)
		if err != nil {
			return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "解析 sessionKey 失败")
		}
		sessionID = resolvedID
	}

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

// MarkSessionRead 前进用户的会话已读游标（单调递增，乱序上报不会回退）
func (s *MessageService) MarkSessionRead(ctx context.Context, userID uint64, sessionID string, readSeq uint64) error {
	if sessionID == "" {
		return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "session_id 不能为空")
	}
	if err := s.svcCtx.SessionDAO.MarkSessionRead(ctx, userID, sessionID, readSeq); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新已读游标失败")
	}
	return nil
}

// maxSessionIDAttempts 新会话 ID 与已有会话冲突时的最大尝试次数（每次换一个新雪花 ID）
const maxSessionIDAttempts = 3

// withSessionIDRetry 用新生成的雪花 ID 调用 create；ID 与已有会话的 session_id 冲突时换号重试。
func (s *MessageService) withSessionIDRetry(sessionKey string, create func(newSessionID string) error) error {
	for attempt := 1; ; attempt++ {
		err := create(s.svcCtx.SnowflakeNode.Generate().String())
		if !errors.Is(err, dao.ErrSessionIDConflict) || attempt == maxSessionIDAttempts {
			return err
		}
		logger.Errorf("[SESSION-ID-CONFLICT] session_key=%s attempt=%d: 新会话 ID 与已有会话重复，换号重试；出现此日志说明有副本的节点号重复",
			sessionKey, attempt)
	}
}

// ResolveSessionID 消息消费热路径专用：按 session_key 解析（不存在则创建）会话 ID。
// 走 SessionDAO 的进程内 LRU 缓存，避免每条消息一次 MySQL 查询。
func (s *MessageService) ResolveSessionID(ctx context.Context, sessionKey string, sessionType int8) (string, error) {
	var sessionID string
	err := s.withSessionIDRetry(sessionKey, func(newSessionID string) (err error) {
		sessionID, err = s.svcCtx.SessionDAO.ResolveSessionIDByKey(ctx, newSessionID, sessionKey, sessionType)
		return err
	})
	return sessionID, err
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
		var session *model.Session
		var created bool
		err := s.withSessionIDRetry(sessionKey, func(newSessionID string) (err error) {
			session, created, err = s.svcCtx.SessionDAO.FindOrCreateBySessionKey(ctx, newSessionID, sessionKey, sessionType)
			return err
		})
		if err != nil {
			return nil, false, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询或创建会话失败")
		}
		return session, created, nil
	}
	return nil, false, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "session_id 和 session_key 至少提供一个")
}
