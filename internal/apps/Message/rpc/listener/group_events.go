package listener

import (
	"context"
	"time"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/social"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// subscribeGroupEvents 订阅网关广播 subject，拦截群操作通知：
//  1. 失效群成员缓存，下一条群消息回源 Group RPC 获取最新成员
//  2. 建群/入群时为成员建立 user_session 行（离线会话索引与已读游标的载体）
//
// 事件为 core NATS 广播，可能丢失：成员缓存有 TTL 兜底最终收敛，
// user_session 有消息路径的 EnsureUserSessions 存量补偿。
// 所有 Message 实例都会收到该广播，操作均幂等，多实例重复执行无副作用。
func (l *NatsListener) subscribeGroupEvents() error {
	sub, err := l.svcCtx.NatsConn.Subscribe(l.svcCtx.Config.Listener.BroadcastSubject, func(natsMsg *nats.Msg) {
		var ws transport.WSMessage
		if err := proto.Unmarshal(natsMsg.Data, &ws); err != nil {
			return
		}
		if ws.Type != transport.MessageType_GROUP_OP_NOTIFICATION {
			return
		}
		var notify social.GroupNotification
		if err := proto.Unmarshal(ws.Payload, &notify); err != nil {
			logger.Errorf("[NatsListener] unmarshal group notification failed: %v", err)
			return
		}
		l.handleGroupEvent(&notify)
	})
	if err != nil {
		return err
	}
	l.groupEventSub = sub
	return nil
}

// handleGroupEvent 处理单条群操作通知
func (l *NatsListener) handleGroupEvent(notify *social.GroupNotification) {
	ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
	defer cancel()

	groupID := notify.GroupId
	// 任何群操作都可能改变成员集合，先失效缓存
	l.svcCtx.Members.Invalidate(ctx, groupID)

	switch notify.OpType {
	case social.GroupOperationType_GROUP_OP_CREATE, social.GroupOperationType_GROUP_OP_JOIN:
		memberIDs := append([]uint64{}, notify.TargetIds...)
		if notify.OpType == social.GroupOperationType_GROUP_OP_CREATE {
			memberIDs = append(memberIDs, notify.OperatorId)
		}
		if len(memberIDs) == 0 {
			return
		}

		// 解析（不存在则创建）群会话，新成员 last_read_seq 取当前 actual_seq，
		// 入群前的历史消息不计未读
		sessionKey := notify.SessionId
		if sessionKey == "" {
			sessionKey = util.GenerateGroupSessionId(groupID)
		}
		sessionID, err := service.NewMessageService(l.svcCtx).ResolveSessionID(ctx, sessionKey, model.SessionTypeGroup)
		if err != nil {
			logger.Errorf("[NatsListener] resolve group session %s failed: %v", sessionKey, err)
			return
		}
		for _, uid := range memberIDs {
			if uid == 0 {
				continue
			}
			if err := l.svcCtx.SessionDAO.AddMemberToSession(ctx, sessionID, uid); err != nil {
				logger.Errorf("[NatsListener] add member %d to session %s failed: %v", uid, sessionID, err)
			}
		}

	case social.GroupOperationType_GROUP_OP_LEAVE,
		social.GroupOperationType_GROUP_OP_KICK,
		social.GroupOperationType_GROUP_OP_DISMISS:
		// 仅失效成员缓存。user_session 行保留作为历史会话入口；
		// 投递与时间线的成员来源是 Group RPC，退群用户自然不再收到新消息
	}
}

// unsubscribeGroupEvents 退订群操作通知
func (l *NatsListener) unsubscribeGroupEvents() {
	if l.groupEventSub != nil {
		if err := l.groupEventSub.Unsubscribe(); err != nil && err != nats.ErrConnectionClosed {
			logger.Errorf("[NatsListener] unsubscribe group events failed: %v", err)
		}
		l.groupEventSub = nil
	}
}
