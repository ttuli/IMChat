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
//  1. 按事件类型增量修正路由表群成员集合——Group 服务在操作落库后已同步直写路由表，
//     此处重放同样的幂等写入，兜底其直写失败（Redis 瞬时异常）造成的窗口
//  2. 建群/入群时为成员建立 user_session 行（离线会话索引与已读游标的载体）
//
// 事件为 core NATS 广播，可能丢失：成员集合有 TTL + 读路径回源兜底最终收敛，
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
	// 按事件类型增量修正路由表（与 Group 服务的同步直写幂等重放）
	l.syncGroupRoute(ctx, notify)

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
		// user_session 行保留作为历史会话入口；
		// 投递与时间线的成员来源是路由表 + Group RPC，退群用户自然不再收到新消息
	}
}

// syncGroupRoute 将群操作事件映射为路由表成员集合的增量写入。
// 建群事件携带完整成员列表，做全量替换；其余事件只增删差量（集合不存在时不写，
// 由读路径回源构建全量数据）。所有操作幂等。
func (l *NatsListener) syncGroupRoute(ctx context.Context, notify *social.GroupNotification) {
	groupID := notify.GroupId
	var err error
	switch notify.OpType {
	case social.GroupOperationType_GROUP_OP_CREATE:
		members := append([]uint64{}, notify.TargetIds...)
		if notify.OperatorId != 0 {
			members = append(members, notify.OperatorId)
		}
		err = l.svcCtx.Routes.ReplaceGroupMembers(ctx, groupID, members, 0)
	case social.GroupOperationType_GROUP_OP_JOIN:
		// 集合缺失时不写（applied=false），由消息路径回源构建全量数据
		_, err = l.svcCtx.Routes.AddGroupMembers(ctx, groupID, notify.TargetIds...)
	case social.GroupOperationType_GROUP_OP_LEAVE,
		social.GroupOperationType_GROUP_OP_KICK:
		err = l.svcCtx.Routes.RemoveGroupMembers(ctx, groupID, notify.TargetIds...)
	case social.GroupOperationType_GROUP_OP_DISMISS:
		err = l.svcCtx.Routes.DeleteGroup(ctx, groupID)
	}
	if err != nil {
		logger.Errorf("[NatsListener] sync group %d route failed: %v", groupID, err)
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
