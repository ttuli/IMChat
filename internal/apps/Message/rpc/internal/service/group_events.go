package service

import (
	"context"

	"IM2/pkg/logger"
	"IM2/pkg/proto/message"

	"google.golang.org/protobuf/proto"
)

// parseGroupNotify 从统一通知载体（NotifyMessage）中解出群操作通知，
// 非群操作通知或解析失败返回 nil。
func parseGroupNotify(payload []byte) *message.GroupNotification {
	var nm message.NotifyMessage
	if err := proto.Unmarshal(payload, &nm); err != nil {
		logger.Errorf("[NatsListener] unmarshal notify message failed: %v", err)
		return nil
	}
	return nm.GetGroupNotify()
}

// handleGroupEvent 处理单条群操作通知（由 DBSubject 落库消费路径在持久化前调用，
// sessionID 为调用方已解析的群会话 ID）：
//  1. 按事件类型增量修正路由表群成员集合——Group 服务在操作落库后已同步直写路由表，
//     此处重放同样的幂等写入，兜底其直写失败（Redis 瞬时异常）造成的窗口
//  2. 建群/入群时为成员建立 user_session 行（离线会话索引与已读游标的载体），
//     新成员 last_read_seq 取当前 actual_seq，入群前的历史消息不计未读
//
// 事件经 JetStream 持久化投递（替代旧的 core NATS 广播拦截），消费方宕机不丢失；
// 消息重投时重复执行，所有操作均幂等，无副作用。
func (s *MessageService) handleGroupEvent(ctx context.Context, sessionID string, notify *message.GroupNotification) {
	// 按事件类型增量修正路由表（与 Group 服务的同步直写幂等重放）
	s.syncGroupRoute(ctx, notify)

	switch notify.OpType {
	case message.GroupOperationType_GROUP_OP_CREATE, message.GroupOperationType_GROUP_OP_JOIN:
		memberIDs := append([]uint64{}, notify.TargetIds...)
		if notify.OpType == message.GroupOperationType_GROUP_OP_CREATE {
			memberIDs = append(memberIDs, notify.OperatorId)
		}
		for _, uid := range memberIDs {
			if uid == 0 {
				continue
			}
			if err := s.svcCtx.SessionDAO.AddMemberToSession(ctx, sessionID, uid); err != nil {
				logger.Errorf("[NatsListener] add member %d to session %s failed: %v", uid, sessionID, err)
			}
		}

	case message.GroupOperationType_GROUP_OP_LEAVE,
		message.GroupOperationType_GROUP_OP_KICK,
		message.GroupOperationType_GROUP_OP_DISMISS:
		// user_session 行保留作为历史会话入口；
		// 投递与时间线的成员来源是路由表 + Group RPC，退群用户自然不再收到新消息
	}
}

// syncGroupRoute 将群操作事件映射为路由表成员集合的增量写入。
// 建群事件携带完整成员列表，做全量替换；其余事件只增删差量（集合不存在时不写，
// 由读路径回源构建全量数据）。所有操作幂等。
func (s *MessageService) syncGroupRoute(ctx context.Context, notify *message.GroupNotification) {
	groupID := notify.GroupId
	var err error
	switch notify.OpType {
	case message.GroupOperationType_GROUP_OP_CREATE:
		members := append([]uint64{}, notify.TargetIds...)
		if notify.OperatorId != 0 {
			members = append(members, notify.OperatorId)
		}
		err = s.svcCtx.Routes.ReplaceGroupMembers(ctx, groupID, members, 0)
	case message.GroupOperationType_GROUP_OP_JOIN:
		// 集合缺失时不写（applied=false），由消息路径回源构建全量数据
		_, err = s.svcCtx.Routes.AddGroupMembers(ctx, groupID, notify.TargetIds...)
	case message.GroupOperationType_GROUP_OP_LEAVE,
		message.GroupOperationType_GROUP_OP_KICK:
		err = s.svcCtx.Routes.RemoveGroupMembers(ctx, groupID, notify.TargetIds...)
	case message.GroupOperationType_GROUP_OP_DISMISS:
		err = s.svcCtx.Routes.DeleteGroup(ctx, groupID)
	}
	if err != nil {
		logger.Errorf("[NatsListener] sync group %d route failed: %v", groupID, err)
	}
}
