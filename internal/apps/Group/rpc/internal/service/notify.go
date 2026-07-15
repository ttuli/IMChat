package service

import (
	"context"

	"IM2/pkg/logger"
	nats_util "IM2/pkg/nats"
	"IM2/pkg/proto/svc"

	"google.golang.org/protobuf/proto"
)

// publishGroupNotify 将群操作通知发布到落库队列（DBSubject / JetStream）。
//
// 通知与聊天消息同链路：Message 服务消费后统一分配 msg_id/seq 落库，
// 重放路由表增量修正与 user_session 建立（幂等，兜底本服务直写失败的窗口），
// 再按成员扇出在线投递；离线成员上线后可按会话 seq 增量拉取到该事件。
// JetStream 持久化保证事件不因消费方短暂宕机而丢失。
//
// deliverTo 携带操作前成员快照：踢人/退群/解散等场景的投递目标（被踢者、
// 解散前全员）在操作后已不在成员列表，投递以快照为准；为空时由 Message
// 服务按当前成员列表扇出。
func (s *GroupService) publishGroupNotify(msg *svc.MessageSend, deliverTo []uint64) {
	if msg == nil {
		return
	}
	msg.DeliverTo = deliverTo
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("[GroupService] marshal group notify failed: %v", err)
		return
	}
	if _, err := s.svcCtx.Nats.JetStream().Publish(nats_util.DBSubject, data); err != nil {
		logger.Errorf("[GroupService] publish group notify to db queue failed: %v", err)
	}
}

// groupMemberSnapshot 获取群成员 ID 快照，用于群操作通知的定向投递：
// 路由表集合优先（直写维护的热数据），miss 时回源 DB 权威数据。
// 两者都失败返回 nil，由 Message 服务按当前成员列表扇出兜底。
func (s *GroupService) groupMemberSnapshot(ctx context.Context, groupID uint64) []uint64 {
	if ids, err := s.svcCtx.Routes.GetGroupMembers(ctx, groupID); err == nil && len(ids) > 0 {
		return ids
	}
	members, err := s.svcCtx.GroupDAO.FindMembersByGroupID(ctx, groupID)
	if err != nil {
		logger.Errorf("[GroupService] load members of group %d for notify failed: %v", groupID, err)
		return nil
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.UserID)
	}
	return ids
}
