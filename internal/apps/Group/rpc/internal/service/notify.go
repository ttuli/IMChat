package service

import (
	"context"

	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
)

// publishGroupNotify 发布面向群全员的群操作通知。
//
// deliverTo 携带成员快照时，各网关节点直接按该列表过滤本地连接投递，
// 免去每个节点回查路由表的开销；快照缺失时退化为兼容广播（网关查路由表投递）。
//
// 通知必须发往 BroadcastSubject 而非节点级单播：Message 服务订阅该 subject
// 重放群事件（幂等修正路由表 + 建立 user_session），改单播会使其丢失事件。
func (s *GroupService) publishGroupNotify(msg *transport.WSMessage, deliverTo []uint64) {
	if msg == nil {
		return
	}
	msg.DeliverTo = deliverTo
	s.svcCtx.Nats.Broadcast(msg)
}

// groupMemberSnapshot 获取群成员 ID 快照，用于群操作通知的定向投递：
// 路由表集合优先（直写维护的热数据），miss 时回源 DB 权威数据。
// 两者都失败返回 nil，调用方发布不带 deliver_to 的兼容广播兜底。
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
