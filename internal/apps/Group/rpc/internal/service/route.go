package service

import (
	"context"

	"IM2/pkg/logger"
)

// ensureGroupRoute 入群后同步维护路由表：
//   - 成员集合已存在 → 增量补入新成员
//   - 集合缺失（冷群，TTL 过期后无消息触发回源）→ 按 DB 权威成员列表全量重建，
//     保证紧随其后发布的入群通知与后续群消息可被路由投递
//
// 路由表是缓存性质数据，任何一步失败仅记录日志，最终由读路径回源 + TTL 收敛。
func (s *GroupService) ensureGroupRoute(ctx context.Context, groupID uint64, newMemberIDs ...uint64) {
	applied, err := s.svcCtx.Routes.AddGroupMembers(ctx, groupID, newMemberIDs...)
	if err != nil {
		logger.Errorf("[GroupService] add group %d route members %v failed: %v", groupID, newMemberIDs, err)
		return
	}
	if applied {
		return
	}

	members, err := s.svcCtx.GroupDAO.FindMembersByGroupID(ctx, groupID)
	if err != nil {
		logger.Errorf("[GroupService] load members of group %d for route rebuild failed: %v", groupID, err)
		return
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.UserID)
	}
	if err := s.svcCtx.Routes.ReplaceGroupMembers(ctx, groupID, ids, 0); err != nil {
		logger.Errorf("[GroupService] rebuild group %d route failed: %v", groupID, err)
	}
}
