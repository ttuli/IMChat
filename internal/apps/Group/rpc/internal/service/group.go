package service

import (
	"context"
	"time"

	"IM2/internal/apps/Idgen/rpc/idgen"
	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 群组管理 ==========

func (s *GroupService) CreateGroup(ctx context.Context, ownerID uint64, name, avatar string, memberIDs []uint64) (*model.Group, error) {
	// 1. 生成群组ID
	resp, err := s.svcCtx.IdGenerator.GetId(ctx, &idgen.GetIdReq{
		IdType: idgen.IDType_ID_TYPE_GROUP,
		Count:  1,
	})
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_RPC, "创建群组失败")
	}
	if len(resp.Ids) == 0 {
		return nil, xerr.New(transport.ErrorCode_ERR_DATABASE, "创建群组失败")
	}

	groupID := uint64(resp.Ids[0])
	now := time.Now()

	// 2. 构建群组
	group := &model.Group{
		ID:          groupID,
		OwnerID:     ownerID,
		Name:        name,
		Avatar:      avatar,
		JoinType:    int(model.JoinTypeVerify),
		MemberCount: 1 + len(memberIDs),
		CreateTime:  now,
		UpdateTime:  now,
	}

	// 3. 构建成员列表（群主 + 初始成员）
	members := make([]*model.GroupMember, 0, 1+len(memberIDs))
	targetIds := make([]uint64, 0, 1+len(memberIDs))
	members = append(members, &model.GroupMember{
		GroupID:  groupID,
		UserID:   ownerID,
		Role:     model.GroupRoleOwner,
		JoinedAt: now,
	})

	for _, memberID := range memberIDs {
		if memberID != ownerID {
			members = append(members, &model.GroupMember{
				GroupID:  groupID,
				UserID:   memberID,
				Role:     model.GroupRoleMember,
				JoinedAt: now,
			})
			targetIds = append(targetIds, memberID)
		}
	}

	// 4. 事务创建群组和成员
	if err := s.svcCtx.GroupDAO.CreateGroupWithMembers(ctx, group, members); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "创建群组失败")
	}

	// 5. 直写路由表：建群时成员列表完整，全量写入，
	// 使后续群消息扇出/通知投递无需回源即可命中（失败由读路径回源兜底）
	allMemberIDs := append([]uint64{ownerID}, targetIds...)
	if err := s.svcCtx.Routes.ReplaceGroupMembers(ctx, groupID, allMemberIDs, 0); err != nil {
		logger.Errorf("[CreateGroup] write group %d route failed: %v", groupID, err)
	}

	// 6. 发送群创建通知（成员列表已知，随消息携带定向投递）
	wsMsg := util.NewGroupOperationMsg(message.GroupOperationType_GROUP_OP_CREATE, groupID, targetIds, ownerID, group)
	s.publishGroupNotify(wsMsg, allMemberIDs)

	return group, nil
}

func (s *GroupService) GetGroups(ctx context.Context, groupIDs []uint64, nameKeyword string, limit, offset int32) ([]*model.Group, int64, error) {
	var groups []*model.Group
	var total int64
	var err error

	if len(groupIDs) > 0 {
		// 通过 IDs 查询
		groups, err = s.svcCtx.GroupDAO.FindByIDs(ctx, groupIDs)
		if err != nil {
			return nil, 0, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询群组失败")
		}
		total = int64(len(groups))
	} else if nameKeyword != "" {
		// 通过名字模糊搜索
		groups, total, err = s.svcCtx.GroupDAO.SearchByName(ctx, nameKeyword, int(limit), int(offset))
		if err != nil {
			return nil, 0, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "搜索群组失败")
		}
	} else {
		return []*model.Group{}, 0, nil
	}

	return groups, total, nil
}

func (s *GroupService) UpdateGroup(ctx context.Context, groupID, operatorID uint64, name, avatar string, joinType int32) error {
	_, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新群组失败")
	}

	// 2. 查询群组
	group, err := s.svcCtx.GroupDAO.FindByID(ctx, groupID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "群组不存在")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新群组失败")
	}

	// 3. 更新字段
	if name != "" {
		group.Name = name
	}
	if avatar != "" {
		group.Avatar = avatar
	}
	if joinType != 0 {
		group.JoinType = int(joinType)
	}
	if err := s.svcCtx.GroupDAO.UpdateGroup(ctx, group); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新群组失败")
	}

	return nil
}

func (s *GroupService) DismissGroup(ctx context.Context, groupID, operatorID uint64) error {
	// 1. 检查权限（必须是群主）
	member, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}
	if member.Role != model.GroupRoleOwner {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "只有群主可以解散群")
	}

	// 2. 成员数据随解散删除：先取成员快照，供解散通知定向投递
	memberIDs := s.groupMemberSnapshot(ctx, groupID)

	// 3. 在事务中删除所有成员和群组
	if err := s.svcCtx.GroupDAO.Transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Where("group_id = ?", groupID).Delete(&model.GroupMember{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.Group{}, groupID).Error
	}); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "解散群组失败")
	}

	// 4. 直写路由表：删除群成员集合
	if err := s.svcCtx.Routes.DeleteGroup(ctx, groupID); err != nil {
		logger.Errorf("[DismissGroup] delete group %d route failed: %v", groupID, err)
	}

	// 5. 发送群解散通知
	wsMsg := util.NewGroupOperationMsg(message.GroupOperationType_GROUP_OP_DISMISS, groupID, []uint64{}, operatorID, nil)
	s.publishGroupNotify(wsMsg, memberIDs)

	return nil
}

func (s *GroupService) GetUserGroupIDs(ctx context.Context, userID uint64) ([]uint64, error) {
	groupIDs, err := s.svcCtx.GroupDAO.FindAllGroupIDsByUserID(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户群组失败")
	}

	// 直写路由表：将用户补进其所属各群的成员集合，
	// 替代旧的 USER_GROUP_SYNC NATS 广播（不再依赖网关侧本地群成员映射）
	if len(groupIDs) > 0 {
		if err := s.svcCtx.Routes.AddUserToGroups(ctx, userID, groupIDs); err != nil {
			logger.Errorf("[GetUserGroupIDs] sync user %d group routes failed: %v", userID, err)
		}
	}

	return groupIDs, nil
}
