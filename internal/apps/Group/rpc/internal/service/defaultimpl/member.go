package defaultimpl

import (
	"context"
	"time"

	"IM2/internal/model"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 群成员管理 ==========

func (s *groupService) InviteMembers(ctx context.Context, groupID, operatorID uint64, memberIDs []uint64) (int32, []uint64, error) {
	// 1. 检查操作者权限
	_, err := s.groupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return 0, nil, xerr.New(xerr.ErrForbidden, "非群成员无权操作")
	}
	if err != nil {
		return 0, nil, xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 2. 批量添加成员
	var successCount int32
	var failedIDs []uint64
	now := time.Now()

	for _, memberID := range memberIDs {
		// 检查是否已是成员
		isMember, _ := s.groupDAO.IsMember(ctx, groupID, memberID)
		if isMember {
			failedIDs = append(failedIDs, memberID)
			continue
		}

		err := s.groupDAO.InsertMember(ctx, &model.GroupMember{
			GroupID:  groupID,
			UserID:   memberID,
			Role:     model.GroupRoleMember,
			JoinedAt: now,
		})
		if err != nil {
			failedIDs = append(failedIDs, memberID)
			continue
		}
		successCount++
	}

	return successCount, failedIDs, nil
}

func (s *groupService) RemoveMember(ctx context.Context, groupID, operatorID, userID uint64) error {
	// 1. 检查操作者权限
	operator, err := s.groupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrForbidden, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 2. 查询被移除者
	target, err := s.groupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrNotFound, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 3. 权限检查：群主可移除任何人，管理员只能移除普通成员
	if operator.Role == model.GroupRoleOwner {
		// 群主不能移除自己
		if userID == operatorID {
			return xerr.New(xerr.ErrInvalidParams, "群主不能移除自己，请使用解散群聊")
		}
	} else if operator.Role == model.GroupRoleAdmin {
		if target.Role != model.GroupRoleMember {
			return xerr.New(xerr.ErrForbidden, "管理员只能移除普通成员")
		}
	} else {
		return xerr.New(xerr.ErrForbidden, "无权移除成员")
	}

	// 4. 移除成员
	if err := s.groupDAO.DeleteMember(ctx, groupID, userID); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "移除成员失败")
	}

	return nil
}

func (s *groupService) LeaveGroup(ctx context.Context, groupID, userID uint64) error {
	// 1. 检查是否是群成员
	member, err := s.groupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrNotFound, "不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 2. 群主不能退出（需要先转让或解散）
	if member.Role == model.GroupRoleOwner {
		return xerr.New(xerr.ErrInvalidParams, "群主不能退出群聊，请先转让群主或解散群")
	}

	// 3. 删除成员
	if err := s.groupDAO.DeleteMember(ctx, groupID, userID); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "退出群聊失败")
	}

	return nil
}

func (s *groupService) SetMemberRole(ctx context.Context, groupID, operatorID, userID uint64, role int8) error {
	// 1. 只有群主可以设置角色
	operator, err := s.groupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrForbidden, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}
	if operator.Role != model.GroupRoleOwner {
		return xerr.New(xerr.ErrForbidden, "只有群主可以设置成员角色")
	}

	// 2. 不能设置自己的角色
	if operatorID == userID {
		return xerr.New(xerr.ErrInvalidParams, "不能修改自己的角色")
	}

	// 3. 检查目标成员存在
	_, err = s.groupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrNotFound, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 4. 不能设置为群主角色
	if role == model.GroupRoleOwner {
		return xerr.New(xerr.ErrInvalidParams, "不能直接设置群主，请使用转让群主功能")
	}

	// 5. 更新角色
	if err := s.groupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"role": role}); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新角色失败")
	}

	return nil
}

func (s *groupService) SetMemberNickname(ctx context.Context, groupID, userID uint64, nickname string) error {
	// 只能修改自己的昵称
	_, err := s.groupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrNotFound, "不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	if err := s.groupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"nickname": nickname}); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "更新昵称失败")
	}

	return nil
}

func (s *groupService) MuteMember(ctx context.Context, groupID, operatorID, userID uint64, muteUntil int64) error {
	// 1. 检查操作者权限
	operator, err := s.groupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrForbidden, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return xerr.New(xerr.ErrForbidden, "无权禁言成员")
	}

	// 2. 查询目标成员
	target, err := s.groupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(xerr.ErrNotFound, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "查询成员失败")
	}

	// 3. 不能禁言群主，管理员只能禁言普通成员
	if target.Role == model.GroupRoleOwner {
		return xerr.New(xerr.ErrForbidden, "不能禁言群主")
	}
	if operator.Role == model.GroupRoleAdmin && target.Role != model.GroupRoleMember {
		return xerr.New(xerr.ErrForbidden, "管理员只能禁言普通成员")
	}

	// 4. 更新禁言时间
	if err := s.groupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"mute_until": muteUntil}); err != nil {
		return xerr.Wrap(err, xerr.ErrDatabase, "设置禁言失败")
	}

	return nil
}

func (s *groupService) GetGroupMemberIDs(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	members, err := s.groupDAO.FindMembersByGroupID(ctx, groupID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询群成员失败")
	}
	return members, nil
}

func (s *groupService) GetGroupManagers(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	managers, err := s.groupDAO.FindManagersByGroupID(ctx, groupID)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "获取群管理角色失败")
	}
	return managers, nil
}
