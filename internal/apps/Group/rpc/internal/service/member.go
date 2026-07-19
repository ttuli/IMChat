package service

import (
	"context"

	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 群成员管理 ==========

// RemoveMember 移除单个成员（兼容旧调用），内部走批量路径
func (s *GroupService) RemoveMember(ctx context.Context, groupID, operatorID, userID uint64) error {
	return s.RemoveMembers(ctx, groupID, operatorID, []uint64{userID})
}

// RemoveMembers 批量移除群成员：单事务批删 + 一次路由摘除 + 一条 KICK 通知
// （发布到 DBSubject 落库后按快照定向扇出，被踢者与操作者均收到）。
func (s *GroupService) RemoveMembers(ctx context.Context, groupID, operatorID uint64, userIDs []uint64) error {
	// 1. 检查操作者权限
	operator, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "无权移除成员")
	}

	// 2. 逐个校验目标：群主可移除任何人（除自己），管理员只能移除普通成员
	targets := make([]uint64, 0, len(userIDs))
	seen := make(map[uint64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, dup := seen[userID]; dup {
			continue
		}
		seen[userID] = struct{}{}

		if userID == operatorID {
			return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "不能移除自己")
		}
		target, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
		if err == gorm.ErrRecordNotFound {
			continue // 已不在群内，跳过
		}
		if err != nil {
			return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
		}
		if target.Role == model.GroupRoleOwner {
			return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "不能移除群主")
		}
		if operator.Role == model.GroupRoleAdmin && target.Role != model.GroupRoleMember {
			return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "管理员只能移除普通成员")
		}
		targets = append(targets, userID)
	}
	if len(targets) == 0 {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "没有可移除的成员")
	}

	// 3. 摘除前快照成员列表供通知定向投递：被踢用户也收到本次通知
	memberIDs := s.groupMemberSnapshot(ctx, groupID)

	// 4. 批量移除成员（单事务，人数按实际删除行数递减）
	if _, err := s.svcCtx.GroupDAO.DeleteMembers(ctx, groupID, targets); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "移除成员失败")
	}

	// 5. 直写路由表：先于通知发布摘除路由，被踢用户不再收到该群后续消息
	if err := s.svcCtx.Routes.RemoveGroupMembers(ctx, groupID, targets...); err != nil {
		logger.Errorf("[RemoveMembers] remove group %d route members %v failed: %v", groupID, targets, err)
	}

	// 6. 发送群组通知（踢人，一条通知携带全部目标）
	wsMsg := util.NewGroupOperationMsg(
		message.GroupOperationType_GROUP_OP_KICK,
		groupID, targets, operatorID, nil,
	)
	s.publishGroupNotify(wsMsg, memberIDs)

	return nil
}

func (s *GroupService) LeaveGroup(ctx context.Context, groupID, userID uint64) error {
	// 1. 检查是否是群成员
	member, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 2. 群主不能退出（需要先转让或解散）
	if member.Role == model.GroupRoleOwner {
		return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "群主不能退出群聊，请先转让群主或解散群")
	}

	// 3. 摘除前快照成员列表供通知定向投递
	memberIDs := s.groupMemberSnapshot(ctx, groupID)

	// 4. 删除成员
	if err := s.svcCtx.GroupDAO.DeleteMember(ctx, groupID, userID); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "退出群聊失败")
	}

	// 5. 直写路由表：摘除该成员的群路由
	if err := s.svcCtx.Routes.RemoveGroupMembers(ctx, groupID, userID); err != nil {
		logger.Errorf("[LeaveGroup] remove group %d route member %d failed: %v", groupID, userID, err)
	}

	// 6. 发送群组通知（主动退群）
	wsMsg := util.NewGroupOperationMsg(
		message.GroupOperationType_GROUP_OP_LEAVE,
		groupID, []uint64{userID}, userID, nil,
	)
	s.publishGroupNotify(wsMsg, memberIDs)

	return nil
}

func (s *GroupService) SetMemberRole(ctx context.Context, groupID, operatorID, userID uint64, role int8) error {
	// 1. 只有群主可以设置角色
	operator, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}
	if operator.Role != model.GroupRoleOwner {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "只有群主可以设置成员角色")
	}

	// 2. 不能设置自己的角色
	if operatorID == userID {
		return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "不能修改自己的角色")
	}

	// 3. 检查目标成员存在
	_, err = s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 4. 不能设置为群主角色
	if role == model.GroupRoleOwner {
		return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "不能直接设置群主，请使用转让群主功能")
	}

	// 5. 更新角色
	if err := s.svcCtx.GroupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"role": role}); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新角色失败")
	}

	return nil
}

func (s *GroupService) SetMemberNickname(ctx context.Context, groupID, userID uint64, nickname string) error {
	// 只能修改自己的昵称
	_, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	if err := s.svcCtx.GroupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"nickname": nickname}); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新昵称失败")
	}

	return nil
}

func (s *GroupService) MuteMember(ctx context.Context, groupID, operatorID, userID uint64, muteUntil int64) error {
	// 1. 检查操作者权限
	operator, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "无权禁言成员")
	}

	// 2. 查询目标成员
	target, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 3. 不能禁言群主，管理员只能禁言普通成员
	if target.Role == model.GroupRoleOwner {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "不能禁言群主")
	}
	if operator.Role == model.GroupRoleAdmin && target.Role != model.GroupRoleMember {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "管理员只能禁言普通成员")
	}

	// 4. 更新禁言时间
	if err := s.svcCtx.GroupDAO.UpdateMember(ctx, groupID, userID, map[string]any{"mute_until": muteUntil}); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "设置禁言失败")
	}

	return nil
}

func (s *GroupService) GetGroupMemberIDs(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	members, err := s.svcCtx.GroupDAO.FindMembersByGroupID(ctx, groupID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询群成员失败")
	}
	return members, nil
}

func (s *GroupService) GetGroupManagers(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	managers, err := s.svcCtx.GroupDAO.FindManagersByGroupID(ctx, groupID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "获取群管理角色失败")
	}
	return managers, nil
}
