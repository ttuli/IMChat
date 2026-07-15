package service

import (
	"context"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 群成员管理 ==========

func (s *GroupService) InviteMembers(ctx context.Context, groupID, operatorID uint64, memberIDs []uint64) (int32, []uint64, error) {
	// 1. 检查操作者权限
	_, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return 0, nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return 0, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 2. 批量添加成员
	var successCount int32
	var failedIDs []uint64
	now := time.Now()

	for _, memberID := range memberIDs {
		// 检查是否已是成员
		isMember, _ := s.svcCtx.GroupDAO.IsMember(ctx, groupID, memberID)
		if isMember {
			failedIDs = append(failedIDs, memberID)
			continue
		}

		err := s.svcCtx.GroupDAO.InsertMember(ctx, &model.GroupMember{
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

	// 3. 直写路由表：将成功加入的成员补进群成员集合
	var successIDs []uint64
	for _, id := range memberIDs {
		// check if it's in failedIDs
		isFailed := false
		for _, fid := range failedIDs {
			if id == fid {
				isFailed = true
				break
			}
		}
		if !isFailed {
			successIDs = append(successIDs, id)
		}
	}
	if len(successIDs) > 0 {
		s.ensureGroupRoute(ctx, groupID, successIDs...)
	}

	return successCount, failedIDs, nil
}

func (s *GroupService) RemoveMember(ctx context.Context, groupID, operatorID, userID uint64) error {
	// 1. 检查操作者权限
	operator, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 2. 查询被移除者
	target, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, userID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "该用户不是群成员")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 3. 权限检查：群主可移除任何人，管理员只能移除普通成员
	switch operator.Role {
	case model.GroupRoleOwner:
		// 群主不能移除自己
		if userID == operatorID {
			return xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "群主不能移除自己，请使用解散群聊")
		}
	case model.GroupRoleAdmin:
		if target.Role != model.GroupRoleMember {
			return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "管理员只能移除普通成员")
		}
	default:
		return xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "无权移除成员")
	}

	// 4. 摘除前快照成员列表供通知定向投递：被踢用户也收到本次通知
	memberIDs := s.groupMemberSnapshot(ctx, groupID)

	// 5. 移除成员
	if err := s.svcCtx.GroupDAO.DeleteMember(ctx, groupID, userID); err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "移除成员失败")
	}

	// 6. 直写路由表：先于通知发布摘除路由，被踢用户不再收到该群后续消息
	if err := s.svcCtx.Routes.RemoveGroupMembers(ctx, groupID, userID); err != nil {
		logger.Errorf("[RemoveMember] remove group %d route member %d failed: %v", groupID, userID, err)
	}

	// 7. 发送群组通知（踢人）
	wsMsg := util.NewGroupOperationMsg(
		message.GroupOperationType_GROUP_OP_KICK,
		groupID, []uint64{userID}, operatorID, nil,
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
