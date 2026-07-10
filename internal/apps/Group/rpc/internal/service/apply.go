package service

import (
	"context"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/social"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"github.com/gogo/protobuf/proto"
	"gorm.io/gorm"
)

// ========== 群申请管理 ==========

func (s *GroupService) JoinGroup(ctx context.Context, groupID, fromUserID uint64, applyMsg string) (*model.GroupApply, *model.GroupMember, error) {
	// 1. 检查群组是否存在
	group, err := s.svcCtx.GroupDAO.FindByID(ctx, groupID)
	if err == gorm.ErrRecordNotFound {
		return nil, nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "群组不存在")
	}
	if err != nil {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询群组失败")
	}

	// 2. 检查是否已是群成员
	isMember, _ := s.svcCtx.GroupDAO.IsMember(ctx, groupID, fromUserID)
	if isMember {
		return nil, nil, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "已是群成员，无需申请")
	}

	// 3. 如果群组设置了"直接同意"，则不创建申请，直接进群
	if group.JoinType == int(model.JoinTypeDirect) {
		member := &model.GroupMember{
			GroupID:  groupID,
			UserID:   fromUserID,
			Role:     model.GroupRoleMember,
			JoinedAt: time.Now(),
		}
		if err := s.svcCtx.GroupDAO.InsertMember(ctx, member); err != nil {
			return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "直接加入群组失败")
		}

		// 直写路由表：先于通知发布补路由，新成员能收到本次入群通知与后续群消息
		s.ensureGroupRoute(ctx, groupID, fromUserID)

		msg := util.NewGroupOperationMsg(social.GroupOperationType_GROUP_OP_JOIN, groupID, []uint64{fromUserID}, 0, group)
		bytes, _ := proto.Marshal(msg)
		err = s.svcCtx.Nats.Publish(s.svcCtx.Config.NATS.BroadcastSubject, bytes)
		if err != nil {
			logger.Errorf("发送nats失败: %v", err)
		}

		return nil, member, nil
	}

	// 4. 检查是否存在重复的待处理申请
	existing, err := s.svcCtx.ApplyDAO.FindExistingPendingApply(ctx, fromUserID, groupID)
	if err == nil && existing != nil {
		return nil, nil, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "已有待处理的入群申请")
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询已有申请失败")
	}

	now := time.Now()
	// 5. 创建群级别申请（任何管理员/群主都可处理）
	apply := &model.GroupApply{
		FromUserID: fromUserID,
		GroupID:    groupID,
		ApplyMsg:   applyMsg,
		Status:     model.GroupApplyStatusPending,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.svcCtx.ApplyDAO.InsertApply(ctx, apply); err != nil {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "创建入群申请失败")
	}

	// 先获取群管理员（包括群主），统一构建一条消息发往 NATS
	managers, err := s.svcCtx.GroupDAO.FindManagersByGroupID(ctx, apply.GroupID)
	if err != nil {
		logger.Errorf("查询群管理员失败: %v", err)
	} else {
		var targetIDs []uint64
		for _, manager := range managers {
			targetIDs = append(targetIDs, manager.UserID)
		}
		if len(targetIDs) > 0 {
			msg, _ := util.ConvertGroupApplyToWSMessage(apply, targetIDs)
			bytes, _ := proto.Marshal(msg)
			err = s.svcCtx.Nats.Publish(s.svcCtx.Config.NATS.BroadcastSubject, bytes)
			if err != nil {
				logger.Errorf("向管理员发送nats消息失败: %v", err)
			}
		}
	}

	return apply, nil, nil
}

func (s *GroupService) HandleGroupApply(ctx context.Context, applyID, operatorID uint64, status uint8, rejectReason string) (*model.GroupApply, error) {
	// 1. 查询申请记录
	apply, err := s.svcCtx.ApplyDAO.FindApplyByID(ctx, applyID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "申请记录不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询申请记录失败")
	}

	// 2. 校验权限：操作者必须是该群的管理员或群主
	member, err := s.svcCtx.GroupDAO.FindMember(ctx, apply.GroupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}
	if member.Role != model.GroupRoleOwner && member.Role != model.GroupRoleAdmin {
		return nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "只有群主或管理员可以处理申请")
	}

	// 3. 校验状态：只能处理待处理的申请
	if apply.Status != model.GroupApplyStatusPending {
		return apply, nil
	}

	// 4. 更新申请状态和处理人
	if err := s.svcCtx.ApplyDAO.UpdateApplyStatusWithHandler(ctx, applyID, status, operatorID, rejectReason); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新申请状态失败")
	}

	// 5. 如果同意，添加成员
	if status == model.GroupApplyStatusAccepted {
		// 再次检查是否已是成员（防止并发）
		isMember, _ := s.svcCtx.GroupDAO.IsMember(ctx, apply.GroupID, apply.FromUserID)
		if !isMember {
			if err := s.svcCtx.GroupDAO.InsertMember(ctx, &model.GroupMember{
				GroupID:  apply.GroupID,
				UserID:   apply.FromUserID,
				Role:     model.GroupRoleMember,
				JoinedAt: time.Now(),
			}); err != nil {
				return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "添加群成员失败")
			}

			// 直写路由表：先于通知发布补路由
			s.ensureGroupRoute(ctx, apply.GroupID, apply.FromUserID)

			msg := util.NewGroupOperationMsg(social.GroupOperationType_GROUP_OP_JOIN, apply.GroupID, []uint64{apply.FromUserID}, operatorID, nil)
			bytes, _ := proto.Marshal(msg)
			err = s.svcCtx.Nats.Publish(s.svcCtx.Config.NATS.BroadcastSubject, bytes)
			if err != nil {
				logger.Errorf("发送nats失败: %v", err)
			}

		}
	}

	// 6. 返回更新后的记录
	apply.Status = status
	apply.HandlerID = operatorID
	apply.UpdateTime = time.Now()

	// 获取群管理员（包括群主）和申请者，统一构建一条消息发往 NATS
	managers, err := s.svcCtx.GroupDAO.FindManagersByGroupID(ctx, apply.GroupID)
	if err != nil {
		logger.Errorf("查询群管理员失败: %v", err)
	} else {
		targets := make(map[uint64]bool)
		for _, manager := range managers {
			targets[manager.UserID] = true
		}
		if apply.HandlerID != 0 {
			targets[apply.FromUserID] = true
		}

		var targetIDs []uint64
		for targetID := range targets {
			targetIDs = append(targetIDs, targetID)
		}

		if len(targetIDs) > 0 {
			msg, _ := util.ConvertGroupApplyToWSMessage(apply, targetIDs)
			bytes, _ := proto.Marshal(msg)
			err = s.svcCtx.Nats.Publish(s.svcCtx.Config.NATS.BroadcastSubject, bytes)
			if err != nil {
				logger.Errorf("向相关用户发送nats消息失败: %v", err)
			}
		}
	}

	return apply, nil
}

func (s *GroupService) GetPendingApplies(ctx context.Context, userID uint64) ([]*model.GroupApply, error) {
	allApplies := make([]*model.GroupApply, 0)

	// 1. 查询用户是管理员/群主的群ID列表
	groupIDs, err := s.svcCtx.GroupDAO.FindAdminGroupIDs(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询管理的群组失败")
	}

	// 2. 查询这些群的待处理申请（不分页，获取全部）
	if len(groupIDs) > 0 {
		applies, _, err := s.svcCtx.ApplyDAO.FindPendingAppliesByGroupIDs(ctx, groupIDs, -1, 0)
		if err != nil {
			return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询待处理申请失败")
		}
		allApplies = append(allApplies, applies...)
	}

	// 3. 查询用户自己发出的申请
	myApplies, _, err := s.svcCtx.ApplyDAO.FindPendingAppliesByFromUserID(ctx, userID, -1, 0)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询个人申请失败")
	}
	allApplies = append(allApplies, myApplies...)

	return allApplies, nil
}
