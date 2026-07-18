package service

import (
	"context"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/proto/message"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 群邀请管理（被邀请人确认制）==========
//
// 邀请不再直接拉人入群，而是为每个被邀请者创建一条待处理 group_invite 记录，
// 由被邀请者本人在收件箱接受/拒绝后才真正入群。批量建记录（单条 SQL）避免了
// 旧实现逐成员事务累积导致的 RPC 超时。

// InviteMembers 群成员邀请他人：为每个有效目标创建待处理邀请记录。
// 返回成功创建的邀请数与失败目标（已是成员 / 已有待处理邀请 / 邀请自己）。
func (s *GroupService) InviteMembers(ctx context.Context, groupID, operatorID uint64, memberIDs []uint64) (int32, []uint64, error) {
	// 1. 邀请人必须是群成员
	_, err := s.svcCtx.GroupDAO.FindMember(ctx, groupID, operatorID)
	if err == gorm.ErrRecordNotFound {
		return 0, nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "非群成员无权操作")
	}
	if err != nil {
		return 0, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询成员失败")
	}

	// 2. 群组必须存在
	if _, err := s.svcCtx.GroupDAO.FindByID(ctx, groupID); err == gorm.ErrRecordNotFound {
		return 0, nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "群组不存在")
	} else if err != nil {
		return 0, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询群组失败")
	}

	// 3. 过滤无效目标，构建待落库邀请
	var invites []*model.GroupInvite
	var failedIDs []uint64
	for _, inviteeID := range memberIDs {
		if inviteeID == 0 || inviteeID == operatorID {
			failedIDs = append(failedIDs, inviteeID)
			continue
		}
		if isMember, _ := s.svcCtx.GroupDAO.IsMember(ctx, groupID, inviteeID); isMember {
			failedIDs = append(failedIDs, inviteeID)
			continue
		}
		if existing, err := s.svcCtx.InviteDAO.FindExistingPendingInvite(ctx, groupID, inviteeID); err == nil && existing != nil {
			failedIDs = append(failedIDs, inviteeID)
			continue
		}
		invites = append(invites, model.NewGroupInvite(groupID, operatorID, inviteeID, ""))
	}

	// 4. 批量落库（单条 SQL）
	if len(invites) > 0 {
		if err := s.svcCtx.InviteDAO.InsertInvites(ctx, invites); err != nil {
			return 0, memberIDs, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "创建邀请失败")
		}
	}

	return int32(len(invites)), failedIDs, nil
}

// HandleGroupInvite 被邀请人处理邀请：accept=true 入群，false 拒绝。
// 幂等：邀请已处理时直接返回，不重复入群。
func (s *GroupService) HandleGroupInvite(ctx context.Context, inviteID, inviteeID uint64, accept bool) (*model.GroupMember, error) {
	// 1. 查询邀请记录
	invite, err := s.svcCtx.InviteDAO.FindInviteByID(ctx, inviteID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "邀请不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询邀请失败")
	}

	// 2. 校验归属：只能处理发给自己的邀请
	if invite.InviteeID != inviteeID {
		return nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "无权处理该邀请")
	}

	// 3. 幂等：非待处理直接返回
	if invite.Status != model.GroupInviteStatusPending {
		return nil, nil
	}

	// 4. 拒绝：仅更新状态
	if !accept {
		if err := s.svcCtx.InviteDAO.UpdateInviteStatus(ctx, inviteID, model.GroupInviteStatusRejected); err != nil {
			return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新邀请状态失败")
		}
		return nil, nil
	}

	// 5. 接受：先置状态，再入群
	if err := s.svcCtx.InviteDAO.UpdateInviteStatus(ctx, inviteID, model.GroupInviteStatusAccepted); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新邀请状态失败")
	}

	// 群可能在邀请待处理期间被解散
	group, err := s.svcCtx.GroupDAO.FindByID(ctx, invite.GroupID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "群组不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询群组失败")
	}

	// 并发去重：期间可能已通过其他途径入群
	if isMember, _ := s.svcCtx.GroupDAO.IsMember(ctx, invite.GroupID, inviteeID); isMember {
		return nil, nil
	}

	member := &model.GroupMember{
		GroupID:  invite.GroupID,
		UserID:   inviteeID,
		Role:     model.GroupRoleMember,
		JoinedAt: time.Now(),
	}
	if err := s.svcCtx.GroupDAO.InsertMember(ctx, member); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "加入群组失败")
	}

	// 直写路由表：先于通知发布补路由，新成员能收到本次入群通知与后续群消息
	s.ensureGroupRoute(ctx, invite.GroupID, inviteeID)

	// 发送入群通知（沿用现有群会话通知链路，定向投递给全体成员）
	wsMsg := util.NewGroupOperationMsg(message.GroupOperationType_GROUP_OP_JOIN, invite.GroupID, []uint64{inviteeID}, inviteeID, group)
	s.publishGroupNotify(wsMsg, s.groupMemberSnapshot(ctx, invite.GroupID))

	return member, nil
}

// GetPendingInvites 查询用户收到的全部待处理邀请（收件箱）
func (s *GroupService) GetPendingInvites(ctx context.Context, userID uint64) ([]*model.GroupInvite, error) {
	invites, err := s.svcCtx.InviteDAO.FindPendingInvitesByInviteeID(ctx, userID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询邀请列表失败")
	}
	return invites, nil
}
