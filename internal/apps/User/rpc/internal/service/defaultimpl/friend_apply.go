package defaultimpl

import (
	"context"

	"IM2/internal/model"
	"IM2/pkg/xerr"

	"gorm.io/gorm"
)

// ========== 好友申请 ==========

// NewFriendApply 发起好友申请
func (s *userService) NewFriendApply(ctx context.Context, fromUserID, toUserID uint64, applyMsg string) (*model.FriendApply, error) {
	// 1. 检查是否已经是好友
	_, err := s.friendDAO.FindFriendRelation(ctx, fromUserID, toUserID)
	if err == nil {
		return nil, xerr.New(xerr.ErrInvalidParams, "已经是好友，无需重复申请")
	}
	if err != gorm.ErrRecordNotFound {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询好友关系失败")
	}

	// 2. 检查是否存在重复的待处理申请
	existing, err := s.friendApplyDAO.FindExistingPendingApply(ctx, fromUserID, toUserID)
	if err == nil && existing != nil {
		return nil, xerr.New(xerr.ErrInvalidParams, "已有待处理的好友申请")
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询已有申请失败")
	}

	// 3. 创建新申请
	apply := &model.FriendApply{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		ApplyMsg:   applyMsg,
		Status:     model.ApplyStatusPending,
	}
	if err := s.friendApplyDAO.InsertFriendApply(ctx, apply); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "创建好友申请失败")
	}

	return apply, nil
}

// HandleFriendApply 处理好友申请（同意/拒绝），返回更新后的申请记录
func (s *userService) HandleFriendApply(ctx context.Context, applyID, operatorID uint64, status uint8, rejectReason string) (*model.FriendApply, error) {
	// 1. 查询申请记录
	apply, err := s.friendApplyDAO.FindFriendApplyByID(ctx, applyID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(xerr.ErrNotFound, "申请记录不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询申请记录失败")
	}

	// 2. 校验权限：只有接收人才能处理
	if apply.ToUserID != operatorID {
		return nil, xerr.New(xerr.ErrForbidden, "无权处理此申请")
	}

	// 3. 校验状态：只能处理待处理的申请
	if apply.Status != model.ApplyStatusPending {
		return nil, xerr.New(xerr.ErrInvalidParams, "申请已被处理")
	}

	// 4. 更新申请状态
	if err := s.friendApplyDAO.UpdateFriendApplyStatus(ctx, applyID, status, rejectReason); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "更新申请状态失败")
	}

	// 5. 如果同意，创建好友关系
	if status == model.ApplyStatusAccepted {
		if err := s.friendDAO.InsertFriend(ctx, apply.FromUserID, apply.ToUserID, model.FriendSourceSearch); err != nil {
			return nil, xerr.Wrap(err, xerr.ErrDatabase, "创建好友关系失败")
		}
	}

	// 6. 返回更新后的记录
	apply.Status = status
	apply.RejectReason = rejectReason
	return apply, nil
}

// GetPendingFriendApplies 获取待处理的好友申请（返回全部）
func (s *userService) GetPendingFriendApplies(ctx context.Context, userID uint64) ([]*model.FriendApply, error) {
	applies, _, err := s.friendApplyDAO.FindPendingAppliesByToUserID(ctx, userID, -1, 0)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDatabase, "查询待处理申请失败")
	}
	return applies, nil
}
