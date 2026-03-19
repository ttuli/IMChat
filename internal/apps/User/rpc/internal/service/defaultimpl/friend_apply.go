package defaultimpl

import (
	"context"
	"time"

	"IM2/internal/common"
	"IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/xerr"

	"github.com/gogo/protobuf/proto"
	"gorm.io/gorm"
)

// ========== 好友申请 ==========

// NewFriendApply 发起好友申请
func (s *userService) NewFriendApply(ctx context.Context, fromUserID, toUserID uint64, applyMsg string, source uint8) (*model.FriendApply, *model.UserFriend, error) {
	// 1. 检查是否已经是好友
	_, err := s.friendDAO.FindFriendRelation(ctx, fromUserID, toUserID)
	if err == nil {
		return nil, nil, xerr.New(xerr.ErrInvalidParams, "已经是好友，无需重复申请")
	}
	if err != gorm.ErrRecordNotFound {
		return nil, nil, xerr.Wrap(err, xerr.ErrDatabase, "查询好友关系失败")
	}

	// 2. 检查对方用户的加好友设置
	toUser, err := s.userDAO.FindOneByID(ctx, toUserID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil, xerr.New(xerr.ErrNotFound, "目标用户不存在")
		}
		return nil, nil, xerr.Wrap(err, xerr.ErrDatabase, "查询用户失败")
	}

	// 3. 如果对方设置了"直接同意"，则不创建申请，直接创建好友关系
	if toUser.JoinType == model.JoinTypeDirect {
		err = s.friendDAO.InsertFriendTx(ctx, s.friendDAO.DB(), fromUserID, toUserID, source)
		if err != nil {
			return nil, nil, xerr.Wrap(err, xerr.ErrDatabase, "直接添加好友失败")
		}

		friendRecord, _ := s.friendDAO.FindFriendRelation(ctx, fromUserID, toUserID)

		msg, _ := common.NewFriendUpdateMsg(common.MessageType_FRIEND_ADD, friendRecord, toUserID)
		data, _ := proto.Marshal(msg)
		_, err = s.js.Publish(s.Config.NATS.BroadcastSubject, data)
		if err != nil {
			logger.Error(err.Error())
		}
		return nil, friendRecord, nil
	}

	// 4. 检查是否存在重复的待处理申请
	existing, err := s.friendApplyDAO.FindExistingPendingApply(ctx, fromUserID, toUserID)
	if err == nil && existing != nil {
		return nil, nil, xerr.New(xerr.ErrInvalidParams, "已有待处理的好友申请")
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, nil, xerr.Wrap(err, xerr.ErrDatabase, "查询已有申请失败")
	}

	now := time.Now()
	// 5. 创建新申请
	apply := &model.FriendApply{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		ApplyMsg:   applyMsg,
		Status:     model.ApplyStatusPending,
		Source:     source,
		CreateTime: now,
		HandleTime: now,
	}
	if err := s.friendApplyDAO.InsertFriendApply(ctx, apply); err != nil {
		return nil, nil, xerr.Wrap(err, xerr.ErrDatabase, "创建好友申请失败")
	}

	msg, _ := common.ConvertFriendApplyToWSMessage(apply, toUserID)
	data, _ := proto.Marshal(msg)
	_, err = s.js.Publish(s.Config.NATS.BroadcastSubject, data)
	if err != nil {
		logger.Error(err.Error())
	}

	return apply, nil, nil
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
		return apply, nil
	}

	// 4. 在事务中更新状态并可能创建好友
	err = s.friendApplyDAO.DB().Transaction(func(tx *gorm.DB) error {
		// 4.1 更新申请状态
		if err := s.friendApplyDAO.UpdateFriendApplyStatusTx(ctx, tx, applyID, status, rejectReason); err != nil {
			return xerr.Wrap(err, xerr.ErrDatabase, "更新申请状态失败")
		}

		// 4.2 如果同意，创建好友关系
		if status == model.ApplyStatusAccepted {
			if err := s.friendDAO.InsertFriendTx(ctx, tx, apply.FromUserID, apply.ToUserID, model.FriendSourceSearch); err != nil {
				return xerr.Wrap(err, xerr.ErrDatabase, "更新申请状态失败")
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// 5. 返回更新后的记录
	apply.HandleTime = time.Now()
	apply.Status = status
	apply.RejectReason = rejectReason

	msg, _ := common.ConvertFriendApplyToWSMessage(apply, apply.FromUserID)
	data, _ := proto.Marshal(msg)
	_, err = s.js.Publish(s.Config.NATS.BroadcastSubject, data)
	if err != nil {
		logger.Error(err.Error())
	}
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
