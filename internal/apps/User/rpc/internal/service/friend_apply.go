package service

import (
	"context"
	"time"

	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"github.com/gogo/protobuf/proto"
	"gorm.io/gorm"
)

// ========== 好友申请 ==========

// NewFriendApply 发起好友申请
func (s *UserService) NewFriendApply(ctx context.Context, fromUserID, toUserID uint64, applyMsg string, source uint8) (*model.FriendApply, *model.UserFriend, error) {
	// 1. 检查是否已经是好友
	_, err := s.svcCtx.FriendDAO.FindFriendRelation(ctx, fromUserID, toUserID)
	if err == nil {
		return nil, nil, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "已经是好友，无需重复申请")
	}
	if err != gorm.ErrRecordNotFound {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询好友关系失败")
	}

	// 2. 检查对方用户的加好友设置
	toUser, err := s.svcCtx.UserDAO.FindOneByID(ctx, toUserID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "目标用户不存在")
		}
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询用户失败")
	}

	// 3. 如果对方设置了"直接同意"，则不创建申请，直接创建好友关系
	if toUser.JoinType == model.JoinTypeDirect {
		err = s.svcCtx.FriendDAO.InsertFriendTx(ctx, s.svcCtx.FriendDAO.DB(), fromUserID, toUserID, source)
		if err != nil {
			return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "直接添加好友失败")
		}
		
		friendRecord, _ := s.svcCtx.FriendDAO.FindFriendRelation(ctx, fromUserID, toUserID)

		msg, _ := util.NewFriendUpdateMsg(transport.MessageType_FRIEND_ADD, friendRecord, toUserID)
		data, _ := proto.Marshal(msg)
		_, err = s.svcCtx.Js.Publish(s.svcCtx.Config.NATS.BroadcastSubject, data)
		if err != nil {
			logger.Error(err.Error())
		}
		return nil, friendRecord, nil
	}

	// 4. 检查是否存在重复的待处理申请
	existing, err := s.svcCtx.FriendApplyDAO.FindExistingPendingApply(ctx, fromUserID, toUserID)
	if err == nil && existing != nil {
		return nil, nil, xerr.New(transport.ErrorCode_ERR_INVALID_PARAMS, "已有待处理的好友申请")
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询已有申请失败")
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
	if err := s.svcCtx.FriendApplyDAO.InsertFriendApply(ctx, apply); err != nil {
		return nil, nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "创建好友申请失败")
	}

	msg, _ := util.ConvertFriendApplyToWSMessage(apply, toUserID)
	data, _ := proto.Marshal(msg)
	_, err = s.svcCtx.Js.Publish(s.svcCtx.Config.NATS.BroadcastSubject, data)
	if err != nil {
		logger.Error(err.Error())
	}

	return apply, nil, nil
}

// HandleFriendApply 处理好友申请（同意/拒绝），返回更新后的申请记录
func (s *UserService) HandleFriendApply(ctx context.Context, applyID, operatorID uint64, status uint8, rejectReason string) (*model.FriendApply, error) {
	// 1. 查询申请记录
	apply, err := s.svcCtx.FriendApplyDAO.FindFriendApplyByID(ctx, applyID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "申请记录不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询申请记录失败")
	}

	// 2. 校验权限：只有接收人才能处理
	if apply.ToUserID != operatorID {
		return nil, xerr.New(transport.ErrorCode_ERR_FORBIDDEN, "无权处理此申请")
	}

	// 3. 校验状态：只能处理待处理的申请
	if apply.Status != model.ApplyStatusPending {
		return apply, nil
	}

	// 4. 在事务中更新状态并可能创建好友
	err = s.svcCtx.FriendApplyDAO.DB().Transaction(func(tx *gorm.DB) error {
		// 4.1 更新申请状态
		if err := s.svcCtx.FriendApplyDAO.UpdateFriendApplyStatusTx(ctx, tx, applyID, status, rejectReason); err != nil {
			return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新申请状态失败")
		}

		// 4.2 如果同意，创建好友关系
		if status == model.ApplyStatusAccepted {
			if err := s.svcCtx.FriendDAO.InsertFriendTx(ctx, tx, apply.FromUserID, apply.ToUserID, model.FriendSourceSearch); err != nil {
				return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新申请状态失败")
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

	msg, _ := util.ConvertFriendApplyToWSMessage(apply, apply.FromUserID)
	data, _ := proto.Marshal(msg)
	_, err = s.svcCtx.Js.Publish(s.svcCtx.Config.NATS.BroadcastSubject, data)
	if err != nil {
		logger.Error(err.Error())
	}
	return apply, nil
}

// GetPendingFriendApplies 获取待处理的好友申请（返回全部）
func (s *UserService) GetPendingFriendApplies(ctx context.Context, userID uint64) ([]*model.FriendApply, error) {
	applies, _, err := s.svcCtx.FriendApplyDAO.FindPendingAppliesByToUserID(ctx, userID, -1, 0)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询待处理申请失败")
	}
	return applies, nil
}
