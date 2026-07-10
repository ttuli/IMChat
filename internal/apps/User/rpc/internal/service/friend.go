package service

import (
	"context"
	"database/sql"

	model "IM2/internal/model"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
	"IM2/pkg/xerr"

	"github.com/gogo/protobuf/proto"
	"gorm.io/gorm"
)

// ========== 好友管理 ==========

// GetFriends 获取好友列表（返回全部）
func (s *UserService) GetFriends(ctx context.Context, userID uint64) ([]*model.UserFriend, error) {
	friends, _, err := s.svcCtx.FriendDAO.FindFriendsByUserID(ctx, userID, -1, 0)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询好友列表失败")
	}
	return friends, nil
}

// UpdateFriend 更新好友信息（备注、拉黑、星标）
func (s *UserService) UpdateFriend(ctx context.Context, userID, friendID uint64, remark string, blocked, starred bool) (*model.UserFriend, error) {
	// 检查好友关系是否存在
	_, err := s.svcCtx.FriendDAO.FindFriendRelation(ctx, userID, friendID)
	if err == gorm.ErrRecordNotFound {
		return nil, xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "好友关系不存在")
	}
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询好友关系失败")
	}

	updates := map[string]any{
		"remark":  remark,
		"blocked": sql.NullBool{Bool: blocked, Valid: true},
		"starred": sql.NullBool{Bool: starred, Valid: true},
	}
	if err := s.svcCtx.FriendDAO.UpdateFriend(ctx, userID, friendID, updates); err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "更新好友信息失败")
	}

	friend, err := s.svcCtx.FriendDAO.FindFriendRelation(ctx, userID, friendID)
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询更新后的好友信息失败")
	}

	return friend, nil
}

// DeleteFriend 删除好友（双向删除）
func (s *UserService) DeleteFriend(ctx context.Context, userID, friendID uint64) error {
	// 检查好友关系是否存在
	friendRecord, err := s.svcCtx.FriendDAO.FindFriendRelation(ctx, userID, friendID)
	if err == gorm.ErrRecordNotFound {
		return xerr.New(transport.ErrorCode_ERR_NOT_FOUND, "好友关系不存在")
	}
	if err != nil {
		return xerr.Wrap(err, transport.ErrorCode_ERR_DATABASE, "查询好友关系失败")
	}

	if msg, err := util.NewFriendUpdateMsg(transport.MessageType_FRIEND_DELETED, friendRecord, userID); err == nil {
		if data, err := proto.Marshal(msg); err == nil {
			if err := s.svcCtx.NatsConn.Publish(s.svcCtx.Config.NATS.BroadcastSubject, data); err != nil {
				logger.Error("DeleteFriend publish error: " + err.Error())
			}
		}
	}

	return nil
}
