package service

import (
	"context"

	"IM2/internal/model"
)

// UserService 用户服务接口
type UserService interface {
	CreateUser(ctx context.Context, info *model.UserInfo) (uint64, error)
	VerifyPassword(ctx context.Context, userId uint64, password string) (bool, error)
	// GetUsersByIDs 根据ID列表获取用户
	GetUsersByIDs(ctx context.Context, ids []uint64) ([]*model.UserInfo, error)
	// GetUserByPhone 根据手机号获取用户
	GetUserByPhone(ctx context.Context, phone string) (*model.UserInfo, error)
	// GetUsersByName 根据名字模糊查询用户
	GetUsersByName(ctx context.Context, name string, limit, offset int32) ([]*model.UserInfo, error)
	// UpdateUserInfo 更新用户信息
	UpdateUserInfo(ctx context.Context, id uint64, name, avatar string, gender, joinType uint8, personalSignature string) error

	// ========== 好友管理 ==========
	// GetFriends 获取好友列表（返回全部）
	GetFriends(ctx context.Context, userID uint64) ([]*model.UserFriend, error)
	// CreateFriend 创建好友关系（双向）
	CreateFriend(ctx context.Context, userID, friendID uint64, source uint8, remark string) error
	// UpdateFriend 更新好友信息（备注、拉黑、星标）
	UpdateFriend(ctx context.Context, userID, friendID uint64, remark string, blocked, starred bool) error
	// DeleteFriend 删除好友（双向删除）
	DeleteFriend(ctx context.Context, userID, friendID uint64) error

	// ========== 好友申请 ==========
	// NewFriendApply 发起好友申请
	NewFriendApply(ctx context.Context, fromUserID, toUserID uint64, applyMsg string) (*model.FriendApply, error)
	// HandleFriendApply 处理好友申请（同意/拒绝），返回更新后的申请记录
	HandleFriendApply(ctx context.Context, applyID, operatorID uint64, status uint8, rejectReason string) (*model.FriendApply, error)
	// GetPendingFriendApplies 获取待处理的好友申请（返回全部）
	GetPendingFriendApplies(ctx context.Context, userID uint64) ([]*model.FriendApply, error)
}
