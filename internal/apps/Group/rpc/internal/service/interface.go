package service

import (
	"context"

	"IM2/internal/model"
)

// GroupWithMembers 群组和成员组合
type GroupWithMembers struct {
	Group   *model.Group
	Members []*model.GroupMember
}

// GroupService 群组服务接口
type GroupService interface {
	// ========== 群组管理 ==========

	// CreateGroup 创建群组，返回群组和成员列表
	CreateGroup(ctx context.Context, ownerID uint64, name, avatar string, memberIDs []uint64) (*model.Group, error)

	// GetGroups 批量获取群组信息，支持通过 IDs 或名字模糊搜索
	GetGroups(ctx context.Context, groupIDs []uint64, nameKeyword string, limit, offset int32) ([]*model.Group, int64, error)

	// UpdateGroup 更新群组信息
	UpdateGroup(ctx context.Context, groupID, operatorID uint64, name, avatar string) error

	// DismissGroup 解散群组
	DismissGroup(ctx context.Context, groupID, operatorID uint64) error

	// GetUserGroupIDs 获取用户所在的群组ID列表
	GetUserGroupIDs(ctx context.Context, userID uint64) ([]uint64, error)

	// ========== 群成员管理 ==========

	// InviteMembers 邀请成员加入群
	InviteMembers(ctx context.Context, groupID, operatorID uint64, memberIDs []uint64) (successCount int32, failedIDs []uint64, err error)

	// RemoveMember 移除群成员
	RemoveMember(ctx context.Context, groupID, operatorID, userID uint64) error

	// LeaveGroup 退出群聊
	LeaveGroup(ctx context.Context, groupID, userID uint64) error

	// SetMemberRole 设置成员角色
	SetMemberRole(ctx context.Context, groupID, operatorID, userID uint64, role int8) error

	// SetMemberNickname 设置群内昵称
	SetMemberNickname(ctx context.Context, groupID, userID uint64, nickname string) error

	// MuteMember 禁言成员
	MuteMember(ctx context.Context, groupID, operatorID, userID uint64, muteUntil int64) error

	// GetGroupMemberIDs 获取群成员列表
	GetGroupMemberIDs(ctx context.Context, groupID uint64) ([]*model.GroupMember, error)

	// ========== 群申请管理 ==========

	// JoinGroup 申请加入群聊（群级别申请，任何管理员/群主都可处理）
	JoinGroup(ctx context.Context, groupID, fromUserID uint64, applyMsg string) (*model.GroupApply, error)

	// HandleGroupApply 处理群申请
	HandleGroupApply(ctx context.Context, applyID, operatorID uint64, status uint8, rejectReason string) error

	// GetPendingApplies 获取待处理的群申请
	GetPendingApplies(ctx context.Context, userID uint64) ([]*model.GroupApply, error)
}
