package service

import (
	"IM2/internal/apps/Group/rpc/svc"
)

// GroupService 群组服务实现
type GroupService struct {
	svcCtx *svc.ServiceContext
}

// NewGroupService 创建群组服务
func NewGroupService(svcCtx *svc.ServiceContext) *GroupService {
	return &GroupService{
		svcCtx: svcCtx,
	}
}
