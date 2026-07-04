package service

import (
	"IM2/internal/apps/User/rpc/svc"
)

// UserService 用户服务实现
type UserService struct {
	svcCtx *svc.ServiceContext
}

// NewUserService 创建用户服务
func NewUserService(svcCtx *svc.ServiceContext) *UserService {
	return &UserService{
		svcCtx: svcCtx,
	}
}
