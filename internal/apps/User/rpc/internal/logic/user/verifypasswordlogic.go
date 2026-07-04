package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/User/rpc/internal/service"
)

type VerifyPasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewVerifyPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *VerifyPasswordLogic {
	return &VerifyPasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *VerifyPasswordLogic) VerifyPassword(in *user.VerifyPasswordReq) (*user.VerifyPasswordResp, error) {
	// 调用 UserService 验证密码
	valid, err := service.NewUserService(l.svcCtx).VerifyPassword(l.ctx, in.UserId, in.Password)
	if err != nil {
		return nil, err
	}

	return &user.VerifyPasswordResp{Valid: valid}, nil
}
