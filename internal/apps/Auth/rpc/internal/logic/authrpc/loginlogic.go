package authrpclogic

import (
	"context"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Auth/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(in *auth.LoginRequest) (*auth.LoginReply, error) {
	resp, err := l.svcCtx.AuthService.Login(l.ctx, &service.LoginRequest{
		Account:   in.Account,
		Password:  in.Password,
		DeviceID:  in.DeviceId,
		RemeberMe: in.RemeberMe,
	})
	if err != nil {
		return nil, err
	}

	return &auth.LoginReply{
		Token:        resp.Token,
		RefreshToken: resp.RefreshToken,
	}, nil
}
