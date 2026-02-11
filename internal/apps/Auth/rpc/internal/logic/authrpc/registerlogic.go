package authrpclogic

import (
	"context"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Auth/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RegisterLogic) Register(in *auth.RegisterReq) (*auth.RegisterResp, error) {
	resp, err := l.svcCtx.AuthService.Register(l.ctx, &service.RegisterRequest{
		Name:     in.Name,
		Password: in.Password,
		Phone:    in.Phone,
		AuthCode: in.AuthCode,
	})
	if err != nil {
		return nil, err
	}

	return &auth.RegisterResp{
		Id: resp.ID,
	}, nil
}
