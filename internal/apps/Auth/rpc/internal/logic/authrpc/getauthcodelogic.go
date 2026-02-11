package authrpclogic

import (
	"context"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Auth/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetAuthCodeLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetAuthCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAuthCodeLogic {
	return &GetAuthCodeLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetAuthCodeLogic) GetAuthCode(in *auth.GetAuthCodeReq) (*auth.GetAuthCodeResp, error) {
	_, err := l.svcCtx.AuthService.GetAuthCode(l.ctx, &service.GetAuthCodeRequest{
		Phone: in.Phone,
	})
	if err != nil {
		return nil, err
	}
	return &auth.GetAuthCodeResp{}, nil
}
