package authrpclogic

import (
	"context"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Auth/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RefreshLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshLogic {
	return &RefreshLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RefreshLogic) Refresh(in *auth.RefreshReq) (*auth.RefreshResp, error) {
	resp, err := l.svcCtx.AuthService.Refresh(l.ctx, &service.RefreshReq{
		RefreshToken: in.RefreshToken,
		DeviceId:     in.DeviceId,
		Platform:     in.Platform,
	})
	if err != nil {
		return nil, err
	}

	return &auth.RefreshResp{
		Token:        resp.Token,
		RefreshToken: resp.RefreshToken,
	}, nil
}
