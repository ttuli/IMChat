package logic

import (
	"context"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/internal/apps/Auth/rpc/client/authrpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RefreshLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRefreshLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshLogic {
	return &RefreshLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RefreshLogic) Refresh(req *types.RefreshReq) (resp *types.RefreshResp, err error) {
	res, err := l.svcCtx.AuthRpc.Refresh(l.ctx, &authrpc.RefreshReq{
		DeviceId:     req.DeviceId,
		Platform:     req.Platform,
		RefreshToken: req.RefreshToken,
	})
	if err != nil {
		return nil, err
	}
	return &types.RefreshResp{
		Token:        res.Token,
		RefreshToken: res.RefreshToken,
	}, nil
}
