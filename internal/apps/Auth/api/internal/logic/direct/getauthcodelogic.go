package logic

import (
	"context"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/internal/apps/Auth/rpc/client/authrpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetAuthCodeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetAuthCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAuthCodeLogic {
	return &GetAuthCodeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetAuthCodeLogic) GetAuthCode(req *types.GetAuthCodeReq) (resp *types.GetAuthCodeResp, err error) {
	_, err = l.svcCtx.AuthRpc.GetAuthCode(l.ctx, &authrpc.GetAuthCodeReq{
		Phone: req.Phone,
	})
	if err != nil {
		return nil, err
	}

	return &types.GetAuthCodeResp{}, nil
}
