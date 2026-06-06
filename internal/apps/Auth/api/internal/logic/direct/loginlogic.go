package logic

import (
	"context"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/internal/apps/Auth/rpc/client/authrpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginLogic) Login(req *types.LoginReq) (resp *types.LoginResp, err error) {
	rpcResp, err := l.svcCtx.AuthRpc.Login(l.ctx, &authrpc.LoginRequest{
		Account:   uint64(req.Account),
		Password:  req.Password,
		DeviceId:  req.DeviceId,
		RemeberMe: req.RemeberMe,
	})
	if err != nil {
		return nil, err
	}

	return &types.LoginResp{
		Token:        rpcResp.Token,
		RefreshToken: rpcResp.RefreshToken,
	}, nil
}
