package logic

import (
	"context"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/internal/apps/Auth/rpc/client/authrpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RegisterLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RegisterLogic) Register(req *types.RegisterReq) (resp *types.RegisterResp, err error) {
	rpcResp, err := l.svcCtx.AuthRpc.Register(l.ctx, &authrpc.RegisterReq{
		Name:     req.Name,
		Password: req.Password,
		Phone:    req.Phone,
		AuthCode: req.AuthCode,
	})
	if err != nil {
		return nil, err
	}

	return &types.RegisterResp{
		Id: rpcResp.Id,
	}, nil
}
