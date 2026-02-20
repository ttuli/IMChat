package group

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取群组信息
func NewGetGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupLogic {
	return &GetGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetGroupLogic) GetGroup(req *types.GetGroupReq) (resp *types.GetGroupResp, err error) {
	rpcResp, err := l.svcCtx.GroupRpc.GetGroup(l.ctx, &grouprpc.GetGroupReq{
		GroupIds:    req.GroupIds,
		NameKeyword: req.NameKeyword,
		Limit:       int32(req.Limit),
		Offset:      int32(req.Offset),
	})
	if err != nil {
		return nil, err
	}

	data := make([]*types.Group, 0, len(rpcResp.Data))
	for _, d := range rpcResp.Data {
		data = append(data, convertGroup(d))
	}

	return &types.GetGroupResp{
		Data:  data,
		Total: rpcResp.Total,
	}, nil
}
