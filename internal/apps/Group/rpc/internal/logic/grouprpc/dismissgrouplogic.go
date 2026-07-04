package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Group/rpc/internal/service"
)

type DismissGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDismissGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DismissGroupLogic {
	return &DismissGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DismissGroupLogic) DismissGroup(in *group.DismissGroupReq) (*group.EmptyResp, error) {
	if err := service.NewGroupService(l.svcCtx).DismissGroup(l.ctx, in.GroupId, in.OperatorId); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
