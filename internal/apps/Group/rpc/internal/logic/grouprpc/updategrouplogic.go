package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Group/rpc/internal/service"
)

type UpdateGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateGroupLogic {
	return &UpdateGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateGroupLogic) UpdateGroup(in *group.UpdateGroupReq) (*group.EmptyResp, error) {
	if err := service.NewGroupService(l.svcCtx).UpdateGroup(l.ctx, in.GroupId, in.OperatorId, in.Name, in.Avatar, in.JoinType); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
