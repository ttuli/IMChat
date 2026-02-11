package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type SetMemberRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSetMemberRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetMemberRoleLogic {
	return &SetMemberRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SetMemberRoleLogic) SetMemberRole(in *group.SetMemberRoleReq) (*group.EmptyResp, error) {
	if err := l.svcCtx.GroupService.SetMemberRole(l.ctx, in.GroupId, in.OperatorId, in.UserId, int8(in.Role)); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
