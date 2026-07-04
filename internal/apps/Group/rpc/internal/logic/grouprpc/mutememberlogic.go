package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Group/rpc/internal/service"
)

type MuteMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewMuteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MuteMemberLogic {
	return &MuteMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *MuteMemberLogic) MuteMember(in *group.MuteMemberReq) (*group.EmptyResp, error) {
	if err := service.NewGroupService(l.svcCtx).MuteMember(l.ctx, in.GroupId, in.OperatorId, in.UserId, in.MuteUntil); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
