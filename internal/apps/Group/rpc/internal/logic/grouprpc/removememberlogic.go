package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RemoveMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRemoveMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RemoveMemberLogic {
	return &RemoveMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RemoveMemberLogic) RemoveMember(in *group.RemoveMemberReq) (*group.EmptyResp, error) {
	if err := l.svcCtx.GroupService.RemoveMember(l.ctx, in.GroupId, in.OperatorId, in.UserId); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
