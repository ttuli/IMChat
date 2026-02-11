package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserGroupsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserGroupsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserGroupsLogic {
	return &GetUserGroupsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserGroupsLogic) GetUserGroups(in *group.GetUserGroupsReq) (*group.GetUserGroupsResp, error) {
	groupIDs, err := l.svcCtx.GroupService.GetUserGroupIDs(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	return &group.GetUserGroupsResp{Data: groupIDs}, nil
}
