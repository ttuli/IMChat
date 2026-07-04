package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Group/rpc/internal/service"
)

type GetGroupManagersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetGroupManagersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupManagersLogic {
	return &GetGroupManagersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetGroupManagersLogic) GetGroupManagers(in *group.GetGroupManagersReq) (*group.GetGroupManagersResp, error) {
	members, err := service.NewGroupService(l.svcCtx).GetGroupManagers(l.ctx, in.GroupId)
	if err != nil {
		return nil, err
	}

	var pbMembers []*group.GroupMember
	for _, m := range members {
		pbMembers = append(pbMembers, &group.GroupMember{
			GroupId:  m.GroupID,
			UserId:   m.UserID,
			Role:     group.GroupRole(m.Role),
			Nickname: m.Nickname,
			JoinedAt: m.JoinedAt.UnixMilli(),
		})
	}

	return &group.GetGroupManagersResp{
		Managers: pbMembers,
	}, nil
}
