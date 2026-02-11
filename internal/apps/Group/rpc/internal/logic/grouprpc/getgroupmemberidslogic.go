package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupMemberIDsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetGroupMemberIDsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupMemberIDsLogic {
	return &GetGroupMemberIDsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetGroupMemberIDsLogic) GetGroupMemberIDs(in *group.GetGroupMemberIDsReq) (*group.GetGroupMemberIDsResp, error) {
	members, err := l.svcCtx.GroupService.GetGroupMemberIDs(l.ctx, in.GroupId)
	if err != nil {
		return nil, err
	}

	var respMembers []*group.GroupMember
	for _, m := range members {
		respMembers = append(respMembers, &group.GroupMember{
			GroupId:   m.GroupID,
			UserId:    m.UserID,
			Role:      group.GroupRole(m.Role),
			Nickname:  m.Nickname,
			MuteUntil: m.MuteUntil,
			JoinedAt:  m.JoinedAt.UnixMilli(),
		})
	}

	return &group.GetGroupMemberIDsResp{Members: respMembers}, nil
}
