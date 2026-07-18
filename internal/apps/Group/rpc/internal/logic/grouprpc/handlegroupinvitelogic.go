package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/internal/service"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleGroupInviteLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHandleGroupInviteLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleGroupInviteLogic {
	return &HandleGroupInviteLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 群邀请管理（被邀请人确认制）
func (l *HandleGroupInviteLogic) HandleGroupInvite(in *group.HandleGroupInviteReq) (*group.HandleGroupInviteResp, error) {
	member, err := service.NewGroupService(l.svcCtx).HandleGroupInvite(l.ctx, in.Id, in.InviteeId, in.Accept)
	if err != nil {
		return nil, err
	}

	resp := &group.HandleGroupInviteResp{}
	if member != nil {
		resp.Member = &group.GroupMember{
			GroupId:  member.GroupID,
			UserId:   member.UserID,
			Role:     group.GroupRole(member.Role),
			Nickname: member.Nickname,
			JoinedAt: member.JoinedAt.UnixMilli(),
		}
	}
	return resp, nil
}
