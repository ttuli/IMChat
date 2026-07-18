package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/internal/service"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetPendingInvitesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPendingInvitesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingInvitesLogic {
	return &GetPendingInvitesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPendingInvitesLogic) GetPendingInvites(in *group.GetPendingInvitesReq) (*group.GetPendingInvitesResp, error) {
	invites, err := service.NewGroupService(l.svcCtx).GetPendingInvites(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	resp := &group.GetPendingInvitesResp{}
	for _, iv := range invites {
		resp.Data = append(resp.Data, &group.GroupInvite{
			Id:         iv.ID,
			GroupId:    iv.GroupID,
			InviterId:  iv.InviterID,
			InviteeId:  iv.InviteeID,
			Status:     group.InviteStatus(iv.Status),
			InviteMsg:  iv.InviteMsg,
			CreateTime: iv.CreateTime.UnixMilli(),
			UpdateTime: iv.UpdateTime.UnixMilli(),
		})
	}
	return resp, nil
}
