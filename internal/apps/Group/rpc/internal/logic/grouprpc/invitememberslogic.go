package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type InviteMembersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewInviteMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteMembersLogic {
	return &InviteMembersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 群成员管理
func (l *InviteMembersLogic) InviteMembers(in *group.InviteMembersReq) (*group.InviteMembersResp, error) {
	successCount, failedIDs, err := l.svcCtx.GroupService.InviteMembers(l.ctx, in.GroupId, in.OperatorId, in.MemberIds)
	if err != nil {
		return nil, err
	}
	return &group.InviteMembersResp{
		SuccessCount: successCount,
		FailedIds:    failedIDs,
	}, nil
}
