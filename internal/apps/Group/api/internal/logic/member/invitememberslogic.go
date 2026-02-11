package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type InviteMembersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 邀请用户加入群
func NewInviteMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteMembersLogic {
	return &InviteMembersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *InviteMembersLogic) InviteMembers(req *types.InviteMembersReq) (resp *types.InviteMembersResp, err error) {
	// TODO: 从 JWT 获取操作者ID
	operatorID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.InviteMembers(l.ctx, &grouprpc.InviteMembersReq{
		GroupId:    req.GroupID,
		OperatorId: operatorID,
		MemberIds:  req.MemberIDs,
	})
	if err != nil {
		return nil, err
	}

	return &types.InviteMembersResp{
		SuccessCount: int(rpcResp.SuccessCount),
		FailedIDs:    rpcResp.FailedIds,
	}, nil
}
