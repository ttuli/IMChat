package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleGroupInviteLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 被邀请人处理入群邀请（接受/拒绝）
func NewHandleGroupInviteLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleGroupInviteLogic {
	return &HandleGroupInviteLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *HandleGroupInviteLogic) HandleGroupInvite(req *types.HandleGroupInviteReq) (*types.HandleGroupInviteResp, error) {
	// 被邀请人取自 JWT，避免越权处理他人邀请
	inviteeID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.HandleGroupInvite(l.ctx, &grouprpc.HandleGroupInviteReq{
		Id:        req.InviteId,
		InviteeId: inviteeID,
		Accept:    req.Accept,
	})
	if err != nil {
		return nil, err
	}

	resp := &types.HandleGroupInviteResp{}
	if rpcResp.Member != nil {
		resp.Member = &types.GroupMember{
			GroupId:  rpcResp.Member.GroupId,
			UserId:   rpcResp.Member.UserId,
			Role:     int32(rpcResp.Member.Role),
			Nickname: rpcResp.Member.Nickname,
			JoinedAt: rpcResp.Member.JoinedAt,
		}
	}
	return resp, nil
}
