package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetPendingInvitesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取我收到的待处理入群邀请
func NewGetPendingInvitesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingInvitesLogic {
	return &GetPendingInvitesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetPendingInvitesLogic) GetPendingInvites(req *types.GetPendingInvitesReq) (*types.GetPendingInvitesResp, error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.GetPendingInvites(l.ctx, &grouprpc.GetPendingInvitesReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	resp := &types.GetPendingInvitesResp{}
	for _, iv := range rpcResp.Data {
		resp.Data = append(resp.Data, &types.GroupInvite{
			Id:         iv.Id,
			GroupId:    iv.GroupId,
			InviterId:  iv.InviterId,
			InviteeId:  iv.InviteeId,
			Status:     int32(iv.Status),
			InviteMsg:  iv.InviteMsg,
			CreateTime: iv.CreateTime,
			UpdateTime: iv.UpdateTime,
		})
	}
	return resp, nil
}
