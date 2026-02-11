package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RemoveMemberLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 移除群成员
func NewRemoveMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RemoveMemberLogic {
	return &RemoveMemberLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RemoveMemberLogic) RemoveMember(req *types.RemoveMemberReq) error {
	_, err := l.svcCtx.GroupRpc.RemoveMember(l.ctx, &grouprpc.RemoveMemberReq{
		GroupId:    req.GroupID,
		OperatorId: req.OperatorID,
		UserId:     req.UserID,
	})
	return err
}
