package group

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type DismissGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 解散群组
func NewDismissGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DismissGroupLogic {
	return &DismissGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DismissGroupLogic) DismissGroup(req *types.DismissGroupReq) error {
	userId := tokenmanager.ExtractIDFromCtx(l.ctx)
	_, err := l.svcCtx.GroupRpc.DismissGroup(l.ctx, &grouprpc.DismissGroupReq{
		GroupId:    req.GroupId,
		OperatorId: userId,	
	})
	return err
}
