package group

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 更新群组信息
func NewUpdateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateGroupLogic {
	return &UpdateGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateGroupLogic) UpdateGroup(req *types.UpdateGroupReq) error {
	// TODO: 从 JWT 获取操作者ID
	operatorID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.GroupRpc.UpdateGroup(l.ctx, &grouprpc.UpdateGroupReq{
		GroupId:    req.GroupId,
		OperatorId: operatorID,
		Name:       req.Name,
		Avatar:     req.Avatar,
	})
	return err
}
