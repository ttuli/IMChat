package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Group/rpc/group"

	"github.com/zeromicro/go-zero/core/logx"
)

type SetMemberRoleLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 设置群成员角色
func NewSetMemberRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetMemberRoleLogic {
	return &SetMemberRoleLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SetMemberRoleLogic) SetMemberRole(req *types.SetMemberRoleReq) error {
	_, err := l.svcCtx.GroupRpc.SetMemberRole(l.ctx, &grouprpc.SetMemberRoleReq{
		GroupId:    req.GroupId,
		OperatorId: req.OperatorId,
		UserId:     req.UserId,
		Role:       group.GroupRole(req.Role),
	})
	return err
}
