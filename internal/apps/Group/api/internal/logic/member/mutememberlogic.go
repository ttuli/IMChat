package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type MuteMemberLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 禁言群成员
func NewMuteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MuteMemberLogic {
	return &MuteMemberLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *MuteMemberLogic) MuteMember(req *types.MuteMemberReq) error {
	// TODO: 从 JWT 获取操作者ID
	operatorID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.GroupRpc.MuteMember(l.ctx, &grouprpc.MuteMemberReq{
		GroupId:    req.GroupId,
		OperatorId: operatorID,
		UserId:     req.UserId,
		MuteUntil:  req.MuteUntil,
	})
	return err
}
