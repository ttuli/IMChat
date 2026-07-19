package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

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
	// 操作者取自 JWT，不信任请求体，防止伪造他人身份移除成员
	_, err := l.svcCtx.GroupRpc.RemoveMember(l.ctx, &grouprpc.RemoveMemberReq{
		GroupId:    req.GroupId,
		OperatorId: tokenmanager.ExtractIDFromCtx(l.ctx),
		UserId:     req.UserId,
		UserIds:    req.UserIds,
	})
	return err
}
