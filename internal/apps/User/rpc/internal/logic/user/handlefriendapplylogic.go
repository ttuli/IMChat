package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleFriendApplyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHandleFriendApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleFriendApplyLogic {
	return &HandleFriendApplyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HandleFriendApplyLogic) HandleFriendApply(in *user.HandleFriendApplyReq) (*user.EmptyResp, error) {
	err := l.svcCtx.UserService.HandleFriendApply(l.ctx, in.Id, in.OperatorId, uint8(in.Status), in.RejectReason)
	if err != nil {
		return nil, err
	}

	return &user.EmptyResp{}, nil
}
