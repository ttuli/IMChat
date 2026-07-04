package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/User/rpc/internal/service"
)

type DeleteFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteFriendLogic {
	return &DeleteFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteFriendLogic) DeleteFriend(in *user.DeleteFriendReq) (*user.EmptyResp, error) {
	err := service.NewUserService(l.svcCtx).DeleteFriend(l.ctx, in.UserId, in.FriendId)
	if err != nil {
		return nil, err
	}

	return &user.EmptyResp{}, nil
}
