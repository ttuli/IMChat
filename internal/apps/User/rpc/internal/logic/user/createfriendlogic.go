package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateFriendLogic {
	return &CreateFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateFriendLogic) CreateFriend(in *user.CreateFriendReq) (*user.EmptyResp, error) {
	err := l.svcCtx.UserService.CreateFriend(l.ctx, in.UserId, in.FriendId, uint8(in.Source), in.Remark)
	if err != nil {
		return nil, err
	}

	return &user.EmptyResp{}, nil
}
