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

func (l *CreateFriendLogic) CreateFriend(in *user.CreateFriendReq) (*user.CreateFriendResp, error) {
	friend, err := l.svcCtx.UserService.CreateFriend(l.ctx, in.UserId, in.FriendId, uint8(in.Source), in.Remark)
	if err != nil {
		return nil, err
	}

	return &user.CreateFriendResp{
		Data: &user.Friend{
			UserId:     friend.UserID,
			FriendId:   friend.FriendID,
			Remark:     friend.Remark,
			Starred:    friend.Starred,
			Blocked:    friend.Blocked,
			Source:     int32(friend.Source),
			CreateTime: friend.CreateTime.UnixMilli(),
			Extra:      friend.Extra,
		},
	}, nil
}
