package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateFriendLogic {
	return &UpdateFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateFriendLogic) UpdateFriend(in *user.UpdateFriendReq) (*user.UpdateFriendResp, error) {
	friendRecord, err := l.svcCtx.UserService.UpdateFriend(l.ctx, in.UserId, in.FriendId, in.Remark, in.Blocked, in.Starred)
	if err != nil {
		return nil, err
	}

	return &user.UpdateFriendResp{
		Data: &user.Friend{
			UserId:     friendRecord.UserID,
			FriendId:   friendRecord.FriendID,
			Remark:     friendRecord.Remark,
			Blocked:    friendRecord.Blocked,
			Source:     int32(friendRecord.Source),
			Starred:    friendRecord.Starred,
			CreateTime: friendRecord.CreateTime.UnixMilli(),
			Extra:      friendRecord.Extra,
		},
	}, nil
}
