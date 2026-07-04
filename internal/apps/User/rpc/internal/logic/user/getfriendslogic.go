package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/User/rpc/internal/service"
)

type GetFriendsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFriendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendsLogic {
	return &GetFriendsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 好友管理
func (l *GetFriendsLogic) GetFriends(in *user.GetFriendsReq) (*user.GetFriendsResp, error) {
	friends, err := service.NewUserService(l.svcCtx).GetFriends(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	// 转换为 proto 格式
	list := make([]*user.Friend, 0, len(friends))
	for _, f := range friends {
		list = append(list, &user.Friend{
			UserId:     f.UserID,
			FriendId:   f.FriendID,
			Remark:     f.Remark,
			Source:     int32(f.Source),
			Blocked:    f.Blocked,
			Starred:    f.Starred,
			CreateTime: f.CreateTime.UnixMilli(),
		})
	}

	return &user.GetFriendsResp{Data: list}, nil
}
