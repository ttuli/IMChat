package friend

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFriendsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取好友列表
func NewGetFriendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendsLogic {
	return &GetFriendsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFriendsLogic) GetFriends(req *types.GetFriendsReq) (resp *types.GetFriendsResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.GetFriends(l.ctx, &user.GetFriendsReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	data := make([]types.Friend, 0, len(res.Data))
	for _, f := range res.Data {
		data = append(data, types.Friend{
			UserID:     f.UserId,
			FriendID:   f.FriendId,
			Remark:     f.Remark,
			Source:     int8(f.Source),
			Blocked:    f.Blocked,
			Starred:    f.Starred,
			CreateTime: f.CreateTime,
			Extra:      f.Extra,
		})
	}

	return &types.GetFriendsResp{
		Data: data,
	}, nil
}
