package friend

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 更新好友信息
func NewUpdateFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateFriendLogic {
	return &UpdateFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateFriendLogic) UpdateFriend(req *types.UpdateFriendReq) (*types.UpdateFriendResp, error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.UpdateFriend(l.ctx, &user.UpdateFriendReq{
		UserId:   userID,
		FriendId: req.FriendId,
		Remark:   req.Remark,
		Blocked:  req.Blocked,
		Starred:  req.Starred,
	})
	if err != nil {
		return nil, err
	}

	return &types.UpdateFriendResp{
		Data: &types.Friend{
			UserId:     res.Data.UserId,
			FriendId:   res.Data.FriendId,
			Source:     res.Data.Source,
			Remark:     res.Data.Remark,
			Blocked:    res.Data.Blocked,
			Starred:    res.Data.Starred,
			CreateTime: res.Data.CreateTime,
			Extra:      res.Data.Extra,
		},
	}, nil
}
