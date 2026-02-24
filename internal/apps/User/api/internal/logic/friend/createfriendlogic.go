package friend

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 创建好友
func NewCreateFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateFriendLogic {
	return &CreateFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateFriendLogic) CreateFriend(req *types.CreateFriendReq) (*types.CreateFriendResp, error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	resp, err := l.svcCtx.CreateFriend(l.ctx, &user.CreateFriendReq{
		UserId:   userID,
		FriendId: req.FriendId,
		Source:   int32(req.Source),
		Remark:   req.Remark,
	})

	return &types.CreateFriendResp{
		Data: &types.Friend{
			UserId:     resp.Data.UserId,
			FriendId:   resp.Data.FriendId,
			Remark:     resp.Data.Remark,
			Starred:    resp.Data.Starred,
			Blocked:    resp.Data.Blocked,
			Source:     int32(resp.Data.Source),
			CreateTime: resp.Data.CreateTime,
			Extra:      resp.Data.Extra,
		},
	}, err
}
