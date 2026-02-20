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

func (l *CreateFriendLogic) CreateFriend(req *types.CreateFriendReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.CreateFriend(l.ctx, &user.CreateFriendReq{
		UserId:   userID,
		FriendId: req.FriendId,
		Source:   int32(req.Source),
		Remark:   req.Remark,
	})

	return err
}
