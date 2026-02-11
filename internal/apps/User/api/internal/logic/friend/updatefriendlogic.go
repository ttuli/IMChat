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

func (l *UpdateFriendLogic) UpdateFriend(req *types.UpdateFriendReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	// 处理可选的 bool 指针
	var blocked, starred bool
	if req.Blocked != nil {
		blocked = *req.Blocked
	}
	if req.Starred != nil {
		starred = *req.Starred
	}

	_, err := l.svcCtx.UpdateFriend(l.ctx, &user.UpdateFriendReq{
		UserId:   userID,
		FriendId: req.FriendID,
		Remark:   req.Remark,
		Blocked:  blocked,
		Starred:  starred,
	})

	return err
}
