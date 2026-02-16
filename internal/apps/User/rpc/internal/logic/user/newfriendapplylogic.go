package logic

import (
	"context"
	"time"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type NewFriendApplyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewNewFriendApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *NewFriendApplyLogic {
	return &NewFriendApplyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 好友申请
func (l *NewFriendApplyLogic) NewFriendApply(in *user.NewFriendApplyReq) (*user.NewFriendApplyResp, error) {
	apply, err := l.svcCtx.UserService.NewFriendApply(l.ctx, in.FromUserId, in.ToUserId, in.ApplyMsg)
	if err != nil {
		return nil, err
	}

	return &user.NewFriendApplyResp{
		Data: &user.FriendRequest{
			Id:          apply.ID,
			FromUserId:  apply.FromUserID,
			ToUserId:    apply.ToUserID,
			ApplyMsg:    apply.ApplyMsg,
			Status:      int32(apply.Status),
			RequestTime: time.Now().UnixMilli(),
		},
	}, nil
}
