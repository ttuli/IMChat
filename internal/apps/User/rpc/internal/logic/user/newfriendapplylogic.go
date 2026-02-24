package logic

import (
	"context"

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
	apply, friend, err := l.svcCtx.UserService.NewFriendApply(l.ctx, in.FromUserId, in.ToUserId, in.ApplyMsg, uint8(in.Source))
	if err != nil {
		return nil, err
	}

	resp := &user.NewFriendApplyResp{}

	if apply != nil {
		resp.Data = &user.FriendRequest{
			Id:           apply.ID,
			FromUserId:   apply.FromUserID,
			ToUserId:     apply.ToUserID,
			ApplyMsg:     apply.ApplyMsg,
			Status:       int32(apply.Status),
			Source:       int32(apply.Source),
			RequestTime:  apply.CreateTime.UnixMilli(),
			HandleTime:   apply.HandleTime.UnixMilli(),
			RejectReason: apply.RejectReason,
		}
	} else if friend != nil {
		resp.Friend = &user.Friend{
			UserId:     friend.UserID,
			FriendId:   friend.FriendID,
			Remark:     friend.Remark,
			Starred:    friend.Starred,
			Blocked:    friend.Blocked,
			Source:     int32(friend.Source),
			CreateTime: friend.CreateTime.UnixMilli(),
			Extra:      friend.Extra,
		}
	}

	return resp, nil
}
