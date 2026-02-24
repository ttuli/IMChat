package apply

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type NewFriendApplyLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 发起好友申请
func NewNewFriendApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *NewFriendApplyLogic {
	return &NewFriendApplyLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *NewFriendApplyLogic) NewFriendApply(req *types.NewFriendApplyReq) (resp *types.NewFriendApplyResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.NewFriendApply(l.ctx, &user.NewFriendApplyReq{
		FromUserId: userID,
		ToUserId:   req.ToUserId,
		ApplyMsg:   req.ApplyMsg,
	})
	if err != nil {
		return nil, err
	}

	resp = &types.NewFriendApplyResp{}

	if res.Data != nil {
		resp.Data = &types.FriendRequest{
			Id:          res.Data.Id,
			FromUserId:  res.Data.FromUserId,
			ToUserId:    res.Data.ToUserId,
			ApplyMsg:    res.Data.ApplyMsg,
			Status:      int32(res.Data.Status),
			RequestTime: res.Data.RequestTime,
		}
	} else if res.Friend != nil {
		resp.Friend = &types.Friend{
			UserId:     res.Friend.UserId,
			FriendId:   res.Friend.FriendId,
			Remark:     res.Friend.Remark,
			Starred:    res.Friend.Starred,
			Blocked:    res.Friend.Blocked,
			Source:     int32(res.Friend.Source),
			CreateTime: res.Friend.CreateTime,
			Extra:      res.Friend.Extra,
		}
	}

	return resp, nil
}
