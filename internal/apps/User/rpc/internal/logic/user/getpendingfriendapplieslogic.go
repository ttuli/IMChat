package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/User/rpc/internal/service"
)

type GetPendingFriendAppliesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPendingFriendAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingFriendAppliesLogic {
	return &GetPendingFriendAppliesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPendingFriendAppliesLogic) GetPendingFriendApplies(in *user.GetPendingFriendAppliesReq) (*user.GetPendingFriendAppliesResp, error) {
	applies, err := service.NewUserService(l.svcCtx).GetPendingFriendApplies(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	// 转换为 proto 格式
	list := make([]*user.FriendRequest, 0, len(applies))
	for _, a := range applies {
		list = append(list, &user.FriendRequest{
			Id:           a.ID,
			FromUserId:   a.FromUserID,
			ToUserId:     a.ToUserID,
			ApplyMsg:     a.ApplyMsg,
			Status:       int32(a.Status),
			RequestTime:  a.CreateTime.UnixMilli(),
			HandleTime:   a.HandleTime.UnixMilli(),
			RejectReason: a.RejectReason,
		})
	}

	return &user.GetPendingFriendAppliesResp{Data: list}, nil
}
