package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/User/rpc/internal/service"
)

type HandleFriendApplyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHandleFriendApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleFriendApplyLogic {
	return &HandleFriendApplyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HandleFriendApplyLogic) HandleFriendApply(in *user.HandleFriendApplyReq) (*user.HandleFriendApplyResp, error) {
	apply, err := service.NewUserService(l.svcCtx).HandleFriendApply(
		l.ctx,
		in.Id,
		in.OperatorId,
		uint8(in.Status),
		in.RejectReason,
	)
	if err != nil {
		return nil, err
	}

	return &user.HandleFriendApplyResp{
		Data: &user.FriendRequest{
			Id:           apply.ID,
			FromUserId:   apply.FromUserID,
			ToUserId:     apply.ToUserID,
			ApplyMsg:     apply.ApplyMsg,
			Status:       int32(apply.Status),
			Source:       int32(apply.Source),
			RequestTime:  apply.CreateTime.UnixMilli(),
			HandleTime:   apply.HandleTime.UnixMilli(),
			RejectReason: apply.RejectReason,
		},
	}, nil
}
