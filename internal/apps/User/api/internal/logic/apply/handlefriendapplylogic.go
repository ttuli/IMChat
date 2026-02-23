package apply

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleFriendApplyLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 处理好友申请
func NewHandleFriendApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleFriendApplyLogic {
	return &HandleFriendApplyLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *HandleFriendApplyLogic) HandleFriendApply(req *types.HandleFriendApplyReq) (*types.HandleFriendApplyResp, error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	resp, err := l.svcCtx.HandleFriendApply(l.ctx, &user.HandleFriendApplyReq{
		Id:           req.RequestId,
		OperatorId:   userID,
		Status:       int32(req.Result),
		RejectReason: req.RejectReason,
	})

	return &types.HandleFriendApplyResp{
		Data: &types.FriendRequest{
			Id:           resp.Data.Id,
			FromUserId:   resp.Data.FromUserId,
			ToUserId:     resp.Data.ToUserId,
			ApplyMsg:     resp.Data.ApplyMsg,
			Status:       resp.Data.Status,
			Source:       resp.Data.Source,
			RequestTime:  resp.Data.RequestTime,
			HandleTime:   resp.Data.HandleTime,
			RejectReason: resp.Data.RejectReason,
		},
	}, err
}
