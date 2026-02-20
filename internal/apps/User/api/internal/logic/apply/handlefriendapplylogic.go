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

func (l *HandleFriendApplyLogic) HandleFriendApply(req *types.HandleFriendApplyReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.HandleFriendApply(l.ctx, &user.HandleFriendApplyReq{
		Id:           req.RequestId,
		OperatorId:   userID,
		Status:       int32(req.Result),
		RejectReason: req.RejectReason,
	})

	return err
}
