package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateSessionLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 更新会话设置
func NewUpdateSessionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateSessionLogic {
	return &UpdateSessionLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateSessionLogic) UpdateSession(req *types.UpdateSessionReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.MessageRpc.UpdateSession(l.ctx, &messagerpc.UpdateSessionReq{
		UserId:     userID,
		SessionId:  req.SessionId,
		SessionKey: req.SessionKey,
		IsTop:      req.IsTop,
		IsDisturb:  req.IsDisturb,
	})
	return err
}
