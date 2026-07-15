package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateSessionLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateSessionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateSessionLogic {
	return &UpdateSessionLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateSessionLogic) UpdateSession(in *message.UpdateSessionReq) (*message.UpdateSessionResp, error) {
	if err := service.NewMessageService(l.svcCtx).UpdateSession(l.ctx,
		in.UserId, in.SessionId, in.SessionKey,
		in.IsTop, in.IsDisturb); err != nil {
		return nil, err
	}
	return &message.UpdateSessionResp{}, nil
}
