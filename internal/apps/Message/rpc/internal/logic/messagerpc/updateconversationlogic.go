package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateConversationLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateConversationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateConversationLogic {
	return &UpdateConversationLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateConversationLogic) UpdateConversation(in *message.UpdateConversationReq) (*message.UpdateConversationResp, error) {
	if err := l.svcCtx.MessageService.UpdateConversation(l.ctx,
		in.UserId, in.ConversationId,
		in.IsTop, in.IsDisturb); err != nil {
		return nil, err
	}
	return &message.UpdateConversationResp{}, nil
}
