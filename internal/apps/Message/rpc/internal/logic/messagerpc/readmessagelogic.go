package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReadMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReadMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReadMessageLogic {
	return &ReadMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReadMessageLogic) ReadMessage(in *message.ReadMessageReq) (*message.ReadMessageResp, error) {
	if err := l.svcCtx.MessageService.ReadMessage(l.ctx, in.UserId, in.ConversationId, in.Seq); err != nil {
		return nil, err
	}
	return &message.ReadMessageResp{}, nil
}
