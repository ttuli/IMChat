package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
)

type SendMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSendMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendMessageLogic {
	return &SendMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SendMessageLogic) SendMessage(in *message.SendMessageReq) (*message.SendMessageResp, error) {
	msg := in.Message
	if msg == nil {
		return nil, xerr.New(xerr.ErrInvalidParams, "message is empty")
	}

	// 核心逻辑已移至 MessageService 中
	respMsg, err := l.svcCtx.MessageService.SendMessage(l.ctx, msg)
	if err != nil {
		return nil, err
	}

	return &message.SendMessageResp{
		Message: respMsg,
	}, nil
}
