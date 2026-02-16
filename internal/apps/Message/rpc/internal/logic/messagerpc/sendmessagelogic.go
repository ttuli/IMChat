package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

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
	result, err := l.svcCtx.MessageService.SendMessage(l.ctx,
		in.MsgId,
		in.ConversationId,
		in.FromUserId,
		int16(in.MsgType),
		in.Content,
		in.MediaUrl,
		nil, // extra 暂时不解析
	)
	if err != nil {
		return nil, err
	}

	return &message.SendMessageResp{
		Seq:        result.Seq,
		CreateTime: result.CreateTime.UnixMilli(),
	}, nil
}
