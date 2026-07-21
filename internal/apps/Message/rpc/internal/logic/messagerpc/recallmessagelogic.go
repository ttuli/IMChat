package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecallMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRecallMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecallMessageLogic {
	return &RecallMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// RecallMessage 撤回消息（校验发送者本人 + 2 分钟窗口，成功后发布撤回通知扇出）
func (l *RecallMessageLogic) RecallMessage(in *message.RecallMessageReq) (*message.RecallMessageResp, error) {
	if err := service.NewMessageService(l.svcCtx).RecallMessage(l.ctx,
		in.UserId, in.MsgId, in.SessionId); err != nil {
		return nil, err
	}
	return &message.RecallMessageResp{}, nil
}
