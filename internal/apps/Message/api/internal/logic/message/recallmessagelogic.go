package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecallMessageLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 消息已读上报
func NewRecallMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecallMessageLogic {
	return &RecallMessageLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RecallMessageLogic) RecallMessage(req *types.RecallMessageReq) error {
	// userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	// _, err := l.svcCtx.MessageRpc.RecallMessage(l.ctx, &messagerpc.RecallMessageReq{
	// 	UserId:         userID,
	// 	ConversationId: req.ConversationId,
	// 	Seq:            req.Seq,
	// })
	// return err
	return nil
}
