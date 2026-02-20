package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReadMessageLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 消息已读上报
func NewReadMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReadMessageLogic {
	return &ReadMessageLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ReadMessageLogic) ReadMessage(req *types.ReadMessageReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.MessageRpc.ReadMessage(l.ctx, &messagerpc.ReadMessageReq{
		UserId:         userID,
		ConversationId: req.ConversationId,
		Seq:            req.Seq,
	})
	return err
}
