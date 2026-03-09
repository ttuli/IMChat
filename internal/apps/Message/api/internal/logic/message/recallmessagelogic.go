package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecallMessageLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 消息撤回
func NewRecallMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecallMessageLogic {
	return &RecallMessageLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RecallMessageLogic) RecallMessage(req *types.RecallMessageReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.MessageRpc.RecallMessage(l.ctx, &messagerpc.RecallMessageReq{
		UserId:    userID,
		SessionId: req.SessionId,
		MsgId:     req.MsgId,
	})
	return err
}
