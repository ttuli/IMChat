package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkSessionReadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 上报会话已读游标
func NewMarkSessionReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkSessionReadLogic {
	return &MarkSessionReadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *MarkSessionReadLogic) MarkSessionRead(req *types.MarkSessionReadReq) error {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.MessageRpc.MarkSessionRead(l.ctx, &messagerpc.MarkSessionReadReq{
		UserId:    userID,
		SessionId: req.SessionId,
		ReadSeq:   req.ReadSeq,
	})
	return err
}
