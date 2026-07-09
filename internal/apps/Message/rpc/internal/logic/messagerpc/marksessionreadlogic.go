package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkSessionReadLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewMarkSessionReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkSessionReadLogic {
	return &MarkSessionReadLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *MarkSessionReadLogic) MarkSessionRead(in *message.MarkSessionReadReq) (*message.MarkSessionReadResp, error) {
	if err := service.NewMessageService(l.svcCtx).MarkSessionRead(l.ctx,
		in.UserId, in.SessionId, in.ReadSeq); err != nil {
		return nil, err
	}
	return &message.MarkSessionReadResp{}, nil
}
