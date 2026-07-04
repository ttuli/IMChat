package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserActiveSessionsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserActiveSessionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserActiveSessionsLogic {
	return &GetUserActiveSessionsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserActiveSessionsLogic) GetUserActiveSessions(in *message.GetUserActiveSessionsReq) (*message.GetUserActiveSessionsResp, error) {
	sess, err := service.NewMessageService(l.svcCtx).GetUserActiveSessions(l.ctx, in.UserId, in.Timestamp)
	if err != nil {
		return nil, err
	}

	var list []*message.Session
	for _, c := range sess {
		list = append(list, &message.Session{
			SessionId:   c.SessionID,
			Type:        int32(c.Type),
			SessionKey:  c.SessionKey,
			MaxSeq:      c.MaxSeq,
			ActualSeq:   c.ActualSeq,
			CreateTime:  c.CreateTime.UnixMilli(),
			UpdateTime:  c.UpdateTime.UnixMilli(),
			LastContent: c.LastContent,
			LastSender:  c.LastSender,
		})
	}

	return &message.GetUserActiveSessionsResp{Sessions: list}, nil
}
