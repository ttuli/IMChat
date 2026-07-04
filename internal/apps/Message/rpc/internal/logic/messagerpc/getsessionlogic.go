package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetSessionLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetSessionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetSessionLogic {
	return &GetSessionLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 内部会话管理接口
func (l *GetSessionLogic) GetSession(in *message.GetSessionReq) (*message.GetSessionResp, error) {
	ss, _, err := service.NewMessageService(l.svcCtx).GetOrCreateSession(
		l.ctx,
		in.SessionId,
		in.SessionKey,
		int8(in.SessionType),
	)
	if err != nil {
		return nil, err
	}

	return &message.GetSessionResp{
		Session: &message.Session{
			SessionId:   ss.SessionID,
			Type:        int32(ss.Type),
			SessionKey:  ss.SessionKey,
			MaxSeq:      ss.MaxSeq,
			ActualSeq:   ss.ActualSeq,
			LastContent: ss.LastContent,
			LastSender:  ss.LastSender,
			CreateTime:  ss.CreateTime.UnixMilli(),
			UpdateTime:  ss.UpdateTime.UnixMilli(),
		},
	}, nil
}
