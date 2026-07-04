package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/message"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserActiveSessionsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取用户的会话列表(根据UserID)
func NewGetUserActiveSessionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserActiveSessionsLogic {
	return &GetUserActiveSessionsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserActiveSessionsLogic) GetUserActiveSessions(req *types.GetUserActiveSessionsReq) (resp *types.GetUserActiveSessionsResp, err error) {
	userId := tokenmanager.ExtractIDFromCtx(l.ctx)
	res, err := l.svcCtx.MessageRpc.GetUserActiveSessions(l.ctx, &message.GetUserActiveSessionsReq{
		UserId:    userId,
		Timestamp: req.Timestamp,
	})
	if err != nil {
		return nil, err
	}

	var sessions []*types.Session
	for _, c := range res.Sessions {
		sessions = append(sessions, &types.Session{
			SessionId:   c.SessionId,
			Type:        c.Type,
			SessionKey:  c.SessionKey,
			MaxSeq:      c.MaxSeq,
			ActualSeq:   c.ActualSeq,
			LastContent: c.LastContent,
			LastSender:  c.LastSender,
			CreateTime:  c.CreateTime,
			UpdateTime:  c.UpdateTime,
		})
	}
	return &types.GetUserActiveSessionsResp{
		Sessions: sessions,
	}, nil
}
