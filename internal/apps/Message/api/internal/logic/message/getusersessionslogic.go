package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserSessionsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取用户的会话列表(根据UserID)
func NewGetUserSessionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserSessionsLogic {
	return &GetUserSessionsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserSessionsLogic) GetUserSessions() (resp *types.GetUserSessionsResp, err error) {
	uid := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.MessageRpc.GetUserSessions(l.ctx, &messagerpc.GetUserSessionsReq{
		UserId: uid,
	})
	if err != nil {
		return nil, err
	}

	list := make([]*types.UserSession, 0, len(res.Sessions))
	for _, c := range res.Sessions {
		list = append(list, &types.UserSession{
			UserId:      c.UserId,
			SessionId:   c.SessionId,
			IsTop:       c.IsTop,
			IsDisturb:   c.IsDisturb,
			LastReadSeq: c.LastReadSeq,
			CreateTime:  c.CreateTime,
			UpdateTime:  c.UpdateTime,
			UnreadCount: c.UnreadCount,
		})
	}

	return &types.GetUserSessionsResp{Sessions: list}, nil
}
