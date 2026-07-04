package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetSessionLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 根据 session_id 或 session_key 查询（或创建）会话
func NewGetSessionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetSessionLogic {
	return &GetSessionLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetSessionLogic) GetSession(req *types.GetSessionReq) (resp *types.GetSessionResp, err error) {
	res, err := l.svcCtx.MessageRpc.GetSession(l.ctx, &messagerpc.GetSessionReq{
		SessionId:   req.SessionId,
		SessionKey:  req.SessionKey,
		SessionType: req.SessionType,
	})
	if err != nil {
		return nil, err
	}

	return &types.GetSessionResp{
		Session: &types.Session{
			SessionId:   res.Session.SessionId,
			Type:        res.Session.Type,
			SessionKey:  res.Session.SessionKey,
			MaxSeq:      res.Session.MaxSeq,
			ActualSeq:   res.Session.ActualSeq,
			LastContent: res.Session.LastContent,
			LastSender:  res.Session.LastSender,
			CreateTime:  res.Session.CreateTime,
			UpdateTime:  res.Session.UpdateTime,
		},
	}, nil
}
