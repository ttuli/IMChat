package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/svc"
	"IM2/internal/apps/Message/rpc/message"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserActiveConversationsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserActiveConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserActiveConversationsLogic {
	return &GetUserActiveConversationsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserActiveConversationsLogic) GetUserActiveConversations(in *message.GetUserActiveConversationsReq) (*message.GetUserActiveConversationsResp, error) {
	// todo: add your logic here and delete this line

	return &message.GetUserActiveConversationsResp{}, nil
}
