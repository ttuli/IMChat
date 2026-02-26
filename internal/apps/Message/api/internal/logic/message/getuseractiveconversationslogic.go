package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/message"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserActiveConversationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取用户的会话列表(根据UserID)
func NewGetUserActiveConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserActiveConversationsLogic {
	return &GetUserActiveConversationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserActiveConversationsLogic) GetUserActiveConversations(req *types.GetUserActiveConversationsReq) (resp *types.GetUserActiveConversationsResp, err error) {
	userId := tokenmanager.ExtractIDFromCtx(l.ctx)
	res, err := l.svcCtx.MessageRpc.GetUserActiveConversations(l.ctx, &message.GetUserActiveConversationsReq{
		UserId:    userId,
		Timestamp: req.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	var conversations []*types.Conversation
	for _, c := range res.Conversations {
		conversations = append(conversations, &types.Conversation{
			ConversationId:   c.ConversationId,
			ConversationType: int32(c.Type),
			MaxSeq:           int64(c.MaxSeq),
			CreateTime:       c.CreateTime,
			UpdateTime:       c.UpdateTime,
		})
	}
	return &types.GetUserActiveConversationsResp{
		Conversations: conversations,
	}, nil
}
