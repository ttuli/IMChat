package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

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
	convs, err := l.svcCtx.MessageService.GetUserActiveConversations(l.ctx, in.UserId, in.Timestamp)
	if err != nil {
		return nil, err
	}

	var list []*message.Conversation
	for _, c := range convs {
		list = append(list, &message.Conversation{
			ConversationId: c.ConversationID,
			Type:           int32(c.Type),
			MaxSeq:         c.MaxSeq,
			CreateTime:     c.CreateTime.UnixMilli(),
			UpdateTime:     c.UpdateTime.UnixMilli(),
			LastContent:    c.LastContent,
			LastSender:     c.LastSender,
		})
	}

	return &message.GetUserActiveConversationsResp{Conversations: list}, nil
}
