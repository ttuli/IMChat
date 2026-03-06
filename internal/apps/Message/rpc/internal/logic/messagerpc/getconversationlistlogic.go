package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetConversationListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetConversationListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetConversationListLogic {
	return &GetConversationListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetConversationListLogic) GetConversationList(in *message.GetConversationListReq) (*message.GetConversationListResp, error) {
	convs, err := l.svcCtx.MessageService.GetConversation(l.ctx, in.SessionIds)
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

	return &message.GetConversationListResp{Conversations: list}, nil
}
