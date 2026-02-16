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
	userConvs, err := l.svcCtx.MessageService.GetConversationList(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	var list []*message.Conversation
	for _, uc := range userConvs {
		list = append(list, &message.Conversation{
			ConversationId: uc.ConversationID,
			UnreadCount:    uc.UnreadCount,
			MaxSeq:         uc.LastReadSeq,
		})
	}

	return &message.GetConversationListResp{Conversations: list}, nil
}
