package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserConversationsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserConversationsLogic {
	return &GetUserConversationsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserConversationsLogic) GetUserConversations(in *message.GetUserConversationsReq) (*message.GetUserConversationsResp, error) {
	userConvs, err := l.svcCtx.MessageService.GetUserConversations(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	var list []*message.UserConversation
	for _, uc := range userConvs {
		list = append(list, &message.UserConversation{
			UserId:         uc.UserID,
			ConversationId: uc.ConversationID,
			IsTop:          int32(uc.IsTop),
			IsDisturb:      int32(uc.IsDisturb),
			LastReadSeq:    uc.LastReadSeq,
			CreateTime:     uc.CreateTime.UnixMilli(),
			UpdateTime:     uc.UpdateTime.UnixMilli(),
		})
	}

	return &message.GetUserConversationsResp{Conversations: list}, nil
}
