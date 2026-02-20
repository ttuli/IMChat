package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetConversationLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 批量获取会话详情
func NewGetConversationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetConversationLogic {
	return &GetConversationLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetConversationLogic) GetConversation(req *types.GetConversationReq) (resp *types.GetConversationResp, err error) {
	res, err := l.svcCtx.MessageRpc.GetConversationList(l.ctx, &messagerpc.GetConversationListReq{
		SessionIds: req.ConversationIds,
	})
	if err != nil {
		return nil, err
	}

	list := make([]*types.Conversation, 0, len(res.Conversations))
	for _, c := range res.Conversations {
		list = append(list, &types.Conversation{
			ConversationId: c.ConversationId,
			Type:           c.Type,
			MaxSeq:         c.MaxSeq,
			CreateTime:     c.CreateTime,
			UpdateTime:     c.UpdateTime,
		})
	}

	return &types.GetConversationResp{Conversations: list}, nil
}
