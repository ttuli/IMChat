package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetConversationListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取会话列表
func NewGetConversationListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetConversationListLogic {
	return &GetConversationListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetConversationListLogic) GetConversationList(req *types.GetConversationListReq) (resp *types.GetConversationListResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.MessageRpc.GetConversationList(l.ctx, &messagerpc.GetConversationListReq{
		UserId: userID,
		Limit:  int32(req.Limit),
		Offset: int32(req.Offset),
	})
	if err != nil {
		return nil, err
	}

	list := make([]types.Conversation, 0, len(res.Conversations))
	for _, c := range res.Conversations {
		list = append(list, types.Conversation{
			ConversationID: c.ConversationId,
			Type:           c.Type,
			UnreadCount:    c.UnreadCount,
		})
	}

	return &types.GetConversationListResp{List: list}, nil
}
