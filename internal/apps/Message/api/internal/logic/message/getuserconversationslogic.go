package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserConversationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取用户的会话列表(根据UserID)
func NewGetUserConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserConversationsLogic {
	return &GetUserConversationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserConversationsLogic) GetUserConversations() (resp *types.GetUserConversationsResp, err error) {
	uid := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.MessageRpc.GetUserConversations(l.ctx, &messagerpc.GetUserConversationsReq{
		UserId: uid,
	})
	if err != nil {
		return nil, err
	}

	list := make([]*types.UserConversation, 0, len(res.Conversations))
	for _, c := range res.Conversations {
		list = append(list, &types.UserConversation{
			UserId:         c.UserId,
			ConversationId: c.ConversationId,
			IsTop:          c.IsTop,
			IsDisturb:      c.IsDisturb,
			LastReadSeq:    c.LastReadSeq,
			CreateTime:     c.CreateTime,
			UpdateTime:     c.UpdateTime,
		})
	}

	return &types.GetUserConversationsResp{Conversations: list}, nil
}
