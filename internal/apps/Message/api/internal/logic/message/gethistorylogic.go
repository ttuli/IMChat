package message

import (
	"context"

	"IM2/internal/apps/Message/api/svc"
	"IM2/internal/apps/Message/api/types"
	"IM2/internal/apps/Message/rpc/client/messagerpc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetHistoryLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取历史消息
func NewGetHistoryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetHistoryLogic {
	return &GetHistoryLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetHistoryLogic) GetHistory(req *types.GetHistoryReq) (resp *types.GetHistoryResp, err error) {
	res, err := l.svcCtx.MessageRpc.GetHistory(l.ctx, &messagerpc.GetHistoryReq{
		SessionId: req.SessionId,
		StartSeq:  req.StartSeq,
		EndSeq:    req.EndSeq,
		Limit:     int32(req.Limit),
	})
	if err != nil {
		return nil, err
	}

	list := make([]*types.Message, 0, len(res.Messages))
	for _, m := range res.Messages {
		list = append(list, &types.Message{
			MsgId:      m.MsgId,
			// client_id 必须回传：本地库以 (session_key, msg_id/client_id) 去重，
			// 发送方本地行可能仅有 client_id（msg_id 尚未回填），缺失会导致重复插行
			ClientId:   m.ClientId,
			SessionId:  m.SessionId,
			FromUserId: m.FromUserId,
			MsgType:    m.MsgType,
			Content:    m.Content,
			MediaUrl:   m.MediaUrl,
			Extra:      string(m.Extra),
			CreateTime: m.CreateTime,
			Seq:        m.Seq,
			Status:     m.Status,
		})
	}

	return &types.GetHistoryResp{List: list}, nil
}
