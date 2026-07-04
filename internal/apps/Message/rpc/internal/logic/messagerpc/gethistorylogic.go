package messagerpclogic

import (
	"context"
	"encoding/json"

	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Message/rpc/internal/service"
)

type GetHistoryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetHistoryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetHistoryLogic {
	return &GetHistoryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetHistoryLogic) GetHistory(in *message.GetHistoryReq) (*message.GetHistoryResp, error) {
	msgs, err := service.NewMessageService(l.svcCtx).GetHistory(l.ctx, in.SessionId, in.StartSeq, in.EndSeq, int(in.Limit))
	if err != nil {
		return nil, err
	}

	var list []*message.Message
	for _, m := range msgs {
		var byteData []byte
		if len(m.Extra) > 0 {
			var err error
			byteData, err = json.Marshal(m.Extra)
			if err != nil {
				return nil, err
			}
		}
		list = append(list, &message.Message{
			MsgId:      m.MsgID,
			SessionId:  m.SessionID,
			FromUserId: m.FromUserID,
			MsgType:    int32(m.MsgType),
			Content:    m.Content,
			MediaUrl:   m.MediaURL,
			Status:     int32(m.Status),
			CreateTime: m.CreateTime.UnixMilli(),
			Seq:        m.Seq,
			Extra:      byteData,
		})
	}

	return &message.GetHistoryResp{Messages: list}, nil
}
