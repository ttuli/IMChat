package messagerpclogic

import (
	"context"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/internal/apps/Message/rpc/message"
	"IM2/internal/apps/Message/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserSessionsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserSessionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserSessionsLogic {
	return &GetUserSessionsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 单次未读计数的扫描上限：超过后按上限返回（客户端展示为 999+ 之类即可）
const unreadCountLimit = 1000

func (l *GetUserSessionsLogic) GetUserSessions(in *message.GetUserSessionsReq) (*message.GetUserSessionsResp, error) {
	userSess, err := service.NewMessageService(l.svcCtx).GetUserSessions(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	var list []*message.UserSession
	for _, uc := range userSess {
		// Lamport seq 不连续，未读数不能再用 actual_seq - last_read_seq 计算，
		// 改为服务端按 {session_id, seq > last_read_seq} 点查 MongoDB（走复合索引）
		unread, cntErr := l.svcCtx.MessageDAO.CountUnread(l.ctx, uc.SessionID, uc.LastReadSeq, in.UserId, unreadCountLimit)
		if cntErr != nil {
			l.Errorf("count unread for session %s failed: %v", uc.SessionID, cntErr)
		}
		list = append(list, &message.UserSession{
			UserId:      uc.UserID,
			SessionId:   uc.SessionID,
			IsTop:       int32(uc.IsTop),
			IsDisturb:   int32(uc.IsDisturb),
			LastReadSeq: uc.LastReadSeq,
			CreateTime:  uc.CreateTime.UnixMilli(),
			UpdateTime:  uc.UpdateTime.UnixMilli(),
			UnreadCount: unread,
		})
	}

	return &message.GetUserSessionsResp{Sessions: list}, nil
}
