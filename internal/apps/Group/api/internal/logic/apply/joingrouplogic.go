package apply

import (
	"context"
	"strconv"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type JoinGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 申请加入群聊
func NewJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *JoinGroupLogic {
	return &JoinGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *JoinGroupLogic) JoinGroup(req *types.JoinGroupReq) (resp *types.JoinGroupResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.JoinGroup(l.ctx, &grouprpc.JoinGroupReq{
		GroupId:    req.GroupID,
		FromUserId: userID,
		ApplyMsg:   req.Message,
	})
	if err != nil {
		return nil, err
	}

	data := rpcResp.Data
	return &types.JoinGroupResp{
		Data: types.GroupRequest{
			RequestID:   strconv.FormatUint(data.Id, 10),
			SenderID:    data.FromUserId,
			GroupID:     data.GroupId,
			Message:     data.ApplyMsg,
			Status:      int(data.Status),
			HandlerID:   data.HandlerId,
			RequestTime: data.RequestTime,
			HandleTime:  data.HandleTime,
		},
	}, nil
}
