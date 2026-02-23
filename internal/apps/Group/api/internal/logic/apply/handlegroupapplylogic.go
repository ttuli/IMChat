package apply

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Group/rpc/group"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleGroupApplyLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 处理群申请
func NewHandleGroupApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleGroupApplyLogic {
	return &HandleGroupApplyLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *HandleGroupApplyLogic) HandleGroupApply(req *types.HandleGroupApplyReq) (*types.HandleGroupApplyResp, error) {
	resp, err := l.svcCtx.GroupRpc.HandleGroupApply(l.ctx, &grouprpc.HandleGroupApplyReq{
		Id:           uint64(req.ApplyId),
		OperatorId:   tokenmanager.ExtractIDFromCtx(l.ctx),
		Status:       group.ApplyStatus(req.Result),
		RejectReason: req.RejectReason,
	})
	return &types.HandleGroupApplyResp{
		Data: &types.GroupRequest{
			Id:           int64(resp.Data.Id),
			SenderId:     uint64(resp.Data.FromUserId),
			GroupId:      uint64(resp.Data.GroupId),
			ApplyMsg:     resp.Data.ApplyMsg,
			Status:       int32(resp.Data.Status),
			HandlerId:    uint64(resp.Data.HandlerId),
			RequestTime:  resp.Data.RequestTime,
			HandleTime:   resp.Data.HandleTime,
			RejectReason: resp.Data.RejectReason,
		},
	}, err
}
