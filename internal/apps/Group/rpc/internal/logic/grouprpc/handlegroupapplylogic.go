package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleGroupApplyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHandleGroupApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleGroupApplyLogic {
	return &HandleGroupApplyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HandleGroupApplyLogic) HandleGroupApply(in *group.HandleGroupApplyReq) (*group.EmptyResp, error) {
	// proto ApplyStatus: 1-pending, 2-accepted, 3-rejected, 4-ignored
	// model status: 0-pending, 1-accepted, 2-rejected, 3-ignored
	status := uint8(in.Status - 1)
	if err := l.svcCtx.GroupService.HandleGroupApply(l.ctx, in.Id, in.OperatorId, status, in.RejectReason); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
