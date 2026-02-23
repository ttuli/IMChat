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

func (l *HandleGroupApplyLogic) HandleGroupApply(in *group.HandleGroupApplyReq) (*group.HandleGroupApplyResp, error) {
	status := uint8(in.Status - 1)
	apply, err := l.svcCtx.GroupService.HandleGroupApply(l.ctx, in.Id, in.OperatorId, status, in.RejectReason)
	if err != nil {
		return nil, err
	}

	return &group.HandleGroupApplyResp{
		Data: &group.GroupRequest{
			Id:          apply.ID,
			FromUserId:  apply.FromUserID,
			GroupId:     apply.GroupID,
			ApplyMsg:    apply.ApplyMsg,
			Status:      group.ApplyStatus(apply.Status),
			HandlerId:   apply.HandlerID,
			RequestTime: apply.CreateTime.UnixMilli(),
			HandleTime:  apply.UpdateTime.UnixMilli(),
		},
	}, nil
}
