package apply

import (
	"context"
	"strconv"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	"IM2/internal/apps/Group/rpc/group"
	"IM2/pkg/xerr"

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

func (l *HandleGroupApplyLogic) HandleGroupApply(req *types.HandleGroupApplyReq) error {
	applyID, err := strconv.ParseUint(req.ApplyId, 10, 64)
	if err != nil {
		return xerr.New(xerr.ErrInvalidParams, "申请ID格式错误")
	}

	_, err = l.svcCtx.GroupRpc.HandleGroupApply(l.ctx, &grouprpc.HandleGroupApplyReq{
		Id:           applyID,
		OperatorId:   req.OperatorId,
		Status:       group.ApplyStatus(req.Result),
		RejectReason: req.RejectReason,
	})
	return err
}
