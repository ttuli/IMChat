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

type GetPendingAppliesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取待处理的群申请
func NewGetPendingAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingAppliesLogic {
	return &GetPendingAppliesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetPendingAppliesLogic) GetPendingApplies(req *types.GetPendingAppliesReq) (resp *types.GetPendingAppliesResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.GetPendingApplies(l.ctx, &grouprpc.GetPendingAppliesReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	data := make([]*types.GroupRequest, 0, len(rpcResp.Data))
	for _, d := range rpcResp.Data {
		data = append(data, &types.GroupRequest{
			Id:   strconv.FormatUint(d.Id, 10),
			SenderId:    d.FromUserId,
			GroupId:     d.GroupId,
			ApplyMsg:     d.ApplyMsg,
			Status:      int32(d.Status),
			HandlerId:   d.HandlerId,
			RequestTime: d.RequestTime,
			HandleTime:  d.HandleTime,
		})
	}

	return &types.GetPendingAppliesResp{
		Data: data,
	}, nil
}
