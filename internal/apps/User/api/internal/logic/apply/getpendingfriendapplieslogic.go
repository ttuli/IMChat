package apply

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	"IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetPendingFriendAppliesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取待处理申请
func NewGetPendingFriendAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingFriendAppliesLogic {
	return &GetPendingFriendAppliesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetPendingFriendAppliesLogic) GetPendingFriendApplies(req *types.GetPendingFriendAppliesReq) (resp *types.GetPendingFriendAppliesResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	res, err := l.svcCtx.GetPendingFriendApplies(l.ctx, &user.GetPendingFriendAppliesReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	data := make([]*types.FriendRequest, 0, len(res.Data))
	for _, a := range res.Data {
		data = append(data, &types.FriendRequest{
			RequestId:    a.Id,
			FromUserId:   a.FromUserId,
			ToUserId:     a.ToUserId,
			ApplyMsg:     a.ApplyMsg,
			Status:       int32(a.Status),
			RequestTime:  a.RequestTime,
			HandleTime:   a.HandleTime,
			RejectReason: a.RejectReason,
		})
	}

	return &types.GetPendingFriendAppliesResp{
		Data: data,
	}, nil
}
