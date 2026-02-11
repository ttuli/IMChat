package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetPendingAppliesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPendingAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPendingAppliesLogic {
	return &GetPendingAppliesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPendingAppliesLogic) GetPendingApplies(in *group.GetPendingAppliesReq) (*group.GetPendingAppliesResp, error) {
	applies, err := l.svcCtx.GroupService.GetPendingApplies(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}

	data := make([]*group.GroupRequest, 0, len(applies))
	for _, a := range applies {
		data = append(data, &group.GroupRequest{
			Id:          a.ID,
			FromUserId:  a.FromUserID,
			GroupId:     a.GroupID,
			ApplyMsg:    a.ApplyMsg,
			Status:      group.ApplyStatus(a.Status + 1), // model: 0-pending → proto: 1-pending
			HandlerId:   a.HandlerID,
			RequestTime: a.CreateTime.Unix(),
			HandleTime:  a.UpdateTime.Unix(),
		})
	}

	return &group.GetPendingAppliesResp{Data: data}, nil
}
