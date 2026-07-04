package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"

	"IM2/internal/apps/Group/rpc/internal/service"
)

type GetGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupLogic {
	return &GetGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetGroupLogic) GetGroup(in *group.GetGroupReq) (*group.GetGroupResp, error) {
	results, total, err := service.NewGroupService(l.svcCtx).GetGroups(l.ctx, in.GroupIds, in.NameKeyword, in.Limit, in.Offset)
	if err != nil {
		return nil, err
	}
	data := make([]*group.Group, 0, len(results))
	for _, r := range results {
		data = append(data, &group.Group{
			GroupId:     r.ID,
			Name:        r.Name,
			Avatar:      r.Avatar,
			OwnerId:     r.OwnerID,
			MemberCount: int32(r.MemberCount),
			JoinType:    int32(r.JoinType),
			CreatedAt:   r.CreateTime.Unix(),
			UpdatedAt:   r.UpdateTime.Unix(),
		})
	}

	return &group.GetGroupResp{Data: data, Total: total}, nil
}
