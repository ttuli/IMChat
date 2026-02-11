package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateGroupLogic {
	return &CreateGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 群组管理
func (l *CreateGroupLogic) CreateGroup(in *group.CreateGroupReq) (*group.CreateGroupResp, error) {
	result, err := l.svcCtx.GroupService.CreateGroup(l.ctx, in.OwnerId, in.Name, in.Avatar, in.MemberIds)
	if err != nil {
		return nil, err
	}

	return &group.CreateGroupResp{
		Data: &group.Group{
			GroupId:     result.ID,
			Name:        result.Name,
			Avatar:      result.Avatar,
			OwnerId:     result.OwnerID,
			CreatedAt:   result.CreateTime.UnixMilli(),
			UpdatedAt:   result.UpdateTime.UnixMilli(),
			MemberCount: int32(result.MemberCount),
		},
	}, nil
}
