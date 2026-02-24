package group

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 创建群组
func NewCreateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateGroupLogic {
	return &CreateGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateGroupLogic) CreateGroup(req *types.CreateGroupReq) (resp *types.CreateGroupResp, err error) {
	// TODO: 从 JWT 获取用户ID作为群主
	ownerID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.CreateGroup(l.ctx, &grouprpc.CreateGroupReq{
		Name:      req.Name,
		OwnerId:   ownerID,
		Avatar:    req.Avatar,
		MemberIds: req.MemberIds,
	})
	if err != nil {
		return nil, err
	}

	return &types.CreateGroupResp{
		Data: convertGroup(rpcResp.Data),
	}, nil
}

func convertGroup(data *grouprpc.Group) *types.Group {
	if data == nil {
		return nil
	}
	return &types.Group{
		Id:          data.GroupId,
		Name:        data.Name,
		Avatar:      data.Avatar,
		OwnerId:     data.OwnerId,
		CreatedAt:   data.CreatedAt,
		JoinType:    data.JoinType,
		UpdatedAt:   data.UpdatedAt,
		MemberCount: int32(data.MemberCount),
		Notice:      data.Notice,
	}
}
