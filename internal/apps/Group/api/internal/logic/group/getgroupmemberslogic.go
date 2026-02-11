package group

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/group"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupMembersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取群成员列表
func NewGetGroupMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupMembersLogic {
	return &GetGroupMembersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetGroupMembersLogic) GetGroupMembers(req *types.GetGroupMembersReq) (resp *types.GetGroupMembersResp, err error) {
	res, err := l.svcCtx.GroupRpc.GetGroupMemberIDs(l.ctx, &group.GetGroupMemberIDsReq{
		GroupId: req.GroupID,
	})
	if err != nil {
		return nil, err
	}

	var members []types.GroupMember
	for _, m := range res.Members {
		members = append(members, types.GroupMember{
			GroupID:   m.GroupId,
			UserID:    m.UserId,
			Role:      int8(m.Role),
			Nickname:  m.Nickname,
			MuteUntil: m.MuteUntil,
			JoinedAt:  m.JoinedAt,
		})
	}

	return &types.GetGroupMembersResp{Data: members}, nil
}
