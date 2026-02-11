package grouprpclogic

import (
	"context"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/Group/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type SetMemberNicknameLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSetMemberNicknameLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetMemberNicknameLogic {
	return &SetMemberNicknameLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SetMemberNicknameLogic) SetMemberNickname(in *group.SetMemberNicknameReq) (*group.EmptyResp, error) {
	if err := l.svcCtx.GroupService.SetMemberNickname(l.ctx, in.GroupId, in.UserId, in.Nickname); err != nil {
		return nil, err
	}
	return &group.EmptyResp{}, nil
}
