package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateInfoLogic {
	return &UpdateInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateInfoLogic) UpdateInfo(in *user.UpdateInfoReq) (*user.EmptyResp, error) {
	err := l.svcCtx.UserService.UpdateUserInfo(l.ctx, in.UserId, in.Name, in.Avatar, uint8(in.Gender), uint8(in.JoinType), in.PersonalSignature)
	if err != nil {
		return nil, err
	}

	return &user.EmptyResp{}, nil
}
