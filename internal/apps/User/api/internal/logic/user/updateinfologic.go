package user

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 更新信息
func NewUpdateInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateInfoLogic {
	return &UpdateInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateInfoLogic) UpdateInfo(req *types.UpdateInfoReq) error {

	_, err := l.svcCtx.UpdateInfo(l.ctx, &user.UpdateInfoReq{
		UserId:            tokenmanager.ExtractIDFromCtx(l.ctx),
		Name:              req.UserName,
		Gender:            int32(req.Gender),
		JoinType:          int32(req.JoinType),
		Avatar:            req.Avatar,
		PersonalSignature: req.PersonalSignature,
	})
	if err != nil {
		return err
	}

	return nil
}
