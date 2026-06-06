package logic

import (
	"context"

	model "IM2/internal/Entity"
	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"
	"IM2/pkg/encrypt"
	"IM2/pkg/proto/transport"
	"IM2/pkg/xerr"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateUserLogic {
	return &CreateUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateUserLogic) CreateUser(in *user.CreateUserReq) (*user.CreateUserResp, error) {
	hashedPassword, err := encrypt.GenPasswordHash([]byte(in.Password))
	if err != nil {
		return nil, xerr.Wrap(err, transport.ErrorCode_ERR_ENCODING, "密码加密失败")
	}
	userId, err := l.svcCtx.UserService.CreateUser(l.ctx, &model.UserInfo{
		UserName: in.Name,
		Phone:    in.Phone,
		Gender:   uint8(in.Gender),
		JoinType: uint8(in.JoinType),
		Avatar:   in.Avatar,
		Password: string(hashedPassword),
	})
	if err != nil {
		return nil, err
	}

	return &user.CreateUserResp{UserId: userId}, nil
}
