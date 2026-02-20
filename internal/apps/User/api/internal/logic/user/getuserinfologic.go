package user

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 获取用户信息
func NewGetUserInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserInfoLogic {
	return &GetUserInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserInfoLogic) GetUserInfo(req *types.GetUserInfoReq) (resp *types.GetUserInfoResp, err error) {
	if len(req.Ids) == 0 && req.Phone == "" && req.Name == "" {
		id := tokenmanager.ExtractIDFromCtx(l.ctx)
		req.Ids = append(req.Ids, id)
	}
	res, err := l.svcCtx.GetUserInfo(l.ctx, &user.GetUserInfoReq{
		Ids:    req.Ids,
		Phone:  req.Phone,
		Name:   req.Name,
		Limit:  int32(req.Limit),
		Offset: int32(req.Offset),
	})
	if err != nil {
		return nil, err
	}

	data := make([]*types.UserInfo, 0)
	for _, v := range res.Data {
		u := &types.UserInfo{
			UserId:            v.UserId,
			UserName:          v.UserName,
			Gender:            v.Gender,
			Avatar:            v.Avatar,
			Phone:             v.Phone,
			PersonalSignature: v.PersonalSignature,
			JoinType:          v.JoinType,
			Status:            v.Status,
			CreateTime:        v.CreateTime,
			UpdateTime:        v.UpdateTime,
		}
		data = append(data, u)
	}
	return &types.GetUserInfoResp{
		Data: data,
	}, nil
}
