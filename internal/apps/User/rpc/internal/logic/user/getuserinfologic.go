package logic

import (
	"context"

	"IM2/internal/apps/User/rpc/svc"
	"IM2/internal/apps/User/rpc/user"
	"IM2/internal/Entity"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserInfoLogic {
	return &GetUserInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserInfoLogic) GetUserInfo(in *user.GetUserInfoReq) (*user.GetUserInfoResp, error) {
	var users []*model.UserInfo
	var err error

	// 根据不同的查询条件调用 service
	if len(in.Ids) != 0 {
		users, err = l.svcCtx.UserService.GetUsersByIDs(l.ctx, in.Ids)
	} else if in.Phone != "" {
		u, e := l.svcCtx.UserService.GetUserByPhone(l.ctx, in.Phone)
		if e != nil {
			return nil, e
		}
		users = append(users, u)
	} else if in.Name != "" {
		users, err = l.svcCtx.UserService.GetUsersByName(l.ctx, in.Name, in.Limit, in.Offset)
	}

	if err != nil {
		return nil, err
	}

	// 转换为响应格式
	list := make([]*user.UserInfo, 0, len(users))
	for _, v := range users {
		list = append(list, &user.UserInfo{
			UserId:            v.UserID,
			UserName:          v.UserName,
			Gender:            int32(v.Gender),
			Avatar:            v.Avatar,
			Phone:             v.Phone,
			PersonalSignature: v.PersonalSignature,
			JoinType:          int32(v.JoinType),
			Status:            int32(v.Status),
			CreateTime:        v.CreateTime.UnixMilli(),
			UpdateTime:        v.UpdateTime.UnixMilli(),
		})
	}

	return &user.GetUserInfoResp{
		Data: list,
	}, nil
}
