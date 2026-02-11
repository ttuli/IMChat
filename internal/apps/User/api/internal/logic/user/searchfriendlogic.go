package user

import (
	"context"

	"IM2/internal/apps/User/api/svc"
	"IM2/internal/apps/User/api/types"
	"IM2/internal/apps/User/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type SearchFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 搜索好友
func NewSearchFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SearchFriendLogic {
	return &SearchFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SearchFriendLogic) SearchFriend(req *types.SearchFriendReq) (resp *types.SearchFriendResp, err error) {
	// 构建查询请求
	rpcReq := &user.GetUserInfoReq{
		Limit:  int32(req.Limit),
		Offset: int32(req.Offset),
	}

	// 根据不同的查询参数设置
	if req.ID != 0 {
		rpcReq.Ids = []uint64{req.ID}
	} else if req.Phone != "" {
		rpcReq.Phone = req.Phone
	} else if req.Name != "" {
		rpcReq.Name = req.Name
	}

	res, err := l.svcCtx.GetUserInfo(l.ctx, rpcReq)
	if err != nil {
		return nil, err
	}

	data := make([]types.UserInfo, 0, len(res.Data))
	for _, v := range res.Data {
		data = append(data, types.UserInfo{
			UserID:            v.UserId,
			UserName:          v.UserName,
			Gender:            v.Gender,
			Avatar:            v.Avatar,
			Phone:             v.Phone,
			PersonalSignature: v.PersonalSignature,
			JoinType:          v.JoinType,
			Status:            v.Status,
			CreateTime:        v.CreateTime,
			UpdateTime:        v.UpdateTime,
		})
	}

	return &types.SearchFriendResp{
		Data:  data,
		Total: int64(len(data)),
	}, nil
}
