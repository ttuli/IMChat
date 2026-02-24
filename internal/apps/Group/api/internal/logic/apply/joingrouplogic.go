package apply

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type JoinGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 申请加入群聊
func NewJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *JoinGroupLogic {
	return &JoinGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *JoinGroupLogic) JoinGroup(req *types.JoinGroupReq) (resp *types.JoinGroupResp, err error) {
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.GroupRpc.JoinGroup(l.ctx, &grouprpc.JoinGroupReq{
		GroupId:    req.GroupId,
		FromUserId: userID,
		ApplyMsg:   req.Message,
	})
	if err != nil {
		return nil, err
	}

	resp = &types.JoinGroupResp{}

	if rpcResp.Data != nil {
		data := rpcResp.Data
		resp.Data = &types.GroupRequest{
			Id:          int64(data.Id),
			SenderId:    data.FromUserId,
			GroupId:     data.GroupId,
			ApplyMsg:    data.ApplyMsg,
			Status:      int32(data.Status),
			HandlerId:   data.HandlerId,
			RequestTime: data.RequestTime,
			HandleTime:  data.HandleTime,
		}
	} else if rpcResp.Member != nil {
		member := rpcResp.Member
		resp.Member = &types.GroupMember{
			GroupId:  member.GroupId,
			UserId:   member.UserId,
			Role:     int32(member.Role),
			Nickname: member.Nickname,
			JoinedAt: member.JoinedAt,
		}
	}

	return resp, nil
}
