package member

import (
	"context"

	"IM2/internal/apps/Group/api/svc"
	"IM2/internal/apps/Group/api/types"
	"IM2/internal/apps/Group/rpc/client/grouprpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type SetMemberNicknameLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 设置群内昵称
func NewSetMemberNicknameLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetMemberNicknameLogic {
	return &SetMemberNicknameLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SetMemberNicknameLogic) SetMemberNickname(req *types.SetMemberNicknameReq) error {
	// TODO: 从 JWT 获取用户ID
	userID := tokenmanager.ExtractIDFromCtx(l.ctx)

	_, err := l.svcCtx.GroupRpc.SetMemberNickname(l.ctx, &grouprpc.SetMemberNicknameReq{
		GroupId:  req.GroupId,
		UserId:   userID,
		Nickname: req.Nickname,
	})
	return err
}
