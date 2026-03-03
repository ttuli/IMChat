package logic

import (
	"context"

	"IM2/internal/apps/Auth/api/svc"
	"IM2/internal/apps/Auth/api/types"
	"IM2/internal/apps/Auth/rpc/client/authrpc"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LogoutLogic) Logout(req *types.LogoutReq) error {
	uid := tokenmanager.ExtractIDFromCtx(l.ctx)
	_, err := l.svcCtx.AuthRpc.Logout(l.ctx, &authrpc.LogoutRequest{
		UserID:   uid,
		RemoveRT: req.RemoveRt,
		Platform: req.Platform,
	})
	return err
}
