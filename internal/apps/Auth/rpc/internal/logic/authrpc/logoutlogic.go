package authrpclogic

import (
	"context"

	"IM2/internal/apps/Auth/rpc/auth"
	"IM2/internal/apps/Auth/rpc/internal/service"
	"IM2/internal/apps/Auth/rpc/svc"
	"IM2/pkg/logger"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LogoutLogic) Logout(in *auth.LogoutRequest) (*auth.LogoutReply, error) {
	_, err := l.svcCtx.AuthService.Logout(l.ctx, &service.LogoutRequest{
		UserID:   in.UserID,
		RemoveRT: in.RemoveRT,
	})
	if err != nil {
		logger.Errorf("Logout failed: %v", err)
	}

	return &auth.LogoutReply{}, nil
}
