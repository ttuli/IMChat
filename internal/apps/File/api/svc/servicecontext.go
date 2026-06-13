package svc

import (
	"IM2/interceptor"
	"IM2/internal/apps/File/api/config"
	"IM2/internal/apps/File/api/internal/manager"
	"IM2/internal/apps/User/rpc/client/user"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	user.User
	*tokenmanager.TokenManager

	AvatarOss *manager.OssManager
	FileOss   *manager.OssManager
	ImageOss  *manager.OssManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		User: user.NewUser(zrpc.MustNewClient(c.UserRpc,
			zrpc.WithUnaryClientInterceptor(interceptor.ClientErrorInterceptor))),
		TokenManager: tokenmanager.NewTokenManager(c.TokenConfig),

		AvatarOss: manager.NewOssManager(c.Oss.Avatar),
		FileOss:   manager.NewOssManager(c.Oss.ChatFile),
		ImageOss:  manager.NewOssManager(c.Oss.ChatImage),
	}
}
