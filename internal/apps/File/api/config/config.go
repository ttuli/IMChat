package config

import (
	"IM2/pkg/service"
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	UserRpc     zrpc.RpcClientConf
	TokenConfig tokenmanager.TokenConfig

	Oss struct {
		Avatar struct {
			BucketName  string
			Region      string
			Dir         string
			CallbackURL string
			Product     string
		}
	}

	APISIX service.APISIXConfig
}
