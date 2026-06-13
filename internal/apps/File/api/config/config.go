package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type OssConfig struct {
	BucketName  string
	Region      string
	Dir         string
	CallbackURL string
	Product     string
}

type Config struct {
	rest.RestConf
	UserRpc     zrpc.RpcClientConf
	TokenConfig tokenmanager.TokenConfig

	Oss struct {
		Avatar    OssConfig
		ChatImage OssConfig
		ChatFile  OssConfig
	}
}
