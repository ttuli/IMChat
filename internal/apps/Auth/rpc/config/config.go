package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf

	AuthDAO string // DSN, e.g. "user:pass@tcp(host:3306)/dbname?parseTime=true"

	TokenConfig tokenmanager.TokenConfig

	IDRpc zrpc.RpcClientConf
}
