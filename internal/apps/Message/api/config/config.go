package config

import (
	tokenmanager "IM2/pkg/tokenManager"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	MessageRpc zrpc.RpcClientConf

	TokenConfig tokenmanager.TokenConfig

	Turn TurnConfig
}

// TurnConfig 下发给客户端的 ICE 服务器配置。
//
// Secret 必须与 coturn 的 static-auth-secret 完全一致——凭证由本服务现算、
// 由 coturn 用同一密钥校验，两边不一致的表现是 coturn 静默拒绝分配中继，
// 客户端只看到 ICE 失败，很难定位。
type TurnConfig struct {
	// coturn 的 static-auth-secret。务必从环境变量/密钥管理注入，不要写进配置仓库。
	Secret string
	// TURN 服务地址，如 turn:42.194.218.82:3478?transport=udp
	Urls []string
	// STUN 地址（可为空）。STUN 不需要凭证，与 TURN 分开下发。
	StunUrls []string `json:",optional"`
	// 凭证有效期（秒）。取值权衡：太短会让振铃期间就过期，太长则泄漏后可被滥用更久。
	// 未配置时取默认值 DefaultTurnTTLSeconds。
	TTLSeconds int64 `json:",optional"`
}
