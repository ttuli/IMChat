package transport

import (
	"net/http"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/server"
	"IM2/middleware"

	"github.com/zeromicro/go-zero/rest"
)

// RegisterHandlers 注册 HTTP 处理器
func RegisterHandlers(s *rest.Server, serverCtx *server.ServiceContext) {
	wsHandler := NewWSHandler(serverCtx, protocol.NewProtoCodec())
	s.Use(middleware.WithRedisJwtAuth(serverCtx.TokenManager))
	s.Use(middleware.WithWsSessionAuth(serverCtx.TokenManager))
	s.AddRoutes(
		[]rest.Route{
			{
				Method:  http.MethodGet,
				Path:    serverCtx.Config.WebSocket.Path,
				Handler: wsHandler.Handle,
			},
		},
	)
}
