package handler

import (
	"net/http"

	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/middleware"

	"github.com/zeromicro/go-zero/rest"
)

// RegisterHandlers 注册 HTTP 处理器
func RegisterHandlers(server *rest.Server, serverCtx *svc.ServiceContext) {
	wsHandler := NewWSHandler(serverCtx)
	server.Use(middleware.WithRedisJwtAuth(serverCtx.TokenManager))
	server.AddRoutes(
		[]rest.Route{
			{
				Method:  http.MethodGet,
				Path:    serverCtx.Config.WebSocket.Path,
				Handler: wsHandler.Handle,
			},
		},
	)
}
