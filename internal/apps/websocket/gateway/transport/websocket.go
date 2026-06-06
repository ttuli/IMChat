package transport

import (
	"context"
	"net/http"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/connection"
	"IM2/internal/apps/websocket/gateway/dispatch"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/server"

	"IM2/pkg/proto/transport"
	"IM2/pkg/resultx"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 开发环境允许所有来源，生产环境应配置
	},
}

// WSHandler WebSocket 处理器
type WSHandler struct {
	svcCtx *server.ServiceContext
	codec  protocol.Codec
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(svcCtx *server.ServiceContext, codec protocol.Codec) *WSHandler {
	upgrader.ReadBufferSize = svcCtx.Config.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = svcCtx.Config.WebSocket.WriteBufferSize
	return &WSHandler{svcCtx: svcCtx, codec: codec}
}

// Handle 处理 WebSocket 连接
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {

	// 从 context 中提取用户ID（JWT 已在 HTTP 中间件层验证）
	userID := tokenmanager.ExtractIDFromCtx(r.Context())

	// 升级 HTTP 连接到 WebSocket
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		resultx.ErrorJsonCtx(r.Context(), w, xerr.Wrap(err, transport.ErrorCode_ERR_WS_UPGRADE, "建立连接失败"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// 创建连接
	conn := connection.NewConnection(userID, wsConn, h.codec, h.svcCtx.Config.WebSocket.Version)
	// 注册连接
	if err := h.svcCtx.ConnectionManager.AddConnection(conn.UserID, conn); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		conn.SendError(&transport.ErrorMessage{
			ErrorCode: int32(transport.ErrorCode_ERR_INTERNAL_SERVER),
			ErrorMsg:  "与服务器建立连接失败",
		})
		conn.Close()
		return
	}

	// 注册路由
	if err := h.svcCtx.Router.RegisterUser(ctx, conn.UserID); err != nil {
		h.svcCtx.TelemetryBus.Publish(err)
		conn.SendError(&transport.ErrorMessage{
			ErrorCode: int32(transport.ErrorCode_ERR_INTERNAL_SERVER),
			ErrorMsg:  "与服务器建立连接失败",
		})
		conn.Close()
		return
	}

	defer func() {
		// 只删除自己：RemoveConnection 内部按指针比对，不会误删新连接
		h.svcCtx.ConnectionManager.RemoveConnection(conn.UserID, conn)
		// 只有当 map 中已不存在该 userID 的连接（即本连接确实是最后一个）时才注销路由
		// 避免旧连接 defer 注销掉新连接刚注册上的路由
		if _, exists := h.svcCtx.ConnectionManager.GetLocalConnection(conn.UserID); !exists {
			h.svcCtx.Router.UnregisterUser(context.Background(), conn.UserID)
		}
	}()

	// 启动写循环
	go conn.WritePump(ctx)

	// 启动读循环(阻塞)
	conn.ReadPump(ctx, h.createMessageHandler(ctx, conn))
}

// createMessageHandler 创建消息处理函数
func (h *WSHandler) createMessageHandler(ctx context.Context, conn *connection.Connection) func(*transport.WSMessage) error {
	msgHandler := dispatch.NewDispatcher(h.svcCtx, conn)
	return func(msg *transport.WSMessage) error {
		return msgHandler.Handle(ctx, msg)
	}
}

// ConfigureUpgrader 配置升级器
func ConfigureUpgrader(c config.Config) {
	upgrader.ReadBufferSize = c.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = c.WebSocket.WriteBufferSize
}
