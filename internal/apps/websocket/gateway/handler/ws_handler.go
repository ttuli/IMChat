package handler

import (
	"context"
	"fmt"
	"net/http"

	"IM2/internal/apps/Group/rpc/group"
	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"
	"IM2/internal/apps/websocket/gateway/types"
	"IM2/pkg/logger"
	"IM2/pkg/resultx"
	tokenmanager "IM2/pkg/tokenManager"
	"IM2/pkg/xerr"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 开发环境允许所有来源，生产环境应配置
	},
}

// WSHandler WebSocket 处理器
type WSHandler struct {
	svcCtx *svc.ServiceContext
	codec  protocol.Codec
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(svcCtx *svc.ServiceContext, codec protocol.Codec) *WSHandler {
	upgrader.ReadBufferSize = svcCtx.Config.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = svcCtx.Config.WebSocket.WriteBufferSize
	return &WSHandler{svcCtx: svcCtx, codec: codec}
}

// Handle 处理 WebSocket 连接
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {

	// 从 context 中提取用户ID（JWT 已在 HTTP 中间件层验证）
	userID := tokenmanager.ExtractIDFromCtx(r.Context())

	// 从 query 参数获取设备信息
	deviceID := r.URL.Query().Get("device_id")
	platform := r.URL.Query().Get("platform")

	// 升级 HTTP 连接到 WebSocket
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		resultx.ErrorCtx(r.Context(), w, xerr.Wrap(err, xerr.ErrWSUpgrade, "建立连接失败"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// 创建连接
	conn := connection.NewConnection(userID, deviceID, platform, wsConn, h.codec)

	fmt.Println("conn add")
	// 注册连接
	if err := h.svcCtx.ConnectionManager.AddConnection(conn.UserID, conn); err != nil {
		resultx.ErrorCtx(r.Context(), w, xerr.Wrap(err, xerr.ErrWsConnAdd, "建立连接失败"))
		conn.Close()
		return
	}

	go h.storeUserJoinedGroup(ctx, userID)

	// 注册路由
	if err := h.svcCtx.Router.RegisterUser(ctx, conn.UserID); err != nil {
		resultx.ErrorCtx(r.Context(), w, xerr.Wrap(err, xerr.ErrWsConnAdd, "注册路由失败"))
		conn.Close()
		return
	}

	defer func() {
		h.svcCtx.ConnectionManager.RemoveConnection(conn.UserID)
		h.svcCtx.Router.UnregisterUser(context.Background(), conn.UserID)
	}()

	// 启动写循环
	go conn.WritePump(ctx)

	// 启动读循环(阻塞)
	conn.ReadPump(ctx, h.createMessageHandler(ctx, conn))

	h.svcCtx.ConnectionManager.RemoveConnection(conn.UserID)
	h.svcCtx.ConnectionManager.RemoveGroupConnection(conn.UserID, conn)
}

// createMessageHandler 创建消息处理函数
func (h *WSHandler) createMessageHandler(ctx context.Context, conn *connection.Connection) func(*types.WSMessage) error {
	msgHandler := NewMessageHandler(h.svcCtx, conn)
	return func(msg *types.WSMessage) error {
		return msgHandler.Handle(ctx, msg)
	}
}

func (h *WSHandler) storeUserJoinedGroup(ctx context.Context, userID uint64) {
	// 获取用户的群列表
	resp, err := h.svcCtx.GroupRpc.GetUserGroups(ctx, &group.GetUserGroupsReq{
		UserId: userID,
	})
	if err != nil {
		logx.Errorf("get user %d groups failed: %v", userID, err)
		// 获取群列表失败是严重错误，断开连接让客户端重连
		if conn, ok := h.svcCtx.ConnectionManager.GetLocalConnection(userID); ok {
			h.sendError(conn.Conn, 500, "internal server error: failed to load groups")
			conn.Close()
		}
		return
	}

	// 获取当前连接
	conn, ok := h.svcCtx.ConnectionManager.GetLocalConnection(userID)
	if !ok {
		return
	}

	// 将连接加入到所有群组
	for _, groupID := range resp.Data {
		if err := h.svcCtx.ConnectionManager.AddGroupConnection(groupID, conn); err != nil {
			logx.Errorf("add group connection failed: %v", err)
		}
	}
	logger.Infof("added user %d to %d groups", userID, len(resp.Data))
}

// sendError 发送错误消息
func (h *WSHandler) sendError(wsConn *websocket.Conn, code int32, message string) {
	errMsg := protocol.NewErrorWSMessage(code, message)
	data, _ := h.codec.Encode(errMsg)
	wsConn.WriteMessage(websocket.TextMessage, data)
}

// ConfigureUpgrader 配置升级器
func ConfigureUpgrader(c config.Config) {
	upgrader.ReadBufferSize = c.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = c.WebSocket.WriteBufferSize
}
