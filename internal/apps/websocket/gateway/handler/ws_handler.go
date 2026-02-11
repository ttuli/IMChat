package handler

import (
	"context"
	"net/http"

	"IM2/internal/apps/websocket/gateway/config"
	"IM2/internal/apps/websocket/gateway/internal/connection"
	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/internal/apps/websocket/gateway/svc"
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
}

// NewWSHandler 创建 WebSocket 处理器
func NewWSHandler(svcCtx *svc.ServiceContext) *WSHandler {
	upgrader.ReadBufferSize = svcCtx.Config.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = svcCtx.Config.WebSocket.WriteBufferSize
	return &WSHandler{svcCtx: svcCtx}
}

// Handle 处理 WebSocket 连接
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// 升级 HTTP 连接到 WebSocket
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logx.Errorf("[WSHandler] upgrade failed: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// 等待认证消息
	conn, err := h.waitForAuth(ctx, wsConn)
	if err != nil {
		logx.Errorf("[WSHandler] auth failed: %v", err)
		h.sendError(wsConn, int32(xerr.ErrUnauthorized), "authentication failed")
		wsConn.Close()
		return
	}

	// 注册连接
	if err := h.svcCtx.ConnectionManager.AddConnection(conn.UserID, conn); err != nil {
		logx.Errorf("[WSHandler] add connection failed: %v", err)
		conn.Close()
		return
	}

	// 注册路由
	if err := h.svcCtx.Router.RegisterUser(ctx, conn.UserID); err != nil {
		logx.Errorf("[WSHandler] register route failed: %v", err)
	}

	defer func() {
		h.svcCtx.ConnectionManager.RemoveConnection(conn.UserID)
		h.svcCtx.Router.UnregisterUser(context.Background(), conn.UserID)
	}()

	// 启动写循环
	go conn.WritePump(ctx)

	// 启动读循环(阻塞)
	conn.ReadPump(ctx, h.createMessageHandler(ctx, conn))
}

// waitForAuth 等待认证消息
func (h *WSHandler) waitForAuth(ctx context.Context, wsConn *websocket.Conn) (*connection.Connection, error) {
	// 读取第一条消息作为认证消息
	_, data, err := wsConn.ReadMessage()
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrWebSocket, "read auth message failed")
	}

	msg, err := protocol.Decode(data)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode auth message failed")
	}

	if msg.Type != protocol.MessageTypeAuth {
		return nil, xerr.New(xerr.ErrInvalidParams, "first message must be auth")
	}

	authData, err := protocol.DecodeData[protocol.AuthData](msg)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrDecoding, "decode auth data failed")
	}

	// TODO: 调用 Auth RPC 验证 token
	// 这里暂时模拟验证成功，实际应调用 AuthRpc 验证
	userID := msg.From
	if userID == 0 {
		return nil, xerr.New(xerr.ErrUnauthorized, "invalid user id")
	}

	// 创建连接
	conn := connection.NewConnection(userID, authData.DeviceID, authData.Platform, wsConn)

	// 发送认证成功响应
	ackData, _ := protocol.EncodeData(&protocol.AuthAckData{
		Success: true,
		UserID:  userID,
		Message: "authenticated",
	})
	ackMsg := &protocol.Message{
		Type: protocol.MessageTypeAuthAck,
		Data: ackData,
	}
	if err := conn.Send(ackMsg); err != nil {
		return nil, xerr.Wrap(err, xerr.ErrWebSocket, "send auth ack failed")
	}

	logx.Infof("[WSHandler] user %d authenticated", userID)
	return conn, nil
}

// createMessageHandler 创建消息处理函数
func (h *WSHandler) createMessageHandler(ctx context.Context, conn *connection.Connection) func(*protocol.Message) error {
	msgHandler := NewMessageHandler(h.svcCtx, conn)
	return func(msg *protocol.Message) error {
		return msgHandler.Handle(ctx, msg)
	}
}

// sendError 发送错误消息
func (h *WSHandler) sendError(wsConn *websocket.Conn, code int32, message string) {
	errMsg := protocol.NewErrorMessage(code, message)
	data, _ := errMsg.Encode()
	wsConn.WriteMessage(websocket.TextMessage, data)
}

// ConfigureUpgrader 配置升级器
func ConfigureUpgrader(c config.Config) {
	upgrader.ReadBufferSize = c.WebSocket.ReadBufferSize
	upgrader.WriteBufferSize = c.WebSocket.WriteBufferSize
}
