package dispatch

import (
	"context"
	
	"IM2/internal/apps/websocket/gateway/connection"
	"IM2/internal/apps/websocket/gateway/server"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
)

// Dispatcher 消息分发器
type Dispatcher struct {
	svcCtx *server.ServiceContext
	conn   *connection.Connection
}

// NewDispatcher 创建消息分发器
func NewDispatcher(svcCtx *server.ServiceContext, conn *connection.Connection) *Dispatcher {
	return &Dispatcher{
		svcCtx: svcCtx,
		conn:   conn,
	}
}

// Handle 处理消息
func (h *Dispatcher) Handle(ctx context.Context, msg *transport.WSMessage) error {
	switch {
	case util.IsChatMessage(msg.Type):
		return h.processMessage(msg)
	default:
		logger.Infof("[Dispatcher] unknown message type: %v", msg.Type)
		return nil
	}
}
