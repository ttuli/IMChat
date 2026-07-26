package dispatch

import (
	"context"
	
	"IM2/internal/apps/websocket/gateway/connection"
	"IM2/internal/apps/websocket/gateway/server"
	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/proto/util"
)

// Dispatcher 消息分发器（随连接创建，生命周期与连接一致）
type Dispatcher struct {
	svcCtx *server.ServiceContext
	conn   *connection.Connection
	// calls 通话对端缓存，供 SDP/ICE 高频转发免去逐帧回查 Redis
	calls peerCache
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
	// 通话信令走独立路径：不落库、不分配 seq，与聊天消息互不影响
	case util.IsCallSignal(msg.Type):
		return h.processCallSignal(ctx, msg)
	default:
		logger.Infof("[Dispatcher] unknown message type: %v", msg.Type)
		return nil
	}
}
