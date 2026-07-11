package nats_util

import (
	"context"

	"IM2/pkg/logger"
	"IM2/pkg/proto/transport"
	"IM2/pkg/routing"
)

// UserNotifier 面向特定用户的通知投递器（User/Group RPC 共用）。
//
// 按路由表把 msg.RouteTarget 中的目标用户聚合到所在网关节点，向每个节点
// 单播一份只含该节点目标的消息副本，与 Message 服务的投递语义一致：
//   - 确认离线的目标：只存不推，通知数据均已落库，上线后由客户端拉取对齐
//   - 路由不可信（Redis 异常/路由指向已死节点）的目标：广播兜底，
//     各网关按本地连接过滤投递，不会漏投
type UserNotifier struct {
	client *Client
	routes *routing.Table
}

func NewUserNotifier(client *Client, routes *routing.Table) *UserNotifier {
	return &UserNotifier{
		client: client,
		routes: routes,
	}
}

// Publish 将一条 TargetType_USER 消息精准投递到各目标用户所在的网关节点。
// 投递失败仅记录日志：通知类数据已持久化，可由客户端拉取兜底。
func (n *UserNotifier) Publish(ctx context.Context, msg *transport.WSMessage) {
	if msg == nil || len(msg.RouteTarget) == 0 {
		return
	}

	nodeTargets, fallback, err := n.routes.LookupUsers(ctx, msg.RouteTarget)
	if err != nil {
		logger.Errorf("[UserNotifier] batch route lookup failed (broadcast fallback): %v", err)
		nodeTargets, fallback = nil, msg.RouteTarget
	}
	for node, uids := range nodeTargets {
		n.client.PublishToNode(node, withRouteTarget(msg, uids))
	}
	if len(fallback) > 0 {
		n.client.Broadcast(withRouteTarget(msg, fallback))
	}
}

// withRouteTarget 复制一份仅 route_target 不同的消息副本（每个节点只投自己的目标），
// 防止多目标消息被整体转发后在其他节点重复投递
func withRouteTarget(msg *transport.WSMessage, targets []uint64) *transport.WSMessage {
	return &transport.WSMessage{
		RouteTarget:     targets,
		RouteTargetType: msg.RouteTargetType,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		Payload:         msg.Payload,
		SenderId:        msg.SenderId,
		Version:         msg.Version,
		MsgId:           msg.MsgId,
		SessionId:       msg.SessionId,
		MsgSeq:          msg.MsgSeq,
	}
}
