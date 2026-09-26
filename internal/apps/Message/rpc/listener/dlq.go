package listener

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"IM2/internal/apps/Message/rpc/internal/service"
	"IM2/pkg/logger"
	protosvc "IM2/pkg/proto/svc"

	"github.com/nats-io/nats.go"
)

// 死信 Header。Data 是 protobuf 二进制，排查（nats stream view）和重放工具的
// 过滤/定位全靠这些 Header，不必反序列化消息体。
const (
	HeaderOriginalSubject = "x-original-subject"
	HeaderStreamSeq       = "x-stream-seq" // 原消息在 WS_MESSAGES 中的 stream sequence
	HeaderDeadReason      = "x-dead-reason"
	HeaderErrorClass      = "x-error-class" // ErrorClassPoison / ErrorClassExhausted
	HeaderDeathTime       = "x-death-time"
	HeaderRetryCount      = "x-retry-count"
	HeaderSessionKey      = "x-session-key"
	HeaderSender          = "x-sender"
	HeaderClientID        = "x-client-id"
	HeaderMsgType         = "x-msg-type"

	// ErrorClassPoison 毒消息：重试多少次都不会成功，重放时应跳过
	ErrorClassPoison = "poison"
	// ErrorClassExhausted 退避重试耗尽：通常是依赖持续故障，修复根因后可重放
	ErrorClassExhausted = "exhausted"
)

// AlertNotifier 定义死信告警通知接口
type AlertNotifier interface {
	Notify(ctx context.Context, title string, content string) error
}

// LoggerAlertNotifier 默认的日志告警通知器（控制台/日志文件输出）。
// 输出行以 [DLQ-ALERT] 开头，供日志系统（Loki）按关键字配置告警规则。
type LoggerAlertNotifier struct{}

func NewLoggerAlertNotifier() *LoggerAlertNotifier {
	return &LoggerAlertNotifier{}
}

func (n *LoggerAlertNotifier) Notify(ctx context.Context, title string, content string) error {
	logger.Errorf("[DLQ-ALERT] %s: %s", title, content)
	return nil
}

// DeadLetter 一条待转存的死信及其上下文
type DeadLetter struct {
	Msg      *nats.Msg             // 原始消息，Data 原样转存
	Send     *protosvc.MessageSend // 已解析的业务消息；反序列化失败的毒消息为 nil
	Reason   error
	Poison   bool
	Delivery service.Delivery
}

// DLQHandler 定义死信处理接口。
// 返回 nil 表示死信**已持久化**，调用方据此才能 Ack 原消息；
// 返回 error 时原消息必须保留（Nak），否则就是静默丢失。
type DLQHandler interface {
	Handle(ctx context.Context, dl DeadLetter) error
}

// NatsDLQHandler 基于 NATS JetStream 的死信处理器
type NatsDLQHandler struct {
	js       nats.JetStreamContext
	subject  string
	notifier AlertNotifier
}

func NewNatsDLQHandler(js nats.JetStreamContext, subject string, notifier AlertNotifier) *NatsDLQHandler {
	if notifier == nil {
		notifier = NewLoggerAlertNotifier()
	}
	return &NatsDLQHandler{
		js:       js,
		subject:  subject,
		notifier: notifier,
	}
}

func (h *NatsDLQHandler) Handle(ctx context.Context, dl DeadLetter) error {
	if h.subject == "" {
		return fmt.Errorf("DLQ subject is empty, message cannot be routed to DLQ")
	}

	class := ErrorClassExhausted
	if dl.Poison {
		class = ErrorClassPoison
	}

	dlqMsg := nats.NewMsg(h.subject)
	dlqMsg.Data = dl.Msg.Data
	dlqMsg.Header.Set(HeaderOriginalSubject, dl.Msg.Subject)
	dlqMsg.Header.Set(HeaderDeadReason, dl.Reason.Error())
	dlqMsg.Header.Set(HeaderErrorClass, class)
	dlqMsg.Header.Set(HeaderDeathTime, time.Now().Format(time.RFC3339))
	dlqMsg.Header.Set(HeaderRetryCount, strconv.FormatUint(dl.Delivery.NumDelivered, 10))
	if dl.Delivery.StreamSeq > 0 {
		dlqMsg.Header.Set(HeaderStreamSeq, strconv.FormatUint(dl.Delivery.StreamSeq, 10))
	}
	if dl.Send != nil {
		dlqMsg.Header.Set(HeaderSessionKey, dl.Send.SessionKey)
		dlqMsg.Header.Set(HeaderSender, strconv.FormatUint(dl.Send.Sender, 10))
		dlqMsg.Header.Set(HeaderClientID, dl.Send.ClientId)
		dlqMsg.Header.Set(HeaderMsgType, strconv.FormatInt(dl.Send.MsgType, 10))
	}

	// JetStream 发布并等待 PubAck：只有 WS_DLQ 确认落盘，才算转存成功。
	// MsgId 取原消息的 stream seq——PubAck 丢失导致的重复转存会被 WS_DLQ 去重折叠。
	opts := []nats.PubOpt{nats.Context(ctx)}
	if dl.Delivery.StreamSeq > 0 {
		opts = append(opts, nats.MsgId("dlq:"+strconv.FormatUint(dl.Delivery.StreamSeq, 10)))
	}
	if _, err := h.js.PublishMsg(dlqMsg, opts...); err != nil {
		return fmt.Errorf("publish dead letter to %s: %w", h.subject, err)
	}

	if h.notifier != nil {
		title := fmt.Sprintf("IMChat Message Persist DLQ Alert [%s]", class)
		content := fmt.Sprintf("stream_seq=%d session_key=%s client_id=%s attempts=%d reason=%v",
			dl.Delivery.StreamSeq, dlqMsg.Header.Get(HeaderSessionKey), dlqMsg.Header.Get(HeaderClientID),
			dl.Delivery.NumDelivered, dl.Reason)
		go func() {
			if err := h.notifier.Notify(context.Background(), title, content); err != nil {
				logger.Errorf("[DLQ] Failed to send alert: %v", err)
			}
		}()
	}
	return nil
}
