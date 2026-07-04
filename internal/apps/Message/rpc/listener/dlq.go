package listener

import (
	"context"
	"fmt"
	"time"

	"IM2/pkg/logger"
	"github.com/nats-io/nats.go"
)

// AlertNotifier 定义死信告警通知接口
type AlertNotifier interface {
	Notify(ctx context.Context, title string, content string) error
}

// LoggerAlertNotifier 默认的日志告警通知器（控制台/日志文件输出）
type LoggerAlertNotifier struct{}

func NewLoggerAlertNotifier() *LoggerAlertNotifier {
	return &LoggerAlertNotifier{}
}

func (n *LoggerAlertNotifier) Notify(ctx context.Context, title string, content string) error {
	logger.Errorf("[DLQ-ALERT] %s: %s", title, content)
	return nil
}

// DLQHandler 定义死信处理接口
type DLQHandler interface {
	Handle(ctx context.Context, msg *nats.Msg, errReason error, attempt int) error
}

// NatsDLQHandler 基于 NATS JetStream 的死信队列处理器
type NatsDLQHandler struct {
	conn       *nats.Conn
	dlqSubject string
	notifier   AlertNotifier
}

func NewNatsDLQHandler(conn *nats.Conn, dlqSubject string, notifier AlertNotifier) *NatsDLQHandler {
	if notifier == nil {
		notifier = NewLoggerAlertNotifier()
	}
	return &NatsDLQHandler{
		conn:       conn,
		dlqSubject: dlqSubject,
		notifier:   notifier,
	}
}

func (h *NatsDLQHandler) Handle(ctx context.Context, msg *nats.Msg, errReason error, attempt int) error {
	logger.Errorf("[DLQ] Processing dead letter. Original Subject: %s, Reason: %v, Attempt: %d", msg.Subject, errReason, attempt)

	if h.dlqSubject == "" {
		err := fmt.Errorf("DLQ subject is empty, message cannot be routed to DLQ")
		logger.Error(err.Error())
		return err
	}

	// 1. 创建死信消息，并使用 NATS Header 传递元数据
	dlqMsg := nats.NewMsg(h.dlqSubject)
	dlqMsg.Data = msg.Data
	dlqMsg.Header.Set("x-original-subject", msg.Subject)
	dlqMsg.Header.Set("x-dead-reason", errReason.Error())
	dlqMsg.Header.Set("x-death-time", time.Now().Format(time.RFC3339))
	dlqMsg.Header.Set("x-retry-count", fmt.Sprintf("%d", attempt))

	// 2. 将消息发布到死信 Subject
	if err := h.conn.PublishMsg(dlqMsg); err != nil {
		logger.Errorf("[DLQ] Failed to publish message to DLQ: %v", err)
		return err
	}

	// 3. 异步触发告警通知
	if h.notifier != nil {
		go func() {
			alertTitle := fmt.Sprintf("IMChat Message Persist DLQ Alert [%s]", msg.Subject)
			alertContent := fmt.Sprintf("Error Reason: %v\nAttempt Count: %d\nTime: %s",
				errReason, attempt, time.Now().Format(time.RFC3339))
			if notifyErr := h.notifier.Notify(context.Background(), alertTitle, alertContent); notifyErr != nil {
				logger.Errorf("[DLQ] Failed to send alert: %v", notifyErr)
			}
		}()
	}

	return nil
}
