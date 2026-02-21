package telemetry

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"IM2/pkg/logger"
)

const (
	defaultBufferSize = 1024
)

// ErrorEvent 错误事件
type ErrorEvent struct {
	Component string    // 来源组件 (如 "Router", "PubSub", "ConnectionManager")
	Operation string    // 操作名称 (如 "RegisterUser", "Publish")
	Err       error     // 原始错误
	NodeID    string    // 节点ID
	Timestamp time.Time // 事件时间
}

// ErrorHandler 错误事件处理函数
type ErrorHandler func(event ErrorEvent)

// Bus 遥测事件总线
// 通过缓冲 channel 实现非阻塞的异步事件分发。
// 使用方式: 先 RegisterHandler, 再 Start, 业务中调用 Publish。
type Bus struct {
	nodeID   string
	eventCh  chan ErrorEvent
	handlers []ErrorHandler
	wg       sync.WaitGroup
}

// NewBus 创建遥测总线
// bufferSize 为事件 channel 缓冲大小, 传 0 则使用默认值 1024
func NewBus(nodeID string, bufferSize int) *Bus {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	return &Bus{
		nodeID:  nodeID,
		eventCh: make(chan ErrorEvent, bufferSize),
	}
}

// RegisterHandler 注册错误事件处理器
// 必须在 Start 之前调用
func (b *Bus) RegisterHandler(handler ErrorHandler) {
	b.handlers = append(b.handlers, handler)
}

// Publish 发布错误事件 (非阻塞)
// Component (文件名) 和 Operation (函数名) 通过 runtime.Caller 自动获取
// 如果 channel 已满, 事件将被丢弃并记录警告日志
func (b *Bus) Publish(err error) {
	// 获取调用方文件名和函数名
	component := "unknown"
	operation := "unknown"
	if pc, file, _, ok := runtime.Caller(1); ok {
		component = strings.TrimSuffix(filepath.Base(file), ".go")
		// 获取函数名 (去掉包路径前缀，只保留方法名)
		if fn := runtime.FuncForPC(pc); fn != nil {
			fullName := fn.Name()
			// 取最后一个 "." 之后的部分作为函数/方法名
			if idx := strings.LastIndex(fullName, "."); idx >= 0 {
				operation = fullName[idx+1:]
			} else {
				operation = fullName
			}
		}
	}

	event := ErrorEvent{
		Component: component,
		Operation: operation,
		Err:       err,
		NodeID:    b.nodeID,
		Timestamp: time.Now(),
	}

	select {
	case b.eventCh <- event:
	default:
		logger.Errorf("[TelemetryBus] event channel full, dropping event: component=%s operation=%s err=%v",
			component, operation, err)
	}
}

// Start 启动事件消费协程
// 消费 channel 中的事件并分发给所有已注册的 handler。
// 响应 ctx.Done() 优雅退出。
func (b *Bus) Start(ctx context.Context) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-ctx.Done():
				// 排空剩余事件
				b.drain()
				return
			case event, ok := <-b.eventCh:
				if !ok {
					return
				}
				b.dispatch(event)
			}
		}
	}()
	logger.Infof("[TelemetryBus] started on node %s with %d handler(s)", b.nodeID, len(b.handlers))
}

// Stop 关闭总线, 等待消费协程退出
func (b *Bus) Stop() {
	close(b.eventCh)
	b.wg.Wait()
	logger.Infof("[TelemetryBus] stopped on node %s", b.nodeID)
}

// drain 排空 channel 中的剩余事件
func (b *Bus) drain() {
	for {
		select {
		case event, ok := <-b.eventCh:
			if !ok {
				return
			}
			b.dispatch(event)
		default:
			return
		}
	}
}

// dispatch 将事件分发给所有 handler
func (b *Bus) dispatch(event ErrorEvent) {
	for _, handler := range b.handlers {
		handler(event)
	}
}

// DefaultLogHandler 默认日志处理器
// 使用 logx 输出错误事件详情
func DefaultLogHandler(event ErrorEvent) {
	logger.Errorf("[TelemetryBus] node=%s component=%s operation=%s err=%v",
		event.NodeID, event.Component, event.Operation, event.Err)
}
