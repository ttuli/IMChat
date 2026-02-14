package connection

import (
	"context"
	"sync"

	"IM2/internal/apps/websocket/gateway/internal/protocol"
	"IM2/pkg/xerr"
)

// Manager 连接管理器接口
type Manager interface {
	// AddConnection 添加连接
	AddConnection(userID uint64, conn *Connection) error
	// RemoveConnection 移除连接
	RemoveConnection(userID uint64) error
	// GetLocalConnection 获取本地连接
	GetLocalConnection(userID uint64) (*Connection, bool)
	// SendToUser 发送消息给用户
	SendToUser(ctx context.Context, userID uint64, msg *protocol.Message) error
	// Broadcast 广播消息给多个用户
	Broadcast(ctx context.Context, userIDs []uint64, msg *protocol.Message) error
	// LocalUserCount 本地用户数量
	LocalUserCount() int
	// GetAllLocalUserIDs 获取所有本地用户ID
	GetAllLocalUserIDs() []uint64
	// Close 关闭管理器
	Close() error
}

// DefaultManager 默认连接管理器实现
type DefaultManager struct {
	connections sync.Map // map[uint64]*Connection
	nodeID      string
	router      MessageRouter
}

// MessageRouter 消息路由接口(用于跨节点通信)
type MessageRouter interface {
	// RouteMessage 路由消息到目标用户
	RouteMessage(ctx context.Context, targetUserID uint64, msg *protocol.Message) error
}

// NewDefaultManager 创建默认连接管理器
func NewDefaultManager(nodeID string, router MessageRouter) *DefaultManager {
	return &DefaultManager{
		nodeID: nodeID,
		router: router,
	}
}

// AddConnection 添加连接
func (m *DefaultManager) AddConnection(userID uint64, conn *Connection) error {
	// 如果已存在旧连接，先关闭
	if old, loaded := m.connections.LoadAndDelete(userID); loaded {
		if oldConn, ok := old.(*Connection); ok {
			oldConn.Close()
		}
	}

	m.connections.Store(userID, conn)
	return nil
}

// RemoveConnection 移除连接
func (m *DefaultManager) RemoveConnection(userID uint64) error {
	if conn, loaded := m.connections.LoadAndDelete(userID); loaded {
		if c, ok := conn.(*Connection); ok {
			c.Close()
		}
	}
	return nil
}

// GetLocalConnection 获取本地连接
func (m *DefaultManager) GetLocalConnection(userID uint64) (*Connection, bool) {
	if conn, ok := m.connections.Load(userID); ok {
		return conn.(*Connection), true
	}
	return nil, false
}

// SendToUser 发送消息给用户
func (m *DefaultManager) SendToUser(ctx context.Context, userID uint64, msg *protocol.Message) error {
	// 先尝试本地发送
	if conn, ok := m.GetLocalConnection(userID); ok {
		return conn.Send(msg)
	}

	// 本地没有连接，通过路由器转发
	if m.router != nil {
		return m.router.RouteMessage(ctx, userID, msg)
	}

	return xerr.New(xerr.ErrInternalServer, "user not connected")
}

// Broadcast 广播消息给多个用户
func (m *DefaultManager) Broadcast(ctx context.Context, userIDs []uint64, msg *protocol.Message) error {
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	for _, userID := range userIDs {
		wg.Add(1)
		go func(uid uint64) {
			defer wg.Done()
			if err := m.SendToUser(ctx, uid, msg); err != nil {
				errOnce.Do(func() {
					firstErr = err
				})
			}
		}(userID)
	}

	wg.Wait()
	return firstErr
}

// LocalUserCount 本地用户数量
func (m *DefaultManager) LocalUserCount() int {
	count := 0
	m.connections.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// GetAllLocalUserIDs 获取所有本地用户ID
func (m *DefaultManager) GetAllLocalUserIDs() []uint64 {
	var userIDs []uint64
	m.connections.Range(func(key, _ any) bool {
		if uid, ok := key.(uint64); ok {
			userIDs = append(userIDs, uid)
		}
		return true
	})
	return userIDs
}

// Close 关闭管理器，断开所有连接
func (m *DefaultManager) Close() error {
	m.connections.Range(func(key, value any) bool {
		if conn, ok := value.(*Connection); ok {
			conn.Close()
		}
		m.connections.Delete(key)
		return true
	})
	return nil
}
