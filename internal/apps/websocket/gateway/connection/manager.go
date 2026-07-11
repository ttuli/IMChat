package connection

import (
	"context"
	"sync"

	"IM2/pkg/proto/transport"

	"github.com/zeromicro/go-zero/core/logx"
)

// Manager 连接管理器接口。
// 只维护「本节点」的连接；用户→节点、群→成员等路由数据统一存放在
// Redis 路由表（pkg/routing）中，群成员在投递时由调用方查表后传入。
type Manager interface {
	// AddConnection 添加连接
	AddConnection(userID uint64, conn *Connection) error
	// RemoveConnection 移除连接（只有当 map 中存的连接指针与 conn 相同时才删除，防止新连接被旧连接的 defer 误删）
	RemoveConnection(userID uint64, conn *Connection) error
	// GetLocalConnection 获取本地连接
	GetLocalConnection(userID uint64) (*Connection, bool)
	// SendToUsersLocal 将消息投递给指定用户中持有本地连接者（跳过发送者本人）
	SendToUsersLocal(ctx context.Context, userIDs []uint64, msg *transport.WSMessage)
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

	nodeID string
}

// NewDefaultManager 创建默认连接管理器
func NewDefaultManager(nodeID string) *DefaultManager {
	return &DefaultManager{
		nodeID: nodeID,
	}
}

// AddConnection 添加连接
func (m *DefaultManager) AddConnection(userID uint64, conn *Connection) error {
	// 如果已存在旧连接，先关闭
	if old, loaded := m.connections.LoadAndDelete(userID); loaded {
		if oldConn, ok := old.(*Connection); ok {
			oldConn.Kick("账号在其他设备登录")
		}
	}
	logx.Infof("[Connection] user %d added", userID)
	m.connections.Store(userID, conn)

	return nil
}

// RemoveConnection 移除连接。
// 只有当 map 中存储的连接指针与 conn 相同时才删除，防止旧连接 defer 误删新连接。
func (m *DefaultManager) RemoveConnection(userID uint64, conn *Connection) error {
	// CompareAndDelete: 原子地比较并删除，确保只删自己
	if m.connections.CompareAndDelete(userID, conn) {
		logx.Infof("[Connection] user %d removed", userID)
		conn.Close()
	}
	return nil
}

// GetLocalConnection 获取本地连接
func (m *DefaultManager) GetLocalConnection(userID uint64) (*Connection, bool) {
	if conn, ok := m.connections.Load(userID); ok {
		logx.Infof("[Connection] user %d found", userID)
		return conn.(*Connection), true
	}
	return nil, false
}

// SendToUsersLocal 将消息投递给指定用户中持有本地连接者（跳过发送者本人）。
// 用于处理来自其他节点的群广播消息：成员列表由调用方查路由表获得，
// 本方法只做「本地连接过滤 + 投递」，不做跨节点转发，避免循环路由。
func (m *DefaultManager) SendToUsersLocal(ctx context.Context, userIDs []uint64, msg *transport.WSMessage) {
	for _, uid := range userIDs {
		if uid == msg.SenderId {
			continue
		}
		if conn, ok := m.GetLocalConnection(uid); ok {
			if err := conn.Send(msg); err != nil {
				logx.Errorf("[ConnectionManager] SendToUsersLocal to user %d failed: %v", uid, err)
			}
		}
	}
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
