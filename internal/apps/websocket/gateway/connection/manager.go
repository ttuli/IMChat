package connection

import (
	"context"
	"errors"
	"sync"

	"IM2/internal/apps/websocket/gateway/router"

	"IM2/pkg/proto/transport"

	"github.com/zeromicro/go-zero/core/logx"
)

// Manager 连接管理器接口
type Manager interface {
	// AddConnection 添加连接
	AddConnection(userID uint64, conn *Connection) error
	// RemoveConnection 移除连接（只有当 map 中存的连接指针与 conn 相同时才删除，防止新连接被旧连接的 defer 误删）
	RemoveConnection(userID uint64, conn *Connection) error
	// GetLocalConnection 获取本地连接
	GetLocalConnection(userID uint64) (*Connection, bool)
	// SendToUser 发送消息给用户
	SendToUser(ctx context.Context, userID uint64, msg *transport.WSMessage) error
	// SendToGroup 发送消息给群组 (并在集群内路由)
	SendToGroup(ctx context.Context, groupID uint64, msg *transport.WSMessage) error
	// SendToGroupLocal 仅发送给本地群组成员
	SendToGroupLocal(ctx context.Context, groupID uint64, msg *transport.WSMessage) error
	// Broadcast 广播消息给多个用户
	Broadcast(ctx context.Context, userIDs []uint64, msg *transport.WSMessage) error
	// LocalUserCount 本地用户数量
	LocalUserCount() int
	// GetAllLocalUserIDs 获取所有本地用户ID
	GetAllLocalUserIDs() []uint64
	// InvalidateGroupCache 清理群组缓存
	InvalidateGroupCache(groupID uint64)
	// AddUsersToGroup 将指定用户加入本地群成员映射
	AddUsersToGroup(groupID uint64, userIDs []uint64)
	// RemoveUsersFromGroup 将指定用户从本地群成员映射中移除
	RemoveUsersFromGroup(groupID uint64, userIDs []uint64)

	// Close 关闭管理器
	Close() error
}

// DefaultManager 默认连接管理器实现
type DefaultManager struct {
	connections sync.Map // map[uint64]*Connection

	groupLock sync.RWMutex
	// groupMembers: groupID → set of userIDs (在线用户)
	groupMembers map[uint64]map[uint64]struct{}
	// userGroups: userID → set of groupIDs (该用户属于哪些群)
	userGroups map[uint64][]uint64

	nodeID string
	msgRouter MessageRouter
}

// MessageRouter 消息路由接口(用于跨节点通信)
type MessageRouter interface {
	// RouteMessage 路由消息到目标用户
	RouteMessage(ctx context.Context, targetUserID uint64, msg *transport.WSMessage) error
	// BroadcastToAllNodes 广播消息到所有节点（BroadcastAll: 所有节点消费; BroadcastQueue: 仅一个节点消费）
	BroadcastToAllNodes(ctx context.Context, msg *transport.WSMessage, mode router.BroadcastMode) error
}

// NewDefaultManager 创建默认连接管理器
func NewDefaultManager(nodeID string, msgRouter MessageRouter) *DefaultManager {
	return &DefaultManager{
		nodeID:       nodeID,
		msgRouter:    msgRouter,
		groupMembers: make(map[uint64]map[uint64]struct{}),
		userGroups:   make(map[uint64][]uint64),
	}
}

// InvalidateGroupCache 清理群组缓存
func (m *DefaultManager) InvalidateGroupCache(groupID uint64) {
	m.groupLock.Lock()
	delete(m.groupMembers, groupID)
	m.groupLock.Unlock()
	logx.Infof("[ConnectionManager] Group %d cache invalidated", groupID)
}

// AddUsersToGroup 将指定用户加入本地群成员映射（仅跟踪当前在线连接的用户）
func (m *DefaultManager) AddUsersToGroup(groupID uint64, userIDs []uint64) {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()
	if m.groupMembers[groupID] == nil {
		m.groupMembers[groupID] = make(map[uint64]struct{})
	}
	for _, uid := range userIDs {
		// 只维护本地有连接的用户
		if _, ok := m.connections.Load(uid); ok {
			m.groupMembers[groupID][uid] = struct{}{}
			m.userGroups[uid] = append(m.userGroups[uid], groupID)
		}
	}
	logx.Infof("[ConnectionManager] AddUsersToGroup: group %d +%d users", groupID, len(userIDs))
}

// RemoveUsersFromGroup 将指定用户从本地群成员映射中移除
func (m *DefaultManager) RemoveUsersFromGroup(groupID uint64, userIDs []uint64) {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()
	members := m.groupMembers[groupID]
	for _, uid := range userIDs {
		if members != nil {
			delete(members, uid)
		}
		// 同步从 userGroups 摘除
		groups := m.userGroups[uid]
		for i, gid := range groups {
			if gid == groupID {
				m.userGroups[uid] = append(groups[:i], groups[i+1:]...)
				break
			}
		}
	}
	logx.Infof("[ConnectionManager] RemoveUsersFromGroup: group %d -%d users", groupID, len(userIDs))
}

// AddConnection 添加连接，并预热该用户所在群的成员集合
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

// RemoveConnection 移除连接，并将用户从所有群成员集合中摘除
// 只有当 map 中存储的连接指针与 conn 相同时才删除，防止旧连接 defer 误删新连接。
func (m *DefaultManager) RemoveConnection(userID uint64, conn *Connection) error {
	// CompareAndDelete: 原子地比较并删除，确保只删自己
	if m.connections.CompareAndDelete(userID, conn) {
		logx.Infof("[Connection] user %d removed", userID)
		conn.Close()

		// 从群成员集合中摘除该用户
		m.groupLock.Lock()
		for _, gid := range m.userGroups[userID] {
			if members, ok := m.groupMembers[gid]; ok {
				delete(members, userID)
			}
		}
		delete(m.userGroups, userID)
		m.groupLock.Unlock()
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

// SendToUser 发送消息给用户
func (m *DefaultManager) SendToUser(ctx context.Context, userID uint64, msg *transport.WSMessage) error {
	// 先尝试本地发送
	if conn, ok := m.GetLocalConnection(userID); ok {
		if err := conn.Send(msg); err != nil {
			return err
		}
		return nil
	}

	// 本地没有连接，通过路由器转发
	if m.msgRouter != nil {
		if err := m.msgRouter.RouteMessage(ctx, userID, msg); err != nil {
			return err
		}
		return nil
	}

	return errors.New("user not connected")
}

func (m *DefaultManager) SendToGroup(ctx context.Context, groupID uint64, msg *transport.WSMessage) error {
	// 总是广播群消息到其他节点
	if m.msgRouter != nil {
		return m.msgRouter.BroadcastToAllNodes(ctx, msg, router.BroadcastAll)
	}
	return errors.New("group message routing failed: router not configured")
}

// SendToGroupLocal 仅发送给本地群组成员(用于处理来自其他节点的广播消息，避免循环路由)
func (m *DefaultManager) SendToGroupLocal(ctx context.Context, groupID uint64, msg *transport.WSMessage) error {
	m.groupLock.RLock()
	members := m.groupMembers[groupID]
	var localIDs []uint64
	for uid := range members {
		if _, ok := m.connections.Load(uid); ok {
			localIDs = append(localIDs, uid)
		}
	}
	m.groupLock.RUnlock()

	for _, uid := range localIDs {
		if uid == msg.SenderId {
			continue
		}
		if conn, ok := m.GetLocalConnection(uid); ok {
			if err := conn.Send(msg); err != nil {
				logx.Errorf("[ConnectionManager] SendToGroupLocal to user %d failed: %v", uid, err)
			}
		}
	}
	return nil
}

// Broadcast 广播消息给多个用户
func (m *DefaultManager) Broadcast(ctx context.Context, userIDs []uint64, msg *transport.WSMessage) error {
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
