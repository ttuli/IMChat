package connection

import (
	"context"
	"errors"
	"sync"
	"time"

	"IM2/internal/common"

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
	SendToUser(ctx context.Context, userID uint64, msg *common.WSMessage) error
	// SendToGroup 发送消息给群组 (并在集群内路由)
	SendToGroup(ctx context.Context, groupID uint64, msg *common.WSMessage) error
	// SendToGroupLocal 仅发送给本地群组成员
	SendToGroupLocal(ctx context.Context, groupID uint64, msg *common.WSMessage) error
	// GetLocalGroupMembers 获取本地群组成员
	GetLocalGroupMembers(ctx context.Context, groupId uint64) []uint64
	// Broadcast 广播消息给多个用户
	Broadcast(ctx context.Context, userIDs []uint64, msg *common.WSMessage) error
	// LocalUserCount 本地用户数量
	LocalUserCount() int
	// GetAllLocalUserIDs 获取所有本地用户ID
	GetAllLocalUserIDs() []uint64
	// SetGroupMemberFetcher 设置获取群成员的方法
	SetGroupMemberFetcher(fetcher func(ctx context.Context, groupID uint64) ([]uint64, error))
	// InvalidateGroupCache 清理群组缓存
	InvalidateGroupCache(groupID uint64)
	// Close 关闭管理器
	Close() error
}

type groupCacheItem struct {
	UserIDs   []uint64
	ExpiresAt time.Time
}

// DefaultManager 默认连接管理器实现
type DefaultManager struct {
	connections sync.Map // map[uint64]*Connection

	groupLock         sync.RWMutex
	groupCache        map[uint64]*groupCacheItem
	groupCacheTTL     time.Duration
	fetchGroupMembers func(ctx context.Context, groupID uint64) ([]uint64, error)

	nodeID string
	router MessageRouter
}

// MessageRouter 消息路由接口(用于跨节点通信)
type MessageRouter interface {
	// RouteMessage 路由消息到目标用户
	RouteMessage(ctx context.Context, targetUserID uint64, msg *common.WSMessage) error
	// RouteGroupMessage 路由群组消息到所有节点
	RouteGroupMessage(ctx context.Context, targetGroupID uint64, msg *common.WSMessage) error
}

// NewDefaultManager 创建默认连接管理器
func NewDefaultManager(nodeID string, router MessageRouter) *DefaultManager {
	return &DefaultManager{
		nodeID:        nodeID,
		router:        router,
		groupCache:    make(map[uint64]*groupCacheItem),
		groupCacheTTL: 5 * time.Minute, // 默认5分钟，可根据需要调整
	}
}

// SetGroupMemberFetcher 设置获取群成员的方法
func (m *DefaultManager) SetGroupMemberFetcher(fetcher func(ctx context.Context, groupID uint64) ([]uint64, error)) {
	m.fetchGroupMembers = fetcher
}

// InvalidateGroupCache 清理群组缓存
func (m *DefaultManager) InvalidateGroupCache(groupID uint64) {
	m.groupLock.Lock()
	delete(m.groupCache, groupID)
	m.groupLock.Unlock()
	logx.Infof("[ConnectionManager] Group %d cache invalidated", groupID)
}

func (m *DefaultManager) getGroupMembersWithCache(ctx context.Context, groupID uint64) ([]uint64, error) {
	m.groupLock.RLock()
	item, ok := m.groupCache[groupID]
	m.groupLock.RUnlock()

	if ok && time.Now().Before(item.ExpiresAt) {
		return item.UserIDs, nil
	}

	if m.fetchGroupMembers == nil {
		return nil, errors.New("group member fetcher not configured")
	}

	// 触发外部调用获取成员 (RPC -> Redis/MySQL)
	userIDs, err := m.fetchGroupMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	m.groupLock.Lock()
	m.groupCache[groupID] = &groupCacheItem{
		UserIDs:   userIDs,
		ExpiresAt: time.Now().Add(m.groupCacheTTL),
	}
	m.groupLock.Unlock()
	logx.Infof("[ConnectionManager] Group %d cached %d members", groupID, len(userIDs))

	return userIDs, nil
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

// RemoveConnection 移除连接
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

// SendToUser 发送消息给用户
func (m *DefaultManager) SendToUser(ctx context.Context, userID uint64, msg *common.WSMessage) error {
	// 先尝试本地发送
	if conn, ok := m.GetLocalConnection(userID); ok {
		if err := conn.Send(msg); err != nil {
			return err
		}
		return nil
	}

	// 本地没有连接，通过路由器转发
	if m.router != nil {
		if err := m.router.RouteMessage(ctx, userID, msg); err != nil {
			return err
		}
		return nil
	}

	return errors.New("user not connected")
}

func (m *DefaultManager) SendToGroup(ctx context.Context, groupID uint64, msg *common.WSMessage) error {
	// 总是广播群消息到其他节点
	if m.router != nil {
		return m.router.RouteGroupMessage(ctx, groupID, msg)
	}
	return errors.New("group message routing failed: router not configured")
}

// SendToGroupLocal 仅发送给本地群组成员(用于处理来自其他节点的广播消息，避免循环路由)
func (m *DefaultManager) SendToGroupLocal(ctx context.Context, groupID uint64, msg *common.WSMessage) error {
	userIDs, err := m.getGroupMembersWithCache(ctx, groupID)
	if err != nil {
		logx.Errorf("[ConnectionManager] failed to get group %d members: %v", groupID, err)
		return err
	}

	for _, uid := range userIDs {
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

// GetLocalGroupMembers 获取本地群组成员
func (m *DefaultManager) GetLocalGroupMembers(ctx context.Context, groupId uint64) []uint64 {
	userIDs, err := m.getGroupMembersWithCache(ctx, groupId)
	if err != nil {
		return nil
	}
	var localSubscribers []uint64
	for _, uid := range userIDs {
		if _, ok := m.GetLocalConnection(uid); ok {
			localSubscribers = append(localSubscribers, uid)
		}
	}
	return localSubscribers
}

// Broadcast 广播消息给多个用户
func (m *DefaultManager) Broadcast(ctx context.Context, userIDs []uint64, msg *common.WSMessage) error {
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
