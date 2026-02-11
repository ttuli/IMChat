# WebSocket 架构设计文档

## Architecture Diagram / 架构图

```
                            ┌──────────────────────────────────────────────────────────────┐
                            │                    客户端层 (Clients)                         │
                            │   ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
                            │   │ Client1 │    │ Client2 │    │ ClientN │                  │
                            │   └────┬────┘    └────┬────┘    └────┬────┘                  │
                            └────────┼──────────────┼──────────────┼───────────────────────┘
                                     │              │              │
                                     └──────────────┼──────────────┘
                                                    ▼
                            ┌──────────────────────────────────────────────────────────────┐
                            │              负载均衡层 (Load Balancer)                       │
                            │         ┌────────────────────────────────┐                   │
                            │         │    Nginx / HAProxy             │                   │
                            │         │    (Sticky Session by UserID)  │                   │
                            │         └────────────────────────────────┘                   │
                            └────────────────────────┬─────────────────────────────────────┘
                                                     │
                     ┌───────────────────────────────┼───────────────────────────────┐
                     ▼                               ▼                               ▼
┌────────────────────────────────┐ ┌────────────────────────────────┐ ┌────────────────────────────────┐
│     WebSocket Node 1           │ │     WebSocket Node 2           │ │     WebSocket Node N           │
│  ┌──────────────────────────┐  │ │  ┌──────────────────────────┐  │ │  ┌──────────────────────────┐  │
│  │    Connection Manager    │  │ │  │    Connection Manager    │  │ │  │    Connection Manager    │  │
│  │  map[userID]*Connection  │  │ │  │  map[userID]*Connection  │  │ │  │  map[userID]*Connection  │  │
│  └──────────────────────────┘  │ │  └──────────────────────────┘  │ │  └──────────────────────────┘  │
│  ┌──────────────────────────┐  │ │  ┌──────────────────────────┐  │ │  ┌──────────────────────────┐  │
│  │    Message Handler       │  │ │  │    Message Handler       │  │ │  │    Message Handler       │  │
│  └──────────────────────────┘  │ │  └──────────────────────────┘  │ │  └──────────────────────────┘  │
└────────────────┬───────────────┘ └────────────────┬───────────────┘ └────────────────┬───────────────┘
                 │                                  │                                  │
                 └──────────────────────────────────┼──────────────────────────────────┘
                                                    ▼
                            ┌──────────────────────────────────────────────────────────────┐
                            │                消息总线层 (Message Bus)                       │
                            │         ┌────────────────────────────────┐                   │
                            │         │       Redis Pub/Sub            │                   │
                            │         │   (跨节点消息广播 / 路由表)      │                   │
                            │         └────────────────────────────────┘                   │
                            └────────────────────────┬─────────────────────────────────────┘
                                                     │
                     ┌───────────────────────────────┼───────────────────────────────┐
                     ▼                               ▼                               ▼
              ┌──────────────┐              ┌──────────────┐              ┌──────────────┐
              │   Auth RPC   │              │   User RPC   │              │  Message RPC │
              └──────┬───────┘              └──────┬───────┘              └──────┬───────┘
                     │                             │                             │
                     └─────────────────────────────┼─────────────────────────────┘
                                                   ▼
                            ┌──────────────────────────────────────────────────────────────┐
                            │                  持久化层 (Storage)                          │
                            │      ┌─────────────────┐    ┌─────────────────┐              │
                            │      │     MySQL       │    │   Redis Cache   │              │
                            │      │   (数据持久化)   │    │   (缓存/会话)    │              │
                            │      └─────────────────┘    └─────────────────┘              │
                            └──────────────────────────────────────────────────────────────┘
```

---

## 水平扩展设计 (Horizontal Scaling)

### 核心组件

| 组件 | 职责 | 扩展方式 |
|------|------|----------|
| WebSocket Node | 维护用户长连接，处理消息收发 | 无状态水平扩展 |
| Redis Pub/Sub | 跨节点消息广播 | Redis Cluster |
| Load Balancer | 连接分发，Sticky Session | 主备/集群 |

### 连接路由策略

```
用户连接时:
  1. LB 根据 UserID 哈希选择目标节点
  2. WebSocket Node 注册连接: Redis SET ws:route:{userID} = nodeID
  
发送消息时:
  1. 查询目标用户节点: Redis GET ws:route:{targetUserID}
  2. 如果在本节点 → 直接推送
  3. 如果在其他节点 → Redis PUBLISH ws:channel:{targetNodeID}
  
用户断开时:
  1. 清理本地连接池
  2. 删除路由: Redis DEL ws:route:{userID}
```

### Redis Key 设计

```
ws:route:{userID}     → nodeID          # 用户路由表
ws:node:{nodeID}      → {host, port}    # 节点信息 (TTL 60s, 心跳续期)
ws:channel:{nodeID}   → Pub/Sub Channel # 节点消息通道
```

---

## 消息流程图 (Message Flow)

```
                跨节点消息投递流程
                
User A (Node 1)                              User B (Node 2)
     │                                            ▲
     │ 1. 发送消息给 User B                        │ 6. WebSocket 推送
     ▼                                            │
┌─────────────┐                             ┌─────────────┐
│ WS Node 1   │                             │ WS Node 2   │
│             │                             │             │
│ 2. 查询路由  │                             │ 5. 接收消息  │
└──────┬──────┘                             └──────▲──────┘
       │                                          │
       │ GET ws:route:userB                       │ 
       ▼                                          │
┌──────────────────────────────────────────────────────────┐
│                                                          │
│                     Redis                                │
│                                                          │
│  3. 返回 nodeID = Node2                                  │
│  4. PUBLISH ws:channel:node2 → 消息内容                   │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## 目录结构 (Directory Structure)

```
websocket/
├── architecture.md           ← 本文档
├── cmd.sh                    # 启动脚本
└── gateway/
    ├── config/
    │   └── config.go         # 配置结构
    ├── etc/
    │   └── gateway.yaml      # 配置文件
    ├── handler/
    │   ├── ws_handler.go     # WebSocket 连接处理
    │   └── message_handler.go
    ├── internal/
    │   ├── connection/       # 连接管理
    │   ├── pubsub/           # Redis Pub/Sub
    │   ├── router/           # 消息路由
    │   └── protocol/         # 协议定义
    ├── svc/
    │   └── servicecontext.go
    └── main.go
```

---

## 关键接口设计, (Key Interfaces)

### ConnectionManager

```go
type ConnectionManager interface {
    AddConnection(userID uint64, conn *Connection) error
    RemoveConnection(userID uint64) error
    GetLocalConnection(userID uint64) (*Connection, bool)
    SendToUser(ctx context.Context, userID uint64, msg *Message) error
    Broadcast(ctx context.Context, userIDs []uint64, msg *Message) error
}
```

### PubSub

```go
type Publisher interface {
    PublishToNode(ctx context.Context, nodeID string, msg *Message) error
}

type Subscriber interface {
    Subscribe(ctx context.Context, nodeID string, handler MessageHandler) error
}
```
