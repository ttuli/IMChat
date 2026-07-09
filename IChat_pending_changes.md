# IMChat 待实现改动清单

> 生成日期：2026-07-09  
> 关联文档：`IMChat_architecture_review.md`（问题清单）

---

## 目录

1. [SendToGroup 本地投递优先](#1-sendtogroup-本地投递优先)
2. [未读计数显式化](#2-未读计数显式化)
3. [SyncSessions 聚合接口](#3-syncsessions-聚合接口)
4. [WS 建连时推送未读摘要](#4-ws-建连时推送未读摘要)
5. [配置管理精简](#5-配置管理精简)
6. [API 限流与熔断保护](#6-api-限流与熔断保护)
7. [实施路线图](#7-实施路线图)

---

## 1. SendToGroup 本地投递优先

### 问题

当前 `SendToGroup`（`connection/manager.go:189`）只执行 `BroadcastToAllNodes`，不投递本地连接。本节点上的群消息发送方和接收方即使连接在同一个 Gateway，消息也必须绕行 NATS 一圈（Publish → Subscribe → `handleSubscribeMessage` → `SendToGroupLocal`），额外增加 1-5ms 的本地网络往返时延。

### 改动范围

- `internal/apps/websocket/gateway/connection/manager.go`
- `internal/apps/websocket/gateway/router/publisher.go`（可选：添加排除本节点的广播模式）

### 设计方案

在 `SendToGroup` 方法中增加本地优先投递逻辑，再通过 NATS 广播到其他节点。广播消息中携带 `exclude_node` 字段，防止本节点收到广播后对相同用户重复投递。

#### 方案 A（推荐）：先本地投递，再广播时透传排除标记

不修改 NATS Subject 和 proto，直接在广播的 WSMessage 中利用已有字段表达"本节点已处理"的语义——由于 `handleSubscribeMessage` 收到群消息时调用 `SendToGroupLocal`，而 `SendToGroupLocal` 内部会跳过 `msg.SenderId`（`manager.go:210`），所以如果仅需排除发送方，当前逻辑已经正确。问题在于发送方以外的**其他本地群成员**会被投递两次。

解决方式：引入一个内部标记。最简单的做法是在 `SendToGroup` 先调用 `SendToGroupLocal`，然后广播时携带一个特殊标记让本节点跳过第二次投递。

```go
// manager.go SendToGroup 改造
func (m *DefaultManager) SendToGroup(ctx context.Context, groupID uint64, msg *transport.WSMessage) error {
    // 1. 先本地投递（排除发送方，逻辑在 SendToGroupLocal 内部处理）
    m.SendToGroupLocal(ctx, groupID, msg)

    // 2. 再广播到其他节点。广播时标记发送方所在节点已处理，
    //    避免本节点收到广播后重复投递。
    if m.msgRouter != nil {
        // 利用现有的 MsgId 字段携带排除节点信息，
        // 或者在 WSMessage 中增加一个 repeated bytes 的 internal_hints 字段
        return m.msgRouter.BroadcastToAllNodes(ctx, msg, router.BroadcastAll)
    }
    return errors.New("group message routing failed: router not configured")
}
```

由于 `handleSubscribeMessage` 收到广播消息后调用 `SendToGroupLocal`，而 `SendToGroupLocal` 只做本地投递，节点收到自己发出的广播后会对本地成员再投递一次。需要在广播消息中标记"本节点已处理"。

**具体做法**：在 `handleSubscribeMessage` 的 TargetType_GROUP 分支中，收到消息后先判断 `msg.SenderId` 是否为 0 或判断消息来源——如果消息的原始发送方就在本节点，说明本节点是消息的入站 Gateway。可以在 dispatch 阶段记录消息来源节点，但更简单的方式是：**在 WSMessage proto 中新增一个 `exclude_node_id` 字段**，`SendToGroup` 在本地投递完成后设置该字段为本节点 ID，然后广播。`handleSubscribeMessage` 收到群消息时检查 `exclude_node_id`，如果等于本节点 ID 则跳过本地投递（因为已在 `SendToGroup` 中投递过了）。

```protobuf
// transport.proto WSMessage 新增字段
message WSMessage {
    // ... 现有字段 ...
    string exclude_node_id = 11;  // 排除投递的节点 ID（群消息本地已投递时设置）
}
```

**改动清单**：

1. `transport.proto`：WSMessage 增加 `exclude_node_id` 字段，重新生成 pb.go
2. `connection/manager.go` `SendToGroup`：先 `SendToGroupLocal`，再设置 `exclude_node_id = m.nodeID`，再广播
3. `server/server.go` `handleSubscribeMessage` TargetType_GROUP 分支：收到消息后检查 `exclude_node_id`，等于本节点 ID 则跳过

#### 方案 B（备选）：双 Subject 广播模式

群消息走两个 Subject：
- `BroadcastSubject`：普通广播，所有节点都收到
- 不修改 Subject，而是让每个节点在收到群广播时检查 sender node

这个方案不需要 proto 变更，但逻辑更隐晦。推荐方案 A。

### 预期收益

- 同一 Gateway 节点上的群成员接收消息延迟降低 1-5ms（省去 NATS 往返）
- 集群内 NATS 流量轻微下降（每条群消息少一次本节点的订阅回调触发）

---

## 2. 未读计数显式化

### 问题

Lamport seq 上线后，seq 值不再连续，之前 `未读数 = actual_seq - last_read_seq` 的公式完全失效。例如：

```
会话最近消息 seq: [1710432000001-3-0, 1710432000001-5-1, 1710432000007-3-0]
last_read_seq = 1710432000001-5-1
actual_seq    = 1710432000007-3-0

实际未读 = 1 条（最后那条）
无法用减法得出
```

### 改动范围

- `internal/model/session.go` — UserSession 新增 `UnreadCount` 字段
- `docker/mysql/init-imchat.sql` — DDL：ALTER TABLE user_session ADD COLUMN
- `internal/apps/Message/rpc/internal/service/message.go` — PersistMessage 增加 HINCRBY
- `internal/apps/Message/rpc/internal/dao/seq_syncer.go` — 刷盘时同步 unread_count
- `internal/apps/Message/rpc/internal/service/session.go` — MarkRead 接口
- `internal/apps/Message/rpc/message.proto` — 新增 MarkRead RPC
- 客户端 SDK — 未读数改用 `unread_count` 字段

### 数据模型变更

```sql
-- MySQL DDL
ALTER TABLE user_session ADD COLUMN unread_count INT UNSIGNED NOT NULL DEFAULT 0
    COMMENT '显式未读计数，Lamport seq 下不再通过 seq 减法计算'
    AFTER last_read_seq;
```

```go
// model/session.go UserSession 新增字段
type UserSession struct {
    // ... 现有字段 ...
    LastReadSeq uint64    `gorm:"column:last_read_seq;..."`
    UnreadCount int       `gorm:"column:unread_count;type:int unsigned;not null;default:0;comment:显式未读计数"`
    // ...
}
```

### 未读计数递增

在 `PersistMessage` 成功落库后，对该会话的所有成员（除发送者外）执行 HINCRBY。使用 Redis Pipeline 批量发送，单次网络往返完成 N 个 HINCRBY。

```go
// service/message.go PersistMessage 成功后新增
func (s *MessageService) incrementUnread(
    ctx context.Context, sessionID string, senderID uint64, sessionType int8,
) error {
    var memberIDs []uint64
    if sessionType == model.SessionTypeGroup {
        // 群聊：查 group_member 表获取所有成员
        var err error
        memberIDs, err = s.svcCtx.SessionDAO.GetGroupMemberIDs(ctx, sessionID)
        if err != nil {
            return err
        }
    } else {
        // 单聊：查 session_key 解析双方
        session, err := s.svcCtx.SessionDAO.FindBySessionID(ctx, sessionID)
        if err != nil {
            return err
        }
        memberIDs = util.ParsePrivateChatMembers(session.SessionKey)
    }

    key := fmt.Sprintf("session:unread:%s", sessionID)
    return s.svcCtx.Redis.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
        for _, uid := range memberIDs {
            if uid == senderID {
                continue
            }
            pipe.HIncrBy(ctx, key, strconv.FormatUint(uid, 10), 1)
        }
        pipe.Expire(ctx, key, 7*24*time.Hour) // 7 天 TTL
        return nil
    })
}
```

### 未读计数清零（MarkRead）

用户打开会话或滑动到最新消息后，客户端上报已读游标。服务端推进 `last_read_seq` 并将对应 field 的未读计数置零。

```go
// service/session.go 新增方法
func (s *MessageService) MarkRead(
    ctx context.Context, userID uint64, sessionID string, readUpTo uint64,
) error {
    // 1. 更新 last_read_seq
    if err := s.svcCtx.SessionDAO.UpdateLastReadSeq(ctx, userID, sessionID, readUpTo); err != nil {
        return err
    }

    // 2. 清零 Redis 未读计数
    key := fmt.Sprintf("session:unread:%s", sessionID)
    field := strconv.FormatUint(userID, 10)
    return s.svcCtx.Redis.HSetCtx(ctx, key, field, "0")
}
```

### SeqSyncer 刷盘同步

SeqSyncer 在定时批量刷盘时，顺带将 `session:unread:{sessionID}` Hash 中的值同步到 MySQL `user_session.unread_count`。不需要新增定时任务，复用已有的 SeqSyncer 机制。

```go
// seq_syncer.go batchFlush 新增步骤
func (s *SeqSyncer) batchFlush(latest map[string]seqUpdate) {
    // ... 现有 MySQL upsert + Redis timeline 逻辑 ...

    // 3. 批量同步 unread_count（新增）
    for convID := range latest {
        s.syncUnreadCount(ctx, convID)
    }
}

func (s *SeqSyncer) syncUnreadCount(ctx context.Context, sessionID string) {
    key := fmt.Sprintf("session:unread:%s", sessionID)
    fields, err := s.cache.HGetAllCtx(ctx, key)
    if err != nil || len(fields) == 0 {
        return
    }
    for uidStr, countStr := range fields {
        uid, _ := strconv.ParseUint(uidStr, 10, 64)
        count, _ := strconv.Atoi(countStr)
        s.db.WithContext(ctx).Model(&model.UserSession{}).
            Where("user_id = ? AND session_id = ?", uid, sessionID).
            Update("unread_count", count)
    }
}
```

### 上线同步的查询适配

客户端查询会话列表时，`unread_count > 0` 即可判断有未读消息，不再需要拿到 `actual_seq` 做减法。

```sql
-- 当前：拿到 last_read_seq 然后算 actual_seq - last_read_seq
-- 改进后：unread_count 已经是最终值

SELECT us.session_id, s.session_key, s.type, s.last_content, s.last_sender,
       us.last_read_seq, us.unread_count, us.is_top, us.is_disturb
FROM user_session us
JOIN session s ON s.session_id = us.session_id
WHERE us.user_id = ?
ORDER BY us.update_time DESC
```

### 预期收益

- Lamport seq 下未读计数正确可算
- 客户端不再需要额外请求 `actual_seq` 做减法
- 未读数由服务端统一维护，客户端展示一致

---

## 3. SyncSessions 聚合接口

### 问题

当前上线后获取会话列表需要多次请求：`GetUserSessions`（拿 user_session 列表）→ `GetSession` × N（拿每个会话的 actual_seq 和 last_content）→ 客户端计算未读数 → `GetHistory` × M（拉未读消息）。20 个会话 = 至少 21 次 HTTP 请求。

### 改动范围

- `internal/apps/Message/api/types/message_api.proto` — 新增 SyncSessions RPC
- `internal/apps/Message/api/handler/` — 新增 handler
- `internal/apps/Message/api/internal/logic/message/` — 新增 logic
- `internal/apps/Message/rpc/message.proto` — 新增 SyncSessions RPC
- `internal/apps/Message/rpc/internal/logic/messagerpc/` — 新增 logic
- `internal/apps/Message/rpc/internal/service/session.go` — 新增 SyncSessions 方法
- `internal/apps/Message/rpc/internal/dao/session.go` — 新增批量查询 DAO 方法

### Proto 定义

#### Message API 层

```protobuf
// message_api.proto 新增
message SyncSessionsReq {
    uint64 user_id  = 1;  // 当前用户 ID（从 JWT 提取）
}

message SyncSessionsResp {
    repeated SessionItem sessions = 1;
}

message SessionItem {
    string session_id       = 1;   // 服务端持久化 ID
    string session_key      = 2;   // 本地会话 Key
    int32  session_type     = 3;   // 1=单聊 2=群聊
    uint64 last_read_seq    = 4;   // 用户当前已读游标
    int32  unread_count     = 5;   // 未读计数（显式维护）
    string last_content     = 6;   // 最后一条消息摘要
    uint64 last_sender      = 7;   // 最后发送者 ID
    int64  update_time      = 8;   // 最后更新时间
    int32  is_top           = 9;   // 是否置顶
    int32  is_disturb       = 10;  // 是否免打扰
}
```

#### Message RPC 层

```protobuf
// message.proto 新增
message SyncSessionsReq {
    uint64 user_id  = 1;
}

message SyncSessionItem {
    string session_id       = 1;
    string session_key      = 2;
    int32  session_type     = 3;
    uint64 last_read_seq    = 4;
    uint64 unread_count     = 5;
    string last_content     = 6;
    uint64 last_sender      = 7;
    int64  update_time      = 8;
    int32  is_top           = 9;
    int32  is_disturb       = 10;
}

message SyncSessionsResp {
    repeated SyncSessionItem items = 1;
}

// 在 service Message 中新增：
service Message {
    // ... 现有 RPC ...
    rpc SyncSessions(SyncSessionsReq) returns (SyncSessionsResp);
}
```

### 服务端实现

```go
// service/session.go 新增
func (s *MessageService) SyncSessions(
    ctx context.Context, userID uint64,
) ([]SyncSessionResult, error) {
    // 1. 从 user_session 表查所有会话（含 last_read_seq、is_top、is_disturb、unread_count）
    userSessions, err := s.svcCtx.SessionDAO.FindUserSessions(ctx, userID)
    if err != nil {
        return nil, err
    }
    if len(userSessions) == 0 {
        return nil, nil
    }

    // 2. 提取所有 sessionID
    sessionIDs := make([]string, len(userSessions))
    for i, us := range userSessions {
        sessionIDs[i] = us.SessionID
    }

    // 3. 批量从 Redis session:info:{id} 读 actual_seq / last_content / last_sender
    //    （复用已有的 FindSessionsByIDs 批量查询逻辑）
    sessions, err := s.svcCtx.SessionDAO.FindSessionsByIDs(ctx, sessionIDs)
    if err != nil {
        // Redis 失败降级：从 MySQL 查
        sessions = nil
    }

    // 4. 构建 sessionID → Session 的索引
    sessionMap := make(map[string]*model.Session, len(sessions))
    for _, sess := range sessions {
        sessionMap[sess.SessionID] = sess
    }

    // 5. 组装返回结果
    results := make([]SyncSessionResult, 0, len(userSessions))
    for _, us := range userSessions {
        result := SyncSessionResult{
            SessionID:    us.SessionID,
            LastReadSeq:  us.LastReadSeq,
            UnreadCount:  us.UnreadCount,
            IsTop:        us.IsTop,
            IsDisturb:    us.IsDisturb,
            UpdateTime:   us.UpdateTime,
        }
        if s, ok := sessionMap[us.SessionID]; ok {
            result.SessionKey  = s.SessionKey
            result.SessionType = s.Type
            result.LastContent = s.LastContent
            result.LastSender  = s.LastSender
        }
        results = append(results, result)
    }

    return results, nil
}
```

```go
// dao/session.go 新增批量查询方法
func (c *SessionDAO) FindUserSessionsFull(
    ctx context.Context, userID uint64,
) ([]UserSessionWithMeta, error) {
    // JOIN 查询：user_session + session，一次 SQL 拿到全部字段
    type Row struct {
        SessionID    string
        SessionKey   string
        SessionType  int8
        LastReadSeq  uint64
        UnreadCount  int
        LastContent  string
        LastSender   uint64
        ActualSeq    uint64
        IsTop        int8
        IsDisturb    int8
        UpdateTime   time.Time
    }

    var rows []Row
    err := c.db.WithContext(ctx).Raw(`
        SELECT
            us.session_id,
            s.session_key,
            s.type AS session_type,
            us.last_read_seq,
            COALESCE(us.unread_count, 0) AS unread_count,
            s.last_content,
            s.last_sender,
            s.actual_seq,
            us.is_top,
            us.is_disturb,
            GREATEST(us.update_time, s.update_time) AS update_time
        FROM user_session us
        JOIN session s ON s.session_id = us.session_id
        WHERE us.user_id = ?
        ORDER BY update_time DESC
    `, userID).Scan(&rows).Error

    // 转换...
    return results, err
}
```

### 关键设计决策

**为什么不直接用 `GetUserActiveSessions` 的增量模式做 SyncSessions**

`GetUserActiveSessions` 依赖 ZSET `user:conv:timeline:{uid}`，而 ZSET 由 SeqSyncer 每 3 秒异步刷入。如果用户在消息写入后 3 秒内上线，增量同步会漏掉这条消息的会话更新。`SyncSessions` 直接走 MySQL JOIN，数据是实时的（SeqSyncer 刷入 MySQL 后立即可见）。代价是每次全量扫描 `user_session` 表——但用户会话数通常最多几百个，全量扫描开销极低。

### 预期收益

- 上线同步请求数：从 N+1 次 → 1 次
- 客户端逻辑简化：一个接口拿到全部所需数据
- 减少移动端弱网下的请求失败概率

---

## 4. WS 建连时推送未读摘要

### 问题

当前用户建立 WS 连接后，Gateway 不做任何推送。客户端必须主动发起 HTTP 请求（GetUserSessions → GetSession × N → GetHistory × M）才能展示会话列表和未读气泡。用户看到的是"先空白，再加载"，而不是"打开即见未读"。

### 改动范围

- `internal/apps/websocket/gateway/transport/websocket.go` — Handle 方法增加推送逻辑
- `internal/apps/websocket/gateway/server/context.go` — ServiceContext 增加 Message RPC Client
- `pkg/proto/transport/transport.proto` — 可能需要新增 `SESSION_SYNC` 消息类型（或复用现有类型）
- `internal/apps/Message/rpc/client/messagerpc/` — 确保 Gateway 可以调 Message RPC

### 设计方案

在 Gateway 的 WS `Handle` 方法中，注册完连接和路由后，异步调用 Message 服务的 `SyncSessions` RPC，将结果包装为 WS 消息推送给客户端。

```go
// transport/websocket.go Handle 方法
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
    userID := tokenmanager.ExtractIDFromCtx(r.Context())

    // ... 现有逻辑：升级 WS、创建连接、注册路由 ...

    defer func() {
        // ... 现有清理逻辑 ...
    }()

    // 启动写循环
    go conn.WritePump(ctx)

    // WS 建连成功后，异步推送未读摘要（新增）
    if h.svcCtx.MessageRpcClient != nil {
        go h.pushSessionSummary(ctx, userID, conn)
    }

    // 启动读循环(阻塞)
    conn.ReadPump(ctx, h.createMessageHandler(ctx, conn))
}

// pushSessionSummary 调 Message RPC 获取会话摘要并通过 WS 推送
func (h *WSHandler) pushSessionSummary(
    ctx context.Context, userID uint64, conn *connection.Connection,
) {
    rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    defer cancel()

    resp, err := h.svcCtx.MessageRpcClient.SyncSessions(rctx, &messagepb.SyncSessionsReq{
        UserId: userID,
    })
    if err != nil {
        logger.Errorf("[WS] push session summary failed for user %d: %v", userID, err)
        return
    }

    // 构建推送消息
    // 方式 A：逐会话推送 UNREAD_SYNC 消息
    // 方式 B：使用现有 SESSION_LIST 或新增 SESSION_SYNC 消息类型
    for _, item := range resp.Items {
        if item.UnreadCount > 0 {
            syncMsg := &messagepb.SessionSync{
                SessionId:   item.SessionId,
                SessionKey:  item.SessionKey,
                SessionType: item.SessionType,
                UnreadCount: item.UnreadCount,
                LastContent: item.LastContent,
                LastSender:  item.LastSender,
                UpdateTime:  item.UpdateTime,
            }
            payload, _ := proto.Marshal(syncMsg)
            conn.Send(&transport.WSMessage{
                Type:    transport.MessageType_UPDATE_SESSION,
                Payload: payload,
            })
        }
    }
}
```

```go
// server/context.go ServiceContext 新增
type ServiceContext struct {
    // ... 现有字段 ...
    MessageRpcClient messagepb.MessageClient  // 新增：Message 服务 RPC 客户端
}
```

### 客户端处理

客户端在 WS 建连后监听 `UPDATE_SESSION`（或新定义的 `SESSION_SYNC`）消息类型，收到后直接更新本地会话列表和未读 badge，无需主动发 HTTP 请求。

### 关键设计决策

**推送全量 vs 只推未读**

全量推送所有会话摘要（包括未读计数为 0 的），可以让客户端一次性完成会话列表初始化。但如果用户有几百个会话，一次推送太多。建议只推 `unread_count > 0` 的，其余会话由客户端按需拉取（懒加载）。

**推送时机**

放在 `WritePump` 启动后、`ReadPump` 启动前。此时连接已完全建立，写通道可用，且不会阻塞读循环。RPC 调用在独立 goroutine 中执行，不影响 WS 连接的主生命周期。

### 预期收益

- 用户打开应用 → WS 建立 → 未读气泡马上出现（无需等 HTTP 请求）
- 弱网环境下减少 HTTP 请求数，提升首屏加载成功率

---

## 5. 配置管理精简

### 问题

项目同时引入了三套配置/发现体系：Nacos（配置中心）、Viper（本地配置文件）、etcd（go-zero 服务注册发现）。Nacos 自身依赖 MySQL 存储配置数据，且生产部署需要额外维护一套 Nacos 集群。`docker-compose.yml` 基础设施组件达 7 个，本地开发和 CI 启动成本高。

### 改动范围

- `pkg/configParser/` — 去掉 Nacos 解析器，或置为可选
- `docker-compose.yml` — 去掉 Nacos 服务定义
- 各服务的 `etc/config.yaml` — 调整为自包含配置（etcd + 本地文件即可）
- `go.mod` — 评估是否可以移除 `nacos-sdk-go` 依赖

### 设计方案

#### 阶段一：本地开发移除 Nacos（低风险，立即执行）

`go-zero` + etcd 本身已支持通过 etcd 做配置热更新。本地开发更简单的做法是直接用 YAML 文件（Viper 已支持），不需要 etcd 也不强依赖 Nacos。

1. **`docker-compose.yml`**：注释或移除 Nacos 服务定义，新增一个 `profiles/minimal.yml` 仅包含 MySQL、Redis、NATS 三个核心依赖。

```yaml
# docker-compose.yml 或 profiles/minimal.yml
version: '3.8'
services:
  mysql:
    image: mysql:8.0
    # ... 现有配置 ...
  redis:
    image: redis:7.0-alpine
    # ... 现有配置 ...
  nats:
    image: nats:2.9-alpine
    # ... 现有配置 ...
  # mongodb 和 etcd 视开发需要保留或移除
```

2. **`pkg/configParser/factory.go`**：让 Nacos 解析器变为可选——如果 Nacos 地址为空，回退到文件解析。

```go
// factory.go
func NewConfigLoader(configPath string) *ConfigLoader {
    loader := &ConfigLoader{path: configPath}
    // 优先 Nacos（如果配置了地址）
    if addr := os.Getenv("NACOS_ADDR"); addr != "" {
        loader.parser = NewNacosParser(addr)
    } else {
        loader.parser = NewFileParser()
    }
    return loader
}
```

3. **各服务的 `etc/config.yaml`** 在 Nacos 不可用时直接使用本地文件，不做额外改动。

#### 阶段二：生产环境评估（需谨慎评估）

- 如果 Nacos 仅用于"配置热更新"且没有太多运行时动态改配的需求，可以用 etcd 替代（go-zero 的 `MustNewEtcdClient` + `ConfigWatcher`）
- 如果 Nacos 的用途包含"配置版本管理/灰度发布/审计"，保留 Nacos 但评估是否可以和其他服务合用一个 Nacos 集群
- 对于本地开发和 CI，提供 `make dev` 命令仅启动 MySQL + Redis + NATS，不启动 Nacos/etcd/MongoDB/LiveKit

### getBase 风险评估

- **风险**：Nacos 配置可能包含生产敏感信息（如阿里云 AK/SK、数据库密码），迁移时需确保新配置存储方案的安全性不低于 Nacos
- **缓解**：敏感信息使用环境变量注入（`${MYSQL_PASSWORD}`），不写入配置文件

### 预期收益

- 本地开发启动依赖从 7 个降到 3 个（MySQL、Redis、NATS）
- CI 构建时间缩短（少拉一个 Docker 镜像）
- 配置链路简化，排查问题更容易

---

## 6. API 限流与熔断保护

### 问题

项目 REST API 接入层没有任何限流措施，NATS 消费端也没有背压流控。突发高并发（如群红包刷屏）可能击穿数据库，消息积压时消费吞吐受限。

### 改动范围

- 各服务的 `main.go` — 在 `rest.NewServer` 时传入限流中间件
- `internal/apps/Message/rpc/listener/index.go` — NATS 消费调整为批量 Fetch + 合理 MaxAckPending
- `internal/interceptor/` — 新增 gRPC 熔断拦截器（或直接使用 go-zero 内置 Breaker）

### 设计方案

#### REST API 限流

go-zero 提供了 `rest.WithLimiter` 选项，内置基于令牌桶的限流器。按接口粒度配置：

```go
// 各 API 服务的 main.go
server := rest.MustNewServer(c.RestConf,
    rest.WithCors("*"),
    rest.WithLimiter(rest.NewPeriodLimit(100, 200, redis, "api-limit")),
)
```

更精细的做法是按用户 ID 限流（防止单用户刷接口）：

```go
// middleware/rate_limit.go（新增中间件）
func WithUserRateLimit(redis *redis.Redis, limit int, period int) rest.Middleware {
    limiter := rest.NewTokenLimiter(limit, period, redis, "user-rate-limit")
    return func(next http.HandlerFunc) http.HandlerFunc {
        return func(w http.ResponseWriter, r *http.Request) {
            userID := tokenmanager.ExtractIDFromCtx(r.Context())
            key := fmt.Sprintf("rate:%d", userID)
            if !limiter.Allow(key) {
                resultx.ErrorJson(w, xerr.New(transport.ErrorCode_ERR_SERVICE_BUSY, "请求过于频繁"))
                return
            }
            next(w, r)
        }
    }
}
```

#### NATS 消费端背压

当前已实现 `batch=32` 批量拉取（`listener/index.go`），并设置了 worker channel 满时阻塞（自然背压）。需要额外配置的是 `MaxAckPending`（控制 NATS 服务器端最多同时有多少条未 Ack 的消息在消费者处）：

```go
// listener/index.go Listen 方法
sub, err := l.svcCtx.Js.PullSubscribe(
    l.svcCtx.Config.Listener.DBSubject,
    durableConsumerName,
    nats.MaxDeliver(maxDeliver),
    nats.MaxAckPending(256),  // 新增：最多 256 条未 Ack 消息，超过则暂停分发
)
```

`MaxAckPending` 与 worker channel size (`128`) 的关系：channel 满 → runLoop 阻塞 → 不再 Fetch → NATS 不会再分配新消息 → 自然形成端到端背压。

#### gRPC 熔断

go-zero 框架在 gRPC 客户端侧内置了 Breaker（断路器），只需在 `zrpc.RpcClientConf` 中配置：

```yaml
# 各 RPC 客户端的 etc/*.yaml
UserRpc:
  Etcd:
    Hosts:
      - localhost:2379
    Key: user.rpc
  NonBlock: true
  Timeout: 3000
  # 熔断配置（go-zero 默认已启用，可自定义阈值）
  # Breaker:
  #   Period: 10s
  #   Requests: 100
  #   FailureRate: 0.5
```

服务端的 gRPC 拦截器已实现（`interceptor/error.go`），无需额外改动。

### 预期收益

- 单用户无法刷爆 API（限流）
- NATS 消息积压时消费端不会被冲垮（背压）
- RPC 下游故障时自动熔断，防止级联雪崩

---

## 7. 实施路线图

| 优先级 | 改动 | 工作量估算 | 依赖 | 风险 |
|--------|------|-----------|------|------|
| P1 | SendToGroup 本地投递优先 | 0.5 天 | 无 | 低：仅改 `manager.go` + proto 加字段 |
| P1 | 未读计数显式化 | 2 天 | DDL migration | 中：涉及 DB schema 变更 |
| P2 | SyncSessions 聚合接口 | 1.5 天 | 未读计数显式化 | 低：新增 RPC，不改现有逻辑 |
| P2 | WS 建连时推送未读摘要 | 1 天 | SyncSessions + MessageRpcClient | 低：异步 goroutine，不影响主链路 |
| P3 | 配置管理精简 | 1 天 | 无（纯配置改动） | 低：本地开发用，不影响生产 |
| P3 | API 限流与熔断 | 1 天 | 无 | 低：go-zero 内置能力 |

**推荐执行顺序**：

```
Week 1: SendToGroup 本地投递优先（半天，收尾优化）
Week 1-2: 未读计数显式化（P1，DB migration 需评审）
Week 2: SyncSessions 聚合接口（依赖未读计数完成）
Week 2-3: WS 建连推送未读摘要（依赖 SyncSessions）
Week 3: 配置精简（P3，可穿插进行）
Week 3: API 限流与熔断（P3，可穿插进行）
```
