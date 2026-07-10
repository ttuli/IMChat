# User & Group RPC 服务非精准投递（广播模式）位置汇总

本文件汇总了 IMChat 项目中 **User RPC** 和 **Group RPC** 服务里尚未使用“精准路由/精准投递”机制（即通过 NATS Broadcast 进行全集群广播，而非单播/多播或使用 `DeliverTo` 定向扇出）的代码位置。

---

## 1. User RPC 服务

User RPC 服务中的通知全部面向特定单个用户（`TargetType_USER`），但目前均发布到 `c.NATS.BroadcastSubject`，导致所有网关节点都需要在内存中进行判断。

| 序号 | 业务场景 | 消息类型 | 代码文件与位置 | 优化建议 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 直接添加好友成功 | `MessageType_FRIEND_ADD` | [friend_apply.go:L50](file:///d:/Project/IMChat/internal/apps/User/rpc/internal/service/friend_apply.go#L50) | 替换为发送至 `QueueBroadcastSubject`，利用网关的 `SendToUser` 自动通过 Redis 路由表进行精准跨节点单播。 |
| 2 | 发起好友申请通知 | `MessageType_FRIEND_APPLY` | [friend_apply.go:L83](file:///d:/Project/IMChat/internal/apps/User/rpc/internal/service/friend_apply.go#L83) | 同上，使用单播取代全网关节点广播。 |
| 3 | 同意/拒绝好友申请结果 | `MessageType_FRIEND_APPLY` | [friend_apply.go:L140](file:///d:/Project/IMChat/internal/apps/User/rpc/internal/service/friend_apply.go#L140) | 同上，使用单播发送给申请发起人。 |
| 4 | 删除好友通知 | `MessageType_FRIEND_DELETED` | [friend.go:L69](file:///d:/Project/IMChat/internal/apps/User/rpc/internal/service/friend.go#L69) | 同上，使用单播发送给被删除的好友。 |

---

## 2. Group RPC 服务

Group RPC 服务中存在两类广播投递，包括面向**特定用户（管理员等）**和面向**群成员全员**的广播。

### A. 面向特定用户的单播/多播通知
以下场景的消息虽然是发给特定的若干用户，但走的是全集群 NATS 广播：

| 序号 | 业务场景 | 消息类型 | 代码文件与位置 | 优化建议 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 申请加入群聊通知 | `MessageType_GROUP_REQUEST` | [apply.go:L96](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/apply.go#L96) | 目标用户为管理员列表。替换为 `QueueBroadcastSubject`，利用网关的多播/单播机制定向投递。 |
| 2 | 审批群申请结果通知 | `MessageType_GROUP_REQUEST` | [apply.go:L191](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/apply.go#L191) | 目标为相关管理员和申请者。替换为 `QueueBroadcastSubject` 精准投递。 |
| 3 | 群组创建通知 | `MessageType_GROUP_OP_CREATE` | [group.go:L86](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/group.go#L86) | 目标为初始群成员。可替换为精准路由投递。 |

### B. 面向群全员的群操作通知（`TargetType_GROUP`）
以下场景通知所有群成员某项群组变更。目前因为发送的 `WSMessage` 没有携带 `DeliverTo` 字段，会导致网关在**所有节点**上都要查 Redis 路由表来获取该群成员，再过滤本地连接进行投递（即兼容的广播路径）。

| 序号 | 业务场景 | 消息类型 | 代码文件与位置 | 优化建议 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 成员直接入群成功通知 | `GROUP_OP_JOIN` | [apply.go:L53](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/apply.go#L53) | 在 Group 服务中提前通过 `Routes.LookupUsers` 查询群成员当前的网关节点分布，填充 `DeliverTo` 并针对存活节点精准单播；或优化网关的群广播消费链。 |
| 2 | 审批通过入群通知 | `GROUP_OP_JOIN` | [apply.go:L157](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/apply.go#L157) | 同上。 |
| 3 | 移除群成员通知 | `GROUP_OP_KICK` | [member.go:L129](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/member.go#L129) | 同上。 |
| 4 | 主动退出群聊通知 | `GROUP_OP_LEAVE` | [member.go:L170](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/member.go#L170) | 同上。 |
| 5 | 解散群组通知 | `GROUP_OP_DISMISS` | [group.go:L187](file:///d:/Project/IMChat/internal/apps/Group/rpc/internal/service/group.go#L187) | 同上。 |

更改过程对应冗余的代码或逻辑要进行清理