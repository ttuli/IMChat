# WebRTC 语音/视频通话 —— 后端实现清单

配对前端清单：`Nexus/CALL_TODO.md`（两边的 proto 段落必须同步改）。

**当前进度：§1-§6 全部完成并通过 `go build ./...` + `go vet`；仅剩 §7 TURN 部署。**
后端主链路已闭环：发起 → 状态平面 → 信令转发 → 上线补投 → 超时收敛 → 落库记录。

## 设计前提（先读，后面所有条目都依赖这三条）

1. **三个平面分离**
   - **信令平面**：invite / accept / reject / cancel / hangup / SDP / ICE —— **不落库**，纯 WS 实时转发。
   - **状态平面**：Redis，TTL = 振铃窗口。这是"这通电话还活着吗"的唯一权威。
   - **记录平面**：一通电话**只写 1 条** `CHAT_CALL(106)` 消息，且只在进入**终态**时写。
2. **TTL 是振铃窗口，不是离线信箱。** 绝不能为了"省得丢"把 TTL 调长，否则退化成幽灵来电。
3. **单会话前提已确认**：`Router.RegisterUser` 抢占式注册 + 踢旧连接（`internal/apps/websocket/gateway/router/router.go:41`），
   所以不存在多端同时振铃、接听后要取消其他端的问题。

---

## 1. proto 定义 ✅

- [x] 新建 `pkg/proto/call/call.proto`：4 枚举（`CallMediaType` / `CallState` / `CallEndReason` / `SdpType`）
      + 10 message（`CallInvite` / `CallAccept` / `CallReject` / `CallCancel` / `CallHangup` / `CallEnd`
      / `CallSdp` / `CallIce` / `CallMediaUpdate` / `CallPending`）
- [x] `CallMediaType`：`AUDIO=0` / `VIDEO=1`。由**入口按钮**决定，发起时确定、**通话期间不变**
- [x] `CallSdp.sdp_type` 区分 offer/answer
- [x] `CallMediaUpdate`：`camera_on` / `mic_on`，**对端 UI 的唯一驱动源**
- [x] `transport.proto` 加 800-809 信令段（7xx 已被 `UPDATE_SESSION`/`USER_GROUP_SYNC` 占用）
      —— 比原计划多了 `CALL_END=809`
- [x] `message.proto` 加 `CallMessage`，跨包引用 `call.CallMediaType` / `call.CallEndReason`
- [x] 同步 Nexus `share/types/proto/call/`，`npm run proto` + `vue-tsc` 通过
- [ ] **改完 pkg/proto 与 Message RPC 必须重新编译部署才生效**（部署时执行）

### 实现中定下的补充

- 增加 `CALL_END_REASON_BUSY=7`：忙线也要落一条记录（"对方忙线"），原枚举缺这个终态
- 增加 `CALL_END=809` 帧：服务端主动终止（振铃超时、主叫掉线、忙线）与用户挂断语义不同，需单独帧
- **`CallMessage` 不设 `caller_id`**：`base.from_user_id` 恒为主叫，客户端据此判断展示视角
- **`CHAT_CALL` 故意不注册进 `transport/parser.go` 的 `msgSpecRegistry`** ——
  该表是客户端发消息的解析入口，不注册即客户端无法伪造通话记录，记录只能服务端铸造
- **握手采用 late offer**：invite 只是通知，被叫 accept 后主叫才产生 SDP offer。
  好处是 pending 记录不必在 Redis 驮 SDP blob；代价是多一个 RTT

## 2. Redis 状态平面 ✅ → `pkg/callstate/`

- [x] `call:state:{callID}` HASH：caller / callee / session_key / media_type / state
      / invite_at / **expire_at** / connected_at / ended_at / end_reason
- [x] `call:busy:{userID}` → callID：**主叫被叫各占一把**，兼作忙线锁
- [x] `call:deadlines` ZSET：sweeper 到期索引
- [x] 状态机 `RINGING → CONNECTED → ENDED` / `RINGING → ENDED`，迁移用单键 Lua CAS
- [x] `RingTTL=60s` / `MaxCallDuration=4h` / `EndedLingerTTL=60s` / `ClaimVisibilityTTL=30s`

### 实现中定下的关键约束

- **HASH 里的 `expire_at` 是权威，ZSET 分数只是调度提示。**
  `Accept` 要同时改状态和 ZSET，两键不同 slot 无法原子；ZSET 续期失败被设计成无害的
  —— sweeper 认领后必须回读 `expire_at` 复核，否则会**掐掉一通正在进行的通话**
- **`Terminate` 是 CAS，返回 nil 即非赢家**，调用方必须据此跳过落库
- **跨键操作不追求原子性**（Cluster 下多键 Lua 会 CROSSSLOT），靠 TTL 与 sweeper 自愈：
  忙线锁泄漏由 TTL 收敛，ZSET 泄漏由 sweeper 发现 ENDED 后清理
- **`Create` 内部顺序：忙线锁 → ZADD → HSET。** ZSET 先于 HASH 是有意的：
  中途失败只留一个指向空状态的索引项，sweeper 读到直接清理；反序则产生无人收敛的孤儿通话
- **`PendingFor` 必须校验 `CalleeID == userID`** —— 忙线锁主被叫都占，
  不校验的话主叫查 pending 会查出自己拨出的电话并给自己弹接听界面
- 主叫存活复核**不在本包做**（不让状态平面反向依赖路由表），由网关编排层负责

## 3. 网关信令分支 ✅ → `gateway/dispatch/call.go`

- [x] `Dispatcher.Handle` 加 `util.IsCallSignal` 分支，与 `IsChatMessage` 并列
- [x] **信令帧不进 `DBSubject`**：独立路径，不调 `ParseMessage`、不分配 msg_id/seq
- [x] **invite 严守顺序**：`CallState.Create` 成功后才推送。被叫离线**不失败**，
      保持振铃等其窗口内上线补投
- [x] SDP/ICE/MEDIA_UPDATE 纯透传，不解析、不做「握手已完成」判断
- [x] `ServiceContext` 加 `CallState`（复用 `RouteStore` 的 Redis）与 `Notifier`

### 实现中定下的补充

- **`peerCache`（per-connection）免掉 ICE 逐帧 Redis 查询**：ICE trickle 一通电话几十个 candidate。
  late offer 下 SDP/ICE 全在 accept 之后，届时两侧 Dispatcher 都已填好缓存；未命中回落 Redis
- **振铃期挂断按语义归一**：客户端发 `CALL_HANGUP` 但状态仍是 RINGING 时，
  主叫记 `CANCELED`、被叫记 `REJECTED` —— 否则记录会写成"通话时长 00:00"
- **安全边界**（前端不必也无法绕过）：
  - `caller_id` 一律以连接身份为准，客户端自报的忽略
  - `call_id` 服务端 uuid 分配，客户端上报的忽略
  - 终止/转发前校验发起者是通话参与方，防止第三方掐断或窃听

## 4. 上线补投 ✅ → `onCallPending`

- [x] **决策已定：走 WS `CALL_PENDING(807)` 帧，不是 HTTP**。客户端发一帧查询，服务端同类型回复
- [x] 复核 1：**主叫仍在线**（`Routes.LookupUser` == `RouteOnline`）。
      掉线则顺手 `Terminate(FAILED)` + 落库，回 `has_pending=false`
- [x] 复核 2：**剩余振铃 ≥ 5s**（`minDeliverRemainMs`），不足则不打扰，留给 sweeper 收敛
- [x] 两项复核都在服务端做完，客户端拿到 `has_pending=true` 可直接弹接听界面

## 5. 超时收敛 sweeper ✅ → `gateway/callsweeper/`

- [x] 2 秒一轮，`ClaimExpired` 批量认领（单批上限 200）
- [x] **不依赖 Redis TTL 过期**：key 过期不执行业务代码；keyspace notification 是 at-most-once
- [x] 四路分支：不存在→清索引 / 已 ENDED→清索引 / `now < expire_at`→重新排期 / 真到期→Terminate
- [x] 终止原因判定：CONNECTED→`COMPLETED`（撞时长上限，保留累计时长）；
      RINGING→查被叫此刻在线状态细分 `MISSED`（人在没接）/ `PEER_OFFLINE`（整窗口没上线）
- [x] 赢家才 `notifyEnd`（双方各发 `CALL_END`）+ 落库
- [x] **多实例并行安全**：认领带可见性超时（重打分不删除，认领者崩溃 30s 后重新可见），
      终态转换是 CAS，只有赢家落库

> RINGING 的 `MISSED` / `PEER_OFFLINE` 细分是近似：被叫全程在线但恰好在截止瞬间掉线会误报为
> `PEER_OFFLINE`。代价仅是气泡文案，未做精确跟踪。

## 6. 终态落库 ✅

- [x] `util.NewCallRecordMsg` 构造记录并发 `DBSubject`，走现有落库/分配 seq 流程
- [x] **一通电话一条记录**：由 `Terminate` 的 CAS 赢家负责；
      JetStream 以 `call:{callID}` 作去重键双保险
- [x] `util.CallPreview` 无主语文案，按 `media_type` 分两套：
      `语音通话 03:21` / `视频通话 已取消` / `语音通话 未接听` / `视频通话 对方忙线`
- [x] **未读口径已定：`CHAT_CALL(106)` 计入未读。**
      `CountUnread` 的排除列表本就只有 `[605, 606]`，**无需改动逻辑**；
      已在 `dao/message.go:146` 的文档注释里写明「有意不排除」及其理由，
      防止后续有人当作遗漏顺手加进排除列表

### 未读口径的副作用与分工（重要）

记录的 `from_user_id` 恒为主叫 → **只有被叫侧可能计入未读**，主叫不会为自己拨出的电话产生未读。
但被叫在「正常通话结束」「自己拒接」之后同样会 +1，刚聊完就冒红点。

**不在服务端按 `end_reason` 细分**：reason 埋在 payload 内，Mongo 查询侧不可见，
提成顶层列需要给通用消息模型加通话专属字段，代价与收益不成比例。

**由客户端消解**：`end_reason ∈ {COMPLETED, REJECTED}` 时改调 `reportSessionRead` 推进服务端游标，
而不是 `incrementUnread`。必须是推进游标而非只改本地数字 ——
服务端未读是点查、不存量化，只改本地数字会在下次会话列表刷新时被打回。详见前端清单 §7。

## 7. TURN/STUN ⬜ 未开始

- [ ] STUN 打洞 + TURN 中继部署与鉴权（建议 coturn，短时凭证鉴权而非静态密码）
- [ ] ICE 配置下发接口（前端不硬编码，见前端清单 §5）
- [ ] **带宽容量按视频估算**：走 TURN 中继时服务器要承担双向全量码率。
      语音每路约 50kbps，视频 720p 每路 1-2Mbps —— 相差 20-40 倍，
      按语音量级估的机器会在视频并发上直接打满
- [ ] 中继比例可观测（多少通话回落到 TURN），否则带宽成本不可控

## 8. 范围

- [x] 语音与视频由**入口按钮**区分，服务端信令完全一致，差异只在 `media_type` 与记录文案
- [x] **语音通话不支持中途升级为视频**（双按钮设计的直接推论）。服务端无需为此做任何事，
      但产品上要确认语音通话界面不出现开摄像头入口
- [ ] **第一版只做私聊**。群通话状态平面要维护 N 个参与者各自 joined/left，复杂度跳一档

---

## 待定决策

| # | 决策 | 状态 |
|---|---|---|
| 1 | 振铃超时秒数 | ✅ 60s（`callstate.RingTTL`） |
| 2 | pending 补投走 HTTP 还是 WS | ✅ WS `CALL_PENDING(807)` 帧 |
| 3 | 通话时长上限 | ✅ 4h（`callstate.MaxCallDuration`） |
| 4 | `CHAT_CALL` 是否计入未读 | ✅ **计入**。后端零改动（106 本就不在排除列表），已补注释固化意图；副作用由客户端 `reportSessionRead` 消解 |
| 5 | 视频默认分辨率与码率上限 | ⬜ **待定**，直接决定 §7 的 TURN 带宽预算 |

## 已交付文件

| 路径 | 说明 |
|---|---|
| `pkg/proto/call/call.proto` | 信令与枚举定义 |
| `pkg/proto/transport/transport.proto` | 800-809 信令类型段 |
| `pkg/proto/message/message.proto` | `CallMessage` 记录结构 |
| `pkg/proto/util/helper.go` | `IsCallSignal` / `NewCallRecordMsg` / `CallPreview` |
| `pkg/callstate/` | 状态平面（`callstate.go` 类型与键约定 + `store.go` Lua 与操作） |
| `gateway/dispatch/call.go` | 信令处理全部逻辑 |
| `gateway/dispatch/dispatcher.go` | `Handle` 分支 + `peerCache` |
| `gateway/callsweeper/sweeper.go` | 超时收敛 |
| `gateway/server/context.go` | `CallState` / `Notifier` 注入 |
| `gateway/server/server.go` | sweeper 协程启动 |
