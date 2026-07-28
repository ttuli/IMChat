# WebRTC 语音/视频通话 —— 后端待办

配对前端清单：`Nexus/CALL_TODO.md`

**主链路已完成**：发起 → 状态平面 → 信令转发 → 上线补投 → 超时收敛 → 落库记录，
通过 `go build ./...` + `go vet`。已完成部分的设计理由都写在对应代码注释里，本文件只留待办。

**前端已全部完成**（含媒体权限、铃声、设备跟随、登出收尾），
**TURN 是整个功能唯一的阻塞项** —— 不部署则跨 NAT 通话建不起来。

---

## 1. TURN / STUN ⬜ 未开始（阻塞真实可用）

**当前只配了公共 STUN（前端 `useCallState.ts` 里硬编码 `stun.l.google.com`），
双方都在对称 NAT 后面时通话建不起来。** 这是上线前必须补的一环。

- [ ] 部署 coturn，**短时凭证鉴权**（HMAC 生成的临时用户名/密码），不要静态密码
- [ ] 提供 ICE 配置下发接口，前端启动或发起通话时拉取，替换硬编码
      —— 前端已留好位置（`useCallState.ts` 的 `ICE_SERVERS`）
- [ ] **带宽容量按视频估算**：走 TURN 中继时服务器承担双向全量码率。
      语音每路约 50kbps，视频 720p 每路 1-2Mbps —— 相差 20-40 倍，
      按语音量级估的机器会在视频并发上直接打满
- [ ] 中继比例可观测（多少通话回落到 TURN），否则带宽成本不可控

## 2. 部署 ⬜

- [ ] **改完 `pkg/proto` 与 Message RPC 必须重新编译部署才生效**，
      通话链路涉及 websocket 网关与 Message 服务两处

## 3. 容量规划 ⬜

- [ ] **按前端已定的采集参数估 TURN 带宽**：前端 `CALL_CONFIG.videoConstraints`
      已定为 720p / 24fps（`Nexus/share/config/constants.ts`）。
      据此估算并发上限与机器规格；若容量不够，是调低这个值还是加机器，需要一起定
- [ ] 定完把**发送码率上限**同步给前端 —— 前端待补 SDP `b=AS` 封顶
      （采集分辨率 ≠ 发送码率，不封顶时编码器会吃满上行，直接推高中继成本）

## 4. 范围外（明确不做，避免以后误当遗漏）

- **群通话**：状态平面要维护 N 个参与者各自 joined/left，复杂度跳一档。
  第一版只做私聊，前端 `GroupCall.vue` 保持 UI 占位、不接信令
- **语音通话中途升级为视频**：双按钮设计的直接推论。音频 SDP 没有 video m-line，
  加视频轨必然触发重协商。服务端无需为此做任何事，产品上确认语音界面不出现开摄像头入口即可
- **按 `end_reason` 细分未读**：reason 埋在 payload 内、Mongo 查询侧不可见，
  提成顶层列要给通用消息模型加通话专属字段，代价与收益不成比例。
  被叫在 `COMPLETED`/`REJECTED` 后的多余红点由前端 `reportSessionRead` 消解

---

## 已完成部分索引

| 模块 | 文件 | 要点 |
|---|---|---|
| proto | `pkg/proto/call/call.proto` | 4 枚举 + 10 message；`transport.proto` 800-809 信令段；`message.proto` 的 `CallMessage` |
| 状态平面 | `pkg/callstate/` | `expire_at` 是权威、ZSET 只是调度提示；终态 CAS 只有赢家落库；跨键靠 TTL 与 sweeper 自愈 |
| 信令分支 | `gateway/dispatch/call.go` | 不进 `DBSubject`；invite 先写状态再推送；SDP/ICE 纯透传；`peerCache` 免 ICE 逐帧查 Redis |
| 上线补投 | `dispatch/call.go` `onCallPending` | WS `CALL_PENDING(807)` 帧；服务端复核主叫在线 + 剩余振铃 ≥ 5s |
| 超时收敛 | `gateway/callsweeper/` | 2s 一轮；可见性超时认领；多实例并行安全 |
| 落库 | `pkg/proto/util/helper.go` | `NewCallRecordMsg` / `CallPreview`；JetStream 以 `call:{callID}` 去重 |
| 未读口径 | `dao/message.go:146` | `CHAT_CALL(106)` **计入未读**，零逻辑改动，注释已固化意图 |
| 记录投递 | `Message/rpc/listener/index.go` | **通话记录额外投一份给主叫**：常规私聊只投 Target（发送方有本地乐观副本），但通话记录是服务端铸造的，主叫从无本地副本，不补投就要等下次拉历史才出现 |
| 记录落 Extra | `Message/rpc/internal/service/message.go` | `end_reason`/`media_type`/`duration`/`call_id` 拆进 Extra（新增 `MessageExtraKey` 40/41/42，时长复用 20）——历史接口只返回 content/media_url/extra，不拆的话翻历史只剩中性文案 |

### 已定参数

| 参数 | 值 | 位置 |
|---|---|---|
| 振铃超时 | 60s | `callstate.RingTTL` |
| 通话时长上限 | 4h | `callstate.MaxCallDuration` |
| 补投最小剩余振铃 | 5s | `dispatch.minDeliverRemainMs` |
| sweeper 轮询 / 认领可见性 | 2s / 30s | `callsweeper.defaultInterval` / `callstate.ClaimVisibilityTTL` |
