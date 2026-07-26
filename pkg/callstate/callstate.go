// Package callstate 提供 WebRTC 通话的状态平面（基于 Redis）。
//
// 三个平面的分工（详见 CALL_TODO.md）：
//   - 信令平面：invite/accept/reject/sdp/ice 等，网关纯透传，不落库
//   - 状态平面：**本包**，回答「这通电话现在还活着吗」，是唯一权威
//   - 记录平面：message.CallMessage（CHAT_CALL=106），终态时落一条聊天记录
//
// 数据结构：
//   - 通话详情  call:state:{callID}  → HASH，含状态机与 expire_at
//   - 忙线锁    call:busy:{userID}   → STRING = callID，主叫被叫各占一把
//   - 到期索引  call:deadlines       → ZSET，member=callID，score=预期到期毫秒
//
// 关键设计：
//
//  1. **TTL 是振铃窗口，不是离线信箱。** 被叫离线时通话保持 RINGING 等其上线补投，
//     但最长只等到 ring_deadline。绝不可为「省得丢」而调长，否则退化成幽灵来电。
//
//  2. **HASH 里的 expire_at 是权威，ZSET 只是调度提示。** sweeper 认领后必须回读
//     expire_at 复核：ZSET 漂移（如 Accept 时续期失败）只会导致一次多余的检查，
//     不会误杀正在进行的通话。
//
//  3. **终态转换是 CAS，只有赢家负责落库。** 与撤回链路的 UpdateMessageStatusCAS 同源：
//     并发的 hangup / reject / sweeper 超时只有一个拿到快照，保证一通电话只落一条记录。
//
//  4. **跨键操作不追求原子性，靠 TTL 与 sweeper 自愈。** Redis Cluster 下多键 Lua 会触发
//     CROSSSLOT，故每个脚本只碰一个键；忙线锁泄漏由 TTL 收敛，ZSET 泄漏由 sweeper
//     发现状态已 ENDED 后清理。
package callstate

import (
	"fmt"
	"time"

	"IM2/pkg/proto/call"
	"IM2/pkg/redisx"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// callStateKeyPrefix 通话详情键前缀
	callStateKeyPrefix = "call:state:"
	// callBusyKeyPrefix 用户忙线锁键前缀
	callBusyKeyPrefix = "call:busy:"
	// callDeadlineKey 到期索引（全局单键；并发通话量远低于消息量，单 slot 足够，
	// 若将来成为热点可按 callID 哈希分片为 call:deadlines:{n}）
	callDeadlineKey = "call:deadlines"
)

const (
	// RingTTL 振铃窗口。被叫离线时通话在此窗口内保持 RINGING 等待其上线补投，
	// 超时由 sweeper 收敛为 MISSED / PEER_OFFLINE。
	RingTTL = 60 * time.Second

	// MaxCallDuration 单通电话时长上限。接通后 expire_at 顺延至此，
	// 防止双方进程同时消失导致状态永久停留在 CONNECTED。
	MaxCallDuration = 4 * time.Hour

	// EndedLingerTTL 终态残留时间。保留一小段供迟到的重复信令（对端 hangup 与服务端
	// 超时撞车）读到 ENDED 而非「不存在」，从而静默忽略而不是报错。
	EndedLingerTTL = 60 * time.Second

	// ClaimVisibilityTTL sweeper 认领后的可见性超时。认领只重打分不删除，
	// 认领者崩溃时条目会在此时间后重新可见，避免通话永远收敛不了。
	ClaimVisibilityTTL = 30 * time.Second
)

// State 通话状态机
type State int32

const (
	StateUnspecified State = iota
	// StateRinging 振铃中（含被叫离线、等待其上线补投的阶段）
	StateRinging
	// StateConnected 已接通
	StateConnected
	// StateEnded 已终止
	StateEnded
)

// Call 一通电话的状态快照
type Call struct {
	CallID     string
	CallerID   uint64
	CalleeID   uint64
	SessionKey string
	MediaType  call.CallMediaType
	State      State

	InviteAt    int64 // 发起时间（毫秒）
	ExpireAt    int64 // 权威到期时间（毫秒）：振铃期=ring_deadline，接通后=connected_at+MaxCallDuration
	ConnectedAt int64 // 接通时间（毫秒），未接通为 0
	EndedAt     int64 // 终止时间（毫秒）

	EndReason call.CallEndReason
}

// Duration 通话秒数。以服务端 connected_at → ended_at 计时为准，
// 不采信客户端上报的 duration（时钟不可信）。
func (c *Call) Duration() int32 {
	if c.ConnectedAt <= 0 || c.EndedAt <= c.ConnectedAt {
		return 0
	}
	return int32((c.EndedAt - c.ConnectedAt) / 1000)
}

// IsRinging 是否仍在振铃窗口内
func (c *Call) IsRinging(nowMs int64) bool {
	return c.State == StateRinging && nowMs < c.ExpireAt
}

// RemainingMs 剩余振铃毫秒数，已过期返回 0
func (c *Call) RemainingMs(nowMs int64) int64 {
	if remain := c.ExpireAt - nowMs; remain > 0 {
		return remain
	}
	return 0
}

// Store 通话状态存储。并发安全，可被多个 goroutine 共享。
type Store struct {
	client *redisx.Client
}

// NewStore 基于已有 redisx 客户端创建。
// 客户端不能配置 keyPrefix，否则键与其他服务写入的通话状态不一致。
func NewStore(client *redisx.Client) *Store {
	return &Store{client: client}
}

// NewStoreFromConf 从 Redis 配置创建（内部客户端不带 keyPrefix）
func NewStoreFromConf(conf redis.RedisConf) (*Store, error) {
	client, err := redisx.NewClient(conf)
	if err != nil {
		return nil, err
	}
	return NewStore(client), nil
}

// stateKey 通话详情键
func stateKey(callID string) string {
	return callStateKeyPrefix + callID
}

// busyKey 用户忙线锁键
func busyKey(userID uint64) string {
	return fmt.Sprintf("%s%d", callBusyKeyPrefix, userID)
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}
