package callstate

import (
	"context"
	"errors"
	"strconv"

	"IM2/pkg/proto/call"
)

// ErrCallNotFound 通话不存在（未创建 / 已过期清理）
var ErrCallNotFound = errors.New("callstate: call not found")

// ErrPeerBusy 主叫或被叫已在另一通电话中
var ErrPeerBusy = errors.New("callstate: peer busy")

// ErrStateConflict 当前状态不允许该转换（并发下的正常结果，非错误路径）
var ErrStateConflict = errors.New("callstate: state conflict")

// ─────────────────────────────────────────────────────────────────────────────
// Lua 脚本：每个脚本只操作**一个** key，兼容 Redis Cluster
// （动态拼第二个 key 会触发 CROSSSLOT）
// ─────────────────────────────────────────────────────────────────────────────

// acquireBusyScript 抢占忙线锁。已被同一通电话占用时视为成功（重入幂等）。
// 返回 1=成功，0=被其他通话占用
const acquireBusyScript = `
local cur = redis.call('GET', KEYS[1])
if cur and cur ~= ARGV[1] then
	return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
`

// releaseBusyScript 仅当锁仍属于本通电话时释放（compare-and-delete），
// 防止误删该用户随后发起的新通话的锁。
const releaseBusyScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`

// createStateScript 创建振铃态。key 已存在则拒绝（callID 冲突，不应发生）。
const createStateScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
	return 0
end
redis.call('HSET', KEYS[1],
	'caller_id', ARGV[1],
	'callee_id', ARGV[2],
	'session_key', ARGV[3],
	'media_type', ARGV[4],
	'state', ARGV[5],
	'invite_at', ARGV[6],
	'expire_at', ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return 1
`

// acceptScript RINGING → CONNECTED 的 CAS。
// 同时把 expire_at 顺延至通话时长上限 —— **expire_at 是权威**，
// ZSET 续期失败也只会让 sweeper 多查一次，不会误杀已接通的通话。
// 返回 1=本次转换成功，0=状态不符（已被 reject/cancel/超时抢先）
const acceptScript = `
if redis.call('HGET', KEYS[1], 'state') ~= ARGV[1] then
	return 0
end
redis.call('HSET', KEYS[1],
	'state', ARGV[2],
	'connected_at', ARGV[3],
	'expire_at', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
`

// terminateScript 任意活跃态 → ENDED 的 CAS，并返回终态全量快照。
//
// **只有赢家拿到非空返回**，由其负责落库通话记录 —— 与撤回链路
// UpdateMessageStatusCAS 同一套「CAS 拿到才承担副作用」的写法，
// 保证并发的 hangup / reject / sweeper 超时不会写出多条记录。
const terminateScript = `
local st = redis.call('HGET', KEYS[1], 'state')
if not st or st == ARGV[1] then
	return nil
end
redis.call('HSET', KEYS[1],
	'state', ARGV[1],
	'end_reason', ARGV[2],
	'ended_at', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return redis.call('HGETALL', KEYS[1])
`

// claimExpiredScript 认领到期条目：重打分而非删除（可见性超时语义）。
// 认领者崩溃时条目会在 ClaimVisibilityTTL 后重新可见，
// 真正的移除由 Terminate 成功后的 ZREM 完成。
const claimExpiredScript = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for _, id in ipairs(due) do
	redis.call('ZADD', KEYS[1], ARGV[2], id)
end
return due
`

// ─────────────────────────────────────────────────────────────────────────────
// 操作
// ─────────────────────────────────────────────────────────────────────────────

// Create 建立振铃态。
//
// **调用顺序契约（漏投防护）**：本方法返回成功后，调用方才可以推送 CALL_INVITE。
// 反序（先查在线、离线才写状态）会与被叫的「注册路由 → 查 pending」交叉出漏投窗口：
//
//	A查路由(未注册) → B注册 → B查pending(空) → A写状态   ← B 永远收不到
//
// 正序下两条投递路径至少一条命中（B 注册早→实时推送；B 注册晚→查 pending 命中），
// 两条都命中时由客户端按 call_id 幂等去重。
//
// 内部写入顺序：忙线锁 → ZSET 到期索引 → 状态 HASH。
// ZSET 先于 HASH 是有意的：中途失败只会留下一个「指向不存在状态」的索引项，
// sweeper 读到空状态直接清理，无副作用；反序则会产生无人收敛的孤儿通话。
func (s *Store) Create(ctx context.Context, c *Call) error {
	now := nowMs()
	if c.InviteAt == 0 {
		c.InviteAt = now
	}
	if c.ExpireAt == 0 {
		c.ExpireAt = now + RingTTL.Milliseconds()
	}
	c.State = StateRinging

	// 1. 抢占双方忙线锁。先被叫（争抢方），失败即忙线，此时尚无任何状态需要清理。
	ok, err := s.acquireBusy(ctx, c.CalleeID, c.CallID, c.ExpireAt-now)
	if err != nil {
		return err
	}
	if !ok {
		return ErrPeerBusy
	}
	ok, err = s.acquireBusy(ctx, c.CallerID, c.CallID, c.ExpireAt-now)
	if err != nil || !ok {
		_ = s.releaseBusy(ctx, c.CalleeID, c.CallID)
		if err != nil {
			return err
		}
		return ErrPeerBusy
	}

	// 2. 到期索引（先于状态写入，见方法注释）
	if err := s.client.ZAddCtx(ctx, callDeadlineKey, float64(c.ExpireAt), c.CallID); err != nil {
		s.releaseBoth(ctx, c)
		return err
	}

	// 3. 状态 HASH
	raw, err := s.client.EvalCtx(ctx, createStateScript, []string{stateKey(c.CallID)},
		strconv.FormatUint(c.CallerID, 10),
		strconv.FormatUint(c.CalleeID, 10),
		c.SessionKey,
		strconv.Itoa(int(c.MediaType)),
		strconv.Itoa(int(StateRinging)),
		strconv.FormatInt(c.InviteAt, 10),
		strconv.FormatInt(c.ExpireAt, 10),
		strconv.FormatInt(c.ExpireAt-now+EndedLingerTTL.Milliseconds(), 10),
	)
	if err != nil {
		s.releaseBoth(ctx, c)
		return err
	}
	if n, _ := raw.(int64); n != 1 {
		s.releaseBoth(ctx, c)
		return ErrStateConflict
	}
	return nil
}

// Get 读取通话快照，不存在返回 ErrCallNotFound
func (s *Store) Get(ctx context.Context, callID string) (*Call, error) {
	raw, err := s.client.EvalCtx(ctx, `return redis.call('HGETALL', KEYS[1])`,
		[]string{stateKey(callID)})
	if err != nil {
		return nil, err
	}
	c := parseCall(callID, raw)
	if c == nil {
		return nil, ErrCallNotFound
	}
	return c, nil
}

// PendingFor 查询该用户是否有**仍在振铃的来电**。
//
// 用于客户端建连/重连后的补投。两次单键读而非 Lua 拼键，兼容 Cluster
// （与 routing.LookupUser 同一处理方式）。
//
// 注意忙线锁主叫被叫都会占，故必须校验 callee == userID，
// 否则主叫自己会查出自己拨出的那通电话并弹出接听界面。
//
// 本方法只负责「是否仍在振铃」；**主叫是否还活着由调用方查 routing.Table 复核**，
// 不在本包做，避免状态平面反向依赖路由表。
func (s *Store) PendingFor(ctx context.Context, userID uint64) (*Call, error) {
	callID, err := s.client.GetCtx(ctx, busyKey(userID))
	if err != nil || callID == "" {
		return nil, err
	}

	c, err := s.Get(ctx, callID)
	if err != nil {
		if errors.Is(err, ErrCallNotFound) {
			// 忙线锁残留（状态已过期清理）：顺手回收，不影响本次结果
			_ = s.releaseBusy(ctx, userID, callID)
			return nil, nil
		}
		return nil, err
	}

	if c.CalleeID != userID || !c.IsRinging(nowMs()) {
		return nil, nil
	}
	return c, nil
}

// Accept RINGING → CONNECTED。
// 返回 ErrStateConflict 表示已被 reject / cancel / 振铃超时抢先，调用方应静默忽略。
func (s *Store) Accept(ctx context.Context, callID string, calleeID uint64) (*Call, error) {
	c, err := s.Get(ctx, callID)
	if err != nil {
		return nil, err
	}
	if c.CalleeID != calleeID {
		return nil, ErrStateConflict
	}

	now := nowMs()
	expireAt := now + MaxCallDuration.Milliseconds()
	raw, err := s.client.EvalCtx(ctx, acceptScript, []string{stateKey(callID)},
		strconv.Itoa(int(StateRinging)),
		strconv.Itoa(int(StateConnected)),
		strconv.FormatInt(now, 10),
		strconv.FormatInt(expireAt, 10),
		strconv.FormatInt(MaxCallDuration.Milliseconds()+EndedLingerTTL.Milliseconds(), 10),
	)
	if err != nil {
		return nil, err
	}
	if n, _ := raw.(int64); n != 1 {
		return nil, ErrStateConflict
	}

	// 到期索引顺延。失败无害：expire_at 才是权威，sweeper 复核后会自行重打分。
	_ = s.client.ZAddCtx(ctx, callDeadlineKey, float64(expireAt), callID)

	c.State = StateConnected
	c.ConnectedAt = now
	c.ExpireAt = expireAt
	return c, nil
}

// Terminate 任意活跃态 → ENDED，返回终态快照供落库。
//
// **返回 (nil, nil) 表示本次调用不是赢家**（通话已被其他路径终止），
// 调用方必须据此跳过落库，否则一通电话会写出多条记录。
func (s *Store) Terminate(ctx context.Context, callID string, reason call.CallEndReason) (*Call, error) {
	now := nowMs()
	raw, err := s.client.EvalCtx(ctx, terminateScript, []string{stateKey(callID)},
		strconv.Itoa(int(StateEnded)),
		strconv.Itoa(int(reason)),
		strconv.FormatInt(now, 10),
		strconv.FormatInt(EndedLingerTTL.Milliseconds(), 10),
	)
	if err != nil {
		return nil, err
	}

	c := parseCall(callID, raw)
	if c == nil {
		// 已是 ENDED 或已被清理：非赢家，静默返回
		return nil, nil
	}

	// 清理索引与锁。失败均可自愈：ZSET 残留由 sweeper 读到 ENDED 后清理，
	// 忙线锁残留由 TTL 收敛。
	s.removeDeadline(ctx, callID)
	s.releaseBoth(ctx, c)
	return c, nil
}

// ClaimExpired 认领到期通话，返回 callID 列表交由 sweeper 逐个收敛。
//
// 认领采用可见性超时（重打分而非删除）：认领者崩溃时条目会重新可见，
// 不会出现「通话永远不收敛、未接记录永远不落库」。
//
// 调用方对每个 callID 应：Get → 若 now < ExpireAt 说明是 Accept 续期后的 ZSET 漂移，
// 重打分跳过；否则按当前状态 Terminate（RINGING→MISSED/PEER_OFFLINE，CONNECTED→COMPLETED）。
func (s *Store) ClaimExpired(ctx context.Context, limit int) ([]string, error) {
	now := nowMs()
	raw, err := s.client.EvalCtx(ctx, claimExpiredScript, []string{callDeadlineKey},
		strconv.FormatInt(now, 10),
		strconv.FormatInt(now+ClaimVisibilityTTL.Milliseconds(), 10),
		strconv.Itoa(limit),
	)
	if err != nil {
		return nil, err
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, nil
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			ids = append(ids, s)
		}
	}
	return ids, nil
}

// RescheduleDeadline 重设到期索引。sweeper 复核发现 expire_at 尚未到达时调用
// （Accept 续期后 ZSET 未及时更新的漂移修正）。
func (s *Store) RescheduleDeadline(ctx context.Context, callID string, expireAt int64) error {
	return s.client.ZAddCtx(ctx, callDeadlineKey, float64(expireAt), callID)
}

// RemoveDeadline 从到期索引移除（通话已收敛）
func (s *Store) RemoveDeadline(ctx context.Context, callID string) {
	s.removeDeadline(ctx, callID)
}

// ─────────────────────────────────────────────────────────────────────────────
// 内部辅助
// ─────────────────────────────────────────────────────────────────────────────

func (s *Store) acquireBusy(ctx context.Context, userID uint64, callID string, ttlMs int64) (bool, error) {
	if ttlMs <= 0 {
		ttlMs = RingTTL.Milliseconds()
	}
	raw, err := s.client.EvalCtx(ctx, acquireBusyScript, []string{busyKey(userID)},
		callID, strconv.FormatInt(ttlMs, 10))
	if err != nil {
		return false, err
	}
	n, _ := raw.(int64)
	return n == 1, nil
}

func (s *Store) releaseBusy(ctx context.Context, userID uint64, callID string) error {
	_, err := s.client.EvalCtx(ctx, releaseBusyScript, []string{busyKey(userID)}, callID)
	return err
}

func (s *Store) releaseBoth(ctx context.Context, c *Call) {
	_ = s.releaseBusy(ctx, c.CalleeID, c.CallID)
	_ = s.releaseBusy(ctx, c.CallerID, c.CallID)
}

func (s *Store) removeDeadline(ctx context.Context, callID string) {
	_, _ = s.client.EvalCtx(ctx, `return redis.call('ZREM', KEYS[1], ARGV[1])`,
		[]string{callDeadlineKey}, callID)
}

// parseCall 将 HGETALL 的扁平返回解析为 Call，空结果返回 nil
func parseCall(callID string, raw interface{}) *Call {
	items, ok := raw.([]interface{})
	if !ok || len(items) < 2 {
		return nil
	}
	m := make(map[string]string, len(items)/2)
	for i := 0; i+1 < len(items); i += 2 {
		k, ok1 := items[i].(string)
		v, ok2 := items[i+1].(string)
		if ok1 && ok2 {
			m[k] = v
		}
	}
	if len(m) == 0 {
		return nil
	}

	atoi64 := func(key string) int64 {
		n, _ := strconv.ParseInt(m[key], 10, 64)
		return n
	}
	atou64 := func(key string) uint64 {
		n, _ := strconv.ParseUint(m[key], 10, 64)
		return n
	}

	return &Call{
		CallID:      callID,
		CallerID:    atou64("caller_id"),
		CalleeID:    atou64("callee_id"),
		SessionKey:  m["session_key"],
		MediaType:   call.CallMediaType(atoi64("media_type")),
		State:       State(atoi64("state")),
		InviteAt:    atoi64("invite_at"),
		ExpireAt:    atoi64("expire_at"),
		ConnectedAt: atoi64("connected_at"),
		EndedAt:     atoi64("ended_at"),
		EndReason:   call.CallEndReason(atoi64("end_reason")),
	}
}
