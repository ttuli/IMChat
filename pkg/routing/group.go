package routing

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// replaceGroupScript 原子全量替换成员集合并续期。
// ARGV[1] = ttl 秒数，ARGV[2..] = 成员 ID。
const replaceGroupScript = `
redis.call('DEL', KEYS[1])
for i = 2, #ARGV do
	redis.call('SADD', KEYS[1], ARGV[i])
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`

// addGroupMembersScript 仅在集合已存在时增量加人，保留剩余 TTL。
// 集合不存在说明尚无读取方关心该群（或已过期），由下次读取回源构建全量数据；
// 此处若无条件 SADD 会造出一个被误当作全量的残缺集合。
const addGroupMembersScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
	return 0
end
for i = 1, #ARGV do
	redis.call('SADD', KEYS[1], ARGV[i])
end
return 1
`

// removeGroupMembersScript 从集合摘除成员（集合不存在时天然无操作）
const removeGroupMembersScript = `
local n = 0
for i = 1, #ARGV do
	n = n + redis.call('SREM', KEYS[1], ARGV[i])
end
return n
`

// addUserToGroupScript 将单个用户增量写入单个群集合（同样仅写已存在的集合）。
// 单键脚本，经 pipeline 逐群执行以兼容 Redis Cluster（多键脚本要求所有 key
// 同哈希槽）；各群集合间无跨键不变量，拆分执行不影响正确性。
// ARGV[1] = 用户 ID。
const addUserToGroupScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
	return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
return 1
`

// ReplaceGroupMembers 全量替换群成员集合（回源权威数据后的原子回填）。
// ttl<=0 时使用 DefaultGroupTTL 并附加随机抖动；memberIDs 为空时仅删除集合。
func (t *Table) ReplaceGroupMembers(ctx context.Context, groupID uint64, memberIDs []uint64, ttl time.Duration) error {
	if len(memberIDs) == 0 {
		return t.DeleteGroup(ctx, groupID)
	}
	if ttl <= 0 {
		ttl = DefaultGroupTTL + time.Duration(rand.Int63n(int64(groupTTLJitter)))
	}

	args := make([]interface{}, 0, len(memberIDs)+1)
	args = append(args, int(ttl.Seconds()))
	for _, id := range memberIDs {
		args = append(args, strconv.FormatUint(id, 10))
	}
	_, err := t.client.EvalCtx(ctx, replaceGroupScript, []string{groupMemberKey(groupID)}, args...)
	return err
}

// AddGroupMembers 向群成员集合增量加人（成员关系变更后的同步维护）。
// 集合不存在时不写入并返回 applied=false，调用方可选择用权威数据全量重建
// （ReplaceGroupMembers）或留给下次读取回源兜底。
func (t *Table) AddGroupMembers(ctx context.Context, groupID uint64, userIDs ...uint64) (applied bool, err error) {
	if len(userIDs) == 0 {
		return true, nil
	}
	raw, err := t.client.EvalCtx(ctx, addGroupMembersScript,
		[]string{groupMemberKey(groupID)}, idsToArgs(userIDs)...)
	if err != nil {
		return false, err
	}
	res, _ := raw.(int64)
	return res == 1, nil
}

// RemoveGroupMembers 从群成员集合摘除成员（退群/踢人后的同步维护）
func (t *Table) RemoveGroupMembers(ctx context.Context, groupID uint64, userIDs ...uint64) error {
	if len(userIDs) == 0 {
		return nil
	}
	_, err := t.client.EvalCtx(ctx, removeGroupMembersScript,
		[]string{groupMemberKey(groupID)}, idsToArgs(userIDs)...)
	return err
}

// AddUserToGroups 将用户增量写入其所属各群的成员集合
// （查询用户群组列表后的路由维护，替代旧的 NATS 广播同步）。
func (t *Table) AddUserToGroups(ctx context.Context, userID uint64, groupIDs []uint64) error {
	if len(groupIDs) == 0 {
		return nil
	}
	uid := strconv.FormatUint(userID, 10)
	return t.client.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		for _, gid := range groupIDs {
			pipe.Eval(ctx, addUserToGroupScript, []string{groupMemberKey(gid)}, uid)
		}
		return nil
	})
}

// GetGroupMembers 读取群成员集合。集合不存在或为空返回 nil
// （群至少有群主一名成员，空集合应视为缓存 miss，由调用方回源）。
func (t *Table) GetGroupMembers(ctx context.Context, groupID uint64) ([]uint64, error) {
	raw, err := t.client.EvalCtx(ctx, `return redis.call('SMEMBERS', KEYS[1])`,
		[]string{groupMemberKey(groupID)})
	if err != nil {
		return nil, err
	}
	rows, ok := raw.([]interface{})
	if !ok {
		return nil, nil
	}
	ids := make([]uint64, 0, len(rows))
	for _, row := range rows {
		id, parseErr := strconv.ParseUint(fmt.Sprintf("%v", row), 10, 64)
		if parseErr != nil {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// DeleteGroup 删除群成员集合（解散群或显式失效）
func (t *Table) DeleteGroup(ctx context.Context, groupID uint64) error {
	_, err := t.client.DelCtx(ctx, groupMemberKey(groupID))
	return err
}

// idsToArgs 将 ID 列表转为 Lua ARGV
func idsToArgs(ids []uint64) []interface{} {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = strconv.FormatUint(id, 10)
	}
	return args
}
