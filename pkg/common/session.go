package common

import (
	"fmt"
)

// GenerateSessionID 生成单聊会话ID
// 规则: min(uid1, uid2)_max(uid1, uid2)
// 保证两个用户ID生成的会话ID一致，无需数据库查询
func GenerateSessionID(uid1, uid2 uint64) string {
	if uid1 < uid2 {
		return fmt.Sprintf("%d_%d", uid1, uid2)
	}
	return fmt.Sprintf("%d_%d", uid2, uid1)
}

// GenerateGroupSessionID 生成群聊会话ID
// 规则: group_groupID
func GenerateGroupSessionID(groupID uint64) string {
	return fmt.Sprintf("group_%d", groupID)
}
