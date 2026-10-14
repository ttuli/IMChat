package svc

import (
	"fmt"
	"hash/crc32"
	"strconv"
)

// maxNodeID 节点号上限：雪花 ID 与 Lamport seq 的节点位都是 10 bits
const maxNodeID = 1023

// 节点号来源，启动时打进日志，便于确认各副本的节点号是怎么来的
const (
	nodeSourceEnv      = "env NODE_ID"
	nodeSourceConfig   = "config SnowflakeNodeID"
	nodeSourceHostname = "hostname hash"
)

// resolveNodeID 决定本实例的节点号，雪花发号器（msg_id / 新会话 session_id）
// 与 Lamport seq 分配器共用。
//
// 多副本时节点号必须两两不同：雪花 ID 每进入新的一毫秒序号都从 0 开始，
// 两个同号实例只要在同一毫秒各发一个号就完全相同。
//
// 取值优先级：
//  1. 环境变量 NODE_ID：由编排层按实例注入（K8s 用 StatefulSet 的 pod-index 标签），
//     每个副本天然不同，是多副本部署唯一可靠的来源；设置了但非法时直接报错，
//     不悄悄退回哈希。0 是合法值（StatefulSet 第一个 Pod 的序号）。
//  2. 配置 SnowflakeNodeID：所有副本共享同一份配置，只适合单实例部署；0 表示未配置。
//  3. 按主机名哈希：仅供本地开发兜底，不同主机名可能哈希到同一个号。
func resolveNodeID(configured int64, getenv func(string) string, hostname func() (string, error)) (int64, string, error) {
	if v := getenv("NODE_ID"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil || id < 0 || id > maxNodeID {
			return 0, "", fmt.Errorf("环境变量 NODE_ID=%q 非法，应为 0-%d 的整数", v, maxNodeID)
		}
		return id, nodeSourceEnv, nil
	}
	if configured != 0 {
		if configured < 0 || configured > maxNodeID {
			return 0, "", fmt.Errorf("配置 SnowflakeNodeID=%d 超出范围 0-%d", configured, maxNodeID)
		}
		return configured, nodeSourceConfig, nil
	}
	host, err := hostname()
	if err != nil {
		return 0, "", fmt.Errorf("未设置 NODE_ID 且获取主机名失败: %w", err)
	}
	return int64(crc32.ChecksumIEEE([]byte("msg-"+host)) % (maxNodeID + 1)), nodeSourceHostname, nil
}
