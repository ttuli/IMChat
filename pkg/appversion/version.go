// Package appversion 客户端版本：最低版本门槛与最新安装包地址。
//
// 允许的最低客户端版本存放在 etcd 的独立 key 中，Gate 监听该 key 并实时生效，
// 供 Auth 服务的版本中间件判断是否要求客户端强制更新。
// ReleaseFeed 读取客户端自动更新用的更新源，给下载入口提供最新安装包地址。
package appversion

import (
	"fmt"
	"strconv"
	"strings"
)

// Header 客户端上报版本号的请求头
const Header = "X-App-Version"

// Version 语义化版本的 MAJOR.MINOR.PATCH 部分；预发布、构建元数据后缀忽略。
// 零值 0.0.0 同时表示「未设置门槛」与「版本未知的旧客户端」。
type Version struct {
	Major, Minor, Patch int
}

// Parse 解析形如 1.2.3 / v1.2.3 / 1.2.3-beta.1 的版本号，非法时返回 false
func Parse(s string) (Version, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "v"), "V")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}

	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return Version{}, false
	}
	nums := make([]int, 3)
	for i, p := range parts {
		// Atoi 会接受 "+1"、"-1"，这里只允许纯数字
		if p == "" || strings.TrimLeft(p, "0123456789") != "" {
			return Version{}, false
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return Version{}, false
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, true
}

// Compare 小于、等于、大于 o 时分别返回 -1、0、1
func (v Version) Compare(o Version) int {
	for _, d := range [...]int{v.Major - o.Major, v.Minor - o.Minor, v.Patch - o.Patch} {
		if d < 0 {
			return -1
		}
		if d > 0 {
			return 1
		}
	}
	return 0
}

// Less 是否低于 o
func (v Version) Less(o Version) bool {
	return v.Compare(o) < 0
}

// IsZero 是否为 0.0.0
func (v Version) IsZero() bool {
	return v == Version{}
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
