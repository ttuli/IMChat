package appversion

import (
	"strings"
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/configcenter/subscriber"
	"github.com/zeromicro/go-zero/core/logx"
)

// DefaultKey 存放最低版本号的默认 etcd key，值为纯版本号字符串，如 1.2.0
const DefaultKey = "config/app.min_version"

// Gate 持有当前生效的最低客户端版本，并订阅 etcd 中对应 key 的变更。
//
// 监听、断线重连、历史版本被压缩后重新监听，都交给 go-zero 的 configcenter subscriber（底层 core/discov）。
// go-zero 须 >= v1.10.2：更早的版本在同一个 key 被覆盖写入时不清理旧值，Value() 会随机读到旧值，
// 删除 key 后旧值还会一直残留。
//
// 取值规则：
//   - key 不存在、被删除或值为空：不限制（0.0.0）。删除 key 就是取消门槛的方式
//   - 值不是合法版本号：保留上一次的有效值并记录错误，不能因为一次手误把所有人挡在登录外面
type Gate struct {
	sub subscriber.Subscriber
	key string
	min atomic.Pointer[Version]
}

// NewGate 订阅 conf.Key（为空时用 DefaultKey），首次读取完成后才返回。
// etcd 连不上时，在 go-zero 的拨号超时（5 秒）后返回错误；
// 连上之后首次读取失败（如账号对该 key 无读权限、集群无 leader），
// go-zero 会一直重试并阻塞调用方，这里不另设超时。
func NewGate(conf subscriber.EtcdConf) (*Gate, error) {
	if conf.Key == "" {
		conf.Key = DefaultKey
	}
	sub, err := subscriber.NewEtcdSubscriber(conf)
	if err != nil {
		return nil, err
	}
	return newGate(sub, conf.Key)
}

func newGate(sub subscriber.Subscriber, key string) (*Gate, error) {
	g := &Gate{sub: sub, key: key}
	g.min.Store(&Version{})
	if err := sub.AddListener(g.refresh); err != nil {
		return nil, err
	}
	// 首次读取发生在订阅创建时，那时监听还没挂上，这里主动取一次
	g.refresh()
	return g, nil
}

// MinVersion 当前生效的最低版本；零值表示不限制。nil 接收者安全（未启用门槛时 Gate 为 nil）
func (g *Gate) MinVersion() Version {
	if g == nil {
		return Version{}
	}
	return *g.min.Load()
}

// refresh 在 key 变化（含删除）时由 subscriber 回调
func (g *Gate) refresh() {
	raw, err := g.sub.Value()
	if err != nil {
		logx.Errorf("[AppVersionGate] 读取 %s 失败，继续沿用 %s: %v", g.key, g.MinVersion(), err)
		return
	}
	g.apply(raw)
}

func (g *Gate) apply(raw string) {
	value := strings.TrimSpace(raw)
	if value == "" {
		g.set(Version{})
		return
	}

	v, ok := Parse(value)
	if !ok {
		logx.Errorf("[AppVersionGate] %s 的值 %q 不是合法版本号，继续沿用 %s", g.key, value, g.MinVersion())
		return
	}
	g.set(v)
}

func (g *Gate) set(v Version) {
	if old := g.min.Swap(&v); *old != v {
		if v.IsZero() {
			logx.Infof("[AppVersionGate] 已取消客户端最低版本限制")
		} else {
			logx.Infof("[AppVersionGate] 客户端最低版本: %s -> %s", *old, v)
		}
	}
}
