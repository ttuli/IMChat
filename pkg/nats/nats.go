package nats_util

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// StreamName 落库队列 Stream
	StreamName = "WS_MESSAGES"
	// DLQStreamName 死信 Stream
	DLQStreamName = "WS_DLQ"

	// streamMaxAge 消息保留时长。
	// 消费端（Message 服务）宕机后，恢复窗口内积压的待落库消息不会被过期清理。
	streamMaxAge = 2 * time.Hour

	// streamMaxBytes 物理存储上限，防止长时间积压耗尽磁盘（超限后按最旧丢弃）。
	streamMaxBytes = 4 * 1024 * 1024 * 1024 // 4 GiB

	// streamDuplicates 发布侧去重窗口：窗口内重复发布同一 nats.MsgId 会被 JetStream
	// 折叠，不产生第二条 stream 消息。网关以 (from_user_id, client_id) 作 MsgId，
	// 因此该窗口决定了「客户端未收到 ACK 而重发同一条消息」能被拦截的时间跨度。
	//
	// 消费侧的幂等判定直接依赖它：Message 服务在首投（NumDelivered == 1）时跳过
	// Mongo 判重查询，成立的前提正是客户端重发已被本窗口拦在 stream 之外。
	// 窗口必须覆盖客户端的最长重发间隔，否则超窗重发会落成两条消息。
	// 显式声明而非依赖 NATS 默认值（2 分钟），因为默认值对离线重连偏短。
	// 受 NATS 约束 Duplicates <= MaxAge。
	streamDuplicates = 10 * time.Minute

	// dlqMaxAge 死信保留时长。死信是留给人排查和手动重放的，保留期按「人发现问题
	// 并修好根因」的时间尺度定，而不是按消费端恢复窗口（主 stream 的 2 小时）。
	// 超过该时长未处理的死信被淘汰，即永久丢失——告警必须保证它在此之前被看到。
	dlqMaxAge = 14 * 24 * time.Hour
	// dlqMaxBytes 死信存储上限。正常系统死信极少，攒满意味着大面积故障；
	// 上限防止一场事故把 NATS 的盘写满，拖垮正常落库链路。
	dlqMaxBytes = 256 * 1024 * 1024 // 256 MiB
	// dlqDuplicates 死信发布去重窗口。转存成功但 PubAck 丢失时，listener 会退避后
	// 重试转存；窗口须长于最大退避间隔，才能把这次重试折叠掉。
	dlqDuplicates = 10 * time.Minute

	// NATS 统一 Subject 与前缀定义
	NodeSubjectPrefix = "ws.node."
	BroadcastSubject  = "ws.broadcast"
	DBSubject         = "ws.db"
	DLQSubject        = "ws.dlq"
)

// InitStream 创建或校准 WS_MESSAGES Stream。
//
// Stream 中只应包含需要持久化重放的 subject（如 DBSubject 落库队列）。
// 广播/节点 subject 走 core NATS 即发即弃，纳入 Stream 只会白白落盘且无回放消费者。
func InitStream(js nats.JetStreamContext, subjects []string) error {
	return ensureStream(js, nats.StreamConfig{
		Name:       StreamName,
		Subjects:   subjects,
		Storage:    nats.FileStorage,
		MaxAge:     streamMaxAge,
		MaxBytes:   streamMaxBytes,
		Duplicates: streamDuplicates,
		Replicas:   1,
	})
}

// InitDLQStream 创建或校准 WS_DLQ 死信 Stream。
//
// 死信必须进 JetStream 而不是 core NATS：core NATS 在没有订阅者时发布照样成功、
// 消息直接消失，「转入 DLQ」就等于静默丢弃。
//
// 独立成 Stream 而不是把 DLQSubject 并进 WS_MESSAGES：两者保留策略相反——主 stream
// 只需覆盖消费端恢复窗口（2 小时）且满了淘汰最旧；而积压最严重、主 stream 在淘汰
// 的时候，恰恰是死信最多、最需要留住的时候。
func InitDLQStream(js nats.JetStreamContext) error {
	return ensureStream(js, nats.StreamConfig{
		Name:       DLQStreamName,
		Subjects:   []string{DLQSubject},
		Storage:    nats.FileStorage,
		MaxAge:     dlqMaxAge,
		MaxBytes:   dlqMaxBytes,
		Duplicates: dlqDuplicates,
		Replicas:   1,
	})
}

// ensureStream 按 want 创建 Stream；已存在时校准可变字段。
// 已存在的 Stream 配置（subjects / MaxAge / MaxBytes / Duplicates）与期望不符时
// 就地更新——每个会被调整的字段都必须纳入比较，否则改动只对全新 Stream 生效，
// 已存在的 Stream 会静默保留旧值。
func ensureStream(js nats.JetStreamContext, want nats.StreamConfig) error {
	info, err := js.StreamInfo(want.Name)
	if err == nil {
		cfg := info.Config
		if cfg.MaxAge == want.MaxAge && cfg.MaxBytes == want.MaxBytes &&
			cfg.Duplicates == want.Duplicates && sameSubjects(cfg.Subjects, want.Subjects) {
			return nil
		}
		cfg.Subjects = want.Subjects
		cfg.MaxAge = want.MaxAge
		cfg.MaxBytes = want.MaxBytes
		cfg.Duplicates = want.Duplicates
		_, err = js.UpdateStream(&cfg)
		return err
	}

	if err != nats.ErrStreamNotFound {
		return err
	}
	_, err = js.AddStream(&want)
	return err
}

// sameSubjects 比较两个 subject 集合是否一致(忽略顺序)
func sameSubjects(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}
