package nats_util

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	streamName = "WS_MESSAGES"

	// streamMaxAge 消息保留时长。
	// 消费端（Message 服务）宕机后，恢复窗口内积压的待落库消息不会被过期清理。
	streamMaxAge = 2 * time.Hour

	// streamMaxBytes 物理存储上限，防止长时间积压耗尽磁盘（超限后按最旧丢弃）。
	streamMaxBytes = 4 * 1024 * 1024 * 1024 // 4 GiB
)

// InitStream 创建或校准 WS_MESSAGES Stream。
//
// Stream 中只应包含需要持久化重放的 subject（如 DBSubject 落库队列）。
// 广播/节点 subject 走 core NATS 即发即弃，纳入 Stream 只会白白落盘且无回放消费者。
// 若已存在的 Stream 配置（subjects / MaxAge / MaxBytes）与期望不符，就地更新校准。
func InitStream(js nats.JetStreamContext, subjects []string) error {
	info, err := js.StreamInfo(streamName)
	if err == nil {
		cfg := info.Config
		if cfg.MaxAge == streamMaxAge && cfg.MaxBytes == streamMaxBytes && sameSubjects(cfg.Subjects, subjects) {
			return nil
		}
		cfg.Subjects = subjects
		cfg.MaxAge = streamMaxAge
		cfg.MaxBytes = streamMaxBytes
		_, err = js.UpdateStream(&cfg)
		return err
	}

	if err != nats.ErrStreamNotFound {
		return err
	}

	// 不存在则创建
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
		Storage:  nats.FileStorage,
		MaxAge:   streamMaxAge,
		MaxBytes: streamMaxBytes,
		Replicas: 1,
	})
	return err
}

// sameSubjects 比较两个 subject 集合是否一致（忽略顺序）
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
