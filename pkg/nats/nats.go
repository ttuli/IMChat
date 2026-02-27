package nats_util

import (
	"time"

	"github.com/nats-io/nats.go"
)

func InitStream(js nats.JetStreamContext, subjects []string) error {
	// 先查询是否已存在
	info, err := js.StreamInfo("WS_MESSAGES")
	if err == nil {
		// Stream 已存在，检查 subjects 是否都已包含
		existing := make(map[string]bool)
		for _, s := range info.Config.Subjects {
			existing[s] = true
		}

		// 找出缺少的 subjects
		var missing []string
		for _, s := range subjects {
			if !existing[s] {
				missing = append(missing, s)
			}
		}

		if len(missing) == 0 {
			return nil
		}

		// 更新 Stream，补充缺少的 subjects
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     "WS_MESSAGES",
			Subjects: append(info.Config.Subjects, missing...),
			Storage:  nats.FileStorage,
			MaxAge:   10 * time.Minute,
			Replicas: 1,
		})
		return err
	}

	if err != nats.ErrStreamNotFound {
		return err
	}

	// 不存在则创建
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "WS_MESSAGES",
		Subjects: subjects,
		Storage:  nats.FileStorage,
		MaxAge:   10 * time.Minute,
		Replicas: 1,
	})
	return err
}
