package service

import (
	"IM2/pkg/xerr"
	"context"
)

// LlmManager 管理多个 LlmService 模型
type LlmManager struct {
	services map[LlmServiceType]LlmService
}

func NewLlmManager() *LlmManager {
	return &LlmManager{
		services: make(map[LlmServiceType]LlmService),
	}
}

// Register 注册一个模型服务
func (m *LlmManager) Register(st LlmServiceType, svc LlmService) {
	m.services[st] = svc
}

// ChatStream 选择对应的模型服务发起流式对话
func (m *LlmManager) Suggestions(ctx context.Context, messages []Message) ([]string, error) {
	svc, ok := m.services[LlmServiceType_Suggest]
	if !ok {
		return nil, xerr.New(xerr.ErrInternalServer, "模型未注册")
	}
	reply, err := svc.Suggestions(ctx, messages)
	if err != nil {
		return nil, xerr.Wrap(err, xerr.ErrInternalServer, "请求失败")
	}
	return reply, nil
}
