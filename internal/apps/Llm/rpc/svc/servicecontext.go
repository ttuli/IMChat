package svc

import (
	"time"

	"IM2/internal/apps/Llm/rpc/config"
	"IM2/internal/apps/Llm/rpc/internal/service"
)

type ServiceContext struct {
	Config     config.Config
	LlmManager *service.LlmManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	timeout := time.Duration(c.Llm.SuggestLlmProvider.Timeout) * time.Second
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	// Initialize LlmManager
	manager := service.NewLlmManager()

	doubaoSvc := service.NewDoubaoLlmService(c.Llm.SuggestLlmProvider)
	manager.Register(service.LlmServiceType_Suggest, doubaoSvc)

	return &ServiceContext{
		Config:     c,
		LlmManager: manager,
	}
}
