package svc

import (
	"time"

	"IM2/internal/apps/Llm/rpc/config"
	"IM2/internal/apps/Llm/rpc/internal/service"
)

type ServiceContext struct {
	Config     config.Config
	LlmService *service.LlmService
}

func NewServiceContext(c config.Config) *ServiceContext {
	timeout := time.Duration(c.Llm.SuggestLlmProvider.Timeout) * time.Second
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	// Initialize LlmService
	doubaoSvc := service.NewLlmService(c.Llm.SuggestLlmProvider)

	return &ServiceContext{
		Config:     c,
		LlmService: doubaoSvc,
	}
}
