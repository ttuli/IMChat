package llmlogic

import (
	"context"

	"IM2/internal/apps/Llm/rpc/internal/service"
	"IM2/internal/apps/Llm/rpc/llm"
	"IM2/internal/apps/Llm/rpc/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type SuggestionsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSuggestionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SuggestionsLogic {
	return &SuggestionsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SuggestionsLogic) Suggestions(in *llm.SuggestionReq) (*llm.SuggestionResp, error) {
	messages := make([]service.Message, 0, len(in.History))
	for _, m := range in.History {
		messages = append(messages, service.Message{
			Role:    service.Role(m.Role),
			Content: m.Content,
		})
	}

	reply, err := l.svcCtx.LlmManager.Suggestions(l.ctx, messages)
	if err != nil {
		return nil, err
	}

	return &llm.SuggestionResp{
		Reply: reply,
	}, nil
}
