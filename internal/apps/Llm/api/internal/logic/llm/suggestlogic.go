package llm

import (
	"context"

	"IM2/internal/apps/Llm/api/svc"
	"IM2/internal/apps/Llm/api/types"
	"IM2/internal/apps/Llm/rpc/client/llm"
	llmType "IM2/internal/apps/Llm/rpc/llm"

	"github.com/zeromicro/go-zero/core/logx"
)

type SuggestLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSuggestLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SuggestLogic {
	return &SuggestLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SuggestLogic) Suggest(req *types.SuggestRequest) (resp *types.SuggestResponse, err error) {
	var msgs []*llm.Message
	for _, msg := range req.History {
		msgs = append(msgs, &llm.Message{
			Role:    llmType.Role(msg.Role),
			Content: msg.Content,
		})
	}

	res, err := l.svcCtx.LlmRpc.Suggestions(l.ctx, &llm.SuggestionReq{
		History: msgs,
	})
	if err != nil {
		return nil, err
	}

	return &types.SuggestResponse{
		Reply: res.Reply,
	}, nil
}
