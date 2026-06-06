package service

import (
	"IM2/internal/apps/Llm/rpc/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/volcengine/volcengine-go-sdk/service/arkruntime"
	armodel "github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

type LlmService struct {
	client *arkruntime.Client
	c      config.LlmProviderConfig
}

func NewLlmService(c config.LlmProviderConfig) *LlmService {
	client := arkruntime.NewClientWithApiKey(
		c.ApiKey,
		arkruntime.WithBaseUrl(c.BaseURL),
	)
	return &LlmService{client: client, c: c}
}

func (s *LlmService) Suggestions(
	ctx context.Context,
	messages []Message,
) ([]string, error) {
	chatMessages := make([]*armodel.ChatCompletionMessage, 0, len(messages)+1)

	if s.c.Prompt != "" {
		chatMessages = append(chatMessages, &armodel.ChatCompletionMessage{
			Role: "system",
			Content: &armodel.ChatCompletionMessageContent{
				StringValue: &s.c.Prompt,
			},
		})
	}

	for _, msg := range messages {
		var role string
		if msg.Role == Role_ROLE_ME {
			role = "assistant"
		} else {
			role = "user"
		}
		content := msg.Content
		chatMessages = append(chatMessages, &armodel.ChatCompletionMessage{
			Role: role,
			Content: &armodel.ChatCompletionMessageContent{
				StringValue: &content, // 注意：不能用 &msg.Content，循环变量地址会变
			},
		})
	}

	req := armodel.CreateChatCompletionRequest{
		Model:    s.c.Model,
		Messages: chatMessages,
	}

	// 非流式接口，直接返回完整响应
	resp, err := s.client.CreateChatCompletion(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(resp.Choices) == 0 {
		return nil, errors.New("no choices returned")
	}

	content := resp.Choices[0].Message.Content.StringValue
	var result struct {
		Suggestions []string `json:"suggestions"`
	}
	if err := json.Unmarshal([]byte(*content), &result); err != nil {
		return nil, fmt.Errorf("parse failed: %w, raw: %s", err, *content)
	}

	return result.Suggestions, nil
}
