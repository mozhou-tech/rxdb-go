package lightrag

import (
	"context"
	"fmt"
	"strings"
)

// SimpleLLM 简单的 LLM 实现，仅用于演示
type SimpleLLM struct {
}

func (l *SimpleLLM) Complete(ctx context.Context, prompt string) (string, error) {
	if strings.Contains(prompt, "Question:") {
		parts := strings.Split(prompt, "Question:")
		if len(parts) > 1 {
			return fmt.Sprintf("I am a simple LLM. You asked: %s. Based on the context provided, I cannot give a complex answer yet.", strings.TrimSpace(parts[1])), nil
		}
	}
	return "Simple LLM response", nil
}

// OpenAIConfig OpenAI 配置
type OpenAIConfig struct {
	APIKey  string
	BaseURL string
	Model   string
}

// TODO: 实现真正的 OpenAI LLM
