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
		question := ""
		if len(parts) > 1 {
			question = strings.TrimSpace(strings.Split(parts[1], "\n")[0])
		}

		contextStr := ""
		if strings.Contains(prompt, "Context:") {
			cParts := strings.Split(prompt, "Context:")
			if len(cParts) > 1 {
				contextStr = strings.TrimSpace(strings.Split(cParts[1], "Question:")[0])
			}
		}

		return fmt.Sprintf("I am a simple LLM. Question: %s. Context: %s", question, contextStr), nil
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
