package lightrag

import (
	"context"
)

// SimpleEmbedder 简单的嵌入生成器
type SimpleEmbedder struct {
	dimensions int
}

func NewSimpleEmbedder(dims int) *SimpleEmbedder {
	if dims <= 0 {
		dims = 1536
	}
	return &SimpleEmbedder{dimensions: dims}
}

func (e *SimpleEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	vec := make([]float64, e.dimensions)
	// 极简实现：取前 N 个字符的 ASCII 值
	for i := 0; i < len(text) && i < e.dimensions; i++ {
		vec[i] = float64(text[i]) / 255.0
	}
	return vec, nil
}

func (e *SimpleEmbedder) Dimensions() int {
	return e.dimensions
}
