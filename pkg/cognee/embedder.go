package cognee

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	openai "github.com/sashabaranov/go-openai"
)

// SimpleEmbedder 简单的嵌入生成器（用于演示）
// 实际使用时应该使用真实的嵌入模型（如 OpenAI, HuggingFace 等）
type SimpleEmbedder struct {
	dimensions int
}

// NewSimpleEmbedder 创建简单的嵌入生成器
func NewSimpleEmbedder(dimensions int) *SimpleEmbedder {
	if dimensions <= 0 {
		dimensions = 384 // 默认维度
	}
	return &SimpleEmbedder{
		dimensions: dimensions,
	}
}

// Embed 将文本转换为向量嵌入
// 这是一个简单的实现，实际应该使用真实的嵌入模型
func (e *SimpleEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	// 简单的哈希向量化（仅用于演示）
	// 实际应该使用真实的嵌入模型，如：
	// - OpenAI text-embedding-ada-002
	// - HuggingFace sentence-transformers
	// - 本地模型如 BGE, M3E 等

	vector := make([]float64, e.dimensions)

	// 简单的字符哈希向量化
	for i := 0; i < len(text) && i < e.dimensions; i++ {
		vector[i] = float64(text[i]) / 255.0
	}

	// 填充剩余维度
	for i := len(text); i < e.dimensions; i++ {
		vector[i] = 0.0
	}

	// 归一化
	sum := 0.0
	for _, v := range vector {
		sum += v * v
	}
	if sum > 0 {
		norm := 1.0 / (1.0 + sum)
		for i := range vector {
			vector[i] *= norm
		}
	}

	return vector, nil
}

// Dimensions 返回向量维度
func (e *SimpleEmbedder) Dimensions() int {
	return e.dimensions
}

// OpenAIEmbedder OpenAI 嵌入生成器
type OpenAIEmbedder struct {
	client     *openai.Client
	model      string
	dimensions int
}

// NewOpenAIEmbedder 创建 OpenAI 嵌入生成器
// config 需要包含:
//   - "api_key" (string): OpenAI API 密钥（必需）
//   - "model" (string): 模型名称，默认为 "text-embedding-ada-002"
//   - "base_url" (string): API 基础 URL，默认为 "https://api.openai.com/v1"（可选，用于自定义端点）
func NewOpenAIEmbedder(config map[string]interface{}) (*OpenAIEmbedder, error) {
	apiKey, ok := config["api_key"].(string)
	if !ok || apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key is required")
	}

	model := "text-embedding-3-small" // 默认使用 text-embedding-3-small
	if m, ok := config["model"].(string); ok && m != "" {
		model = m
	}

	// 创建 OpenAI 客户端配置
	clientConfig := openai.DefaultConfig(apiKey)

	// 如果提供了自定义 base URL，则使用它（用于兼容其他 OpenAI API 服务）
	if baseURL, ok := config["base_url"].(string); ok && baseURL != "" {
		clientConfig.BaseURL = baseURL
	}

	// 创建客户端
	client := openai.NewClientWithConfig(clientConfig)

	// 根据模型确定维度
	dimensions := 1536 // text-embedding-ada-002 的默认维度
	switch model {
	case "text-embedding-3-small":
		dimensions = 1536
	case "text-embedding-3-large":
		dimensions = 3072
	case "text-embedding-ada-002":
		dimensions = 1536
	case "text-embedding-v4":
		dimensions = 1024 // text-embedding-v4 默认维度为 1024
	default:
		// 如果提供了自定义维度，使用它
		if d, ok := config["dimensions"].(int); ok && d > 0 {
			dimensions = d
		}
	}

	return &OpenAIEmbedder{
		client:     client,
		model:      model,
		dimensions: dimensions,
	}, nil
}

// Embed 将文本转换为向量嵌入
func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if text == "" {
		return make([]float64, e.dimensions), nil
	}

	// 构建嵌入请求
	req := openai.EmbeddingRequest{
		Input: []string{text},
		Model: openai.EmbeddingModel(e.model),
	}

	// 对于 embedding-3 模型，可以指定维度
	if e.dimensions > 0 && (e.model == "text-embedding-3-small" || e.model == "text-embedding-3-large") {
		req.Dimensions = e.dimensions
	}

	// 调用 OpenAI API
	resp, err := e.client.CreateEmbeddings(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create embedding: %w", err)
	}

	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("no embedding data in response")
	}

	// 返回第一个嵌入向量
	embedding := resp.Data[0].Embedding

	// 更新实际维度（如果与预期不同）
	if len(embedding) != e.dimensions {
		e.dimensions = len(embedding)
	}

	// 将 []float32 转换为 []float64
	result := make([]float64, len(embedding))
	for i, v := range embedding {
		result[i] = float64(v)
	}

	return result, nil
}

// Dimensions 返回向量维度
func (e *OpenAIEmbedder) Dimensions() int {
	return e.dimensions
}

// HuggingFaceEmbedder HuggingFace 嵌入生成器
type HuggingFaceEmbedder struct {
	apiKey     string
	model      string
	dimensions int
	baseURL    string
	httpClient *http.Client
}

// NewHuggingFaceEmbedder 创建 HuggingFace 嵌入生成器
// config 需要包含:
//   - "api_key" (string): HuggingFace API 密钥（必需）
//   - "model" (string): 模型名称，默认为 "sentence-transformers/all-MiniLM-L6-v2"
//   - "base_url" (string): API 基础 URL，默认为 "https://api-inference.huggingface.co"
func NewHuggingFaceEmbedder(config map[string]interface{}) (*HuggingFaceEmbedder, error) {
	apiKey, ok := config["api_key"].(string)
	if !ok || apiKey == "" {
		return nil, fmt.Errorf("HuggingFace API key is required")
	}

	model := "sentence-transformers/all-MiniLM-L6-v2"
	if m, ok := config["model"].(string); ok && m != "" {
		model = m
	}

	baseURL := "https://api-inference.huggingface.co"
	if u, ok := config["base_url"].(string); ok && u != "" {
		baseURL = u
	}

	// 根据常见模型确定维度
	dimensions := 384 // all-MiniLM-L6-v2 的默认维度
	if strings.Contains(model, "all-mpnet-base-v2") {
		dimensions = 768
	} else if strings.Contains(model, "all-MiniLM-L12-v2") {
		dimensions = 384
	} else if d, ok := config["dimensions"].(int); ok && d > 0 {
		dimensions = d
	}

	return &HuggingFaceEmbedder{
		apiKey:     apiKey,
		model:      model,
		dimensions: dimensions,
		baseURL:    baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second, // HuggingFace 可能需要更长时间
		},
	}, nil
}

// Embed 将文本转换为向量嵌入
func (e *HuggingFaceEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if text == "" {
		return make([]float64, e.dimensions), nil
	}

	// 构建请求体
	requestBody := map[string]interface{}{
		"inputs": text,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// 创建 HTTP 请求
	url := fmt.Sprintf("%s/pipeline/feature-extraction/%s", e.baseURL, e.model)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.apiKey)

	// 发送请求
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		// HuggingFace 可能返回 503 表示模型正在加载
		if resp.StatusCode == http.StatusServiceUnavailable {
			return nil, fmt.Errorf("HuggingFace model is loading, please try again in a few seconds")
		}
		return nil, fmt.Errorf("HuggingFace API error (status %d): %s", resp.StatusCode, string(body))
	}

	// 解析响应 - HuggingFace 返回的是二维数组（即使只有一个输入）
	var embeddings [][]float64
	if err := json.Unmarshal(body, &embeddings); err != nil {
		// 如果解析失败，尝试解析为一维数组
		var embedding []float64
		if err2 := json.Unmarshal(body, &embedding); err2 != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %w (also tried 1D array: %v)", err, err2)
		}
		return embedding, nil
	}

	if len(embeddings) == 0 {
		return nil, fmt.Errorf("no embedding data in response")
	}

	// 返回第一个嵌入向量
	result := embeddings[0]
	e.dimensions = len(result) // 更新实际维度

	return result, nil
}

// Dimensions 返回向量维度
func (e *HuggingFaceEmbedder) Dimensions() int {
	return e.dimensions
}

// EmbedderFactory 嵌入生成器工厂函数类型
type EmbedderFactory func() (Embedder, error)

// CreateEmbedder 创建嵌入生成器
// 可以根据配置选择不同的嵌入模型
func CreateEmbedder(embedderType string, config map[string]interface{}) (Embedder, error) {
	switch embedderType {
	case "simple":
		dimensions := 384
		if d, ok := config["dimensions"].(int); ok {
			dimensions = d
		}
		return NewSimpleEmbedder(dimensions), nil
	case "openai":
		return NewOpenAIEmbedder(config)
	case "huggingface":
		return NewHuggingFaceEmbedder(config)
	default:
		return NewSimpleEmbedder(384), nil
	}
}
