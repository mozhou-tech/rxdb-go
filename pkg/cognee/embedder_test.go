package cognee

import (
	"context"
	"testing"
)

func TestSimpleEmbedder_NewSimpleEmbedder(t *testing.T) {
	tests := []struct {
		name       string
		dimensions int
		want       int
	}{
		{
			name:       "正常维度",
			dimensions: 384,
			want:       384,
		},
		{
			name:       "零维度应使用默认值",
			dimensions: 0,
			want:       384,
		},
		{
			name:       "负数维度应使用默认值",
			dimensions: -1,
			want:       384,
		},
		{
			name:       "大维度",
			dimensions: 1536,
			want:       1536,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder := NewSimpleEmbedder(tt.dimensions)
			if embedder == nil {
				t.Fatal("NewSimpleEmbedder returned nil")
			}
			if embedder.Dimensions() != tt.want {
				t.Errorf("Expected dimensions %d, got %d", tt.want, embedder.Dimensions())
			}
		})
	}
}

func TestSimpleEmbedder_Embed(t *testing.T) {
	ctx := context.Background()
	embedder := NewSimpleEmbedder(384)

	tests := []struct {
		name      string
		text      string
		wantError bool
	}{
		{
			name:      "正常文本",
			text:      "这是一个测试文本",
			wantError: false,
		},
		{
			name:      "空文本",
			text:      "",
			wantError: false,
		},
		{
			name:      "长文本",
			text:      "这是一个很长的测试文本，用来测试嵌入生成器是否能正确处理较长的输入内容。",
			wantError: false,
		},
		{
			name:      "英文文本",
			text:      "This is a test text in English",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vector, err := embedder.Embed(ctx, tt.text)
			if (err != nil) != tt.wantError {
				t.Errorf("Embed() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError {
				if len(vector) != embedder.Dimensions() {
					t.Errorf("Expected vector length %d, got %d", embedder.Dimensions(), len(vector))
				}
				// 检查向量是否归一化（所有值应该在合理范围内）
				for i, v := range vector {
					if v < 0 || v > 1 {
						t.Errorf("Vector[%d] = %f, expected value between 0 and 1", i, v)
					}
				}
			}
		})
	}
}

func TestSimpleEmbedder_Dimensions(t *testing.T) {
	embedder := NewSimpleEmbedder(1536)
	if embedder.Dimensions() != 1536 {
		t.Errorf("Expected dimensions 1536, got %d", embedder.Dimensions())
	}
}

func TestSimpleEmbedder_Consistency(t *testing.T) {
	ctx := context.Background()
	embedder := NewSimpleEmbedder(384)
	text := "测试一致性"

	// 多次调用应该产生相同的结果
	vector1, err1 := embedder.Embed(ctx, text)
	if err1 != nil {
		t.Fatalf("First embed failed: %v", err1)
	}

	vector2, err2 := embedder.Embed(ctx, text)
	if err2 != nil {
		t.Fatalf("Second embed failed: %v", err2)
	}

	if len(vector1) != len(vector2) {
		t.Fatalf("Vector lengths differ: %d vs %d", len(vector1), len(vector2))
	}

	for i := range vector1 {
		if vector1[i] != vector2[i] {
			t.Errorf("Vectors differ at index %d: %f vs %f", i, vector1[i], vector2[i])
		}
	}
}

func TestNewOpenAIEmbedder(t *testing.T) {
	tests := []struct {
		name      string
		config    map[string]interface{}
		wantError bool
	}{
		{
			name: "有效配置",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantError: false,
		},
		{
			name: "缺少 API 密钥",
			config: map[string]interface{}{
				"model": "text-embedding-ada-002",
			},
			wantError: true,
		},
		{
			name: "空 API 密钥",
			config: map[string]interface{}{
				"api_key": "",
			},
			wantError: true,
		},
		{
			name: "自定义模型",
			config: map[string]interface{}{
				"api_key": "test-key",
				"model":   "text-embedding-3-small",
			},
			wantError: false,
		},
		{
			name: "自定义 base URL",
			config: map[string]interface{}{
				"api_key":  "test-key",
				"base_url": "https://custom-api.com/v1",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder, err := NewOpenAIEmbedder(tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("NewOpenAIEmbedder() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && embedder == nil {
				t.Error("Expected embedder, got nil")
			}
		})
	}
}

func TestNewHuggingFaceEmbedder(t *testing.T) {
	tests := []struct {
		name      string
		config    map[string]interface{}
		wantError bool
	}{
		{
			name: "有效配置",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantError: false,
		},
		{
			name: "缺少 API 密钥",
			config: map[string]interface{}{
				"model": "sentence-transformers/all-MiniLM-L6-v2",
			},
			wantError: true,
		},
		{
			name: "空 API 密钥",
			config: map[string]interface{}{
				"api_key": "",
			},
			wantError: true,
		},
		{
			name: "自定义模型",
			config: map[string]interface{}{
				"api_key": "test-key",
				"model":   "sentence-transformers/all-mpnet-base-v2",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder, err := NewHuggingFaceEmbedder(tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("NewHuggingFaceEmbedder() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && embedder == nil {
				t.Error("Expected embedder, got nil")
			}
		})
	}
}

func TestCreateEmbedder(t *testing.T) {
	tests := []struct {
		name         string
		embedderType string
		config       map[string]interface{}
		wantError    bool
	}{
		{
			name:         "simple 类型",
			embedderType: "simple",
			config: map[string]interface{}{
				"dimensions": 384,
			},
			wantError: false,
		},
		{
			name:         "openai 类型（需要 API 密钥）",
			embedderType: "openai",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantError: false,
		},
		{
			name:         "openai 类型（缺少 API 密钥）",
			embedderType: "openai",
			config:       map[string]interface{}{},
			wantError:    true,
		},
		{
			name:         "huggingface 类型（需要 API 密钥）",
			embedderType: "huggingface",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantError: false,
		},
		{
			name:         "huggingface 类型（缺少 API 密钥）",
			embedderType: "huggingface",
			config:       map[string]interface{}{},
			wantError:    true,
		},
		{
			name:         "未知类型（应使用默认 simple）",
			embedderType: "unknown",
			config:       map[string]interface{}{},
			wantError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder, err := CreateEmbedder(tt.embedderType, tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("CreateEmbedder() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && embedder == nil {
				t.Error("Expected embedder, got nil")
			}
		})
	}
}

func TestOpenAIEmbedder_Dimensions(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		wantDimensions int
	}{
		{
			name: "默认模型",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantDimensions: 1536, // text-embedding-ada-002
		},
		{
			name: "text-embedding-3-small",
			config: map[string]interface{}{
				"api_key": "test-key",
				"model":   "text-embedding-3-small",
			},
			wantDimensions: 1536,
		},
		{
			name: "text-embedding-3-large",
			config: map[string]interface{}{
				"api_key": "test-key",
				"model":   "text-embedding-3-large",
			},
			wantDimensions: 3072,
		},
		{
			name: "自定义维度",
			config: map[string]interface{}{
				"api_key":    "test-key",
				"dimensions": 512,
			},
			wantDimensions: 512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder, err := NewOpenAIEmbedder(tt.config)
			if err != nil {
				t.Fatalf("Failed to create embedder: %v", err)
			}
			if embedder.Dimensions() != tt.wantDimensions {
				t.Errorf("Expected dimensions %d, got %d", tt.wantDimensions, embedder.Dimensions())
			}
		})
	}
}

func TestHuggingFaceEmbedder_Dimensions(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		wantDimensions int
	}{
		{
			name: "默认模型",
			config: map[string]interface{}{
				"api_key": "test-key",
			},
			wantDimensions: 384, // all-MiniLM-L6-v2
		},
		{
			name: "all-mpnet-base-v2",
			config: map[string]interface{}{
				"api_key": "test-key",
				"model":   "sentence-transformers/all-mpnet-base-v2",
			},
			wantDimensions: 768,
		},
		{
			name: "自定义维度",
			config: map[string]interface{}{
				"api_key":    "test-key",
				"dimensions": 512,
			},
			wantDimensions: 512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder, err := NewHuggingFaceEmbedder(tt.config)
			if err != nil {
				t.Fatalf("Failed to create embedder: %v", err)
			}
			if embedder.Dimensions() != tt.wantDimensions {
				t.Errorf("Expected dimensions %d, got %d", tt.wantDimensions, embedder.Dimensions())
			}
		})
	}
}
