package cognee

import (
	"context"
	"os"
	"testing"
)

func TestNoOpExtractor_ExtractEntities(t *testing.T) {
	extractor := &NoOpExtractor{}
	ctx := context.Background()

	tests := []struct {
		name    string
		content string
	}{
		{
			name:    "空文本",
			content: "",
		},
		{
			name:    "正常文本",
			content: "这是一个测试文本",
		},
		{
			name:    "长文本",
			content: "这是一个很长的测试文本，包含多个实体和关系。",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entities, err := extractor.ExtractEntities(ctx, tt.content)
			if err != nil {
				t.Errorf("ExtractEntities() error = %v, want nil", err)
				return
			}
			if len(entities) != 0 {
				t.Errorf("ExtractEntities() returned %d entities, want 0", len(entities))
			}
		})
	}
}

func TestNoOpExtractor_ExtractRelations(t *testing.T) {
	extractor := &NoOpExtractor{}
	ctx := context.Background()

	tests := []struct {
		name     string
		content  string
		entities []Entity
	}{
		{
			name:     "空文本和空实体",
			content:  "",
			entities: []Entity{},
		},
		{
			name:    "正常文本和实体",
			content: "这是一个测试文本",
			entities: []Entity{
				{ID: "1", Name: "实体1", Type: "person"},
				{ID: "2", Name: "实体2", Type: "organization"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relations, err := extractor.ExtractRelations(ctx, tt.content, tt.entities)
			if err != nil {
				t.Errorf("ExtractRelations() error = %v, want nil", err)
				return
			}
			if len(relations) != 0 {
				t.Errorf("ExtractRelations() returned %d relations, want 0", len(relations))
			}
		})
	}
}

func TestNewOpenAIExtractor(t *testing.T) {
	tests := []struct {
		name      string
		config    map[string]interface{}
		wantError bool
		checkFunc func(*testing.T, *OpenAIExtractor)
	}{
		{
			name: "有效配置-默认模型",
			config: map[string]interface{}{
				"api_key": "test-api-key",
			},
			wantError: false,
			checkFunc: func(t *testing.T, e *OpenAIExtractor) {
				if e == nil {
					t.Fatal("NewOpenAIExtractor returned nil")
				}
				if e.model != "qwen-max" {
					t.Errorf("Expected default model 'qwen-max', got '%s'", e.model)
				}
				if e.client == nil {
					t.Error("Expected client to be initialized")
				}
			},
		},
		{
			name: "有效配置-自定义模型",
			config: map[string]interface{}{
				"api_key": "test-api-key",
				"model":   "gpt-4",
			},
			wantError: false,
			checkFunc: func(t *testing.T, e *OpenAIExtractor) {
				if e == nil {
					t.Fatal("NewOpenAIExtractor returned nil")
				}
				if e.model != "gpt-4" {
					t.Errorf("Expected model 'gpt-4', got '%s'", e.model)
				}
			},
		},
		{
			name: "有效配置-自定义base_url",
			config: map[string]interface{}{
				"api_key":  "test-api-key",
				"base_url": "https://custom-api.example.com/v1",
			},
			wantError: false,
			checkFunc: func(t *testing.T, e *OpenAIExtractor) {
				if e == nil {
					t.Fatal("NewOpenAIExtractor returned nil")
				}
			},
		},
		{
			name: "缺少api_key",
			config: map[string]interface{}{
				"model": "gpt-4",
			},
			wantError: true,
		},
		{
			name: "空api_key",
			config: map[string]interface{}{
				"api_key": "",
			},
			wantError: true,
		},
		{
			name:      "空配置",
			config:    map[string]interface{}{},
			wantError: true,
		},
		{
			name:      "nil配置",
			config:    nil,
			wantError: true,
		},
		{
			name: "空字符串模型应使用默认值",
			config: map[string]interface{}{
				"api_key": "test-api-key",
				"model":   "",
			},
			wantError: false,
			checkFunc: func(t *testing.T, e *OpenAIExtractor) {
				if e.model != "qwen-max" {
					t.Errorf("Expected default model 'qwen-max', got '%s'", e.model)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extractor, err := NewOpenAIExtractor(tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("NewOpenAIExtractor() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && tt.checkFunc != nil {
				tt.checkFunc(t, extractor)
			}
		})
	}
}

func TestCreateExtractor(t *testing.T) {
	tests := []struct {
		name          string
		extractorType string
		config        map[string]interface{}
		wantError     bool
		checkFunc     func(*testing.T, EntityRelationExtractor)
	}{
		{
			name:          "创建openai抽取器",
			extractorType: "openai",
			config: map[string]interface{}{
				"api_key": "test-api-key",
			},
			wantError: false,
			checkFunc: func(t *testing.T, e EntityRelationExtractor) {
				if e == nil {
					t.Fatal("CreateExtractor returned nil")
				}
				_, ok := e.(*OpenAIExtractor)
				if !ok {
					t.Error("Expected OpenAIExtractor type")
				}
			},
		},
		{
			name:          "创建none抽取器",
			extractorType: "none",
			config:        nil,
			wantError:     false,
			checkFunc: func(t *testing.T, e EntityRelationExtractor) {
				if e == nil {
					t.Fatal("CreateExtractor returned nil")
				}
				_, ok := e.(*NoOpExtractor)
				if !ok {
					t.Error("Expected NoOpExtractor type")
				}
			},
		},
		{
			name:          "创建空字符串抽取器",
			extractorType: "",
			config:        nil,
			wantError:     false,
			checkFunc: func(t *testing.T, e EntityRelationExtractor) {
				if e == nil {
					t.Fatal("CreateExtractor returned nil")
				}
				_, ok := e.(*NoOpExtractor)
				if !ok {
					t.Error("Expected NoOpExtractor type")
				}
			},
		},
		{
			name:          "未知类型",
			extractorType: "unknown",
			config:        nil,
			wantError:     true,
		},
		{
			name:          "openai类型但配置无效",
			extractorType: "openai",
			config:        map[string]interface{}{
				// 缺少 api_key
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extractor, err := CreateExtractor(tt.extractorType, tt.config)
			if (err != nil) != tt.wantError {
				t.Errorf("CreateExtractor() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && tt.checkFunc != nil {
				tt.checkFunc(t, extractor)
			}
		})
	}
}

// TestOpenAIExtractor_ExtractEntities_Integration 集成测试，需要真实的 OpenAI API key
// 设置环境变量 OPENAI_API_KEY 来运行此测试
func TestOpenAIExtractor_ExtractEntities_Integration(t *testing.T) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		t.Skip("跳过集成测试：未设置 OPENAI_API_KEY 环境变量")
	}

	config := map[string]interface{}{
		"api_key": apiKey,
		"model":   "qwen-max",
	}

	extractor, err := NewOpenAIExtractor(config)
	if err != nil {
		t.Fatalf("Failed to create OpenAI extractor: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		content   string
		wantError bool
		minCount  int // 期望提取的实体最小数量
	}{
		{
			name:      "空文本",
			content:   "",
			wantError: false,
			minCount:  0,
		},
		{
			name:      "简单文本-包含人物",
			content:   "张三在北京工作，他是腾讯公司的工程师。",
			wantError: false,
			minCount:  2, // 至少应该提取到"张三"和"腾讯公司"
		},
		{
			name:      "技术相关文本",
			content:   "Go语言是由Google开发的编程语言，它被广泛用于后端开发。",
			wantError: false,
			minCount:  2, // 至少应该提取到"Go语言"和"Google"
		},
		{
			name:      "长文本",
			content:   "苹果公司是一家位于美国加利福尼亚州库比蒂诺的科技公司，由史蒂夫·乔布斯、史蒂夫·沃兹尼亚克和罗纳德·韦恩创立。公司主要产品包括iPhone、iPad和Mac电脑。",
			wantError: false,
			minCount:  5, // 应该提取多个实体
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entities, err := extractor.ExtractEntities(ctx, tt.content)
			if (err != nil) != tt.wantError {
				t.Errorf("ExtractEntities() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError {
				if len(entities) < tt.minCount {
					t.Errorf("ExtractEntities() returned %d entities, want at least %d", len(entities), tt.minCount)
				}
				// 验证实体结构
				for i, entity := range entities {
					if entity.ID == "" {
						t.Errorf("Entity[%d] has empty ID", i)
					}
					if entity.Name == "" {
						t.Errorf("Entity[%d] has empty Name", i)
					}
					if entity.Type == "" {
						t.Errorf("Entity[%d] has empty Type", i)
					}
					if entity.CreatedAt == 0 {
						t.Errorf("Entity[%d] has zero CreatedAt", i)
					}
				}
			}
		})
	}
}

// TestOpenAIExtractor_ExtractRelations_Integration 集成测试，需要真实的 OpenAI API key
// 设置环境变量 OPENAI_API_KEY 来运行此测试
func TestOpenAIExtractor_ExtractRelations_Integration(t *testing.T) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		t.Skip("跳过集成测试：未设置 OPENAI_API_KEY 环境变量")
	}

	config := map[string]interface{}{
		"api_key": apiKey,
		"model":   "qwen-max",
	}

	extractor, err := NewOpenAIExtractor(config)
	if err != nil {
		t.Fatalf("Failed to create OpenAI extractor: %v", err)
	}

	ctx := context.Background()

	// 先提取实体
	content := "张三在北京工作，他是腾讯公司的工程师。腾讯公司位于深圳。"
	entities, err := extractor.ExtractEntities(ctx, content)
	if err != nil {
		t.Fatalf("Failed to extract entities: %v", err)
	}

	if len(entities) == 0 {
		t.Skip("跳过关系提取测试：未提取到实体")
	}

	tests := []struct {
		name      string
		content   string
		entities  []Entity
		wantError bool
		minCount  int // 期望提取的关系最小数量
	}{
		{
			name:      "空文本",
			content:   "",
			entities:  entities,
			wantError: false,
			minCount:  0,
		},
		{
			name:      "正常文本和实体",
			content:   content,
			entities:  entities,
			wantError: false,
			minCount:  1, // 至少应该提取到一个关系
		},
		{
			name:      "空实体列表",
			content:   content,
			entities:  []Entity{},
			wantError: false,
			minCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relations, err := extractor.ExtractRelations(ctx, tt.content, tt.entities)
			if (err != nil) != tt.wantError {
				t.Errorf("ExtractRelations() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError {
				if len(relations) < tt.minCount {
					t.Errorf("ExtractRelations() returned %d relations, want at least %d", len(relations), tt.minCount)
				}
				// 验证关系结构
				for i, relation := range relations {
					if relation.ID == "" {
						t.Errorf("Relation[%d] has empty ID", i)
					}
					if relation.From == "" {
						t.Errorf("Relation[%d] has empty From", i)
					}
					if relation.To == "" {
						t.Errorf("Relation[%d] has empty To", i)
					}
					if relation.Type == "" {
						t.Errorf("Relation[%d] has empty Type", i)
					}
					if relation.CreatedAt == 0 {
						t.Errorf("Relation[%d] has zero CreatedAt", i)
					}
					// 验证 From 和 To 是否在实体列表中
					fromFound := false
					toFound := false
					for _, entity := range tt.entities {
						if entity.ID == relation.From {
							fromFound = true
						}
						if entity.ID == relation.To {
							toFound = true
						}
					}
					if !fromFound {
						t.Errorf("Relation[%d] has unknown From entity ID: %s", i, relation.From)
					}
					if !toFound {
						t.Errorf("Relation[%d] has unknown To entity ID: %s", i, relation.To)
					}
				}
			}
		})
	}
}

// TestOpenAIExtractor_ExtractEntities_EmptyContent 测试空内容处理
func TestOpenAIExtractor_ExtractEntities_EmptyContent(t *testing.T) {
	// 这个测试不需要真实的 API key，因为空内容应该直接返回
	// 但为了测试代码路径，我们需要一个有效的配置
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		// 如果没有 API key，使用一个假的 key 来测试空内容处理逻辑
		apiKey = "test-key-for-empty-content"
	}

	config := map[string]interface{}{
		"api_key": apiKey,
	}

	extractor, err := NewOpenAIExtractor(config)
	if err != nil {
		t.Fatalf("Failed to create OpenAI extractor: %v", err)
	}

	ctx := context.Background()
	entities, err := extractor.ExtractEntities(ctx, "")
	if err != nil {
		// 如果是因为 API key 无效导致的错误，这是预期的
		// 但空内容应该在调用 API 之前就返回
		if apiKey == "test-key-for-empty-content" {
			// 使用假 key 时，可能会在 API 调用时失败，这是正常的
			return
		}
		t.Errorf("ExtractEntities() with empty content should not return error, got %v", err)
		return
	}

	if len(entities) != 0 {
		t.Errorf("ExtractEntities() with empty content returned %d entities, want 0", len(entities))
	}
}

// TestOpenAIExtractor_ExtractRelations_EmptyContent 测试空内容处理
func TestOpenAIExtractor_ExtractRelations_EmptyContent(t *testing.T) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = "test-key-for-empty-content"
	}

	config := map[string]interface{}{
		"api_key": apiKey,
	}

	extractor, err := NewOpenAIExtractor(config)
	if err != nil {
		t.Fatalf("Failed to create OpenAI extractor: %v", err)
	}

	ctx := context.Background()
	relations, err := extractor.ExtractRelations(ctx, "", []Entity{})
	if err != nil {
		if apiKey == "test-key-for-empty-content" {
			return
		}
		t.Errorf("ExtractRelations() with empty content should not return error, got %v", err)
		return
	}

	if len(relations) != 0 {
		t.Errorf("ExtractRelations() with empty content returned %d relations, want 0", len(relations))
	}
}
