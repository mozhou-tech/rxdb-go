package cognee

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"github.com/sirupsen/logrus"
)

// EntityRelationExtractor 实体关系抽取器接口
type EntityRelationExtractor interface {
	// ExtractEntities 从文本中提取实体
	ExtractEntities(ctx context.Context, content string) ([]Entity, error)
	// ExtractRelations 从文本中提取关系
	ExtractRelations(ctx context.Context, content string, entities []Entity) ([]Relation, error)
}

// OpenAIExtractor OpenAI 实体关系抽取器
type OpenAIExtractor struct {
	client *openai.Client
	model  string
}

// NewOpenAIExtractor 创建 OpenAI 实体关系抽取器
// config 需要包含:
//   - "api_key" (string): OpenAI API 密钥（必需）
//   - "model" (string): 模型名称，默认为 "qwen-max"
//   - "base_url" (string): API 基础 URL，默认为 "https://api.openai.com/v1"（可选，用于自定义端点）
func NewOpenAIExtractor(config map[string]interface{}) (*OpenAIExtractor, error) {
	apiKey, ok := config["api_key"].(string)
	if !ok || apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key is required")
	}

	model := "qwen-max" // 默认使用 qwen-max
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

	return &OpenAIExtractor{
		client: client,
		model:  model,
	}, nil
}

// ExtractEntities 从文本中提取实体
func (e *OpenAIExtractor) ExtractEntities(ctx context.Context, content string) ([]Entity, error) {
	if content == "" {
		return []Entity{}, nil
	}

	// 构建提示词
	prompt := fmt.Sprintf(`请从以下文本中提取所有实体。实体类型包括：人物(person)、组织(organization)、地点(location)、概念(concept)、技术(technology)、产品(product)等。

请以 JSON 格式返回结果，格式如下：
{
  "entities": [
    {
      "name": "实体名称",
      "type": "实体类型",
      "description": "实体描述（可选）"
    }
  ]
}

文本内容：
%s

请只返回 JSON 格式，不要包含其他文字说明。`, content)

	// 调用 OpenAI API
	resp, err := e.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: e.model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: prompt,
			},
		},
		Temperature: 0.3, // 降低温度以获得更一致的结果
		ResponseFormat: &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONObject,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call OpenAI API: %w", err)
	}

	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("no response from OpenAI")
	}

	// 解析响应
	responseText := resp.Choices[0].Message.Content
	responseText = strings.TrimSpace(responseText)

	// 移除可能的 markdown 代码块标记
	if strings.HasPrefix(responseText, "```json") {
		responseText = strings.TrimPrefix(responseText, "```json")
		responseText = strings.TrimSuffix(responseText, "```")
	} else if strings.HasPrefix(responseText, "```") {
		responseText = strings.TrimPrefix(responseText, "```")
		responseText = strings.TrimSuffix(responseText, "```")
	}
	responseText = strings.TrimSpace(responseText)

	var result struct {
		Entities []struct {
			Name        string `json:"name"`
			Type        string `json:"type"`
			Description string `json:"description,omitempty"`
		} `json:"entities"`
	}

	if err := json.Unmarshal([]byte(responseText), &result); err != nil {
		logrus.WithError(err).WithField("response", responseText).Warn("Failed to parse OpenAI response, trying to extract JSON")
		// 尝试从响应中提取 JSON
		startIdx := strings.Index(responseText, "{")
		endIdx := strings.LastIndex(responseText, "}")
		if startIdx >= 0 && endIdx > startIdx {
			jsonStr := responseText[startIdx : endIdx+1]
			if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
				return nil, fmt.Errorf("failed to parse JSON response: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse JSON response: %w", err)
		}
	}

	// 转换为 Entity 结构
	entities := make([]Entity, 0, len(result.Entities))
	now := time.Now().Unix()
	for _, e := range result.Entities {
		if e.Name == "" {
			continue
		}

		metadata := make(map[string]interface{})
		if e.Description != "" {
			metadata["description"] = e.Description
		}

		entity := Entity{
			ID:        generateID(),
			Name:      e.Name,
			Type:      e.Type,
			Metadata:  metadata,
			CreatedAt: now,
		}
		entities = append(entities, entity)
	}

	logrus.WithField("count", len(entities)).Debug("Extracted entities using OpenAI")
	return entities, nil
}

// ExtractRelations 从文本中提取关系
func (e *OpenAIExtractor) ExtractRelations(ctx context.Context, content string, entities []Entity) ([]Relation, error) {
	if content == "" {
		return []Relation{}, nil
	}

	// 构建实体列表字符串
	entityList := make([]string, 0, len(entities))
	entityMap := make(map[string]Entity)
	for _, entity := range entities {
		entityList = append(entityList, fmt.Sprintf("- %s (%s)", entity.Name, entity.Type))
		entityMap[entity.Name] = entity
	}
	entityListStr := strings.Join(entityList, "\n")
	if entityListStr == "" {
		entityListStr = "无"
	}

	// 构建提示词
	prompt := fmt.Sprintf(`请从以下文本中提取实体之间的关系。关系类型包括：属于(belongs_to)、相关(related_to)、使用(uses)、开发(develops)、位于(located_in)、创建(creates)、影响(affects)、包含(contains)等。

已提取的实体列表：
%s

请以 JSON 格式返回结果，格式如下：
{
  "relations": [
    {
      "from": "源实体名称",
      "to": "目标实体名称",
      "type": "关系类型",
      "description": "关系描述（可选）"
    }
  ]
}

文本内容：
%s

请只返回 JSON 格式，不要包含其他文字说明。如果文本中没有明确的关系，请返回空的 relations 数组。`, entityListStr, content)

	// 调用 OpenAI API
	resp, err := e.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: e.model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: prompt,
			},
		},
		Temperature: 0.3, // 降低温度以获得更一致的结果
		ResponseFormat: &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONObject,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call OpenAI API: %w", err)
	}

	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("no response from OpenAI")
	}

	// 解析响应
	responseText := resp.Choices[0].Message.Content
	responseText = strings.TrimSpace(responseText)

	// 移除可能的 markdown 代码块标记
	if strings.HasPrefix(responseText, "```json") {
		responseText = strings.TrimPrefix(responseText, "```json")
		responseText = strings.TrimSuffix(responseText, "```")
	} else if strings.HasPrefix(responseText, "```") {
		responseText = strings.TrimPrefix(responseText, "```")
		responseText = strings.TrimSuffix(responseText, "```")
	}
	responseText = strings.TrimSpace(responseText)

	var result struct {
		Relations []struct {
			From        string `json:"from"`
			To          string `json:"to"`
			Type        string `json:"type"`
			Description string `json:"description,omitempty"`
		} `json:"relations"`
	}

	if err := json.Unmarshal([]byte(responseText), &result); err != nil {
		logrus.WithError(err).WithField("response", responseText).Warn("Failed to parse OpenAI response, trying to extract JSON")
		// 尝试从响应中提取 JSON
		startIdx := strings.Index(responseText, "{")
		endIdx := strings.LastIndex(responseText, "}")
		if startIdx >= 0 && endIdx > startIdx {
			jsonStr := responseText[startIdx : endIdx+1]
			if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
				return nil, fmt.Errorf("failed to parse JSON response: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse JSON response: %w", err)
		}
	}

	// 转换为 Relation 结构
	relations := make([]Relation, 0, len(result.Relations))
	now := time.Now().Unix()
	for _, r := range result.Relations {
		if r.From == "" || r.To == "" || r.Type == "" {
			continue
		}

		// 查找源实体和目标实体
		fromEntity, fromExists := entityMap[r.From]
		toEntity, toExists := entityMap[r.To]

		// 如果实体不存在，跳过该关系
		if !fromExists || !toExists {
			logrus.WithFields(logrus.Fields{
				"from": r.From,
				"to":   r.To,
			}).Debug("Skipping relation with unknown entities")
			continue
		}

		metadata := make(map[string]interface{})
		if r.Description != "" {
			metadata["description"] = r.Description
		}

		relation := Relation{
			ID:        generateID(),
			From:      fromEntity.ID,
			To:        toEntity.ID,
			Type:      r.Type,
			Metadata:  metadata,
			CreatedAt: now,
		}
		relations = append(relations, relation)
	}

	logrus.WithField("count", len(relations)).Debug("Extracted relations using OpenAI")
	return relations, nil
}

// CreateExtractor 创建实体关系抽取器
// 可以根据配置选择不同的抽取器
func CreateExtractor(extractorType string, config map[string]interface{}) (EntityRelationExtractor, error) {
	switch extractorType {
	case "openai":
		return NewOpenAIExtractor(config)
	case "none", "":
		// 返回一个空的抽取器（不进行抽取）
		return &NoOpExtractor{}, nil
	default:
		return nil, fmt.Errorf("unknown extractor type: %s", extractorType)
	}
}

// NoOpExtractor 空操作抽取器（不进行抽取）
type NoOpExtractor struct{}

func (e *NoOpExtractor) ExtractEntities(ctx context.Context, content string) ([]Entity, error) {
	return []Entity{}, nil
}

func (e *NoOpExtractor) ExtractRelations(ctx context.Context, content string, entities []Entity) ([]Relation, error) {
	return []Relation{}, nil
}
