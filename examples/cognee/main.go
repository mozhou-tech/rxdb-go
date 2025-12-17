package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mozy/rxdb-go/pkg/cognee"
	"github.com/mozy/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// 全局嵌入器，用于生成文本向量
var embedder cognee.Embedder

// 全局抽取器，用于提取实体和关系
var extractor cognee.EntityRelationExtractor

func main() {
	ctx := context.Background()

	// 初始化嵌入器（从环境变量读取配置）
	// 必须配置嵌入模型才能运行此示例
	// 优先使用 OPENAI_API_KEY，如果没有设置则使用 EMBEDDING_API_KEY（向后兼容）
	if err := initEmbedder(ctx); err != nil {
		logrus.WithError(err).Fatal("Failed to initialize embedder. Please set OPENAI_API_KEY (or EMBEDDING_API_KEY) environment variable")
	}
	if embedder == nil {
		logrus.Fatal("Embedder is not initialized. Please set OPENAI_API_KEY (or EMBEDDING_API_KEY) environment variable")
	}

	// 初始化抽取器（从环境变量读取配置）
	// 可选：如果设置了 OPENAI_API_KEY，则使用 OpenAI 进行实体关系抽取
	if err := initExtractor(ctx); err != nil {
		logrus.WithError(err).Warn("Failed to initialize extractor. Entity and relation extraction will be disabled. Set OPENAI_API_KEY to enable it.")
	}

	// 从环境变量读取配置
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/cognee-memory"
	}

	// 确保数据目录存在
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		logrus.WithError(err).Fatal("Failed to create data directory")
	}

	// 创建数据库（启用图数据库功能）
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "cognee-memory",
		Path: dbPath,
		GraphOptions: &rxdb.GraphOptions{
			Enabled:  true,
			Backend:  "badger",
			Path:     filepath.Join(dbPath, "graph"),
			AutoSync: true,
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer db.Close(ctx)

	// 创建记忆服务（使用真实的嵌入模型和抽取器）
	service, err := cognee.NewMemoryService(ctx, db, cognee.MemoryServiceOptions{
		Embedder:  embedder,
		Extractor: extractor,
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create memory service")
	}

	// 示例：添加测试数据
	testMemories := []struct {
		content  string
		memType  string
		dataset  string
		metadata map[string]interface{}
	}{
		{
			content: "AI 正在改变我们的工作和生活方式。机器学习算法可以帮助我们自动化重复性任务，提高生产效率。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "人工智能",
				"category": "技术",
			},
		},
		{
			content: "Go 语言是一种开源的编程语言，由 Google 开发。它具有简洁的语法、高效的并发模型和强大的标准库。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "编程语言",
				"category": "技术",
			},
		},
		{
			content: "数据库设计需要考虑数据一致性、查询性能和可扩展性。关系型数据库使用 SQL 进行数据操作，而 NoSQL 数据库提供了更灵活的数据模型。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "数据库",
				"category": "技术",
			},
		},
		{
			content: "向量搜索是语义搜索的核心技术。通过将文本转换为高维向量，我们可以找到语义相似的内容，即使它们使用不同的词汇。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "搜索技术",
				"category": "技术",
			},
		},
		{
			content: "func main() {\n    fmt.Println(\"Hello, World!\")\n}",
			memType: "code",
			dataset: "code_examples",
			metadata: map[string]interface{}{
				"language": "go",
				"category": "示例代码",
			},
		},
		{
			content: "https://github.com/mozy/rxdb-go - RxDB Go 实现，提供本地数据库、全文搜索、向量搜索和图数据库功能。",
			memType: "url",
			dataset: "resources",
			metadata: map[string]interface{}{
				"category": "项目资源",
				"source":   "github",
			},
		},
		{
			content: "图数据库使用节点和边来表示实体和关系。这种结构非常适合表示复杂的关系网络，如社交网络、知识图谱等。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "图数据库",
				"category": "技术",
			},
		},
		{
			content: "全文搜索允许用户通过关键词快速查找文档。现代全文搜索引擎支持模糊匹配、同义词扩展和相关性排序。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "搜索技术",
				"category": "技术",
			},
		},
		{
			content: "混合搜索结合了向量搜索和全文搜索的优势。它既能理解语义相似性，又能进行精确的关键词匹配，提供更准确的搜索结果。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "搜索技术",
				"category": "技术",
			},
		},
		{
			content: "分布式系统设计需要考虑容错性、一致性和可用性。CAP 定理说明了这三个属性不能同时满足。",
			memType: "text",
			dataset: "main_dataset",
			metadata: map[string]interface{}{
				"topic":    "系统设计",
				"category": "技术",
			},
		},
	}

	logrus.Info("开始添加测试数据...")
	var memoryIDs []string
	for i, tm := range testMemories {
		memory, err := service.AddMemory(ctx, tm.content, tm.memType, tm.dataset, tm.metadata)
		if err != nil {
			logrus.WithError(err).WithField("index", i).Error("Failed to add memory")
			continue
		}
		memoryIDs = append(memoryIDs, memory.ID)
		logrus.WithFields(logrus.Fields{
			"id":      memory.ID,
			"type":    tm.memType,
			"dataset": tm.dataset,
			"index":   i + 1,
		}).Info("Added memory")
	}
	logrus.WithField("count", len(memoryIDs)).Info("测试数据添加完成")

	// 示例：处理所有记忆
	logrus.Info("开始处理记忆数据...")
	for i, memoryID := range memoryIDs {
		if err := service.ProcessMemory(ctx, memoryID); err != nil {
			logrus.WithError(err).WithField("id", memoryID).WithField("index", i+1).Error("Failed to process memory")
		} else {
			logrus.WithField("id", memoryID).WithField("index", i+1).Info("Processed memory")
		}
	}
	logrus.Info("记忆数据处理完成")

	// 示例：检索测试 - 通过ID获取记忆
	logrus.Info("========== 检索测试：通过ID获取记忆 ==========")
	if len(memoryIDs) > 0 {
		testID := memoryIDs[0]
		memory, err := service.GetMemory(ctx, testID)
		if err != nil {
			logrus.WithError(err).Error("Failed to get memory by ID")
		} else {
			logrus.WithFields(logrus.Fields{
				"id":      memory.ID,
				"content": memory.Content[:min(50, len(memory.Content))] + "...",
				"type":    memory.Type,
				"dataset": memory.Dataset,
			}).Info("✅ 成功通过ID获取记忆")
		}
	}

	// 示例：检索测试 - 全文搜索
	logrus.Info("========== 检索测试：全文搜索 ==========")
	searchQueries := []struct {
		query       string
		searchType  string
		description string
	}{
		{"AI", "FULLTEXT", "全文搜索：AI"},
		{"数据库", "FULLTEXT", "全文搜索：数据库"},
		{"搜索", "FULLTEXT", "全文搜索：搜索"},
		{"Go", "FULLTEXT", "全文搜索：Go"},
		{"向量", "VECTOR", "向量搜索：向量"},
		{"语义", "VECTOR", "向量搜索：语义"},
		{"机器学习", "VECTOR", "向量搜索：机器学习"},
		{"图数据库", "HYBRID", "混合搜索：图数据库"},
		{"搜索技术", "HYBRID", "混合搜索：搜索技术"},
		{"分布式系统", "HYBRID", "混合搜索：分布式系统"},
		{"编程语言", "GRAPH", "图搜索：编程语言"},
		{"技术", "GRAPH", "图搜索：技术"},
	}

	for _, sq := range searchQueries {
		logrus.WithField("query", sq.query).WithField("type", sq.searchType).Info(sq.description)
		results, err := service.Search(ctx, sq.query, sq.searchType, 5)
		if err != nil {
			logrus.WithError(err).WithField("query", sq.query).Error("Failed to search")
			continue
		}

		logrus.WithFields(logrus.Fields{
			"query": sq.query,
			"type":  sq.searchType,
			"count": len(results),
		}).Info("搜索结果")

		for i, result := range results {
			contentPreview := result.Content
			if len(contentPreview) > 60 {
				contentPreview = contentPreview[:60] + "..."
			}
			logrus.WithFields(logrus.Fields{
				"rank":    i + 1,
				"id":      result.ID,
				"score":   result.Score,
				"source":  result.Source,
				"type":    result.Type,
				"content": contentPreview,
			}).Info("  - 结果")
		}
		logrus.Info("")
	}

	// 示例：检索测试 - 验证搜索结果的相关性
	logrus.Info("========== 检索测试：验证搜索结果相关性 ==========")
	relevanceTests := []struct {
		query        string
		expectedKeys []string // 期望在结果中找到的关键词
		searchType   string
	}{
		{
			query:        "人工智能",
			expectedKeys: []string{"AI", "人工智能", "机器学习"},
			searchType:   "HYBRID",
		},
		{
			query:        "向量搜索",
			expectedKeys: []string{"向量", "搜索", "语义"},
			searchType:   "HYBRID",
		},
		{
			query:        "数据库",
			expectedKeys: []string{"数据库", "SQL", "NoSQL"},
			searchType:   "FULLTEXT",
		},
		{
			query:        "Go语言",
			expectedKeys: []string{"Go", "编程语言", "Google"},
			searchType:   "VECTOR",
		},
	}

	for _, rt := range relevanceTests {
		results, err := service.Search(ctx, rt.query, rt.searchType, 3)
		if err != nil {
			logrus.WithError(err).WithField("query", rt.query).Error("Failed to search")
			continue
		}

		foundKeys := make(map[string]bool)
		for _, result := range results {
			for _, key := range rt.expectedKeys {
				if contains(result.Content, key) {
					foundKeys[key] = true
				}
			}
		}

		logrus.WithFields(logrus.Fields{
			"query":        rt.query,
			"results":      len(results),
			"expectedKeys": rt.expectedKeys,
			"foundKeys":    getKeys(foundKeys),
		}).Info("相关性验证")
	}

	// 示例：检索测试 - 边界情况
	logrus.Info("========== 检索测试：边界情况 ==========")

	// 测试空查询
	emptyResults, err := service.Search(ctx, "", "HYBRID", 5)
	if err != nil {
		logrus.WithError(err).Warn("空查询搜索失败")
	} else {
		logrus.WithField("count", len(emptyResults)).Info("空查询结果")
	}

	// 测试不存在的关键词
	noResults, err := service.Search(ctx, "不存在的关键词xyz123", "HYBRID", 5)
	if err != nil {
		logrus.WithError(err).Warn("不存在关键词搜索失败")
	} else {
		logrus.WithField("count", len(noResults)).Info("不存在关键词搜索结果")
	}

	// 测试限制结果数量
	limitedResults, err := service.Search(ctx, "技术", "HYBRID", 2)
	if err != nil {
		logrus.WithError(err).Warn("限制结果数量搜索失败")
	} else {
		logrus.WithFields(logrus.Fields{
			"requested": 2,
			"returned":  len(limitedResults),
		}).Info("限制结果数量测试")
	}

	// 示例：健康检查
	health, err := service.Health(ctx)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get health")
	}
	logrus.WithFields(logrus.Fields{
		"status":   health.Status,
		"memory":   health.Stats.Memories,
		"entity":   health.Stats.Entities,
		"relation": health.Stats.Relations,
	}).Info("Health check")
}

// 辅助函数

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// contains 检查字符串是否包含子字符串
func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	// 使用 Go 标准库的 strings.Contains，支持 Unicode
	return strings.Contains(s, substr)
}

// getKeys 从 map 中提取所有键
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// initEmbedder 从环境变量初始化嵌入器
// 优先使用 OPENAI_* 环境变量，如果没有设置则回退到 EMBEDDING_* 环境变量（向后兼容）
func initEmbedder(ctx context.Context) error {
	// 优先使用 OPENAI_* 环境变量
	apiKey := os.Getenv("OPENAI_API_KEY")
	baseURL := os.Getenv("OPENAI_BASE_URL")
	model := os.Getenv("OPENAI_MODEL")

	// 如果没有设置 OPENAI_* 变量，尝试使用 EMBEDDING_* 变量（向后兼容）
	if apiKey == "" {
		apiKey = os.Getenv("EMBEDDING_API_KEY")
	}
	if baseURL == "" {
		baseURL = os.Getenv("EMBEDDING_BASE_URL")
	}
	if model == "" {
		model = os.Getenv("EMBEDDING_MODEL")
	}

	// 必须设置 API 密钥才能运行
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY (或 EMBEDDING_API_KEY) 必须设置。示例: export OPENAI_API_KEY=your-api-key")
	}

	// 构建配置
	config := map[string]interface{}{
		"api_key": apiKey,
	}

	// 可选：设置模型名称
	if model != "" {
		config["model"] = model
	}

	// 如果设置了 BASE_URL，使用它（支持 OpenAI 兼容的 API）
	if baseURL != "" {
		config["base_url"] = baseURL
		// 默认使用 OpenAI 格式的嵌入器
		embedderType := "openai"
		if embedderTypeEnv := os.Getenv("EMBEDDING_TYPE"); embedderTypeEnv != "" {
			embedderType = embedderTypeEnv
		}

		var err error
		embedder, err = cognee.CreateEmbedder(embedderType, config)
		if err != nil {
			return fmt.Errorf("failed to create embedder: %w", err)
		}

		logFields := logrus.Fields{
			"base_url": baseURL,
			"type":     embedderType,
		}
		if model, ok := config["model"].(string); ok {
			logFields["model"] = model
		}
		logrus.WithFields(logFields).Info("✅ 嵌入器初始化成功")
	} else {
		// 如果没有设置 BASE_URL，尝试使用默认的 OpenAI API
		config["base_url"] = "https://api.openai.com/v1"
		var err error
		embedder, err = cognee.CreateEmbedder("openai", config)
		if err != nil {
			return fmt.Errorf("failed to create OpenAI embedder: %w", err)
		}

		logFields := logrus.Fields{}
		if model, ok := config["model"].(string); ok {
			logFields["model"] = model
		}
		logrus.WithFields(logFields).Info("✅ 使用默认 OpenAI API 初始化嵌入器")
	}

	return nil
}

// initExtractor 从环境变量初始化实体关系抽取器
func initExtractor(ctx context.Context) error {
	apiKey := os.Getenv("OPENAI_API_KEY")

	// 如果没有设置 OPENAI_API_KEY，则不使用抽取器
	if apiKey == "" {
		logrus.Info("OPENAI_API_KEY 未设置，实体关系抽取功能将被禁用")
		return nil
	}

	// 构建配置
	config := map[string]interface{}{
		"api_key": apiKey,
	}

	// 可选：设置模型名称（默认为 gpt-4o-mini）
	if model := os.Getenv("OPENAI_MODEL"); model != "" {
		config["model"] = model
	}

	// 可选：设置自定义 base URL（用于兼容其他 OpenAI API 服务）
	if baseURL := os.Getenv("OPENAI_BASE_URL"); baseURL != "" {
		config["base_url"] = baseURL
	}

	// 创建 OpenAI 抽取器
	var err error
	extractor, err = cognee.CreateExtractor("openai", config)
	if err != nil {
		return fmt.Errorf("failed to create extractor: %w", err)
	}

	logFields := logrus.Fields{
		"type": "openai",
	}
	if model, ok := config["model"].(string); ok {
		logFields["model"] = model
	} else {
		logFields["model"] = "gpt-4o-mini" // 默认模型
	}
	if baseURL, ok := config["base_url"].(string); ok {
		logFields["base_url"] = baseURL
	}
	logrus.WithFields(logFields).Info("✅ 实体关系抽取器初始化成功")

	return nil
}
