package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mozhou-tech/rxdb-go/pkg/cognee"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// 全局嵌入器，用于生成文本向量
var embedder cognee.Embedder

func main() {
	ctx := context.Background()

	// 初始化嵌入器（从环境变量读取配置）
	// 必须配置嵌入模型才能运行此示例
	if err := initEmbedder(ctx); err != nil {
		logrus.WithError(err).Fatal("Failed to initialize embedder. Please set EMBEDDING_BASE_URL and EMBEDDING_API_KEY environment variables")
	}
	if embedder == nil {
		logrus.Fatal("Embedder is not initialized. Please set EMBEDDING_BASE_URL and EMBEDDING_API_KEY environment variables")
	}

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "hybrid-demo",
		Path: "./hybrid-demo.db",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./hybrid-demo.db")
	}()

	// 定义产品集合的 schema
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "product",
			"description": "产品集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":          map[string]any{"type": "string"},
				"name":        map[string]any{"type": "string"},
				"description": map[string]any{"type": "string"},
				"category":    map[string]any{"type": "string"},
				"price":       map[string]any{"type": "number"},
				"embedding":   map[string]any{"type": "array"},
			},
			"required": []string{"id", "name", "description"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "products", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 定义产品数据
	products := []map[string]any{
		{
			"id":          "prod-001",
			"name":        "iPhone 15 Pro",
			"description": "Apple 旗舰智能手机，搭载 A17 Pro 芯片，支持 5G 网络，拥有出色的拍照功能",
			"category":    "electronics",
			"price":       8999.0,
		},
		{
			"id":          "prod-002",
			"name":        "Samsung Galaxy S24",
			"description": "三星旗舰智能手机，搭载 AI 功能，支持智能翻译和图像识别",
			"category":    "electronics",
			"price":       6999.0,
		},
		{
			"id":          "prod-003",
			"name":        "MacBook Pro 16",
			"description": "Apple 专业笔记本电脑，M3 Max 芯片，适合编程和设计工作",
			"category":    "electronics",
			"price":       19999.0,
		},
		{
			"id":          "prod-004",
			"name":        "Nike Air Max 运动鞋",
			"description": "经典运动鞋，舒适透气，适合跑步和日常穿着",
			"category":    "clothing",
			"price":       899.0,
		},
		{
			"id":          "prod-005",
			"name":        "Adidas Ultraboost 跑鞋",
			"description": "高性能跑步鞋，Boost 中底技术，提供卓越的缓震效果",
			"category":    "clothing",
			"price":       1299.0,
		},
		{
			"id":          "prod-006",
			"name":        "Levi's 501 牛仔裤",
			"description": "经典直筒牛仔裤，百搭款式，适合各种场合",
			"category":    "clothing",
			"price":       599.0,
		},
		{
			"id":          "prod-007",
			"name":        "Sony WH-1000XM5 降噪耳机",
			"description": "旗舰降噪耳机，卓越音质，支持 LDAC 高解析度音频",
			"category":    "electronics",
			"price":       2999.0,
		},
		{
			"id":          "prod-008",
			"name":        "《深入理解计算机系统》",
			"description": "计算机科学经典教材，深入讲解系统底层原理",
			"category":    "books",
			"price":       139.0,
		},
		{
			"id":          "prod-009",
			"name":        "《设计模式：可复用面向对象软件的基础》",
			"description": "GoF 经典设计模式书籍，软件开发的必读之作",
			"category":    "books",
			"price":       89.0,
		},
		{
			"id":          "prod-010",
			"name":        "《三体》科幻小说",
			"description": "刘慈欣科幻小说代表作，雨果奖获奖作品",
			"category":    "books",
			"price":       49.0,
		},
	}

	// 使用真实嵌入模型为每个产品生成 embedding
	logrus.Info("🔄 使用真实嵌入模型生成产品向量...")
	for i, product := range products {
		name, _ := product["name"].(string)
		description, _ := product["description"].(string)
		category, _ := product["category"].(string)

		// 组合文本用于生成 embedding
		text := fmt.Sprintf("%s %s %s", name, description, category)

		embedding, err := embedder.Embed(ctx, text)
		if err != nil {
			logrus.WithError(err).WithField("product_id", product["id"]).Fatal("Failed to generate embedding")
		}

		product["embedding"] = embedding
		logrus.WithFields(logrus.Fields{
			"index":     i + 1,
			"total":     len(products),
			"name":      name,
			"dimension": len(embedding),
		}).Info("✅ 生成产品向量")
	}
	logrus.Info("")

	logrus.Info("🛒 插入示例产品...")
	for _, product := range products {
		_, err := collection.Insert(ctx, product)
		if err != nil {
			logrus.WithError(err).WithField("product_id", product["id"]).Error("Failed to insert product")
		}
	}
	logrus.WithField("count", len(products)).Info("✅ 已插入产品")
	logrus.Info("")

	// ========================================
	// 创建全文搜索索引
	// ========================================
	logrus.Info("🔍 创建全文搜索索引...")
	fts, err := rxdb.AddFulltextSearch(collection, rxdb.FulltextSearchConfig{
		Identifier: "product-fulltext",
		DocToString: func(doc map[string]any) string {
			name, _ := doc["name"].(string)
			description, _ := doc["description"].(string)
			category, _ := doc["category"].(string)
			// 名称权重更高（重复以增加权重）
			return name + " " + name + " " + description + " " + category
		},
		IndexOptions: &rxdb.FulltextIndexOptions{
			Tokenize:      "jieba",
			MinLength:     2,
			CaseSensitive: false,
			StopWords:     []string{"的", "是", "和", "了", "在", "有"},
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create fulltext search")
	}
	defer fts.Close()
	logrus.WithField("count", fts.Count()).Info("✅ 全文搜索索引创建完成，已索引产品")
	logrus.Info("")

	// ========================================
	// 创建向量搜索索引
	// ========================================
	logrus.Info("🔍 创建向量搜索索引...")

	// 确定向量维度
	dimensions := embedder.Dimensions()
	logrus.WithField("dimensions", dimensions).Info("📊 使用真实嵌入模型，向量维度")

	vs, err := rxdb.AddVectorSearch(collection, rxdb.VectorSearchConfig{
		Identifier: "product-vector",
		Dimensions: dimensions,
		DocToEmbedding: func(doc map[string]any) (rxdb.Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, fmt.Errorf("no embedding found")
		},
		DistanceMetric: "cosine",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create vector search")
	}
	defer vs.Close()
	logrus.WithField("count", vs.Count()).Info("✅ 向量搜索索引创建完成，已索引产品")
	logrus.Info("")

	// ========================================
	// 混合搜索示例
	// ========================================

	// 示例 1: 混合搜索 "智能手机"
	logrus.Info("===========================================")
	logrus.WithField("query", "智能手机").Info("🔎 混合搜索")
	logrus.Info("===========================================")
	query := "智能手机"
	queryVector, err := getQueryVector(ctx, query)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate query vector")
	}
	hybridResults, _ := rxdb.PerformHybridSearch(ctx, fts, vs, query, queryVector, rxdb.HybridSearchOptions{
		Limit:          5,
		FulltextWeight: 0.5,
		VectorWeight:   0.5,
	})
	logrus.WithField("count", len(hybridResults)).Info("找到相关产品")
	for i, r := range hybridResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":         i + 1,
			"hybrid_score": r.HybridScore,
			"name":         data["name"],
			"category":     data["category"],
		}).Info("📦 产品")
		logrus.WithFields(logrus.Fields{
			"fulltext_score":  r.FulltextScore,
			"vector_score":    r.VectorScore,
			"vector_distance": r.VectorDistance,
		}).Info("   分数详情")
	}
	logrus.Info("")

	// 示例 2: 混合搜索 "运动鞋"
	logrus.Info("===========================================")
	logrus.WithField("query", "运动鞋").Info("🔎 混合搜索")
	logrus.Info("===========================================")
	query = "运动鞋"
	queryVector, err = getQueryVector(ctx, query)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate query vector")
	}
	hybridResults, _ = rxdb.PerformHybridSearch(ctx, fts, vs, query, queryVector, rxdb.HybridSearchOptions{
		Limit:          5,
		FulltextWeight: 0.5,
		VectorWeight:   0.5,
	})
	logrus.WithField("count", len(hybridResults)).Info("找到相关产品")
	for i, r := range hybridResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":         i + 1,
			"hybrid_score": r.HybridScore,
			"name":         data["name"],
			"price":        data["price"],
		}).Info("📦 产品")
	}
	logrus.Info("")

	// 示例 3: 对比不同搜索方式
	logrus.Info("===========================================")
	logrus.WithField("query", "Apple").Info("🔎 搜索方式对比")
	logrus.Info("===========================================")
	query = "Apple"

	// 仅全文搜索
	logrus.Info("📝 仅全文搜索:")
	fulltextResults, err := fts.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: 5})
	if err != nil {
		logrus.WithError(err).Warn("全文搜索失败")
	}
	for i, r := range fulltextResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":  i + 1,
			"score": r.Score,
			"name":  data["name"],
		}).Info("结果")
	}

	// 仅向量搜索
	logrus.Info("🔢 仅向量搜索:")
	queryText := "electronics phone smartphone"
	queryVectorForApple, err := embedder.Embed(ctx, queryText)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate embedding for vector search")
	}
	vectorResults, err := vs.Search(ctx, queryVectorForApple, rxdb.VectorSearchOptions{Limit: 5})
	if err != nil {
		logrus.WithError(err).Warn("向量搜索失败")
	}
	for i, r := range vectorResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":     i + 1,
			"score":    r.Score,
			"distance": r.Distance,
			"name":     data["name"],
		}).Info("结果")
	}

	// 混合搜索
	logrus.Info("🔀 混合搜索 (全文权重: 0.5, 向量权重: 0.5):")
	queryVectorForApple, err = getQueryVector(ctx, query)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate query vector")
	}
	hybridResults, _ = rxdb.PerformHybridSearch(ctx, fts, vs, query, queryVectorForApple, rxdb.HybridSearchOptions{
		Limit:          5,
		FulltextWeight: 0.5,
		VectorWeight:   0.5,
	})
	for i, r := range hybridResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":         i + 1,
			"hybrid_score": r.HybridScore,
			"name":         data["name"],
		}).Info("结果")
	}
	logrus.Info("")

	// 示例 4: 不同权重比例的混合搜索
	logrus.Info("===========================================")
	logrus.WithField("query", "编程").Info("🔎 不同权重比例的混合搜索")
	logrus.Info("===========================================")
	query = "编程"

	weights := []struct {
		fulltextWeight float64
		vectorWeight   float64
		name           string
	}{
		{0.8, 0.2, "偏重全文搜索"},
		{0.5, 0.5, "平衡混合搜索"},
		{0.2, 0.8, "偏重向量搜索"},
	}

	for _, w := range weights {
		logrus.WithFields(logrus.Fields{
			"name":            w.name,
			"fulltext_weight": w.fulltextWeight,
			"vector_weight":   w.vectorWeight,
		}).Info("权重配置")
		queryVector, err = getQueryVector(ctx, query)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to generate query vector")
		}
		hybridResults, _ = rxdb.PerformHybridSearch(ctx, fts, vs, query, queryVector, rxdb.HybridSearchOptions{
			Limit:          3,
			FulltextWeight: w.fulltextWeight,
			VectorWeight:   w.vectorWeight,
		})
		for i, r := range hybridResults {
			data := r.Document.Data()
			logrus.WithFields(logrus.Fields{
				"rank":         i + 1,
				"hybrid_score": r.HybridScore,
				"name":         data["name"],
			}).Info("结果")
		}
	}
	logrus.Info("")

	// 示例 5: 混合搜索的优势演示
	logrus.Info("===========================================")
	logrus.WithField("query", "高性能").Info("🔎 混合搜索优势演示")
	logrus.Info("===========================================")
	query = "高性能"

	// 全文搜索可能找不到（因为"高性能"可能被分词）
	logrus.Info("📝 仅全文搜索:")
	fulltextResults, err = fts.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: 5})
	if err != nil {
		logrus.WithError(err).Warn("全文搜索失败")
	}
	if len(fulltextResults) == 0 {
		logrus.Info("  未找到结果（关键词可能被分词）")
	} else {
		for i, r := range fulltextResults {
			data := r.Document.Data()
			logrus.WithFields(logrus.Fields{
				"rank":  i + 1,
				"score": r.Score,
				"name":  data["name"],
			}).Info("结果")
		}
	}

	// 向量搜索可以理解语义
	logrus.Info("🔢 仅向量搜索:")
	queryText2 := "electronics laptop performance"
	var queryVector2 []float64
	queryVector2, err = embedder.Embed(ctx, queryText2)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate embedding for vector search")
	}
	vectorResults, err = vs.Search(ctx, queryVector2, rxdb.VectorSearchOptions{Limit: 5})
	if err != nil {
		logrus.WithError(err).Warn("向量搜索失败")
	}
	for i, r := range vectorResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":  i + 1,
			"score": r.Score,
			"name":  data["name"],
		}).Info("结果")
	}

	// 混合搜索结合两者优势
	logrus.Info("🔀 混合搜索:")
	queryVector2, err = getQueryVector(ctx, query)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate query vector")
	}
	hybridResults, _ = rxdb.PerformHybridSearch(ctx, fts, vs, query, queryVector2, rxdb.HybridSearchOptions{
		Limit:          5,
		FulltextWeight: 0.4,
		VectorWeight:   0.6,
	})
	for i, r := range hybridResults {
		data := r.Document.Data()
		logrus.WithFields(logrus.Fields{
			"rank":         i + 1,
			"hybrid_score": r.HybridScore,
			"name":         data["name"],
			"description":  data["description"],
		}).Info("结果")
	}
	logrus.Info("")

	logrus.Info("🎉 混合搜索演示完成!")
}

// initEmbedder 从环境变量初始化嵌入器
func initEmbedder(ctx context.Context) error {
	baseURL := os.Getenv("EMBEDDING_BASE_URL")
	apiKey := os.Getenv("EMBEDDING_API_KEY")

	// 必须设置环境变量才能运行
	if baseURL == "" && apiKey == "" {
		return fmt.Errorf("EMBEDDING_BASE_URL and EMBEDDING_API_KEY must be set. Example: export EMBEDDING_BASE_URL=https://api.openai.com/v1 && export EMBEDDING_API_KEY=your-api-key")
	}

	// 如果只设置了其中一个，给出提示
	if apiKey == "" {
		return fmt.Errorf("EMBEDDING_API_KEY 未设置，但 EMBEDDING_BASE_URL 已设置")
	}

	// 构建配置
	config := map[string]interface{}{
		"api_key": apiKey,
	}

	// 可选：设置模型名称
	if model := os.Getenv("EMBEDDING_MODEL"); model != "" {
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

// getQueryVector 根据查询文本生成向量
// 使用真实的嵌入模型生成查询向量
func getQueryVector(ctx context.Context, query string) (rxdb.Vector, error) {
	queryVector, err := embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding for query: %w", err)
	}
	return queryVector, nil
}
