package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"

	"github.com/mozy/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// HybridSearchResult 混合搜索结果
type HybridSearchResult struct {
	Document       rxdb.Document
	FulltextScore  float64
	VectorScore    float64
	VectorDistance float64
	HybridScore    float64
}

func main() {
	ctx := context.Background()

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
			"embedding":   generateCategoryEmbedding("electronics", "phone", "smartphone"),
		},
		{
			"id":          "prod-002",
			"name":        "Samsung Galaxy S24",
			"description": "三星旗舰智能手机，搭载 AI 功能，支持智能翻译和图像识别",
			"category":    "electronics",
			"price":       6999.0,
			"embedding":   generateCategoryEmbedding("electronics", "phone", "smartphone"),
		},
		{
			"id":          "prod-003",
			"name":        "MacBook Pro 16",
			"description": "Apple 专业笔记本电脑，M3 Max 芯片，适合编程和设计工作",
			"category":    "electronics",
			"price":       19999.0,
			"embedding":   generateCategoryEmbedding("electronics", "laptop", "computer"),
		},
		{
			"id":          "prod-004",
			"name":        "Nike Air Max 运动鞋",
			"description": "经典运动鞋，舒适透气，适合跑步和日常穿着",
			"category":    "clothing",
			"price":       899.0,
			"embedding":   generateCategoryEmbedding("clothing", "shoes", "sports"),
		},
		{
			"id":          "prod-005",
			"name":        "Adidas Ultraboost 跑鞋",
			"description": "高性能跑步鞋，Boost 中底技术，提供卓越的缓震效果",
			"category":    "clothing",
			"price":       1299.0,
			"embedding":   generateCategoryEmbedding("clothing", "shoes", "running"),
		},
		{
			"id":          "prod-006",
			"name":        "Levi's 501 牛仔裤",
			"description": "经典直筒牛仔裤，百搭款式，适合各种场合",
			"category":    "clothing",
			"price":       599.0,
			"embedding":   generateCategoryEmbedding("clothing", "pants", "casual"),
		},
		{
			"id":          "prod-007",
			"name":        "Sony WH-1000XM5 降噪耳机",
			"description": "旗舰降噪耳机，卓越音质，支持 LDAC 高解析度音频",
			"category":    "electronics",
			"price":       2999.0,
			"embedding":   generateCategoryEmbedding("electronics", "audio", "headphone"),
		},
		{
			"id":          "prod-008",
			"name":        "《深入理解计算机系统》",
			"description": "计算机科学经典教材，深入讲解系统底层原理",
			"category":    "books",
			"price":       139.0,
			"embedding":   generateCategoryEmbedding("books", "tech", "programming"),
		},
		{
			"id":          "prod-009",
			"name":        "《设计模式：可复用面向对象软件的基础》",
			"description": "GoF 经典设计模式书籍，软件开发的必读之作",
			"category":    "books",
			"price":       89.0,
			"embedding":   generateCategoryEmbedding("books", "tech", "design"),
		},
		{
			"id":          "prod-010",
			"name":        "《三体》科幻小说",
			"description": "刘慈欣科幻小说代表作，雨果奖获奖作品",
			"category":    "books",
			"price":       49.0,
			"embedding":   generateCategoryEmbedding("books", "fiction", "sci-fi"),
		},
	}

	fmt.Println("🛒 插入示例产品...")
	for _, product := range products {
		_, err := collection.Insert(ctx, product)
		if err != nil {
			logrus.WithError(err).WithField("product_id", product["id"]).Error("Failed to insert product")
		}
	}
	fmt.Printf("✅ 已插入 %d 个产品\n\n", len(products))

	// ========================================
	// 创建全文搜索索引
	// ========================================
	fmt.Println("🔍 创建全文搜索索引...")
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
	fmt.Printf("✅ 全文搜索索引创建完成，已索引 %d 个产品\n\n", fts.Count())

	// ========================================
	// 创建向量搜索索引
	// ========================================
	fmt.Println("🔍 创建向量搜索索引...")
	vs, err := rxdb.AddVectorSearch(collection, rxdb.VectorSearchConfig{
		Identifier: "product-vector",
		Dimensions: 8,
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
	fmt.Printf("✅ 向量搜索索引创建完成，已索引 %d 个产品\n\n", vs.Count())

	// ========================================
	// 混合搜索示例
	// ========================================

	// 示例 1: 混合搜索 "智能手机"
	fmt.Println("===========================================")
	fmt.Println("🔎 混合搜索: \"智能手机\"")
	fmt.Println("===========================================")
	query := "智能手机"
	hybridResults := performHybridSearch(ctx, fts, vs, query, 5, 0.5, 0.5)
	fmt.Printf("找到 %d 个相关产品:\n", len(hybridResults))
	for i, r := range hybridResults {
		data := r.Document.Data()
		fmt.Printf("  %d. 📦 [混合分数: %.4f] %s - %s\n",
			i+1,
			r.HybridScore,
			data["name"],
			data["category"])
		fmt.Printf("     全文搜索分数: %.4f, 向量搜索分数: %.4f (距离: %.4f)\n",
			r.FulltextScore,
			r.VectorScore,
			r.VectorDistance)
	}
	fmt.Println()

	// 示例 2: 混合搜索 "运动鞋"
	fmt.Println("===========================================")
	fmt.Println("🔎 混合搜索: \"运动鞋\"")
	fmt.Println("===========================================")
	query = "运动鞋"
	hybridResults = performHybridSearch(ctx, fts, vs, query, 5, 0.5, 0.5)
	fmt.Printf("找到 %d 个相关产品:\n", len(hybridResults))
	for i, r := range hybridResults {
		data := r.Document.Data()
		fmt.Printf("  %d. 📦 [混合分数: %.4f] %s - ¥%.2f\n",
			i+1,
			r.HybridScore,
			data["name"],
			data["price"])
	}
	fmt.Println()

	// 示例 3: 对比不同搜索方式
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索方式对比: \"Apple\"")
	fmt.Println("===========================================")
	query = "Apple"

	// 仅全文搜索
	fmt.Println("\n📝 仅全文搜索:")
	fulltextResults, _ := fts.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: 5})
	for i, r := range fulltextResults {
		data := r.Document.Data()
		fmt.Printf("  %d. [分数: %.4f] %s\n", i+1, r.Score, data["name"])
	}

	// 仅向量搜索
	fmt.Println("\n🔢 仅向量搜索:")
	queryVector := generateCategoryEmbedding("electronics", "phone", "smartphone")
	vectorResults, _ := vs.Search(ctx, queryVector, rxdb.VectorSearchOptions{Limit: 5})
	for i, r := range vectorResults {
		data := r.Document.Data()
		fmt.Printf("  %d. [相似度: %.4f, 距离: %.4f] %s\n",
			i+1, r.Score, r.Distance, data["name"])
	}

	// 混合搜索
	fmt.Println("\n🔀 混合搜索 (全文权重: 0.5, 向量权重: 0.5):")
	hybridResults = performHybridSearch(ctx, fts, vs, query, 5, 0.5, 0.5)
	for i, r := range hybridResults {
		data := r.Document.Data()
		fmt.Printf("  %d. [混合分数: %.4f] %s\n", i+1, r.HybridScore, data["name"])
	}
	fmt.Println()

	// 示例 4: 不同权重比例的混合搜索
	fmt.Println("===========================================")
	fmt.Println("🔎 不同权重比例的混合搜索: \"编程\"")
	fmt.Println("===========================================")
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
		fmt.Printf("\n%s (全文: %.1f, 向量: %.1f):\n", w.name, w.fulltextWeight, w.vectorWeight)
		hybridResults = performHybridSearch(ctx, fts, vs, query, 3, w.fulltextWeight, w.vectorWeight)
		for i, r := range hybridResults {
			data := r.Document.Data()
			fmt.Printf("  %d. [混合分数: %.4f] %s\n", i+1, r.HybridScore, data["name"])
		}
	}
	fmt.Println()

	// 示例 5: 混合搜索的优势演示
	fmt.Println("===========================================")
	fmt.Println("🔎 混合搜索优势演示: \"高性能\"")
	fmt.Println("===========================================")
	query = "高性能"

	// 全文搜索可能找不到（因为"高性能"可能被分词）
	fmt.Println("\n📝 仅全文搜索:")
	fulltextResults, _ = fts.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: 5})
	if len(fulltextResults) == 0 {
		fmt.Println("  未找到结果（关键词可能被分词）")
	} else {
		for i, r := range fulltextResults {
			data := r.Document.Data()
			fmt.Printf("  %d. [分数: %.4f] %s\n", i+1, r.Score, data["name"])
		}
	}

	// 向量搜索可以理解语义
	fmt.Println("\n🔢 仅向量搜索:")
	queryVector = generateCategoryEmbedding("electronics", "laptop", "performance")
	vectorResults, _ = vs.Search(ctx, queryVector, rxdb.VectorSearchOptions{Limit: 5})
	for i, r := range vectorResults {
		data := r.Document.Data()
		fmt.Printf("  %d. [相似度: %.4f] %s\n", i+1, r.Score, data["name"])
	}

	// 混合搜索结合两者优势
	fmt.Println("\n🔀 混合搜索:")
	hybridResults = performHybridSearch(ctx, fts, vs, query, 5, 0.4, 0.6)
	for i, r := range hybridResults {
		data := r.Document.Data()
		fmt.Printf("  %d. [混合分数: %.4f] %s - %s\n",
			i+1, r.HybridScore, data["name"], data["description"])
	}
	fmt.Println()

	fmt.Println("🎉 混合搜索演示完成!")
}

// performHybridSearch 执行混合搜索
// fulltextWeight: 全文搜索权重 (0-1)
// vectorWeight: 向量搜索权重 (0-1)
func performHybridSearch(
	ctx context.Context,
	fts *rxdb.FulltextSearch,
	vs *rxdb.VectorSearch,
	query string,
	limit int,
	fulltextWeight, vectorWeight float64,
) []HybridSearchResult {
	// 执行全文搜索
	fulltextResults, _ := fts.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{
		Limit: limit * 2, // 获取更多结果以便合并
	})

	// 执行向量搜索（需要将查询文本转换为向量）
	// 这里使用简化的方法：根据查询关键词生成向量
	queryVector := generateQueryVector(query)
	vectorResults, _ := vs.Search(ctx, queryVector, rxdb.VectorSearchOptions{
		Limit: limit * 2,
	})

	// 合并结果
	resultMap := make(map[string]*HybridSearchResult)

	// 添加全文搜索结果
	for _, r := range fulltextResults {
		docID := r.Document.ID()
		if existing, ok := resultMap[docID]; ok {
			// 如果已存在，更新全文搜索分数（取较高值）
			if r.Score > existing.FulltextScore {
				existing.FulltextScore = r.Score
			}
		} else {
			resultMap[docID] = &HybridSearchResult{
				Document:      r.Document,
				FulltextScore: r.Score,
				VectorScore:   0,
				HybridScore:   r.Score * fulltextWeight,
			}
		}
	}

	// 添加向量搜索结果
	for _, r := range vectorResults {
		docID := r.Document.ID()
		if existing, ok := resultMap[docID]; ok {
			// 如果已存在，更新向量搜索分数
			existing.VectorScore = r.Score
			existing.VectorDistance = r.Distance
			// 重新计算混合分数
			existing.HybridScore = existing.FulltextScore*fulltextWeight + r.Score*vectorWeight
		} else {
			resultMap[docID] = &HybridSearchResult{
				Document:       r.Document,
				FulltextScore:  0,
				VectorScore:    r.Score,
				VectorDistance: r.Distance,
				HybridScore:    r.Score * vectorWeight,
			}
		}
	}

	// 转换为切片并排序
	results := make([]HybridSearchResult, 0, len(resultMap))
	for _, r := range resultMap {
		results = append(results, *r)
	}

	// 按混合分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].HybridScore > results[j].HybridScore
	})

	// 限制结果数量
	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// generateQueryVector 根据查询文本生成向量
// 这是一个简化的实现，实际应用中应使用嵌入模型
func generateQueryVector(query string) []float64 {
	// 根据查询关键词生成向量
	// 这里使用简单的关键词匹配
	embedding := make([]float64, 8)

	// 检测关键词并设置相应的向量维度
	keywords := map[string][]int{
		"手机": {0, 3}, "智能手机": {0, 3}, "iPhone": {0, 3}, "Samsung": {0, 3},
		"电脑": {0, 4}, "笔记本": {0, 4}, "MacBook": {0, 4}, "laptop": {0, 4},
		"鞋": {1, 5}, "运动鞋": {1, 5}, "跑鞋": {1, 5}, "Nike": {1, 5}, "Adidas": {1, 5},
		"书": {2, 6}, "编程": {2, 6}, "设计": {2, 6}, "小说": {2, 7},
		"耳机": {0, 3}, "音频": {0, 3},
		"高性能": {0, 4}, "性能": {0, 4},
	}

	for keyword, dims := range keywords {
		if strings.Contains(query, keyword) {
			for _, dim := range dims {
				embedding[dim] += 0.5
			}
		}
	}

	// 归一化
	return rxdb.NormalizeVector(embedding)
}

// generateCategoryEmbedding 生成基于分类的简化嵌入向量
func generateCategoryEmbedding(category, subCategory, detail string) []float64 {
	embedding := make([]float64, 8)

	// 基础分类权重
	switch category {
	case "electronics":
		embedding[0] = 1.0
	case "clothing":
		embedding[1] = 1.0
	case "books":
		embedding[2] = 1.0
	}

	// 子分类权重
	switch subCategory {
	case "phone", "smartphone":
		embedding[3] = 0.8
	case "laptop", "computer":
		embedding[4] = 0.8
	case "audio", "headphone":
		embedding[3] = 0.3
		embedding[4] = 0.3
	case "shoes", "sports", "running":
		embedding[5] = 0.8
	case "pants", "casual":
		embedding[5] = 0.3
	case "tech", "programming", "design":
		embedding[6] = 0.8
	case "fiction", "sci-fi":
		embedding[7] = 0.8
	}

	// 添加随机噪声
	for i := range embedding {
		embedding[i] += rand.Float64() * 0.1
	}

	// 归一化
	return rxdb.NormalizeVector(embedding)
}
