package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"

	"github.com/mozy/rxdb-go/pkg/rxdb"
)

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "vector-demo",
		Path: "./vector-demo.db",
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./vector-demo.db")
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
				"category":    map[string]any{"type": "string"},
				"description": map[string]any{"type": "string"},
				"embedding":   map[string]any{"type": "array"},
			},
			"required": []string{"id", "name"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "products", schema)
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// 定义产品数据（模拟带有嵌入向量的产品）
	// 在实际应用中，嵌入向量通常由机器学习模型生成
	// 这里我们使用简化的分类向量来演示
	products := []map[string]any{
		{
			"id":          "prod-001",
			"name":        "iPhone 15 Pro",
			"category":    "electronics",
			"description": "Apple 旗舰智能手机，搭载 A17 Pro 芯片",
			"embedding":   generateCategoryEmbedding("electronics", "phone"),
		},
		{
			"id":          "prod-002",
			"name":        "Samsung Galaxy S24",
			"category":    "electronics",
			"description": "三星旗舰智能手机，搭载 AI 功能",
			"embedding":   generateCategoryEmbedding("electronics", "phone"),
		},
		{
			"id":          "prod-003",
			"name":        "MacBook Pro 16",
			"category":    "electronics",
			"description": "Apple 专业笔记本电脑，M3 Max 芯片",
			"embedding":   generateCategoryEmbedding("electronics", "laptop"),
		},
		{
			"id":          "prod-004",
			"name":        "Nike Air Max",
			"category":    "clothing",
			"description": "经典运动鞋，舒适透气",
			"embedding":   generateCategoryEmbedding("clothing", "shoes"),
		},
		{
			"id":          "prod-005",
			"name":        "Adidas Ultraboost",
			"category":    "clothing",
			"description": "高性能跑步鞋，Boost 中底",
			"embedding":   generateCategoryEmbedding("clothing", "shoes"),
		},
		{
			"id":          "prod-006",
			"name":        "Levi's 501 牛仔裤",
			"category":    "clothing",
			"description": "经典直筒牛仔裤",
			"embedding":   generateCategoryEmbedding("clothing", "pants"),
		},
		{
			"id":          "prod-007",
			"name":        "Sony WH-1000XM5",
			"category":    "electronics",
			"description": "旗舰降噪耳机，卓越音质",
			"embedding":   generateCategoryEmbedding("electronics", "audio"),
		},
		{
			"id":          "prod-008",
			"name":        "《深入理解计算机系统》",
			"category":    "books",
			"description": "计算机科学经典教材",
			"embedding":   generateCategoryEmbedding("books", "tech"),
		},
		{
			"id":          "prod-009",
			"name":        "《设计模式》",
			"category":    "books",
			"description": "GoF 经典设计模式书籍",
			"embedding":   generateCategoryEmbedding("books", "tech"),
		},
		{
			"id":          "prod-010",
			"name":        "《三体》",
			"category":    "books",
			"description": "刘慈欣科幻小说代表作",
			"embedding":   generateCategoryEmbedding("books", "fiction"),
		},
	}

	fmt.Println("🛒 插入示例产品...")
	for _, product := range products {
		_, err := collection.Insert(ctx, product)
		if err != nil {
			log.Printf("Failed to insert product %s: %v", product["id"], err)
		}
	}
	fmt.Printf("✅ 已插入 %d 个产品\n\n", len(products))

	// ========================================
	// 创建向量搜索实例
	// ========================================
	fmt.Println("🔍 创建向量搜索索引...")
	vs, err := rxdb.AddVectorSearch(collection, rxdb.VectorSearchConfig{
		Identifier: "product-similarity",
		Dimensions: 8, // 我们的简化向量是 8 维
		// DocToEmbedding 定义如何从文档提取嵌入向量
		DocToEmbedding: func(doc map[string]any) (rxdb.Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			// 处理 JSON 反序列化后的 []any 类型
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
		DistanceMetric: "cosine", // 使用余弦距离
	})
	if err != nil {
		log.Fatalf("Failed to create vector search: %v", err)
	}
	defer vs.Close()
	fmt.Printf("✅ 索引创建完成，已索引 %d 个产品\n\n", vs.Count())

	// ========================================
	// 向量搜索示例
	// ========================================

	// 示例 1: 查找与 iPhone 相似的产品
	fmt.Println("===========================================")
	fmt.Println("🔎 查找与 \"iPhone 15 Pro\" 相似的产品")
	fmt.Println("===========================================")
	results, err := vs.SearchByID(ctx, "prod-001", rxdb.VectorSearchOptions{
		Limit: 5,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 个相似产品:\n", len(results))
	for _, r := range results {
		fmt.Printf("  📦 [相似度: %.2f] %s - %s\n",
			r.Score,
			r.Document.Data()["name"],
			r.Document.Data()["category"])
	}
	fmt.Println()

	// 示例 2: 查找与运动鞋相似的产品
	fmt.Println("===========================================")
	fmt.Println("🔎 查找与 \"Nike Air Max\" 相似的产品")
	fmt.Println("===========================================")
	results, err = vs.SearchByID(ctx, "prod-004", rxdb.VectorSearchOptions{
		Limit: 5,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 个相似产品:\n", len(results))
	for _, r := range results {
		fmt.Printf("  📦 [相似度: %.2f] %s - %s\n",
			r.Score,
			r.Document.Data()["name"],
			r.Document.Data()["category"])
	}
	fmt.Println()

	// 示例 3: 使用查询向量搜索电子产品
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索电子产品 (使用查询向量)")
	fmt.Println("===========================================")
	queryVector := generateCategoryEmbedding("electronics", "phone")
	results, err = vs.Search(ctx, queryVector, rxdb.VectorSearchOptions{
		Limit: 5,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 个相关产品:\n", len(results))
	for _, r := range results {
		fmt.Printf("  📦 [距离: %.4f, 相似度: %.2f] %s\n",
			r.Distance,
			r.Score,
			r.Document.Data()["name"])
	}
	fmt.Println()

	// 示例 4: KNN 搜索 - 查找最近的 K 个邻居
	fmt.Println("===========================================")
	fmt.Println("🔎 KNN 搜索: 查找 3 个最相似的书籍")
	fmt.Println("===========================================")
	bookVector := generateCategoryEmbedding("books", "tech")
	results, err = vs.KNNSearch(ctx, bookVector, 3)
	if err != nil {
		log.Fatalf("KNN search failed: %v", err)
	}
	fmt.Printf("找到 %d 个最近邻:\n", len(results))
	for _, r := range results {
		fmt.Printf("  📦 [相似度: %.2f] %s - %s\n",
			r.Score,
			r.Document.Data()["name"],
			r.Document.Data()["category"])
	}
	fmt.Println()

	// 示例 5: 范围搜索 - 查找距离在阈值内的产品
	fmt.Println("===========================================")
	fmt.Println("🔎 范围搜索: 查找距离 < 0.5 的产品")
	fmt.Println("===========================================")
	results, err = vs.RangeSearch(ctx, queryVector, 0.5)
	if err != nil {
		log.Fatalf("Range search failed: %v", err)
	}
	fmt.Printf("找到 %d 个在范围内的产品:\n", len(results))
	for _, r := range results {
		fmt.Printf("  📦 [距离: %.4f] %s\n",
			r.Distance,
			r.Document.Data()["name"])
	}
	fmt.Println()

	// ========================================
	// 向量距离计算示例
	// ========================================
	fmt.Println("===========================================")
	fmt.Println("📐 向量距离计算演示")
	fmt.Println("===========================================")

	vec1 := []float64{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
	vec2 := []float64{0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
	vec3 := []float64{0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0}

	fmt.Printf("向量 A: %v\n", vec1)
	fmt.Printf("向量 B: %v\n", vec2)
	fmt.Printf("向量 C: %v\n", vec3)
	fmt.Println()

	fmt.Printf("余弦相似度 (A, B): %.4f\n", rxdb.CosineSimilarity(vec1, vec2))
	fmt.Printf("余弦相似度 (A, C): %.4f\n", rxdb.CosineSimilarity(vec1, vec3))
	fmt.Printf("余弦距离 (A, B): %.4f\n", rxdb.CosineDistance(vec1, vec2))
	fmt.Printf("余弦距离 (A, C): %.4f\n", rxdb.CosineDistance(vec1, vec3))
	fmt.Printf("欧几里得距离 (A, B): %.4f\n", rxdb.EuclideanDistance(vec1, vec2))
	fmt.Printf("欧几里得距离 (A, C): %.4f\n", rxdb.EuclideanDistance(vec1, vec3))
	fmt.Println()

	// ========================================
	// 向量归一化示例
	// ========================================
	fmt.Println("===========================================")
	fmt.Println("📏 向量归一化演示")
	fmt.Println("===========================================")
	original := []float64{3.0, 4.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
	normalized := rxdb.NormalizeVector(original)
	fmt.Printf("原始向量: %v\n", original)
	fmt.Printf("归一化后: %v\n", normalized)
	fmt.Printf("原始向量长度: %.4f\n", vectorLength(original))
	fmt.Printf("归一化后长度: %.4f\n", vectorLength(normalized))
	fmt.Println()

	// ========================================
	// 持久化索引示例
	// ========================================
	fmt.Println("===========================================")
	fmt.Println("💾 持久化向量索引")
	fmt.Println("===========================================")
	err = vs.Persist(ctx)
	if err != nil {
		log.Printf("Failed to persist index: %v", err)
	} else {
		fmt.Println("✅ 向量索引已持久化到存储")
	}
	fmt.Println()

	fmt.Println("🎉 向量搜索演示完成!")
}

// generateCategoryEmbedding 生成基于分类的简化嵌入向量
// 这是一个演示用的简化实现，实际应用中应使用机器学习模型
func generateCategoryEmbedding(category, subCategory string) []float64 {
	// 8 维向量，每个维度代表一个特征
	// [电子产品, 服装, 书籍, 手机, 电脑, 鞋子, 技术书, 小说]
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
	case "phone":
		embedding[3] = 0.8
	case "laptop":
		embedding[4] = 0.8
	case "audio":
		embedding[3] = 0.3
		embedding[4] = 0.3
	case "shoes":
		embedding[5] = 0.8
	case "pants":
		embedding[5] = 0.3
	case "tech":
		embedding[6] = 0.8
	case "fiction":
		embedding[7] = 0.8
	}

	// 添加一些随机噪声使向量更真实
	for i := range embedding {
		embedding[i] += rand.Float64() * 0.1
	}

	// 归一化
	return rxdb.NormalizeVector(embedding)
}

// vectorLength 计算向量长度
func vectorLength(v []float64) float64 {
	var sum float64
	for _, val := range v {
		sum += val * val
	}
	return math.Sqrt(sum)
}
