//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/mozy/rxdb-go/pkg/rxdb"
)

// generateCategoryEmbedding 生成简化的分类向量（用于演示）
func generateCategoryEmbedding(category, subcategory string) []float64 {
	// 使用简化的 8 维向量来演示
	// 实际应用中应该使用真实的嵌入模型
	rand.Seed(int64(len(category) + len(subcategory)))
	embedding := make([]float64, 8)
	for i := range embedding {
		embedding[i] = rand.Float64()*0.5 + 0.25
	}
	return embedding
}

func main() {
	// 从环境变量读取数据库配置（与 API 服务器保持一致）
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "browser-db"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/browser-db"
	}

	// 确保数据目录存在
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	ctx := context.Background()

	// 创建或打开数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: dbName,
		Path: dbPath,
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	fmt.Println("🌱 开始生成示例数据...")
	fmt.Println()

	// ========================================
	// 创建 articles 集合（用于全文搜索）
	// ========================================
	fmt.Println("📚 创建 articles 集合...")
	articlesSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "article",
			"description": "文章集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":      map[string]any{"type": "string"},
				"title":   map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
				"author":  map[string]any{"type": "string"},
				"tags":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required": []string{"id", "title", "content"},
		},
	}

	articlesCollection, err := db.Collection(ctx, "articles", articlesSchema)
	if err != nil {
		log.Fatalf("Failed to create articles collection: %v", err)
	}

	articles := []map[string]any{
		{
			"id":      "article-001",
			"title":   "Go 语言入门指南",
			"content": "Go 是一种静态类型、编译型语言，由 Google 开发。它具有简洁的语法和强大的并发支持，非常适合构建高性能的服务端应用程序。Go 语言的设计哲学是简洁、高效和可读性强。",
			"author":  "张三",
			"tags":    []string{"Go", "编程", "入门"},
		},
		{
			"id":      "article-002",
			"title":   "深入理解 Go 并发编程",
			"content": "Go 的 goroutine 和 channel 是其并发模型的核心。通过 goroutine 可以轻松创建轻量级线程，而 channel 则提供了安全的通信方式。这种设计使得编写并发程序变得简单而优雅。",
			"author":  "李四",
			"tags":    []string{"Go", "并发", "高级"},
		},
		{
			"id":      "article-003",
			"title":   "Python 机器学习实战",
			"content": "Python 是数据科学和机器学习的首选语言。本文介绍如何使用 scikit-learn 和 TensorFlow 构建机器学习模型。从数据预处理到模型训练，全面覆盖机器学习工作流程。",
			"author":  "王五",
			"tags":    []string{"Python", "机器学习", "AI"},
		},
		{
			"id":      "article-004",
			"title":   "JavaScript 前端框架对比",
			"content": "React、Vue 和 Angular 是目前最流行的前端框架。本文将从性能、学习曲线和生态系统等方面进行详细对比，帮助开发者选择最适合的框架。",
			"author":  "赵六",
			"tags":    []string{"JavaScript", "前端", "框架"},
		},
		{
			"id":      "article-005",
			"title":   "Go 微服务架构设计",
			"content": "微服务架构已成为现代应用开发的主流模式。Go 语言凭借其出色的性能和简单的部署方式，成为微服务开发的热门选择。本文将介绍如何设计可扩展的微服务系统。",
			"author":  "张三",
			"tags":    []string{"Go", "微服务", "架构"},
		},
		{
			"id":      "article-006",
			"title":   "数据库设计最佳实践",
			"content": "良好的数据库设计是应用成功的基础。本文介绍关系型数据库和 NoSQL 数据库的设计原则，包括索引优化、查询性能调优和数据结构选择等关键话题。",
			"author":  "李四",
			"tags":    []string{"数据库", "设计", "优化"},
		},
		{
			"id":      "article-007",
			"title":   "容器化部署指南",
			"content": "Docker 和 Kubernetes 是现代应用部署的标准工具。本文详细介绍如何使用容器技术打包、部署和管理应用程序，包括最佳实践和常见问题解决方案。",
			"author":  "王五",
			"tags":    []string{"Docker", "Kubernetes", "部署"},
		},
		{
			"id":      "article-008",
			"title":   "RESTful API 设计规范",
			"content": "RESTful API 是 Web 服务的主流架构风格。本文介绍 REST API 的设计原则、HTTP 方法的使用、状态码的选择以及版本控制策略，帮助开发者构建高质量的 API。",
			"author":  "赵六",
			"tags":    []string{"API", "REST", "设计"},
		},
	}

	fmt.Printf("  插入 %d 篇文章...\n", len(articles))
	for i, article := range articles {
		_, err := articlesCollection.Insert(ctx, article)
		if err != nil {
			log.Printf("  ❌ 插入失败 %s: %v", article["id"], err)
		} else {
			fmt.Printf("  ✅ [%d/%d] %s\n", i+1, len(articles), article["id"])
		}
	}
	fmt.Printf("✅ articles 集合创建完成，共 %d 篇文章\n\n", len(articles))

	// ========================================
	// 为 articles 创建全文搜索索引
	// ========================================
	fmt.Println("🔍 创建 articles 全文搜索索引...")
	fts, err := rxdb.AddFulltextSearch(articlesCollection, rxdb.FulltextSearchConfig{
		Identifier: "article-search",
		// DocToString 定义如何将文档转换为可搜索的字符串
		DocToString: func(doc map[string]any) string {
			title, _ := doc["title"].(string)
			content, _ := doc["content"].(string)
			author, _ := doc["author"].(string)
			// 合并标题、内容和作者，标题权重更高（重复以增加权重）
			return title + " " + title + " " + content + " " + author
		},
		// 索引选项
		IndexOptions: &rxdb.FulltextIndexOptions{
			Tokenize:      "jieba",                                // 使用 gojieba 中文分词
			MinLength:     2,                                      // 最小词长度
			CaseSensitive: false,                                  // 不区分大小写
			StopWords:     []string{"的", "是", "和", "了", "在", "有"}, // 中文停用词
		},
		Initialization: "instant", // 立即建立索引
	})
	if err != nil {
		log.Printf("⚠️  创建全文搜索索引失败: %v", err)
		fmt.Println("   提示: 全文搜索功能可能不可用，但数据已成功插入")
	} else {
		defer fts.Close()
		fmt.Printf("✅ 全文搜索索引创建完成，已索引 %d 篇文章\n\n", fts.Count())
	}

	// ========================================
	// 创建 products 集合（用于向量搜索）
	// ========================================
	fmt.Println("🛒 创建 products 集合...")
	productsSchema := rxdb.Schema{
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

	productsCollection, err := db.Collection(ctx, "products", productsSchema)
	if err != nil {
		log.Fatalf("Failed to create products collection: %v", err)
	}

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
			"name":        "《深入理解计算机系统》",
			"category":    "books",
			"description": "计算机科学经典教材",
			"embedding":   generateCategoryEmbedding("books", "tech"),
		},
		{
			"id":          "prod-008",
			"name":        "《三体》",
			"category":    "books",
			"description": "刘慈欣科幻小说代表作",
			"embedding":   generateCategoryEmbedding("books", "fiction"),
		},
		{
			"id":          "prod-009",
			"name":        "iPad Pro",
			"category":    "electronics",
			"description": "Apple 专业平板电脑，M2 芯片",
			"embedding":   generateCategoryEmbedding("electronics", "tablet"),
		},
		{
			"id":          "prod-010",
			"name":        "AirPods Pro",
			"category":    "electronics",
			"description": "Apple 主动降噪无线耳机",
			"embedding":   generateCategoryEmbedding("electronics", "audio"),
		},
	}

	fmt.Printf("  插入 %d 个产品...\n", len(products))
	for i, product := range products {
		_, err := productsCollection.Insert(ctx, product)
		if err != nil {
			log.Printf("  ❌ 插入失败 %s: %v", product["id"], err)
		} else {
			fmt.Printf("  ✅ [%d/%d] %s\n", i+1, len(products), product["id"])
		}
	}
	fmt.Printf("✅ products 集合创建完成，共 %d 个产品\n\n", len(products))

	// ========================================
	// 统计信息
	// ========================================
	articlesCount, _ := articlesCollection.Count(ctx)
	productsCount, _ := productsCollection.Count(ctx)

	fmt.Println("📊 数据统计:")
	fmt.Printf("  - articles: %d 篇\n", articlesCount)
	fmt.Printf("  - products: %d 个\n", productsCount)
	fmt.Println("\n✨ 示例数据生成完成！")
	fmt.Println("\n💡 提示:")
	fmt.Println("  - 在浏览器中访问 http://localhost:3000 查看数据")
	fmt.Println("  - 使用 'articles' 集合测试全文搜索")
	fmt.Println("  - 使用 'products' 集合测试向量搜索")
}
