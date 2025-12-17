//go:build ignore
// +build ignore

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/mozy/rxdb-go/pkg/rxdb"
)

// DashScope API 结构
type DashScopeEmbeddingRequest struct {
	Model string         `json:"model"`
	Input DashScopeInput `json:"input"`
}

type DashScopeInput struct {
	Texts []string `json:"texts"`
}

type DashScopeEmbeddingResponse struct {
	Output DashScopeOutput `json:"output"`
}

type DashScopeOutput struct {
	Embeddings []DashScopeEmbedding `json:"embeddings"`
}

type DashScopeEmbedding struct {
	Embedding []float32 `json:"embedding"`
}

// generateEmbedding 使用 DashScope API 生成文本的 embedding 向量
func generateEmbedding(text string) ([]float64, error) {
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("DASHSCOPE_API_KEY environment variable is not set")
	}

	// DashScope embedding API 端点
	url := "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding"

	// 构建请求
	reqBody := DashScopeEmbeddingRequest{
		Model: "text-embedding-v4", // DashScope 文本嵌入模型 v4
		Input: DashScopeInput{
			Texts: []string{text},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// 创建 HTTP 请求
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))

	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	var apiResp DashScopeEmbeddingResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(apiResp.Output.Embeddings) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	// 将 embedding 转换为 []float64
	embedding := apiResp.Output.Embeddings[0].Embedding
	result := make([]float64, len(embedding))
	for i, v := range embedding {
		result[i] = float64(v)
	}

	return result, nil
}

// generateCategoryEmbedding 基于分类信息生成 embedding
func generateCategoryEmbedding(category, subcategory, description string) []float64 {
	// 组合文本用于生成 embedding
	text := strings.Join([]string{category, subcategory, description}, " ")

	embedding, err := generateEmbedding(text)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"category":    category,
			"subcategory": subcategory,
		}).Warn("生成 embedding 失败，使用随机向量")
		// 回退到随机向量
		return generateRandomEmbedding(1536) // DashScope 默认维度是 1536
	}

	return embedding
}

// generateRandomEmbedding 生成随机向量（作为回退方案）
func generateRandomEmbedding(dim int) []float64 {
	embedding := make([]float64, dim)
	for i := range embedding {
		embedding[i] = float64(i%100) / 100.0 // 简单的伪随机
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

	// 删除旧的数据目录（如果存在）
	fmt.Println("🗑️  清理旧数据目录...")
	if _, err := os.Stat(dbPath); err == nil {
		fmt.Printf("   删除目录: %s\n", dbPath)
		if err := os.RemoveAll(dbPath); err != nil {
			logrus.WithError(err).Fatal("Failed to remove old data directory")
		}
		fmt.Println("   ✅ 旧数据目录已删除")
	} else if os.IsNotExist(err) {
		fmt.Println("   ℹ️  数据目录不存在，跳过删除")
	} else {
		logrus.WithError(err).Fatal("Failed to check data directory")
	}

	// 确保数据目录存在
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		logrus.WithError(err).Fatal("Failed to create data directory")
	}
	fmt.Println("   ✅ 数据目录已准备就绪")
	fmt.Println()

	ctx := context.Background()

	// 创建或打开数据库（启用图数据库功能）
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: dbName,
		Path: dbPath,
		GraphOptions: &rxdb.GraphOptions{
			Enabled:  true,
			Backend:  "badger", // 使用 Badger 后端（持久化）
			Path:     filepath.Join(dbPath, "graph"),
			AutoSync: true, // 启用自动同步
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
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
		logrus.WithError(err).Fatal("Failed to create articles collection")
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
			logrus.WithError(err).WithField("article_id", article["id"]).Error("插入失败")
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
		logrus.WithError(err).Warn("创建全文搜索索引失败")
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
		logrus.WithError(err).Fatal("Failed to create products collection")
	}

	products := []map[string]any{
		{
			"id":          "prod-001",
			"name":        "iPhone 15 Pro",
			"category":    "electronics",
			"description": "Apple 旗舰智能手机，搭载 A17 Pro 芯片",
		},
		{
			"id":          "prod-002",
			"name":        "Samsung Galaxy S24",
			"category":    "electronics",
			"description": "三星旗舰智能手机，搭载 AI 功能",
		},
		{
			"id":          "prod-003",
			"name":        "MacBook Pro 16",
			"category":    "electronics",
			"description": "Apple 专业笔记本电脑，M3 Max 芯片",
		},
		{
			"id":          "prod-004",
			"name":        "Nike Air Max",
			"category":    "clothing",
			"description": "经典运动鞋，舒适透气",
		},
		{
			"id":          "prod-005",
			"name":        "Adidas Ultraboost",
			"category":    "clothing",
			"description": "高性能跑步鞋，Boost 中底",
		},
		{
			"id":          "prod-006",
			"name":        "Levi's 501 牛仔裤",
			"category":    "clothing",
			"description": "经典直筒牛仔裤",
		},
		{
			"id":          "prod-007",
			"name":        "《深入理解计算机系统》",
			"category":    "books",
			"description": "计算机科学经典教材",
		},
		{
			"id":          "prod-008",
			"name":        "《三体》",
			"category":    "books",
			"description": "刘慈欣科幻小说代表作",
		},
		{
			"id":          "prod-009",
			"name":        "iPad Pro",
			"category":    "electronics",
			"description": "Apple 专业平板电脑，M2 芯片",
		},
		{
			"id":          "prod-010",
			"name":        "AirPods Pro",
			"category":    "electronics",
			"description": "Apple 主动降噪无线耳机",
		},
	}

	fmt.Printf("  插入 %d 个产品...\n", len(products))
	fmt.Println("  ⚠️  正在使用 DashScope 生成 embedding，这可能需要一些时间...")

	// 检查是否设置了 API Key
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	if apiKey == "" {
		logrus.Warn("DASHSCOPE_API_KEY 未设置，将使用随机向量")
		logrus.Info("提示: 设置环境变量 DASHSCOPE_API_KEY 以使用真实的 embedding")
	}

	for i, product := range products {
		// 为每个产品生成 embedding
		name := product["name"].(string)
		description := product["description"].(string)
		category := product["category"].(string)
		text := fmt.Sprintf("%s %s %s", name, category, description)

		fmt.Printf("  🔄 [%d/%d] 正在为 %s 生成 embedding...\n", i+1, len(products), name)
		embedding, err := generateEmbedding(text)
		if err != nil {
			logrus.WithError(err).WithField("product_id", product["id"]).Warn("生成 embedding 失败，使用随机向量")
			embedding = generateRandomEmbedding(1536)
		}

		// 验证 embedding 维度（text-embedding-v4 支持多种维度，通常为 1536）
		if len(embedding) == 0 {
			logrus.Warn("embedding dimension is 0")
		} else {
			logrus.WithField("dimension", len(embedding)).Debug("Generated embedding dimension (text-embedding-v4)")
		}

		product["embedding"] = embedding

		// 验证赋值后的 embedding
		if emb, ok := product["embedding"].([]float64); ok {
			logrus.WithFields(logrus.Fields{
				"type":      "[]float64",
				"dimension": len(emb),
			}).Debug("Embedding assigned")
		} else {
			logrus.WithField("type", fmt.Sprintf("%T", product["embedding"])).Warn("Warning: embedding type after assignment")
		}

		_, err = productsCollection.Insert(ctx, product)
		if err != nil {
			logrus.WithError(err).WithField("product_id", product["id"]).Error("插入失败")
		} else {
			fmt.Printf("  ✅ [%d/%d] %s (embedding 维度: %d)\n", i+1, len(products), product["id"], len(embedding))

			// 验证插入后的数据
			insertedDoc, findErr := productsCollection.FindByID(ctx, product["id"].(string))
			if findErr == nil {
				docData := insertedDoc.Data()
				if emb, ok := docData["embedding"].([]float64); ok {
					log.Printf("  ✅ 验证: 插入后 embedding 类型 []float64, 维度: %d", len(emb))
				} else if embAny, ok := docData["embedding"].([]interface{}); ok {
					log.Printf("  ✅ 验证: 插入后 embedding 类型 []interface{}, 维度: %d", len(embAny))
					if len(embAny) == 0 {
						log.Printf("  ⚠️  Warning: 插入后 embedding 维度为 0")
					}
				} else {
					log.Printf("  ⚠️  Warning: 插入后 embedding 类型异常: %T", docData["embedding"])
				}
			}
		}
	}
	fmt.Printf("✅ products 集合创建完成，共 %d 个产品\n\n", len(products))

	// ========================================
	// 创建 users 集合（用于图数据库）
	// ========================================
	fmt.Println("👥 创建 users 集合...")
	usersSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "user",
			"description": "用户集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":      map[string]any{"type": "string"},
				"name":    map[string]any{"type": "string"},
				"email":   map[string]any{"type": "string"},
				"follows": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required": []string{"id", "name"},
		},
	}

	usersCollection, err := db.Collection(ctx, "users", usersSchema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create users collection")
	}

	users := []map[string]any{
		{
			"id":      "user1",
			"name":    "Alice",
			"email":   "alice@example.com",
			"follows": []string{"user2", "user3"},
		},
		{
			"id":      "user2",
			"name":    "Bob",
			"email":   "bob@example.com",
			"follows": []string{"user3", "user4"},
		},
		{
			"id":      "user3",
			"name":    "Charlie",
			"email":   "charlie@example.com",
			"follows": []string{"user4"},
		},
		{
			"id":      "user4",
			"name":    "Diana",
			"email":   "diana@example.com",
			"follows": []string{"user1"},
		},
		{
			"id":      "user5",
			"name":    "Eve",
			"email":   "eve@example.com",
			"follows": []string{"user1", "user2"},
		},
	}

	fmt.Printf("  插入 %d 个用户...\n", len(users))
	for i, user := range users {
		_, err := usersCollection.Insert(ctx, user)
		if err != nil {
			logrus.WithError(err).WithField("user_id", user["id"]).Error("插入失败")
		} else {
			fmt.Printf("  ✅ [%d/%d] %s\n", i+1, len(users), user["id"])
		}
	}
	fmt.Printf("✅ users 集合创建完成，共 %d 个用户\n\n", len(users))

	// ========================================
	// 创建图关系（关注关系）
	// ========================================
	fmt.Println("🔗 创建图关系...")
	graphDB := db.Graph()
	if graphDB == nil {
		logrus.WithError(err).Fatal("❌ 图数据库不可用！请检查数据库配置是否正确启用了图数据库功能")
	}

	fmt.Println("  ✅ 图数据库已初始化")

	// 创建关注关系
	relations := []struct {
		from     string
		relation string
		to       string
	}{
		{"user1", "follows", "user2"},
		{"user1", "follows", "user3"},
		{"user2", "follows", "user3"},
		{"user2", "follows", "user4"},
		{"user3", "follows", "user4"},
		{"user4", "follows", "user1"},
		{"user5", "follows", "user1"},
		{"user5", "follows", "user2"},
	}

	fmt.Printf("  创建 %d 个关注关系...\n", len(relations))
	successCount := 0
	for i, rel := range relations {
		if err := graphDB.Link(ctx, rel.from, rel.relation, rel.to); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"index":    i + 1,
				"total":    len(relations),
				"from":     rel.from,
				"relation": rel.relation,
				"to":       rel.to,
			}).Error("创建关系失败")
		} else {
			fmt.Printf("  ✅ [%d/%d] %s --%s--> %s\n", i+1, len(relations), rel.from, rel.relation, rel.to)
			successCount++
		}
	}
	fmt.Printf("✅ 图关系创建完成，成功创建 %d/%d 个关系\n\n", successCount, len(relations))

	// 验证图关系是否创建成功
	fmt.Println("🔍 验证图关系...")
	testNeighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		logrus.WithError(err).Warn("验证失败")
	} else {
		fmt.Printf("  ✅ user1 的邻居: %v\n", testNeighbors)
		if len(testNeighbors) == 0 {
			logrus.Warn("user1 没有邻居，图关系可能没有正确创建")
		}
	}
	fmt.Println()

	// ========================================
	// 统计信息
	// ========================================
	articlesCount, _ := articlesCollection.Count(ctx)
	productsCount, _ := productsCollection.Count(ctx)
	usersCount, _ := usersCollection.Count(ctx)

	fmt.Println("📊 数据统计:")
	fmt.Printf("  - articles: %d 篇\n", articlesCount)
	fmt.Printf("  - products: %d 个\n", productsCount)
	fmt.Printf("  - users: %d 个\n", usersCount)
	fmt.Println("\n✨ 示例数据生成完成！")
	fmt.Println("\n💡 提示:")
	fmt.Println("  - 在浏览器中访问 http://localhost:3001 查看数据")
	fmt.Println("  - 使用 'articles' 集合测试全文搜索")
	fmt.Println("  - 使用 'products' 集合测试向量搜索")
	fmt.Println("  - 使用 'users' 集合和图数据库测试图查询")
}

// 辅助函数：min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
