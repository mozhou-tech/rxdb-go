package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mozy/rxdb-go/pkg/rxdb"
)

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "fulltext-demo",
		Path: "./fulltext-demo.db",
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./fulltext-demo.db")
	}()

	// 定义文章集合的 schema
	schema := rxdb.Schema{
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

	// 创建集合
	collection, err := db.Collection(ctx, "articles", schema)
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// 插入示例文章
	articles := []map[string]any{
		{
			"id":      "article-001",
			"title":   "Go 语言入门指南",
			"content": "Go 是一种静态类型、编译型语言，由 Google 开发。它具有简洁的语法和强大的并发支持，非常适合构建高性能的服务端应用程序。",
			"author":  "张三",
			"tags":    []string{"Go", "编程", "入门"},
		},
		{
			"id":      "article-002",
			"title":   "深入理解 Go 并发编程",
			"content": "Go 的 goroutine 和 channel 是其并发模型的核心。通过 goroutine 可以轻松创建轻量级线程，而 channel 则提供了安全的通信方式。",
			"author":  "李四",
			"tags":    []string{"Go", "并发", "高级"},
		},
		{
			"id":      "article-003",
			"title":   "Python 机器学习实战",
			"content": "Python 是数据科学和机器学习的首选语言。本文介绍如何使用 scikit-learn 和 TensorFlow 构建机器学习模型。",
			"author":  "王五",
			"tags":    []string{"Python", "机器学习", "AI"},
		},
		{
			"id":      "article-004",
			"title":   "JavaScript 前端框架对比",
			"content": "React、Vue 和 Angular 是目前最流行的前端框架。本文将从性能、学习曲线和生态系统等方面进行详细对比。",
			"author":  "赵六",
			"tags":    []string{"JavaScript", "前端", "框架"},
		},
		{
			"id":      "article-005",
			"title":   "Go 微服务架构设计",
			"content": "微服务架构已成为现代应用开发的主流模式。Go 语言凭借其出色的性能和简单的部署方式，成为微服务开发的热门选择。",
			"author":  "张三",
			"tags":    []string{"Go", "微服务", "架构"},
		},
	}

	fmt.Println("📚 插入示例文章...")
	for i, article := range articles {
		fmt.Printf("  正在插入第 %d/%d 篇文章: %s\n", i+1, len(articles), article["id"])
		_, err := collection.Insert(ctx, article)
		if err != nil {
			log.Printf("Failed to insert article %s: %v", article["id"], err)
		} else {
			fmt.Printf("  ✅ 成功插入: %s\n", article["id"])
		}
	}
	fmt.Printf("✅ 已插入 %d 篇文章\n\n", len(articles))

	// ========================================
	// 创建全文搜索实例
	// ========================================
	fmt.Println("🔍 创建全文搜索索引...")
	fts, err := rxdb.AddFulltextSearch(collection, rxdb.FulltextSearchConfig{
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
			Tokenize:      "forward",                              // 支持前缀匹配
			MinLength:     2,                                      // 最小词长度
			CaseSensitive: false,                                  // 不区分大小写
			StopWords:     []string{"的", "是", "和", "了", "在", "有"}, // 中文停用词
		},
	})
	if err != nil {
		log.Fatalf("Failed to create fulltext search: %v", err)
	}
	defer fts.Close()
	fmt.Printf("✅ 索引创建完成，已索引 %d 篇文章\n\n", fts.Count())

	// ========================================
	// 执行搜索示例
	// ========================================

	// 示例 1: 搜索 "Go"
	fmt.Println("=" + "===========================================")
	fmt.Println("🔎 搜索: \"Go\"")
	fmt.Println("===========================================")
	results, err := fts.Find(ctx, "Go")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s - %s\n", doc.ID(), doc.Data()["title"], doc.Data()["author"])
	}
	fmt.Println()

	// 示例 2: 搜索 "并发"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"并发\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "并发")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 3: 搜索 "机器学习" 并返回带分数的结果
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"机器学习\" (带相关性分数)")
	fmt.Println("===========================================")
	resultsWithScores, err := fts.FindWithScores(ctx, "机器学习")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		fmt.Printf("  📄 [分数: %.2f] %s\n", r.Score, r.Document.Data()["title"])
	}
	fmt.Println()

	// 示例 4: 多词搜索
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"Go 微服务\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "Go 微服务")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 5: 带限制的搜索
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"语言\" (限制返回 2 条)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "语言", rxdb.FulltextSearchOptions{
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (限制 2 条):\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 6: 前缀匹配搜索（利用 forward tokenize 模式）
	// 注意：由于配置了 Tokenize: "forward"，搜索 "Go" 会匹配所有以 "Go" 开头的词
	fmt.Println("===========================================")
	fmt.Println("🔎 前缀匹配搜索: \"Go\" (forward 模式支持前缀匹配)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "Go")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (包含 \"Go\" 或以其开头的词):\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 7: 大小写不敏感搜索
	fmt.Println("===========================================")
	fmt.Println("🔎 大小写不敏感搜索: \"python\" (小写)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "python")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 8: 空查询处理
	fmt.Println("===========================================")
	fmt.Println("🔎 空查询测试: \"\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (空查询应返回空结果):\n", len(results))
	fmt.Println()

	// 示例 9: 不存在的关键词
	fmt.Println("===========================================")
	fmt.Println("🔎 不存在的关键词: \"不存在的内容\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "不存在的内容")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (应该为 0):\n", len(results))
	fmt.Println()

	// 示例 10: 停用词过滤测试
	fmt.Println("===========================================")
	fmt.Println("🔎 停用词测试: \"的\" (应该被过滤)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "的")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (停用词应返回空结果):\n", len(results))
	fmt.Println()

	// 示例 11: 相关性阈值过滤
	fmt.Println("===========================================")
	fmt.Println("🔎 相关性阈值过滤: \"Go\" (阈值 5.0)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "Go", rxdb.FulltextSearchOptions{
		Threshold: 5.0,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (分数 >= 5.0):\n", len(results))
	resultsWithScores, _ = fts.FindWithScores(ctx, "Go")
	for _, r := range resultsWithScores {
		if r.Score >= 5.0 {
			fmt.Printf("  📄 [分数: %.2f] %s\n", r.Score, r.Document.Data()["title"])
		}
	}
	fmt.Println()

	// 示例 12: 中文分词测试
	fmt.Println("===========================================")
	fmt.Println("🔎 中文分词测试: \"编程\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "编程")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 13: 数字搜索测试
	fmt.Println("===========================================")
	fmt.Println("🔎 数字搜索测试: \"001\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "001")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (通过文档ID匹配):\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 14: 组合搜索（多个关键词）
	fmt.Println("===========================================")
	fmt.Println("🔎 组合搜索: \"Go 并发 编程\"")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "Go 并发 编程")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (包含多个关键词):\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// 示例 15: 短词搜索（测试最小长度限制）
	fmt.Println("===========================================")
	fmt.Println("🔎 短词搜索: \"G\" (长度小于 MinLength=2)")
	fmt.Println("===========================================")
	results, err = fts.Find(ctx, "G")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 篇相关文章 (短词应被过滤):\n", len(results))
	fmt.Println()

	// ========================================
	// 实时索引更新示例
	// ========================================
	fmt.Println("===========================================")
	fmt.Println("📝 实时索引更新测试")
	fmt.Println("===========================================")

	// 插入新文章
	fmt.Println("插入新文章: \"Rust 系统编程\"...")
	_, err = collection.Insert(ctx, map[string]any{
		"id":      "article-006",
		"title":   "Rust 系统编程入门",
		"content": "Rust 是一种系统编程语言，专注于安全性、速度和并发性。它通过所有权系统实现内存安全。",
		"author":  "周七",
		"tags":    []string{"Rust", "系统编程", "安全"},
	})
	if err != nil {
		log.Printf("Insert failed: %v", err)
	}

	// 手动重建索引以包含新文档（实际应用中会自动更新）
	fts.Reindex(ctx)

	// 搜索新文章
	fmt.Println("搜索 \"系统\"...")
	results, _ = fts.Find(ctx, "系统")
	fmt.Printf("找到 %d 篇相关文章:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  📄 [%s] %s\n", doc.ID(), doc.Data()["title"])
	}
	fmt.Println()

	// ========================================
	// 持久化索引示例
	// ========================================
	fmt.Println("===========================================")
	fmt.Println("💾 持久化索引")
	fmt.Println("===========================================")
	err = fts.Persist(ctx)
	if err != nil {
		log.Printf("Failed to persist index: %v", err)
	} else {
		fmt.Println("✅ 索引已持久化到存储")
	}
	fmt.Println()

	fmt.Println("🎉 全文搜索演示完成!")
}
