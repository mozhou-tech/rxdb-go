package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "book-fulltext-demo",
		Path: "./data/book-fulltext-demo.db",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./data/book-fulltext-demo.db")
	}()

	// 定义书籍集合的 schema
	// 方案1: 整本书作为一个文档
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "book",
			"description": "书籍集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":          map[string]any{"type": "string"},
				"title":       map[string]any{"type": "string"},
				"author":      map[string]any{"type": "string"},
				"isbn":        map[string]any{"type": "string"},
				"publisher":   map[string]any{"type": "string"},
				"publishDate": map[string]any{"type": "string"},
				"content":     map[string]any{"type": "string"}, // 整本书的内容
				"chapters": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"chapterNumber": map[string]any{"type": "integer"},
							"chapterTitle":  map[string]any{"type": "string"},
							"content":       map[string]any{"type": "string"},
						},
					},
				},
			},
			"required": []string{"id", "title", "content"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "books", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 插入示例书籍
	book := map[string]any{
		"id":          "book-001",
		"title":       "Go 语言程序设计",
		"author":      "Alan Donovan & Brian Kernighan",
		"isbn":        "978-7-111-55842-2",
		"publisher":   "机械工业出版社",
		"publishDate": "2017-01",
		"content": `Go 语言是 Google 开发的一种编程语言。它是一种编译型语言，具有静态类型系统。
Go 语言的设计目标是提供一种简单、高效、可靠的编程语言，特别适合构建大型软件系统。

Go 语言的主要特点包括：
1. 简洁的语法：Go 语言的语法非常简洁，易于学习和使用。
2. 并发支持：Go 语言内置了 goroutine 和 channel，使得并发编程变得简单。
3. 快速编译：Go 语言的编译速度非常快，适合大型项目的开发。
4. 垃圾回收：Go 语言具有自动垃圾回收机制，无需手动管理内存。

Go 语言的应用领域非常广泛，包括：
- Web 开发：Go 语言可以用来开发高性能的 Web 服务器和 API。
- 系统编程：Go 语言可以用来开发操作系统、网络工具等系统软件。
- 云计算：Go 语言在云计算领域有广泛应用，如 Docker、Kubernetes 等。
- 微服务：Go 语言非常适合构建微服务架构。

Go 语言的并发模型是其最重要的特性之一。通过 goroutine，可以轻松创建成千上万个并发任务。
Channel 提供了 goroutine 之间安全通信的机制，避免了传统并发编程中的竞态条件问题。

Go 语言的包管理系统也非常完善。通过 go mod 命令，可以方便地管理项目依赖。
Go 语言的工具链包括编译器、格式化工具、测试工具等，都集成在 go 命令中。

总的来说，Go 语言是一种现代化的编程语言，它结合了静态类型语言的安全性和动态语言的灵活性。
无论是初学者还是有经验的开发者，都可以从 Go 语言中受益。`,
		"chapters": []map[string]any{
			{
				"chapterNumber": 1,
				"chapterTitle":  "Go 语言入门",
				"content":       "本章介绍 Go 语言的基本概念、安装方法和第一个 Go 程序。",
			},
			{
				"chapterNumber": 2,
				"chapterTitle":  "程序结构",
				"content":       "本章介绍 Go 语言程序的基本结构，包括包、变量、函数等。",
			},
			{
				"chapterNumber": 3,
				"chapterTitle":  "基础数据类型",
				"content":       "本章介绍 Go 语言的基础数据类型，包括整数、浮点数、字符串等。",
			},
			{
				"chapterNumber": 4,
				"chapterTitle":  "复合数据类型",
				"content":       "本章介绍 Go 语言的复合数据类型，包括数组、切片、映射、结构体等。",
			},
			{
				"chapterNumber": 5,
				"chapterTitle":  "函数",
				"content":       "本章介绍 Go 语言的函数定义、调用、参数传递等。",
			},
			{
				"chapterNumber": 6,
				"chapterTitle":  "方法",
				"content":       "本章介绍 Go 语言的方法定义、接收者、方法集等。",
			},
			{
				"chapterNumber": 7,
				"chapterTitle":  "接口",
				"content":       "本章介绍 Go 语言的接口定义、实现、类型断言等。",
			},
			{
				"chapterNumber": 8,
				"chapterTitle":  "Goroutine 和 Channel",
				"content":       "本章介绍 Go 语言的并发编程，包括 goroutine、channel、select 等。",
			},
		},
	}

	fmt.Println("📚 插入示例书籍...")
	_, err = collection.Insert(ctx, book)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to insert book")
	}
	fmt.Printf("✅ 已插入书籍: %s\n\n", book["title"])

	// ========================================
	// 创建全文搜索实例 - 方案1: 索引整本书
	// ========================================
	fmt.Println("🔍 创建全文搜索索引（整本书）...")
	fts, err := rxdb.AddFulltextSearch(collection, rxdb.FulltextSearchConfig{
		Identifier: "book-fulltext-search",
		// DocToString 定义如何将文档转换为可搜索的字符串
		DocToString: func(doc map[string]any) string {
			var parts []string

			// 书名（高权重，重复3次）
			if title, ok := doc["title"].(string); ok && title != "" {
				parts = append(parts, title, title, title)
			}

			// 作者（中等权重，重复2次）
			if author, ok := doc["author"].(string); ok && author != "" {
				parts = append(parts, author, author)
			}

			// 出版社
			if publisher, ok := doc["publisher"].(string); ok && publisher != "" {
				parts = append(parts, publisher)
			}

			// 整本书的内容
			if content, ok := doc["content"].(string); ok && content != "" {
				parts = append(parts, content)
			}

			// 章节信息（章节标题权重较高）
			if chapters, ok := doc["chapters"].([]any); ok {
				for _, ch := range chapters {
					if chapter, ok := ch.(map[string]any); ok {
						// 章节标题（重复2次以增加权重）
						if chapterTitle, ok := chapter["chapterTitle"].(string); ok && chapterTitle != "" {
							parts = append(parts, chapterTitle, chapterTitle)
						}
						// 章节内容
						if chapterContent, ok := chapter["content"].(string); ok && chapterContent != "" {
							parts = append(parts, chapterContent)
						}
					}
				}
			}

			return strings.Join(parts, " ")
		},
		// 索引选项
		IndexOptions: &rxdb.FulltextIndexOptions{
			Tokenize:      "jieba", // 使用 gojieba 中文分词
			MinLength:     2,       // 最小词长度
			CaseSensitive: false,   // 不区分大小写
			StopWords: []string{
				"的", "是", "和", "了", "在", "有", "与", "及", "或", "但", "而",
				"这", "那", "它", "他", "她", "我们", "你们", "他们",
				"一个", "一种", "一些", "可以", "能够", "应该", "必须",
			}, // 中文停用词
		},
		Initialization: "instant", // 立即建立索引
		BatchSize:      10,        // 批量大小
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create fulltext search")
	}
	defer fts.Close()
	fmt.Printf("✅ 索引创建完成，已索引 %d 本书\n\n", fts.Count())

	// ========================================
	// 执行搜索示例
	// ========================================

	// 示例 1: 搜索 "并发"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"并发\"")
	fmt.Println("===========================================")
	resultsWithScores, err := fts.FindWithScores(ctx, "并发")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 本相关书籍:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		fmt.Printf("  📖 [分数: %.2f] [%s] %s - %s\n",
			r.Score, r.Document.ID(), doc["title"], doc["author"])
	}
	fmt.Println()

	// 示例 2: 搜索 "goroutine"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"goroutine\"")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "goroutine")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 本相关书籍:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		fmt.Printf("  📖 [分数: %.2f] [%s] %s\n",
			r.Score, r.Document.ID(), doc["title"])
	}
	fmt.Println()

	// 示例 3: 搜索 "接口"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"接口\"")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "接口")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 本相关书籍:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		fmt.Printf("  📖 [分数: %.2f] [%s] %s\n",
			r.Score, r.Document.ID(), doc["title"])
	}
	fmt.Println()

	// 示例 4: 搜索 "微服务"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"微服务\"")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "微服务")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 本相关书籍:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		fmt.Printf("  📖 [分数: %.2f] [%s] %s\n",
			r.Score, r.Document.ID(), doc["title"])
	}
	fmt.Println()

	// 示例 5: 搜索作者名
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"Alan\"")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "Alan")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 本相关书籍:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		fmt.Printf("  📖 [分数: %.2f] [%s] %s - %s\n",
			r.Score, r.Document.ID(), doc["title"], doc["author"])
	}
	fmt.Println()

	fmt.Println("🎉 书籍全文搜索演示完成!")
}
