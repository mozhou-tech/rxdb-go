package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// 这个示例展示如何按章节索引书籍
// 每个章节作为一个独立的文档，可以更精确地定位到具体章节

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "book-chapter-demo",
		Path: "./data/book-chapter-demo.db",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./data/book-chapter-demo.db")
	}()

	// 定义章节集合的 schema
	// 方案2: 每个章节作为一个文档
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "chapter",
			"description": "书籍章节集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":            map[string]any{"type": "string"},
				"bookId":        map[string]any{"type": "string"}, // 所属书籍ID
				"bookTitle":     map[string]any{"type": "string"}, // 书籍标题
				"bookAuthor":    map[string]any{"type": "string"}, // 书籍作者
				"chapterNumber": map[string]any{"type": "integer"},
				"chapterTitle":  map[string]any{"type": "string"},
				"content":       map[string]any{"type": "string"},  // 章节内容
				"pageStart":     map[string]any{"type": "integer"}, // 起始页码
				"pageEnd":       map[string]any{"type": "integer"}, // 结束页码
			},
			"required": []string{"id", "bookId", "chapterNumber", "chapterTitle", "content"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "chapters", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 插入示例书籍的章节
	bookTitle := "Go 语言程序设计"
	bookAuthor := "Alan Donovan & Brian Kernighan"
	bookId := "book-001"

	chapters := []map[string]any{
		{
			"id":            "chapter-001-01",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 1,
			"chapterTitle":  "Go 语言入门",
			"content":       "本章介绍 Go 语言的基本概念、安装方法和第一个 Go 程序。Go 语言是 Google 开发的一种编程语言，具有简洁的语法和强大的并发支持。",
			"pageStart":     1,
			"pageEnd":       50,
		},
		{
			"id":            "chapter-001-02",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 2,
			"chapterTitle":  "程序结构",
			"content":       "本章介绍 Go 语言程序的基本结构，包括包、变量、函数等。Go 语言的程序由包组成，每个包可以包含多个文件。",
			"pageStart":     51,
			"pageEnd":       100,
		},
		{
			"id":            "chapter-001-03",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 3,
			"chapterTitle":  "基础数据类型",
			"content":       "本章介绍 Go 语言的基础数据类型，包括整数、浮点数、字符串、布尔值等。Go 语言是静态类型语言，所有变量都必须有明确的类型。",
			"pageStart":     101,
			"pageEnd":       150,
		},
		{
			"id":            "chapter-001-04",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 4,
			"chapterTitle":  "复合数据类型",
			"content":       "本章介绍 Go 语言的复合数据类型，包括数组、切片、映射、结构体等。这些类型可以用来组织更复杂的数据结构。",
			"pageStart":     151,
			"pageEnd":       200,
		},
		{
			"id":            "chapter-001-05",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 5,
			"chapterTitle":  "函数",
			"content":       "本章介绍 Go 语言的函数定义、调用、参数传递、返回值等。函数是 Go 语言的基本构建块，支持多返回值。",
			"pageStart":     201,
			"pageEnd":       250,
		},
		{
			"id":            "chapter-001-06",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 6,
			"chapterTitle":  "方法",
			"content":       "本章介绍 Go 语言的方法定义、接收者、方法集等。方法是与特定类型关联的函数。",
			"pageStart":     251,
			"pageEnd":       300,
		},
		{
			"id":            "chapter-001-07",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 7,
			"chapterTitle":  "接口",
			"content":       "本章介绍 Go 语言的接口定义、实现、类型断言等。接口是 Go 语言实现多态的关键机制。",
			"pageStart":     301,
			"pageEnd":       350,
		},
		{
			"id":            "chapter-001-08",
			"bookId":        bookId,
			"bookTitle":     bookTitle,
			"bookAuthor":    bookAuthor,
			"chapterNumber": 8,
			"chapterTitle":  "Goroutine 和 Channel",
			"content":       "本章介绍 Go 语言的并发编程，包括 goroutine、channel、select 等。Go 语言的并发模型是其最重要的特性之一，通过 goroutine 可以轻松创建成千上万个并发任务，channel 提供了 goroutine 之间安全通信的机制。",
			"pageStart":     351,
			"pageEnd":       400,
		},
	}

	fmt.Println("📚 插入书籍章节...")
	for i, chapter := range chapters {
		_, err := collection.Insert(ctx, chapter)
		if err != nil {
			logrus.WithError(err).WithField("chapter_id", chapter["id"]).Error("Failed to insert chapter")
		} else {
			fmt.Printf("  ✅ [%d/%d] 第 %d 章: %s\n",
				i+1, len(chapters), chapter["chapterNumber"], chapter["chapterTitle"])
		}
	}
	fmt.Printf("✅ 已插入 %d 个章节\n\n", len(chapters))

	// ========================================
	// 创建全文搜索实例 - 方案2: 按章节索引
	// ========================================
	fmt.Println("🔍 创建全文搜索索引（按章节）...")
	fts, err := rxdb.AddFulltextSearch(collection, rxdb.FulltextSearchConfig{
		Identifier: "chapter-fulltext-search",
		// DocToString 定义如何将文档转换为可搜索的字符串
		DocToString: func(doc map[string]any) string {
			var parts []string

			// 书籍标题（高权重，重复2次）
			if bookTitle, ok := doc["bookTitle"].(string); ok && bookTitle != "" {
				parts = append(parts, bookTitle, bookTitle)
			}

			// 书籍作者
			if bookAuthor, ok := doc["bookAuthor"].(string); ok && bookAuthor != "" {
				parts = append(parts, bookAuthor)
			}

			// 章节标题（高权重，重复3次）
			if chapterTitle, ok := doc["chapterTitle"].(string); ok && chapterTitle != "" {
				parts = append(parts, chapterTitle, chapterTitle, chapterTitle)
			}

			// 章节内容
			if content, ok := doc["content"].(string); ok && content != "" {
				parts = append(parts, content)
			}

			// 章节编号（转换为字符串，便于搜索）
			if chapterNum, ok := doc["chapterNumber"].(int); ok {
				parts = append(parts, strconv.Itoa(chapterNum))
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
		BatchSize:      50,        // 批量大小
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create fulltext search")
	}
	defer fts.Close()
	fmt.Printf("✅ 索引创建完成，已索引 %d 个章节\n\n", fts.Count())

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
	fmt.Printf("找到 %d 个相关章节:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		chapterNum := doc["chapterNumber"]
		chapterTitle := doc["chapterTitle"]
		pageStart := doc["pageStart"]
		pageEnd := doc["pageEnd"]
		fmt.Printf("  📖 [分数: %.2f] 第 %v 章: %s (页码: %v-%v)\n",
			r.Score, chapterNum, chapterTitle, pageStart, pageEnd)
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
	fmt.Printf("找到 %d 个相关章节:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		chapterNum := doc["chapterNumber"]
		chapterTitle := doc["chapterTitle"]
		fmt.Printf("  📖 [分数: %.2f] 第 %v 章: %s\n",
			r.Score, chapterNum, chapterTitle)
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
	fmt.Printf("找到 %d 个相关章节:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		chapterNum := doc["chapterNumber"]
		chapterTitle := doc["chapterTitle"]
		fmt.Printf("  📖 [分数: %.2f] 第 %v 章: %s\n",
			r.Score, chapterNum, chapterTitle)
	}
	fmt.Println()

	// 示例 4: 搜索 "函数"
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"函数\"")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "函数")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 个相关章节:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		chapterNum := doc["chapterNumber"]
		chapterTitle := doc["chapterTitle"]
		fmt.Printf("  📖 [分数: %.2f] 第 %v 章: %s\n",
			r.Score, chapterNum, chapterTitle)
	}
	fmt.Println()

	// 示例 5: 搜索章节编号
	fmt.Println("===========================================")
	fmt.Println("🔎 搜索: \"8\" (章节编号)")
	fmt.Println("===========================================")
	resultsWithScores, err = fts.FindWithScores(ctx, "8")
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("找到 %d 个相关章节:\n", len(resultsWithScores))
	for _, r := range resultsWithScores {
		doc := r.Document.Data()
		chapterNum := doc["chapterNumber"]
		chapterTitle := doc["chapterTitle"]
		fmt.Printf("  📖 [分数: %.2f] 第 %v 章: %s\n",
			r.Score, chapterNum, chapterTitle)
	}
	fmt.Println()

	fmt.Println("🎉 章节全文搜索演示完成!")
}
