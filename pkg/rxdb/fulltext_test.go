package rxdb

import (
	"context"
	"os"
	"testing"
)

func TestFulltextSearch_Basic(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-fulltext-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-fulltext",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "articles", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试文档
	testDocs := []map[string]any{
		{"id": "1", "title": "Introduction to Go Programming", "body": "Go is a statically typed language designed for simplicity"},
		{"id": "2", "title": "Advanced Go Patterns", "body": "Learn advanced patterns in Go programming language"},
		{"id": "3", "title": "Python for Data Science", "body": "Python is great for data analysis and machine learning"},
		{"id": "4", "title": "JavaScript Web Development", "body": "JavaScript is essential for modern web development"},
		{"id": "5", "title": "Go Concurrency", "body": "Go provides excellent concurrency support with goroutines"},
	}

	for _, doc := range testDocs {
		_, err := coll.Insert(context.Background(), doc)
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	// 创建全文搜索实例
	fts, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier: "article-search",
		DocToString: func(doc map[string]any) string {
			title, _ := doc["title"].(string)
			body, _ := doc["body"].(string)
			return title + " " + body
		},
	})
	if err != nil {
		t.Fatalf("failed to create fulltext search: %v", err)
	}
	defer fts.Close()

	// 测试搜索 "Go"
	results, err := fts.Find(context.Background(), "Go")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results for 'Go', got %d", len(results))
	}

	// 测试搜索 "Python"
	results, err = fts.Find(context.Background(), "Python")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'Python', got %d", len(results))
	}

	// 测试搜索 "programming language"
	results, err = fts.Find(context.Background(), "programming language")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for 'programming language', got %d", len(results))
	}

	// 测试带限制的搜索
	results, err = fts.Find(context.Background(), "Go", FulltextSearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("failed to search with limit: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result with limit, got %d", len(results))
	}
}

func TestFulltextSearch_WithScores(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-fulltext-scores-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-fulltext-scores",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "docs", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试文档
	testDocs := []map[string]any{
		{"id": "1", "content": "apple apple apple"}, // 高频
		{"id": "2", "content": "apple banana"},      // 中频
		{"id": "3", "content": "banana orange"},     // 无匹配
	}

	for _, doc := range testDocs {
		_, err := coll.Insert(context.Background(), doc)
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	// 创建全文搜索
	fts, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier: "content-search",
		DocToString: func(doc map[string]any) string {
			content, _ := doc["content"].(string)
			return content
		},
	})
	if err != nil {
		t.Fatalf("failed to create fulltext search: %v", err)
	}
	defer fts.Close()

	// 搜索并获取分数
	results, err := fts.FindWithScores(context.Background(), "apple")
	if err != nil {
		t.Fatalf("failed to search with scores: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// 验证分数排序（高频文档应该排在前面）
	if len(results) >= 2 {
		if results[0].Score < results[1].Score {
			t.Errorf("expected first result to have higher score")
		}
	}
}

func TestFulltextSearch_RealTimeIndex(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-fulltext-realtime-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-fulltext-realtime",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "posts", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 创建全文搜索（空集合）
	fts, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier: "post-search",
		DocToString: func(doc map[string]any) string {
			content, _ := doc["content"].(string)
			return content
		},
	})
	if err != nil {
		t.Fatalf("failed to create fulltext search: %v", err)
	}
	defer fts.Close()

	// 验证初始索引为空
	if fts.Count() != 0 {
		t.Errorf("expected empty index, got %d documents", fts.Count())
	}

	// 插入文档
	_, err = coll.Insert(context.Background(), map[string]any{
		"id":      "1",
		"content": "hello world",
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// 等待索引更新（实时索引通过变更监听实现）
	// 由于是异步的，可能需要短暂等待
	// 这里使用 Reindex 来确保索引已更新
	fts.Reindex(context.Background())

	// 搜索新插入的文档
	results, err := fts.Find(context.Background(), "hello")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result after insert, got %d", len(results))
	}
}

func TestFulltextSearch_IndexOptions(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-fulltext-options-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-fulltext-options",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "items", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试文档
	testDocs := []map[string]any{
		{"id": "1", "text": "The quick brown fox"},
		{"id": "2", "text": "A lazy dog"},
		{"id": "3", "text": "The fox is quick"},
	}

	for _, doc := range testDocs {
		_, err := coll.Insert(context.Background(), doc)
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	// 创建带停用词的全文搜索
	fts, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier: "item-search",
		DocToString: func(doc map[string]any) string {
			text, _ := doc["text"].(string)
			return text
		},
		IndexOptions: &FulltextIndexOptions{
			MinLength: 3,                          // 最小长度 3
			StopWords: []string{"the", "a", "is"}, // 停用词
		},
	})
	if err != nil {
		t.Fatalf("failed to create fulltext search: %v", err)
	}
	defer fts.Close()

	// 搜索 "the" 应该没有结果（停用词）
	results, err := fts.Find(context.Background(), "the")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for stop word 'the', got %d", len(results))
	}

	// 搜索 "fox" 应该有结果
	results, err = fts.Find(context.Background(), "fox")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'fox', got %d", len(results))
	}
}

func TestFulltextSearch_Persist(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-fulltext-persist-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-fulltext-persist",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "notes", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入文档
	_, err = coll.Insert(context.Background(), map[string]any{
		"id":   "1",
		"text": "persistent fulltext search test",
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// 创建全文搜索并持久化
	fts, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier: "note-search",
		DocToString: func(doc map[string]any) string {
			text, _ := doc["text"].(string)
			return text
		},
	})
	if err != nil {
		t.Fatalf("failed to create fulltext search: %v", err)
	}

	// 持久化索引
	err = fts.Persist(context.Background())
	if err != nil {
		t.Fatalf("failed to persist index: %v", err)
	}
	fts.Close()

	// 创建新的全文搜索实例并加载
	fts2, err := AddFulltextSearch(coll, FulltextSearchConfig{
		Identifier:     "note-search",
		Initialization: "lazy", // 懒加载
		DocToString: func(doc map[string]any) string {
			text, _ := doc["text"].(string)
			return text
		},
	})
	if err != nil {
		t.Fatalf("failed to create second fulltext search: %v", err)
	}
	defer fts2.Close()

	// 加载持久化的索引
	err = fts2.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load index: %v", err)
	}

	// 验证索引已加载
	if fts2.Count() != 1 {
		t.Errorf("expected 1 document in loaded index, got %d", fts2.Count())
	}

	// 搜索验证
	results, err := fts2.Find(context.Background(), "persistent")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	db.Close(context.Background())
}
