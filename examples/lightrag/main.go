package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/lightrag"
)

func main() {
	ctx := context.Background()
	workingDir := "./rag_storage_test"

	// 清理之前的测试数据
	os.RemoveAll(workingDir)
	defer os.RemoveAll(workingDir)

	// 创建 LightRAG 实例
	// 使用 SimpleEmbedder 和 SimpleLLM 进行演示
	rag := lightrag.New(lightrag.Options{
		WorkingDir: workingDir,
		Embedder:   lightrag.NewSimpleEmbedder(768),
		LLM:        &lightrag.SimpleLLM{},
	})

	// 初始化存储后端（包括 RxDB 数据库、全文搜索、向量搜索和图数据库）
	fmt.Println("Initializing storages...")
	if err := rag.InitializeStorages(ctx); err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	// 使用 InsertBatch 批量插入带元数据的文档
	fmt.Println("Inserting documents...")
	documents := []map[string]any{
		{"content": "The capital of France is Paris.", "tags": []string{"geography"}},
		{"content": "The Eiffel Tower is located in Paris.", "tags": []string{"landmark"}},
		{"content": "RxDB is a reactive database for JavaScript applications.", "tags": []string{"database", "js"}},
		{"content": "rxdb-go is a Golang implementation of RxDB.", "tags": []string{"database", "go"}},
	}

	if _, err := rag.InsertBatch(ctx, documents); err != nil {
		log.Fatalf("Failed to insert batch: %v", err)
	}

	// 给异步索引和知识图谱提取一点时间
	// LightRAG 在插入后会在后台提取实体和关系并构建知识图谱
	fmt.Println("Waiting for indexing and graph extraction...")
	time.Sleep(2 * time.Second)

	// 1. 执行混合查询 (Hybrid Mode)
	// 结合向量搜索和全文搜索的结果，提供更准确的召回
	fmt.Println("\n--- Query Mode: Hybrid ---")
	fmt.Println("Query: 'What is rxdb-go?'")
	ans, err := rag.Query(ctx, "What is rxdb-go?", lightrag.QueryParam{
		Mode:  lightrag.ModeHybrid,
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Hybrid query failed: %v", err)
	}
	fmt.Printf("Answer: %s\n", ans)

	// 2. 执行向量查询 (Vector Mode)
	// 纯语义搜索，适用于语义相似但关键词不匹配的场景
	fmt.Println("\n--- Query Mode: Vector ---")
	fmt.Println("Query: 'Where is Eiffel Tower?'")
	ans, err = rag.Query(ctx, "Where is Eiffel Tower?", lightrag.QueryParam{
		Mode:  lightrag.ModeVector,
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Vector query failed: %v", err)
	}
	fmt.Printf("Answer: %s\n", ans)

	// 3. 执行局部查询 (Local/Graph Mode)
	// 利用知识图谱中的实体关联进行检索，能够发现隐藏的连接
	fmt.Println("\n--- Query Mode: Local (Knowledge Graph) ---")
	fmt.Println("Query: 'Tell me about Paris.'")
	ans, err = rag.Query(ctx, "Tell me about Paris.", lightrag.QueryParam{
		Mode:  lightrag.ModeLocal,
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Local query failed: %v", err)
	}
	fmt.Printf("Answer: %s\n", ans)

	// 4. 展示原始检索结果 (Retrieve)
	// 直接获取检索到的原始文档及其分数
	fmt.Println("\n--- Raw Retrieval Results ---")
	results, err := rag.Retrieve(ctx, "Paris", lightrag.QueryParam{
		Mode:  lightrag.ModeHybrid,
		Limit: 5,
	})
	if err != nil {
		log.Fatalf("Retrieve failed: %v", err)
	}
	for i, res := range results {
		fmt.Printf("[%d] Score: %.4f, Content: %s\n", i+1, res.Score, res.Content)
	}

	// 5. 执行带元数据过滤的查询
	// 仅在 tags 包含 "database" 的文档中搜索
	fmt.Println("\n--- Query with Metadata Filtering ---")
	fmt.Println("Query: 'Paris', Filter: tags contains 'geography'")
	ans, err = rag.Query(ctx, "Paris", lightrag.QueryParam{
		Mode:  lightrag.ModeHybrid,
		Limit: 2,
		Filters: map[string]any{
			"tags": map[string]any{
				"$in": []string{"geography"},
			},
		},
	})
	if err != nil {
		log.Fatalf("Filtered query failed: %v", err)
	}
	fmt.Printf("Answer (Filtered): %s\n", ans)
}
