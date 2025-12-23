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
	rag := lightrag.New(lightrag.Options{
		WorkingDir: workingDir,
		Embedder:   lightrag.NewSimpleEmbedder(1536),
		LLM:        &lightrag.SimpleLLM{},
	})

	// 初始化存储
	fmt.Println("Initializing storages...")
	if err := rag.InitializeStorages(ctx); err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	// 插入文本
	fmt.Println("Inserting text...")
	texts := []string{
		"The capital of France is Paris.",
		"The Eiffel Tower is located in Paris.",
		"RxDB is a reactive database for JavaScript applications.",
		"rxdb-go is a Golang implementation of RxDB.",
	}

	for _, t := range texts {
		if err := rag.Insert(ctx, t); err != nil {
			log.Fatalf("Failed to insert: %v", err)
		}
	}

	// 给异步索引一点时间
	fmt.Println("Waiting for indexing...")
	time.Sleep(2 * time.Second)

	// 执行查询
	fmt.Println("\nPerforming query: 'What is rxdb-go?'")
	ans, err := rag.Query(ctx, "What is rxdb-go?", lightrag.QueryParam{
		Mode:  lightrag.ModeHybrid,
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Answer: %s\n", ans)

	fmt.Println("\nPerforming query: 'Where is Eiffel Tower?'")
	ans, err = rag.Query(ctx, "Where is Eiffel Tower?", lightrag.QueryParam{
		Mode:  lightrag.ModeVector,
		Limit: 2,
	})
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Answer: %s\n", ans)
}
