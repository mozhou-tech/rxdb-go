package lightrag

import (
	"context"
	"fmt"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// LightRAG 基于 rxdb-go 实现的 LightRAG
type LightRAG struct {
	db         rxdb.Database
	workingDir string
	embedder   Embedder
	llm        LLM

	// 集合
	docs   rxdb.Collection
	chunks rxdb.Collection

	// 搜索组件
	fulltext *rxdb.FulltextSearch
	vector   *rxdb.VectorSearch

	initialized bool
}

// Options LightRAG 配置选项
type Options struct {
	WorkingDir string
	Embedder   Embedder
	LLM        LLM
}

// New 创建 LightRAG 实例
func New(opts Options) *LightRAG {
	return &LightRAG{
		workingDir: opts.WorkingDir,
		embedder:   opts.Embedder,
		llm:        opts.LLM,
	}
}

// InitializeStorages 初始化存储后端
func (r *LightRAG) InitializeStorages(ctx context.Context) error {
	if r.initialized {
		return nil
	}

	if r.workingDir == "" {
		r.workingDir = "./rag_storage"
	}

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "lightrag",
		Path: r.workingDir,
	})
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	r.db = db

	// 初始化文档集合
	docSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	docs, err := db.Collection(ctx, "documents", docSchema)
	if err != nil {
		return fmt.Errorf("failed to create documents collection: %w", err)
	}
	r.docs = docs

	// 初始化全文搜索
	fulltext, err := rxdb.AddFulltextSearch(docs, rxdb.FulltextSearchConfig{
		Identifier: "docs_fulltext",
		DocToString: func(doc map[string]any) string {
			content, _ := doc["content"].(string)
			return content
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add fulltext search: %w", err)
	}
	r.fulltext = fulltext

	// 初始化向量搜索
	if r.embedder != nil {
		vector, err := rxdb.AddVectorSearch(docs, rxdb.VectorSearchConfig{
			Identifier: "docs_vector",
			DocToEmbedding: func(doc map[string]any) ([]float64, error) {
				content, _ := doc["content"].(string)
				return r.embedder.Embed(ctx, content)
			},
			Dimensions: r.embedder.Dimensions(),
		})
		if err != nil {
			return fmt.Errorf("failed to add vector search: %w", err)
		}
		r.vector = vector
	}

	r.initialized = true
	logrus.Info("LightRAG storages initialized successfully")
	return nil
}

// Insert 插入文本
func (r *LightRAG) Insert(ctx context.Context, text string) error {
	if !r.initialized {
		return fmt.Errorf("storages not initialized")
	}

	doc := map[string]any{
		"id":         fmt.Sprintf("%d", time.Now().UnixNano()),
		"content":    text,
		"created_at": time.Now().Unix(),
	}

	_, err := r.docs.Insert(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	return nil
}

// Query 执行查询
func (r *LightRAG) Query(ctx context.Context, query string, param QueryParam) (string, error) {
	if !r.initialized {
		return "", fmt.Errorf("storages not initialized")
	}

	if param.Limit <= 0 {
		param.Limit = 5
	}

	var results []rxdb.FulltextSearchResult
	var err error

	switch param.Mode {
	case ModeVector:
		if r.vector == nil {
			return "", fmt.Errorf("vector search not available")
		}
		emb, err := r.embedder.Embed(ctx, query)
		if err != nil {
			return "", err
		}
		vecResults, err := r.vector.Search(ctx, emb, rxdb.VectorSearchOptions{Limit: param.Limit})
		if err != nil {
			return "", err
		}
		// 转换结果格式以统一处理
		for _, v := range vecResults {
			results = append(results, rxdb.FulltextSearchResult{
				Document: v.Document,
				Score:    v.Score,
			})
		}
	case ModeFulltext:
		results, err = r.fulltext.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: param.Limit})
		if err != nil {
			return "", err
		}
	case ModeHybrid:
		// 简单实现混合搜索，实际可能需要更复杂的 RRF 算法
		ftResults, _ := r.fulltext.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: param.Limit})
		results = ftResults
		// 这里可以继续合并向量搜索结果
	default:
		results, err = r.fulltext.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{Limit: param.Limit})
	}

	if len(results) == 0 {
		return "No relevant information found.", nil
	}

	// 简单的上下文拼接
	contextText := ""
	for i, res := range results {
		content, _ := res.Document.Data()["content"].(string)
		contextText += fmt.Sprintf("[%d] %s\n", i+1, content)
	}

	if r.llm != nil {
		prompt := fmt.Sprintf("Context:\n%s\n\nQuestion: %s\n\nAnswer the question based on the context.", contextText, query)
		return r.llm.Complete(ctx, prompt)
	}

	return contextText, nil
}

// FinalizeStorages 关闭存储资源
func (r *LightRAG) FinalizeStorages(ctx context.Context) error {
	if r.fulltext != nil {
		r.fulltext.Close()
	}
	if r.vector != nil {
		r.vector.Close()
	}
	if r.db != nil {
		return r.db.Close(ctx)
	}
	return nil
}
