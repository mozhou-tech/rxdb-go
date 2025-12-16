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
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mozy/rxdb-go/pkg/rxdb"
)

var (
	db        rxdb.Database
	dbContext context.Context
)

type DatabaseConfig struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type CollectionInfo struct {
	Name   string                 `json:"name"`
	Schema map[string]interface{} `json:"schema"`
}

type DocumentResponse struct {
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data"`
}

type FulltextSearchRequest struct {
	Collection string  `json:"collection"`
	Query      string  `json:"query"`
	Limit      int     `json:"limit"`
	Threshold  float64 `json:"threshold"`
}

type VectorSearchRequest struct {
	Collection string    `json:"collection,omitempty"` // 可选，通常从 URL 获取
	Query      []float64 `json:"query,omitempty"`      // 向量查询（如果提供）
	QueryText  string    `json:"query_text,omitempty"` // 文本查询（如果提供，将生成 embedding）
	Limit      int       `json:"limit,omitempty"`
	Field      string    `json:"field,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func main() {
	// 从环境变量读取数据库配置
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

	// 创建数据库
	ctx := context.Background()
	var err error
	db, err = rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: dbName,
		Path: dbPath,
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	dbContext = ctx

	// 设置 Gin 路由
	r := gin.Default()

	// 配置 CORS
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	config.AllowHeaders = []string{"Origin", "Content-Type", "Accept", "Authorization"}
	r.Use(cors.New(config))

	// API 路由
	api := r.Group("/api")
	{
		// 数据库信息
		api.GET("/db/info", getDBInfo)
		api.GET("/db/collections", getCollections)

		// 集合操作
		api.GET("/collections/:name", getCollection)
		api.GET("/collections/:name/documents", getDocuments)
		api.GET("/collections/:name/documents/:id", getDocument)
		api.POST("/collections/:name/documents", createDocument)
		api.PUT("/collections/:name/documents/:id", updateDocument)
		api.DELETE("/collections/:name/documents/:id", deleteDocument)

		// 全文搜索
		api.POST("/collections/:name/fulltext/search", fulltextSearch)

		// 向量搜索
		api.POST("/collections/:name/vector/search", vectorSearch)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// getDBInfo 获取数据库信息
func getDBInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"name": db.Name(),
		"path": dbContext.Value("path"),
	})
}

// getCollections 获取所有集合
func getCollections(c *gin.Context) {
	// 注意：rxdb-go 可能没有直接列出所有集合的 API
	// 这里返回一个示例响应，实际实现可能需要从存储中读取
	c.JSON(http.StatusOK, gin.H{
		"collections": []CollectionInfo{},
		"message":     "Collections listing not fully implemented. Use specific collection endpoints.",
	})
}

// getCollection 获取集合信息
func getCollection(c *gin.Context) {
	name := c.Param("name")
	// 这里需要根据实际 API 实现
	c.JSON(http.StatusOK, gin.H{
		"name": name,
	})
}

// getDocuments 获取集合中的所有文档
func getDocuments(c *gin.Context) {
	name := c.Param("name")
	limitStr := c.DefaultQuery("limit", "100")
	skipStr := c.DefaultQuery("skip", "0")
	tagFilter := c.Query("tag") // 支持按 tag 过滤

	limit, _ := strconv.Atoi(limitStr)
	skip, _ := strconv.Atoi(skipStr)

	log.Printf("📄 getDocuments: collection=%s, limit=%d, skip=%d, tag=%s", name, limit, skip, tagFilter)

	collection, err := getCollectionByName(name)
	if err != nil {
		log.Printf("❌ Failed to get collection %s: %v", name, err)
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	log.Printf("✅ Collection %s retrieved successfully", name)

	var allDocs []rxdb.Document

	// 如果指定了 tag 过滤，使用查询 API
	if tagFilter != "" {
		// 对于数组字段，使用 $in 操作符检查数组是否包含指定值
		// 注意：这里需要检查 tags 数组中的元素是否等于 tagFilter
		// 由于 rxdb-go 的查询实现，我们需要获取所有文档然后手动过滤
		// 或者使用 $all 操作符（如果支持）
		log.Printf("🔍 Filtering by tag: %s", tagFilter)
		allDocs, err = collection.Find(map[string]any{
			"tags": map[string]any{
				"$all": []any{tagFilter},
			},
		}).Exec(dbContext)
		if err != nil {
			log.Printf("❌ Query failed: %v", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		log.Printf("📊 Found %d documents with tag %s", len(allDocs), tagFilter)
	} else {
		// 获取所有文档
		log.Printf("📋 Getting all documents from collection %s", name)
		allDocs, err = collection.All(dbContext)
		if err != nil {
			log.Printf("❌ Failed to get all documents: %v", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		log.Printf("📊 Found %d total documents in collection %s", len(allDocs), name)
	}

	// 分页处理
	total := len(allDocs)
	start := skip
	end := skip + limit
	if end > total {
		end = total
	}
	if start > total {
		start = total
	}

	var docs []rxdb.Document
	if start < end {
		docs = allDocs[start:end]
	}

	log.Printf("📄 Returning %d documents (total: %d, skip: %d, limit: %d)", len(docs), total, skip, limit)

	response := make([]DocumentResponse, len(docs))
	for i, doc := range docs {
		response[i] = DocumentResponse{
			ID:   doc.ID(),
			Data: doc.Data(),
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"documents": response,
		"total":     total,
		"skip":      skip,
		"limit":     limit,
	})
}

// getDocument 获取单个文档
func getDocument(c *gin.Context) {
	name := c.Param("name")
	id := c.Param("id")

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	doc, err := collection.FindByID(dbContext, id)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, DocumentResponse{
		ID:   doc.ID(),
		Data: doc.Data(),
	})
}

// createDocument 创建文档
func createDocument(c *gin.Context) {
	name := c.Param("name")

	var data map[string]interface{}
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	doc, err := collection.Insert(dbContext, data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, DocumentResponse{
		ID:   doc.ID(),
		Data: doc.Data(),
	})
}

// updateDocument 更新文档
func updateDocument(c *gin.Context) {
	name := c.Param("name")
	id := c.Param("id")

	var updates map[string]interface{}
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	doc, err := collection.FindByID(dbContext, id)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	if err := doc.Update(dbContext, updates); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	if err := doc.Save(dbContext); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, DocumentResponse{
		ID:   doc.ID(),
		Data: doc.Data(),
	})
}

// deleteDocument 删除文档
func deleteDocument(c *gin.Context) {
	name := c.Param("name")
	id := c.Param("id")

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	if err := collection.Remove(dbContext, id); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Document deleted"})
}

// fulltextSearch 全文搜索
func fulltextSearch(c *gin.Context) {
	name := c.Param("name")

	var req FulltextSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	// 获取全文搜索实例（需要先创建）
	// 这里假设已经通过 AddFulltextSearch 创建了全文搜索
	// 实际实现中可能需要从某个注册表中获取
	fts, err := getFulltextSearch(collection, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: fmt.Sprintf("Fulltext search not configured for collection: %v", err),
		})
		return
	}

	opts := rxdb.FulltextSearchOptions{}
	if req.Limit > 0 {
		opts.Limit = req.Limit
	}
	if req.Threshold > 0 {
		opts.Threshold = req.Threshold
	}

	results, err := fts.FindWithScores(dbContext, req.Query, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	response := make([]gin.H, len(results))
	for i, result := range results {
		response[i] = gin.H{
			"document": DocumentResponse{
				ID:   result.Document.ID(),
				Data: result.Document.Data(),
			},
			"score": result.Score,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"results": response,
		"query":   req.Query,
	})
}

// vectorSearch 向量搜索
func vectorSearch(c *gin.Context) {
	name := c.Param("name")

	// 先读取原始请求体用于调试（如果需要）
	bodyBytes, _ := io.ReadAll(c.Request.Body)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var req VectorSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Failed to bind JSON: %v", err)
		log.Printf("Request body: %s", string(bodyBytes))
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	log.Printf("Vector search request: collection=%s, hasQuery=%v, hasQueryText=%v, queryText=%s, limit=%d, field=%s",
		req.Collection, len(req.Query) > 0, req.QueryText != "", req.QueryText, req.Limit, req.Field)

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	// 获取向量搜索实例
	vs, err := getVectorSearch(collection, name, req.Field)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: fmt.Sprintf("Vector search not configured: %v", err),
		})
		return
	}

	// 如果提供了文本查询，生成 embedding
	var queryVector []float64
	if req.QueryText != "" {
		log.Printf("🔄 Generating embedding from text: '%s'", req.QueryText)
		embedding, err := generateEmbeddingFromText(req.QueryText)
		if err != nil {
			log.Printf("❌ Failed to generate embedding from text '%s': %v", req.QueryText, err)
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: fmt.Sprintf("Failed to generate embedding from text: %v", err),
			})
			return
		}
		queryVector = embedding
		log.Printf("✅ Generated embedding with dimension: %d (first 3 values: %v)", len(queryVector), queryVector[:min(3, len(queryVector))])
	} else if len(req.Query) > 0 {
		queryVector = req.Query
		log.Printf("Using provided vector with dimension: %d", len(queryVector))
	} else {
		log.Printf("No query or query_text provided. QueryText='%s', Query length=%d", req.QueryText, len(req.Query))
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: "Either 'query' (vector) or 'query_text' (text) must be provided",
		})
		return
	}

	opts := rxdb.VectorSearchOptions{}
	if req.Limit > 0 {
		opts.Limit = req.Limit
	}

	// 验证查询向量维度（在调用 Search 之前）
	// 注意：Search 方法内部也会验证，但提前验证可以提供更清晰的错误信息
	log.Printf("Executing vector search with query dimension: %d, limit: %d", len(queryVector), opts.Limit)
	log.Printf("Vector search instance count: %d", vs.Count())

	results, err := vs.Search(dbContext, queryVector, opts)
	if err != nil {
		log.Printf("Vector search failed: %v", err)
		// 检查是否是维度不匹配错误
		if strings.Contains(err.Error(), "dimension mismatch") {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: fmt.Sprintf("Vector dimension mismatch: %v", err),
			})
		} else {
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Error: fmt.Sprintf("Vector search failed: %v", err),
			})
		}
		return
	}

	log.Printf("Vector search succeeded, found %d results", len(results))

	response := make([]gin.H, len(results))
	for i, result := range results {
		response[i] = gin.H{
			"document": DocumentResponse{
				ID:   result.Document.ID(),
				Data: result.Document.Data(),
			},
			"score": result.Score,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"results":    response,
		"query":      queryVector,
		"query_text": req.QueryText,
	})
}

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

// generateEmbeddingFromText 使用 DashScope API 从文本生成 embedding
func generateEmbeddingFromText(text string) ([]float64, error) {
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

// getCollectionByName 根据名称获取集合
// 注意：这是一个辅助函数，实际实现可能需要缓存或从存储中读取
func getCollectionByName(name string) (rxdb.Collection, error) {
	// 这里需要根据实际需求实现
	// 可能需要维护一个集合缓存或从数据库配置中读取 schema
	// 简化实现：使用默认 schema
	// 注意：如果集合已存在，rxdb-go 会使用已存在的 schema，这里传入的 schema 主要用于创建新集合
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id": map[string]interface{}{"type": "string"},
			},
		},
	}

	log.Printf("🔍 Getting collection: %s", name)
	collection, err := db.Collection(dbContext, name, schema)
	if err != nil {
		log.Printf("❌ Failed to get collection %s: %v", name, err)
		return nil, err
	}

	// 检查集合中是否有数据
	count, countErr := collection.Count(dbContext)
	if countErr != nil {
		log.Printf("⚠️  Failed to count documents in collection %s: %v", name, countErr)
	} else {
		log.Printf("📊 Collection %s has %d documents", name, count)
	}

	return collection, nil
}

// 全文搜索缓存
var fulltextSearchCache = make(map[string]*rxdb.FulltextSearch)

// getFulltextSearch 获取或创建全文搜索实例
func getFulltextSearch(collection rxdb.Collection, collectionName string) (*rxdb.FulltextSearch, error) {
	key := collectionName
	if fts, ok := fulltextSearchCache[key]; ok {
		return fts, nil
	}

	// 创建全文搜索配置
	config := rxdb.FulltextSearchConfig{
		Identifier: fmt.Sprintf("%s-fulltext", collectionName),
		DocToString: func(doc map[string]interface{}) string {
			// 将所有字段转换为字符串并连接
			var parts []string
			for k, v := range doc {
				if k == "id" || k == "_rev" {
					continue
				}
				if str, ok := v.(string); ok {
					parts = append(parts, str)
				} else {
					parts = append(parts, fmt.Sprintf("%v", v))
				}
			}
			return strings.Join(parts, " ")
		},
		Initialization: "instant",
	}

	fts, err := rxdb.AddFulltextSearch(collection, config)
	if err != nil {
		return nil, err
	}

	fulltextSearchCache[key] = fts
	return fts, nil
}

// 向量搜索缓存
var vectorSearchCache = make(map[string]*rxdb.VectorSearch)

// getVectorSearch 获取或创建向量搜索实例
func getVectorSearch(collection rxdb.Collection, collectionName, field string) (*rxdb.VectorSearch, error) {
	if field == "" {
		field = "embedding"
	}

	key := fmt.Sprintf("%s:%s", collectionName, field)
	if vs, ok := vectorSearchCache[key]; ok {
		return vs, nil
	}

	// 尝试从集合中获取一个文档来推断维度
	var dimensions int
	allDocs, err := collection.All(dbContext)
	if err != nil {
		log.Printf("Failed to get documents to infer dimension: %v", err)
	} else if len(allDocs) > 0 {
		doc := allDocs[0]
		data := doc.Data()
		log.Printf("Inspecting first document (ID: %s) to infer embedding dimension", doc.ID())
		log.Printf("Document keys: %v", getMapKeys(data))

		// 检查 embedding 字段
		embeddingValue, exists := data[field]
		if !exists {
			log.Printf("Embedding field '%s' not found in document. Available fields: %v", field, getMapKeys(data))
		} else {
			log.Printf("Found embedding field '%s', type: %T", field, embeddingValue)

			// 尝试不同的类型转换
			if embedding, ok := embeddingValue.([]float64); ok {
				dimensions = len(embedding)
				log.Printf("Found embedding field with type []float64, dimension: %d", dimensions)
				if dimensions > 0 && dimensions <= 20 {
					log.Printf("First few values: %v", embedding[:min(5, dimensions)])
				}
			} else if embeddingAny, ok := embeddingValue.([]interface{}); ok {
				dimensions = len(embeddingAny)
				log.Printf("Found embedding field with type []interface{}, dimension: %d", dimensions)
				if dimensions > 0 && dimensions <= 20 {
					log.Printf("First few values (types): %v", getFirstFewTypes(embeddingAny, 5))
				}
				// 检查第一个元素的类型
				if dimensions > 0 {
					log.Printf("First element type: %T, value: %v", embeddingAny[0], embeddingAny[0])
				}
			} else {
				log.Printf("Embedding field '%s' has unexpected type: %T, value sample: %v", field, embeddingValue, getValueSample(embeddingValue))
			}
		}
	} else {
		log.Printf("No documents found in collection to infer dimension")
	}

	if dimensions == 0 {
		dimensions = 1536 // text-embedding-v4 常用维度（支持 2048、1536、1024 等）
		log.Printf("No documents found or no embedding field, using default dimension: %d (text-embedding-v4)", dimensions)
	} else {
		log.Printf("Inferred embedding dimension from documents: %d", dimensions)
	}

	// 创建向量搜索配置
	config := rxdb.VectorSearchConfig{
		Identifier:     fmt.Sprintf("%s-vector-%s", collectionName, field),
		Dimensions:     dimensions,
		DistanceMetric: "cosine",
		Initialization: "instant", // 立即建立索引
		DocToEmbedding: func(doc map[string]any) (rxdb.Vector, error) {
			docID, _ := doc["id"].(string)
			if docID == "" {
				docID = "unknown"
			}

			embeddingValue, exists := doc[field]
			if !exists {
				log.Printf("⚠️  Document %s: embedding field '%s' not found", docID, field)
				return nil, fmt.Errorf("no embedding field '%s' found in document %s", field, docID)
			}

			log.Printf("📄 Document %s: embedding field type: %T, value sample: %v", docID, embeddingValue, getValueSample(embeddingValue))

			if emb, ok := embeddingValue.([]float64); ok {
				log.Printf("✅ Document %s: using []float64 embedding, dimension: %d", docID, len(emb))
				return emb, nil
			}
			// 处理 JSON 反序列化后的 []any 类型
			if embAny, ok := embeddingValue.([]interface{}); ok {
				log.Printf("🔄 Document %s: converting []interface{} to []float64, dimension: %d", docID, len(embAny))
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					switch val := v.(type) {
					case float64:
						emb[i] = val
					case float32:
						emb[i] = float64(val)
					case int:
						emb[i] = float64(val)
					default:
						log.Printf("❌ Document %s: invalid embedding value type at index %d: %T, value: %v", docID, i, val, val)
						return nil, fmt.Errorf("invalid embedding value type at index %d: %T", i, val)
					}
				}
				log.Printf("✅ Document %s: converted embedding, dimension: %d", docID, len(emb))
				return emb, nil
			}
			log.Printf("❌ Document %s: embedding field '%s' has unexpected type: %T", docID, field, embeddingValue)
			return nil, fmt.Errorf("embedding field '%s' has unexpected type: %T", field, embeddingValue)
		},
	}

	log.Printf("Creating vector search with identifier: %s, dimensions: %d", config.Identifier, config.Dimensions)
	vs, err := rxdb.AddVectorSearch(collection, config)
	if err != nil {
		log.Printf("Failed to create vector search: %v", err)
		return nil, fmt.Errorf("failed to create vector search: %w", err)
	}

	vectorSearchCache[key] = vs
	log.Printf("Vector search created successfully, indexed documents: %d", vs.Count())
	return vs, nil
}

// 辅助函数：获取 map 的键
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// 辅助函数：获取前几个元素的类型
func getFirstFewTypes(arr []interface{}, n int) []string {
	result := make([]string, 0, min(n, len(arr)))
	for i := 0; i < min(n, len(arr)); i++ {
		result = append(result, fmt.Sprintf("%T", arr[i]))
	}
	return result
}

// 辅助函数：获取值的样本（用于日志）
func getValueSample(v interface{}) interface{} {
	switch val := v.(type) {
	case []interface{}:
		if len(val) > 0 {
			return fmt.Sprintf("[]interface{} with %d elements, first: %v", len(val), val[0])
		}
		return "[]interface{} (empty)"
	case []float64:
		if len(val) > 0 {
			return fmt.Sprintf("[]float64 with %d elements, first: %v", len(val), val[0])
		}
		return "[]float64 (empty)"
	case []float32:
		if len(val) > 0 {
			return fmt.Sprintf("[]float32 with %d elements, first: %v", len(val), val[0])
		}
		return "[]float32 (empty)"
	default:
		return fmt.Sprintf("%T: %v", v, v)
	}
}

// 辅助函数：min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
