package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	Collection string    `json:"collection"`
	Query      []float64 `json:"query"`
	Limit      int       `json:"limit"`
	Field      string    `json:"field"`
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

	collection, err := getCollectionByName(name)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	var allDocs []rxdb.Document

	// 如果指定了 tag 过滤，使用查询 API
	if tagFilter != "" {
		// 对于数组字段，使用 $in 操作符检查数组是否包含指定值
		// 注意：这里需要检查 tags 数组中的元素是否等于 tagFilter
		// 由于 rxdb-go 的查询实现，我们需要获取所有文档然后手动过滤
		// 或者使用 $all 操作符（如果支持）
		allDocs, err = collection.Find(map[string]any{
			"tags": map[string]any{
				"$all": []any{tagFilter},
			},
		}).Exec(dbContext)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
	} else {
		// 获取所有文档
		allDocs, err = collection.All(dbContext)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
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

	docs := allDocs[start:end]
	results := make([]DocumentResponse, len(docs))
	for i, doc := range docs {
		results[i] = DocumentResponse{
			ID:   doc.ID(),
			Data: doc.Data(),
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"documents": results,
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

	var req VectorSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

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

	opts := rxdb.VectorSearchOptions{}
	if req.Limit > 0 {
		opts.Limit = req.Limit
	}

	results, err := vs.Search(dbContext, req.Query, opts)
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

// getCollectionByName 根据名称获取集合
// 注意：这是一个辅助函数，实际实现可能需要缓存或从存储中读取
func getCollectionByName(name string) (rxdb.Collection, error) {
	// 这里需要根据实际需求实现
	// 可能需要维护一个集合缓存或从数据库配置中读取 schema
	// 简化实现：使用默认 schema
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

	return db.Collection(dbContext, name, schema)
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
	if err == nil && len(allDocs) > 0 {
		data := allDocs[0].Data()
		if embedding, ok := data[field].([]float64); ok {
			dimensions = len(embedding)
		} else if embeddingAny, ok := data[field].([]interface{}); ok {
			dimensions = len(embeddingAny)
		}
	}

	if dimensions == 0 {
		dimensions = 128 // 默认维度
	}

	// 创建向量搜索配置
	config := rxdb.VectorSearchConfig{
		Identifier:     fmt.Sprintf("%s-vector-%s", collectionName, field),
		Dimensions:     dimensions,
		DistanceMetric: "cosine",
		DocToEmbedding: func(doc map[string]any) (rxdb.Vector, error) {
			if emb, ok := doc[field].([]float64); ok {
				return emb, nil
			}
			// 处理 JSON 反序列化后的 []any 类型
			if embAny, ok := doc[field].([]interface{}); ok {
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
						return nil, fmt.Errorf("invalid embedding value type at index %d", i)
					}
				}
				return emb, nil
			}
			return nil, fmt.Errorf("no embedding field '%s' found in document", field)
		},
	}

	vs, err := rxdb.AddVectorSearch(collection, config)
	if err != nil {
		return nil, err
	}

	vectorSearchCache[key] = vs
	return vs, nil
}
