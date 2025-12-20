package cognee

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// MemoryService 记忆服务，提供类似 Cognee 的 AI 记忆功能
type MemoryService struct {
	db           rxdb.Database
	memories     rxdb.Collection // 记忆数据集合
	chunks       rxdb.Collection // 文本块集合
	entities     rxdb.Collection // 实体集合
	relations    rxdb.Collection // 关系集合
	fulltext     *rxdb.FulltextSearch
	vectorSearch *rxdb.VectorSearch
	graphDB      rxdb.GraphDatabase
	embedder     Embedder                // 向量嵌入生成器
	extractor    EntityRelationExtractor // 实体关系抽取器
	// 混合搜索权重：fulltextWeight + vectorWeight 应该等于 1.0
	fulltextWeight float64 // 全文搜索权重，默认 0.7
	vectorWeight   float64 // 向量搜索权重，默认 0.3
}

// Embedder 向量嵌入生成器接口
type Embedder interface {
	// Embed 将文本转换为向量嵌入
	Embed(ctx context.Context, text string) ([]float64, error)
	// Dimensions 返回向量维度
	Dimensions() int
}

// Memory 记忆数据结构
type Memory struct {
	ID          string                 `json:"id"`
	Content     string                 `json:"content"`
	Type        string                 `json:"type"` // text, code, url, etc.
	Dataset     string                 `json:"dataset"`
	Metadata    map[string]interface{} `json:"metadata"`
	CreatedAt   int64                  `json:"created_at"`
	ProcessedAt int64                  `json:"processed_at"`
	Chunks      []string               `json:"chunks"` // 关联的文本块 ID
}

// Chunk 文本块结构
type Chunk struct {
	ID        string                 `json:"id"`
	MemoryID  string                 `json:"memory_id"`
	Content   string                 `json:"content"`
	Index     int                    `json:"index"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt int64                  `json:"created_at"`
}

// Entity 实体结构
type Entity struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Type      string                 `json:"type"` // person, organization, concept, etc.
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt int64                  `json:"created_at"`
}

// Relation 关系结构
type Relation struct {
	ID        string                 `json:"id"`
	From      string                 `json:"from"` // 源实体 ID
	To        string                 `json:"to"`   // 目标实体 ID
	Type      string                 `json:"type"` // 关系类型
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt int64                  `json:"created_at"`
}

// Dataset 数据集结构
type Dataset struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata"`
	CreatedAt   int64                  `json:"created_at"`
	Status      string                 `json:"status"` // pending, processing, completed, error
}

// MemoryServiceOptions 记忆服务配置选项
type MemoryServiceOptions struct {
	// Embedder 向量嵌入生成器（必需）
	Embedder Embedder
	// Extractor 实体关系抽取器（可选，如果不提供则不进行实体关系抽取）
	Extractor EntityRelationExtractor
	// FulltextIndexOptions 全文搜索索引选项
	FulltextIndexOptions *rxdb.FulltextIndexOptions
	// VectorSearchOptions 向量搜索选项
	VectorSearchOptions *VectorSearchOptions
	// HybridSearchWeights 混合搜索权重配置
	// FulltextWeight: 全文搜索权重，默认 0.7
	// VectorWeight: 向量搜索权重，默认 0.3
	// 两者之和应该等于 1.0
	HybridSearchWeights *HybridSearchWeights
}

// HybridSearchWeights 混合搜索权重配置
type HybridSearchWeights struct {
	FulltextWeight float64 // 全文搜索权重，默认 0.7
	VectorWeight   float64 // 向量搜索权重，默认 0.3
}

// VectorSearchOptions 向量搜索选项
type VectorSearchOptions struct {
	DistanceMetric string // cosine, euclidean, dot
	IndexType      string // flat, ivf
}

// NewMemoryService 创建新的记忆服务
func NewMemoryService(ctx context.Context, db rxdb.Database, opts MemoryServiceOptions) (*MemoryService, error) {
	logrus.WithFields(logrus.Fields{
		"hasEmbedder":          opts.Embedder != nil,
		"hasFulltextIndexOpts": opts.FulltextIndexOptions != nil,
		"hasVectorSearchOpts":  opts.VectorSearchOptions != nil,
	}).Info("🔧 NewMemoryService: 开始创建记忆服务")

	if opts.Embedder == nil {
		logrus.Error("❌ NewMemoryService: embedder is required")
		return nil, fmt.Errorf("embedder is required")
	}

	// 创建或获取集合
	logrus.Debug("📦 NewMemoryService: 创建 memories 集合")
	memoriesSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	memories, err := db.Collection(ctx, "memories", memoriesSchema)
	if err != nil {
		logrus.WithError(err).Error("❌ NewMemoryService: 创建 memories 集合失败")
		return nil, fmt.Errorf("failed to create memories collection: %w", err)
	}
	logrus.Debug("✅ NewMemoryService: memories 集合创建成功")

	logrus.Debug("📦 NewMemoryService: 创建 chunks 集合")
	chunksSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	chunks, err := db.Collection(ctx, "chunks", chunksSchema)
	if err != nil {
		logrus.WithError(err).Error("❌ NewMemoryService: 创建 chunks 集合失败")
		return nil, fmt.Errorf("failed to create chunks collection: %w", err)
	}
	logrus.Debug("✅ NewMemoryService: chunks 集合创建成功")

	logrus.Debug("📦 NewMemoryService: 创建 entities 集合")
	entitiesSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	entities, err := db.Collection(ctx, "entities", entitiesSchema)
	if err != nil {
		logrus.WithError(err).Error("❌ NewMemoryService: 创建 entities 集合失败")
		return nil, fmt.Errorf("failed to create entities collection: %w", err)
	}
	logrus.Debug("✅ NewMemoryService: entities 集合创建成功")

	logrus.Debug("📦 NewMemoryService: 创建 relations 集合")
	relationsSchema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	relations, err := db.Collection(ctx, "relations", relationsSchema)
	if err != nil {
		logrus.WithError(err).Error("❌ NewMemoryService: 创建 relations 集合失败")
		return nil, fmt.Errorf("failed to create relations collection: %w", err)
	}
	logrus.Debug("✅ NewMemoryService: relations 集合创建成功")

	// 创建全文搜索
	// 配置全文搜索选项
	logrus.Debug("🔍 NewMemoryService: 开始配置全文搜索选项")
	fulltextOpts := opts.FulltextIndexOptions
	if fulltextOpts == nil {
		fulltextOpts = &rxdb.FulltextIndexOptions{
			Tokenize:      "jieba",
			CaseSensitive: false,
		}
		logrus.WithFields(logrus.Fields{
			"tokenize":      fulltextOpts.Tokenize,
			"caseSensitive": fulltextOpts.CaseSensitive,
		}).Debug("📝 NewMemoryService: 使用默认全文搜索选项")
	} else {
		logrus.WithFields(logrus.Fields{
			"tokenize":      fulltextOpts.Tokenize,
			"caseSensitive": fulltextOpts.CaseSensitive,
		}).Debug("📝 NewMemoryService: 使用自定义全文搜索选项")
	}

	logrus.Info("🔍 NewMemoryService: 开始创建全文搜索索引（这可能需要一些时间，特别是使用 jieba 分词器时）")
	fulltext, err := rxdb.AddFulltextSearch(memories, rxdb.FulltextSearchConfig{
		Identifier: "memories_search",
		DocToString: func(doc map[string]any) string {
			content, _ := doc["content"].(string)
			return content
		},
		IndexOptions:   fulltextOpts,
		Initialization: "instant",
	})
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"tokenize":      fulltextOpts.Tokenize,
			"caseSensitive": fulltextOpts.CaseSensitive,
		}).Error("❌ NewMemoryService: 创建全文搜索失败")
		return nil, fmt.Errorf("failed to create fulltext search: %w", err)
	}
	logrus.Info("✅ NewMemoryService: 全文搜索索引创建成功")

	// 配置向量搜索选项
	logrus.Debug("🔢 NewMemoryService: 开始配置向量搜索选项")
	distanceMetric := "cosine"
	if opts.VectorSearchOptions != nil && opts.VectorSearchOptions.DistanceMetric != "" {
		distanceMetric = opts.VectorSearchOptions.DistanceMetric
		logrus.WithField("distanceMetric", distanceMetric).Debug("📝 NewMemoryService: 使用自定义向量搜索选项")
	} else {
		logrus.WithField("distanceMetric", distanceMetric).Debug("📝 NewMemoryService: 使用默认向量搜索选项")
	}

	// 创建向量搜索
	logrus.WithFields(logrus.Fields{
		"dimensions":     opts.Embedder.Dimensions(),
		"distanceMetric": distanceMetric,
	}).Info("🔢 NewMemoryService: 开始创建向量搜索索引")
	vectorSearch, err := rxdb.AddVectorSearch(memories, rxdb.VectorSearchConfig{
		Identifier: "memories_vector",
		DocToEmbedding: func(doc map[string]any) ([]float64, error) {
			content, _ := doc["content"].(string)
			if content == "" {
				return nil, fmt.Errorf("empty content")
			}
			return opts.Embedder.Embed(ctx, content)
		},
		Dimensions:     opts.Embedder.Dimensions(),
		DistanceMetric: distanceMetric,
		Initialization: "instant",
	})
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"dimensions":     opts.Embedder.Dimensions(),
			"distanceMetric": distanceMetric,
		}).Error("❌ NewMemoryService: 创建向量搜索失败")
		return nil, fmt.Errorf("failed to create vector search: %w", err)
	}
	logrus.Info("✅ NewMemoryService: 向量搜索索引创建成功")

	// 获取图数据库
	logrus.Debug("🕸️  NewMemoryService: 获取图数据库")
	graphDB := db.Graph()
	if graphDB != nil {
		logrus.Debug("✅ NewMemoryService: 图数据库已获取")
	} else {
		logrus.Warn("⚠️  NewMemoryService: 图数据库不可用")
	}

	// 配置混合搜索权重
	fulltextWeight := 0.7 // 默认全文搜索权重
	vectorWeight := 0.3   // 默认向量搜索权重
	if opts.HybridSearchWeights != nil {
		if opts.HybridSearchWeights.FulltextWeight > 0 {
			fulltextWeight = opts.HybridSearchWeights.FulltextWeight
		}
		if opts.HybridSearchWeights.VectorWeight > 0 {
			vectorWeight = opts.HybridSearchWeights.VectorWeight
		}
		// 归一化权重，确保总和为 1.0
		totalWeight := fulltextWeight + vectorWeight
		if totalWeight > 0 {
			fulltextWeight = fulltextWeight / totalWeight
			vectorWeight = vectorWeight / totalWeight
		}
	}
	logrus.WithFields(logrus.Fields{
		"fulltextWeight": fulltextWeight,
		"vectorWeight":   vectorWeight,
	}).Debug("⚖️  NewMemoryService: 混合搜索权重配置")

	// 如果没有提供抽取器，使用空操作抽取器
	extractor := opts.Extractor
	if extractor == nil {
		extractor = &NoOpExtractor{}
		logrus.Debug("📝 NewMemoryService: 未提供抽取器，使用空操作抽取器")
	} else {
		logrus.Debug("✅ NewMemoryService: 已配置实体关系抽取器")
	}

	service := &MemoryService{
		db:             db,
		memories:       memories,
		chunks:         chunks,
		entities:       entities,
		relations:      relations,
		fulltext:       fulltext,
		vectorSearch:   vectorSearch,
		graphDB:        graphDB,
		embedder:       opts.Embedder,
		extractor:      extractor,
		fulltextWeight: fulltextWeight,
		vectorWeight:   vectorWeight,
	}

	logrus.Info("✅ NewMemoryService: 记忆服务创建成功")
	return service, nil
}

// AddMemory 添加记忆数据
func (s *MemoryService) AddMemory(ctx context.Context, content string, memoryType string, dataset string, metadata map[string]interface{}) (*Memory, error) {
	now := time.Now().Unix()

	memory := Memory{
		ID:        generateID(),
		Content:   content,
		Type:      memoryType,
		Dataset:   dataset,
		Metadata:  metadata,
		CreatedAt: now,
		Chunks:    []string{},
	}

	doc := map[string]any{
		"id":         memory.ID,
		"content":    memory.Content,
		"type":       memory.Type,
		"dataset":    memory.Dataset,
		"metadata":   memory.Metadata,
		"created_at": memory.CreatedAt,
		"chunks":     memory.Chunks,
	}

	docObj, err := s.memories.Insert(ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("failed to insert memory: %w", err)
	}

	memory.ID = docObj.ID()
	return &memory, nil
}

// ProcessMemory 处理记忆数据，提取实体和关系
func (s *MemoryService) ProcessMemory(ctx context.Context, memoryID string) error {
	// 获取记忆
	memoryDoc, err := s.memories.FindByID(ctx, memoryID)
	if err != nil {
		return fmt.Errorf("memory not found: %w", err)
	}

	memoryData := memoryDoc.Data()
	content, _ := memoryData["content"].(string)

	// 使用抽取器提取实体和关系
	var entities []Entity
	var relations []Relation

	if s.extractor != nil {
		var extractErr error
		entities, extractErr = s.extractor.ExtractEntities(ctx, content)
		if extractErr != nil {
			logrus.WithError(extractErr).WithField("memory_id", memoryID).Warn("Failed to extract entities, continuing without entities")
			entities = []Entity{}
		}

		relations, extractErr = s.extractor.ExtractRelations(ctx, content, entities)
		if extractErr != nil {
			logrus.WithError(extractErr).WithField("memory_id", memoryID).Warn("Failed to extract relations, continuing without relations")
			relations = []Relation{}
		}
	} else {
		// 如果没有配置抽取器，使用旧的简单提取方法（向后兼容）
		entities = extractEntities(content)
		relations = extractRelations(content, entities)
	}

	// 保存实体
	for _, entity := range entities {
		entityDoc := map[string]any{
			"id":         entity.ID,
			"name":       entity.Name,
			"type":       entity.Type,
			"metadata":   entity.Metadata,
			"created_at": entity.CreatedAt,
		}
		_, err := s.entities.Upsert(ctx, entityDoc)
		if err != nil {
			return fmt.Errorf("failed to upsert entity: %w", err)
		}

		// 在图数据库中创建节点
		if s.graphDB != nil {
			nodeID := fmt.Sprintf("entity:%s", entity.ID)
			_ = s.graphDB.Link(ctx, nodeID, "type", entity.Type)
		}
	}

	// 保存关系
	for _, relation := range relations {
		relationDoc := map[string]any{
			"id":         relation.ID,
			"from":       relation.From,
			"to":         relation.To,
			"type":       relation.Type,
			"metadata":   relation.Metadata,
			"created_at": relation.CreatedAt,
		}
		_, err := s.relations.Upsert(ctx, relationDoc)
		if err != nil {
			return fmt.Errorf("failed to upsert relation: %w", err)
		}

		// 在图数据库中创建链接
		if s.graphDB != nil {
			fromNode := fmt.Sprintf("entity:%s", relation.From)
			toNode := fmt.Sprintf("entity:%s", relation.To)
			_ = s.graphDB.Link(ctx, fromNode, relation.Type, toNode)
		}
	}

	// 更新记忆的处理状态
	now := time.Now().Unix()
	memoryData["processed_at"] = now
	_, err = s.memories.Upsert(ctx, memoryData)
	if err != nil {
		return fmt.Errorf("failed to update memory: %w", err)
	}

	return nil
}

// Search 搜索记忆
func (s *MemoryService) Search(ctx context.Context, query string, searchType string, limit int) ([]SearchResult, error) {
	switch searchType {
	case "CHUNKS", "FULLTEXT":
		return s.searchFulltext(ctx, query, limit)
	case "VECTOR", "SEMANTIC":
		return s.searchVector(ctx, query, limit)
	case "GRAPH", "INSIGHTS":
		return s.searchGraph(ctx, query, limit)
	case "HYBRID":
		return s.searchHybrid(ctx, query, limit)
	default:
		return s.searchHybrid(ctx, query, limit)
	}
}

// searchFulltext 全文搜索
func (s *MemoryService) searchFulltext(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	results, err := s.fulltext.FindWithScores(ctx, query, rxdb.FulltextSearchOptions{
		Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("fulltext search failed: %w", err)
	}

	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		data := r.Document.Data()
		searchResults[i] = SearchResult{
			ID:      r.Document.ID(),
			Content: getString(data, "content"),
			Type:    getString(data, "type"),
			Score:   r.Score,
			Source:  "fulltext",
		}
	}

	return searchResults, nil
}

// searchVector 向量搜索
func (s *MemoryService) searchVector(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	// 生成查询向量
	queryEmbedding, err := s.embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	results, err := s.vectorSearch.Search(ctx, queryEmbedding, rxdb.VectorSearchOptions{
		Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		data := r.Document.Data()
		searchResults[i] = SearchResult{
			ID:       r.Document.ID(),
			Content:  getString(data, "content"),
			Type:     getString(data, "type"),
			Score:    r.Score,
			Distance: r.Distance,
			Source:   "vector",
		}
	}

	return searchResults, nil
}

// searchGraph 图搜索
func (s *MemoryService) searchGraph(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	if s.graphDB == nil {
		return []SearchResult{}, nil
	}

	// 使用全文搜索找到相关实体
	entityResults, err := s.searchFulltext(ctx, query, 10)
	if err != nil {
		return nil, err
	}

	// 从图数据库中找到相关节点
	var searchResults []SearchResult
	seen := make(map[string]bool)

	for _, entityResult := range entityResults {
		nodeID := fmt.Sprintf("entity:%s", entityResult.ID)
		neighbors, err := s.graphDB.GetNeighbors(ctx, nodeID, "")
		if err == nil {
			for _, neighbor := range neighbors {
				if !seen[neighbor] {
					seen[neighbor] = true
					searchResults = append(searchResults, SearchResult{
						ID:     neighbor,
						Source: "graph",
						Score:  0.8, // 图搜索的默认分数
					})
				}
			}
		}
	}

	if len(searchResults) > limit {
		searchResults = searchResults[:limit]
	}

	return searchResults, nil
}

// searchHybrid 混合搜索（结合全文和向量）
func (s *MemoryService) searchHybrid(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	// 并行执行全文和向量搜索
	fulltextResults, _ := s.searchFulltext(ctx, query, limit)
	vectorResults, _ := s.searchVector(ctx, query, limit)

	// 创建两个映射来存储原始分数
	fulltextScores := make(map[string]float64)
	vectorScores := make(map[string]float64)
	resultMap := make(map[string]*SearchResult)

	// 收集全文搜索结果
	for _, r := range fulltextResults {
		fulltextScores[r.ID] = r.Score
		if _, ok := resultMap[r.ID]; !ok {
			resultMap[r.ID] = &SearchResult{
				ID:      r.ID,
				Content: r.Content,
				Type:    r.Type,
				Source:  "hybrid",
			}
		}
	}

	// 收集向量搜索结果
	for _, r := range vectorResults {
		vectorScores[r.ID] = r.Score
		if existing, ok := resultMap[r.ID]; ok {
			existing.Distance = r.Distance
		} else {
			resultMap[r.ID] = &SearchResult{
				ID:       r.ID,
				Content:  r.Content,
				Type:     r.Type,
				Distance: r.Distance,
				Source:   "hybrid",
			}
		}
	}

	// 计算加权平均分数
	for id, result := range resultMap {
		var finalScore float64
		fulltextScore, hasFulltext := fulltextScores[id]
		vectorScore, hasVector := vectorScores[id]

		if hasFulltext && hasVector {
			// 同时出现在两个结果中，使用加权平均
			finalScore = fulltextScore*s.fulltextWeight + vectorScore*s.vectorWeight
		} else if hasFulltext {
			// 只出现在全文搜索结果中
			finalScore = fulltextScore * s.fulltextWeight
		} else if hasVector {
			// 只出现在向量搜索结果中
			finalScore = vectorScore * s.vectorWeight
		}

		result.Score = finalScore
	}

	// 转换为切片并排序
	results := make([]SearchResult, 0, len(resultMap))
	for _, r := range resultMap {
		results = append(results, *r)
	}

	// 按分数排序
	sortResultsByScore(results)

	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// SearchResult 搜索结果
type SearchResult struct {
	ID       string  `json:"id"`
	Content  string  `json:"content"`
	Type     string  `json:"type"`
	Score    float64 `json:"score"`
	Distance float64 `json:"distance,omitempty"`
	Source   string  `json:"source"` // fulltext, vector, graph, hybrid
}

// 辅助函数
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func getString(data map[string]any, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

func extractEntities(content string) []Entity {
	// 简单的实体提取（实际应该使用 NLP 模型）
	// 这里返回空列表作为示例
	return []Entity{}
}

func extractRelations(content string, entities []Entity) []Relation {
	// 简单的关系提取（实际应该使用 NLP 模型）
	// 这里返回空列表作为示例
	return []Relation{}
}

func sortResultsByScore(results []SearchResult) {
	// 按分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
}

// ProcessDataset 处理整个数据集
func (s *MemoryService) ProcessDataset(ctx context.Context, datasetID string) (int, error) {
	query := s.memories.Find(map[string]any{"dataset": datasetID})
	docs, err := query.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to query memories: %w", err)
	}

	count := 0
	for _, doc := range docs {
		if err := s.ProcessMemory(ctx, doc.ID()); err == nil {
			count++
		}
	}

	return count, nil
}

// DeleteMemory 删除记忆
func (s *MemoryService) DeleteMemory(ctx context.Context, memoryID string) error {
	return s.memories.Remove(ctx, memoryID)
}

// DeleteDataset 删除整个数据集
func (s *MemoryService) DeleteDataset(ctx context.Context, datasetID string) error {
	query := s.memories.Find(map[string]any{"dataset": datasetID})
	docs, err := query.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to query memories: %w", err)
	}

	for _, doc := range docs {
		if err := s.memories.Remove(ctx, doc.ID()); err != nil {
			return fmt.Errorf("failed to remove memory %s: %w", doc.ID(), err)
		}
	}

	return nil
}

// ListDatasets 列出所有数据集
func (s *MemoryService) ListDatasets(ctx context.Context) ([]*Dataset, error) {
	allDocs, err := s.memories.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all memories: %w", err)
	}

	datasetMap := make(map[string]*Dataset)
	for _, doc := range allDocs {
		data := doc.Data()
		datasetName, _ := data["dataset"].(string)
		if datasetName == "" {
			datasetName = "main_dataset"
		}

		if _, exists := datasetMap[datasetName]; !exists {
			createdAt, _ := data["created_at"].(int64)
			datasetMap[datasetName] = &Dataset{
				ID:        datasetName,
				Name:      datasetName,
				CreatedAt: createdAt,
				Status:    "completed",
			}
		}
	}

	datasets := make([]*Dataset, 0, len(datasetMap))
	for _, dataset := range datasetMap {
		datasets = append(datasets, dataset)
	}

	return datasets, nil
}

// GetDatasetData 获取数据集的数据
func (s *MemoryService) GetDatasetData(ctx context.Context, datasetID string) ([]map[string]interface{}, error) {
	query := s.memories.Find(map[string]any{"dataset": datasetID})
	docs, err := query.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query memories: %w", err)
	}

	data := make([]map[string]interface{}, len(docs))
	for i, doc := range docs {
		data[i] = doc.Data()
	}

	return data, nil
}

// GetDatasetStatus 获取数据集状态
func (s *MemoryService) GetDatasetStatus(ctx context.Context, datasetID string) (*DatasetStatus, error) {
	query := s.memories.Find(map[string]any{"dataset": datasetID})
	docs, err := query.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query memories: %w", err)
	}

	processedCount := 0
	for _, doc := range docs {
		data := doc.Data()
		if processedAt, ok := data["processed_at"].(int64); ok && processedAt > 0 {
			processedCount++
		}
	}

	status := "completed"
	if processedCount < len(docs) {
		status = "processing"
	}

	return &DatasetStatus{
		Dataset:   datasetID,
		Status:    status,
		Total:     len(docs),
		Processed: processedCount,
		Pending:   len(docs) - processedCount,
	}, nil
}

// DatasetStatus 数据集状态
type DatasetStatus struct {
	Dataset   string `json:"dataset"`
	Status    string `json:"status"`
	Total     int    `json:"total"`
	Processed int    `json:"processed"`
	Pending   int    `json:"pending"`
}

// GetGraphData 获取图谱数据用于可视化
func (s *MemoryService) GetGraphData(ctx context.Context) (*GraphData, error) {
	// 获取所有实体
	allEntities, err := s.entities.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get entities: %w", err)
	}

	// 获取所有关系
	allRelations, err := s.relations.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get relations: %w", err)
	}

	nodes := make([]GraphNode, len(allEntities))
	for i, entity := range allEntities {
		data := entity.Data()
		nodes[i] = GraphNode{
			ID:   entity.ID(),
			Name: getString(data, "name"),
			Type: getString(data, "type"),
		}
	}

	edges := make([]GraphEdge, len(allRelations))
	for i, relation := range allRelations {
		data := relation.Data()
		edges[i] = GraphEdge{
			From: getString(data, "from"),
			To:   getString(data, "to"),
			Type: getString(data, "type"),
		}
	}

	return &GraphData{
		Nodes: nodes,
		Edges: edges,
	}, nil
}

// GraphData 图谱数据
type GraphData struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// GraphNode 图谱节点
type GraphNode struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// GraphEdge 图谱边
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

// GetMemory 获取记忆
func (s *MemoryService) GetMemory(ctx context.Context, memoryID string) (*Memory, error) {
	doc, err := s.memories.FindByID(ctx, memoryID)
	if err != nil {
		return nil, fmt.Errorf("memory not found: %w", err)
	}

	data := doc.Data()
	memory := &Memory{
		ID:          doc.ID(),
		Content:     getString(data, "content"),
		Type:        getString(data, "type"),
		Dataset:     getString(data, "dataset"),
		Metadata:    getMap(data, "metadata"),
		CreatedAt:   getInt64(data, "created_at"),
		ProcessedAt: getInt64(data, "processed_at"),
	}

	if chunks, ok := data["chunks"].([]interface{}); ok {
		memory.Chunks = make([]string, len(chunks))
		for i, chunk := range chunks {
			if chunkStr, ok := chunk.(string); ok {
				memory.Chunks[i] = chunkStr
			}
		}
	}

	return memory, nil
}

// Health 健康检查
func (s *MemoryService) Health(ctx context.Context) (*HealthStatus, error) {
	memoryCount, _ := s.memories.Count(ctx)
	entityCount, _ := s.entities.Count(ctx)
	relationCount, _ := s.relations.Count(ctx)

	return &HealthStatus{
		Status: "healthy",
		Stats: HealthStats{
			Memories:  memoryCount,
			Entities:  entityCount,
			Relations: relationCount,
		},
	}, nil
}

// HealthStatus 健康状态
type HealthStatus struct {
	Status string      `json:"status"`
	Stats  HealthStats `json:"stats"`
}

// HealthStats 健康统计
type HealthStats struct {
	Memories  int `json:"memories"`
	Entities  int `json:"entities"`
	Relations int `json:"relations"`
}

// 辅助函数
func getInt64(data map[string]any, key string) int64 {
	if val, ok := data[key].(int64); ok {
		return val
	}
	if val, ok := data[key].(float64); ok {
		return int64(val)
	}
	return 0
}

func getMap(data map[string]any, key string) map[string]interface{} {
	if val, ok := data[key].(map[string]interface{}); ok {
		return val
	}
	return make(map[string]interface{})
}
