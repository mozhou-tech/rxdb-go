package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sirupsen/logrus"
)

// Vector 表示一个嵌入向量。
type Vector = []float64

// VectorSearchConfig 向量搜索配置。
// 参考 RxDB 向量数据库文档。
type VectorSearchConfig struct {
	// Identifier 唯一标识符，用于存储元数据和在重启时继续索引。
	Identifier string
	// DocToEmbedding 将文档转换为嵌入向量的函数。
	// 这通常使用机器学习模型来生成。
	DocToEmbedding func(doc map[string]any) (Vector, error)
	// Dimensions 向量维度。
	Dimensions int
	// DistanceMetric 距离度量方式："euclidean"（欧几里得）、"cosine"（余弦）、"dot"（点积）。
	// 默认为 "cosine"。
	DistanceMetric string
	// IndexType 索引类型："flat"（平面/暴力搜索）、"ivf"（倒排文件）。
	// 默认为 "flat"。
	// 注意：bleve 使用自己的索引优化，此选项保留用于兼容性。
	IndexType string
	// NumIndexes 用于索引的采样向量数量（用于 IVF 索引）。
	// 默认为 5。
	// 注意：bleve 使用自己的索引优化，此选项保留用于兼容性。
	NumIndexes int
	// IndexDistance 索引距离阈值（用于 IVF 查询优化）。
	// 注意：bleve 使用自己的索引优化，此选项保留用于兼容性。
	IndexDistance float64
	// BatchSize 每次索引的文档数量。
	BatchSize int
	// Initialization 初始化模式："instant" 或 "lazy"。
	Initialization string
	// MetadataFields 需要在向量索引中建立索引的元数据字段列表。
	// 只有包含在这里的字段才能用于前置过滤。
	MetadataFields []string
	// PartitionField 用于物理分区的字段名。
	// 如果提供，将根据该字段的值创建不同的物理索引库，完全隔离不同分区的数据。
	PartitionField string
	// CacheSize 向量缓存的最大容量（条目数）。
	// 默认为 10000。设置为 -1 则禁用缓存。
	CacheSize int
}

// VectorSearchResult 向量搜索结果。
type VectorSearchResult struct {
	Document Document
	Distance float64 // 与查询向量的距离
	Score    float64 // 相似度分数（1 - 归一化距离）
}

// VectorSearchOptions 向量搜索选项。
type VectorSearchOptions struct {
	// Limit 返回结果数量限制。
	Limit int
	// MaxDistance 最大距离阈值。
	MaxDistance float64
	// MinScore 最小相似度分数阈值（0-1）。
	MinScore float64
	// DocsPerIndexSide 每个索引侧获取的文档数量（用于 IVF 索引优化）。
	// 注意：bleve 使用自己的索引优化，此选项保留用于兼容性。
	DocsPerIndexSide int
	// UseFullScan 是否使用全表扫描（而不是索引）。
	// 注意：bleve 总是使用索引，此选项保留用于兼容性。
	UseFullScan bool
	// Selector 元数据过滤选择器（Mango 语法）。
	// 如果提供，将在向量搜索时进行前置过滤。
	Selector map[string]any
	// Partition 指定搜索的分区值。
	// 仅当 VectorSearch 配置了 PartitionField 时有效。
	Partition string
}

// VectorSearch 向量搜索实例。
// 参考 RxDB 的向量数据库实现。
type VectorSearch struct {
	identifier     string
	collection     *collection
	docToEmbedding func(doc map[string]any) (Vector, error)
	dimensions     int
	distanceMetric string
	indexType      string
	numIndexes     int
	indexDistance  float64
	batchSize      int
	initMode       string
	metadataFields []string
	partitionField string

	index                 bleve.Index // 默认索引（未启用分区或作为后备）
	partitions            map[string]bleve.Index
	partitionBloomFilters map[string]*BloomFilter
	indexPath             string

	embeddingCache *lru.Cache[string, Vector]
	cacheSize      int

	mu                         sync.RWMutex
	initialized                bool
	closeChan                  chan struct{}
	idBloomFilter              *BloomFilter // 已索引向量的布隆过滤器
	idBloomNeedsRebuild        bool
	partitionBloomNeedsRebuild map[string]bool
}

// AddVectorSearch 在集合上创建向量搜索实例。
// 参考 RxDB 向量数据库文档。
func AddVectorSearch(coll Collection, config VectorSearchConfig) (*VectorSearch, error) {
	col, ok := coll.(*collection)
	if !ok {
		return nil, fmt.Errorf("invalid collection type")
	}

	if config.Identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}
	if config.DocToEmbedding == nil {
		return nil, fmt.Errorf("docToEmbedding function is required")
	}
	if config.Dimensions <= 0 {
		return nil, fmt.Errorf("dimensions must be positive")
	}

	// 默认值
	distanceMetric := config.DistanceMetric
	if distanceMetric == "" {
		distanceMetric = "cosine"
	}

	indexType := config.IndexType
	if indexType == "" {
		indexType = "flat"
	}

	numIndexes := config.NumIndexes
	if numIndexes <= 0 {
		numIndexes = 5
	}

	indexDistance := config.IndexDistance
	if indexDistance <= 0 {
		indexDistance = 0.003
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	initMode := config.Initialization
	if initMode == "" {
		initMode = "instant"
	}

	cacheSize := config.CacheSize
	if cacheSize == 0 {
		cacheSize = 2000 // 默认 2000 条
	}

	// 确定索引存储路径
	storePath := col.store.Path()
	var indexPath string
	if storePath != "" {
		// 使用数据库路径下的子目录存储 bleve 索引
		indexPath = filepath.Join(storePath, "vector", col.name, config.Identifier)
	} else {
		// 内存模式，使用临时目录
		indexPath = filepath.Join(os.TempDir(), "rxdb-vector", col.name, config.Identifier)
	}

	vs := &VectorSearch{
		identifier:                 config.Identifier,
		collection:                 col,
		docToEmbedding:             config.DocToEmbedding,
		dimensions:                 config.Dimensions,
		distanceMetric:             distanceMetric,
		indexType:                  indexType,
		numIndexes:                 numIndexes,
		indexDistance:              indexDistance,
		batchSize:                  batchSize,
		initMode:                   initMode,
		metadataFields:             config.MetadataFields,
		partitionField:             config.PartitionField,
		partitions:                 make(map[string]bleve.Index),
		partitionBloomFilters:      make(map[string]*BloomFilter),
		partitionBloomNeedsRebuild: make(map[string]bool),
		cacheSize:                  cacheSize,
		indexPath:                  indexPath,
		closeChan:                  make(chan struct{}),
		idBloomFilter:              NewBloomFilter(20000, 0.01),
	}

	if cacheSize > 0 {
		var err error
		vs.embeddingCache, err = lru.New[string, Vector](cacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create lru cache: %w", err)
		}
	}

	// 如果没有设置分区字段，初始化默认索引
	if vs.partitionField == "" {
		if err := vs.openOrCreateIndex(""); err != nil {
			return nil, fmt.Errorf("failed to open/create bleve index: %w", err)
		}
	}

	// 加载持久化的布隆过滤器
	if err := vs.loadBloomFilters(context.Background()); err != nil {
		logrus.WithField("identifier", vs.identifier).WithError(err).Debug("Failed to load vector bloom filters")
	}

	// 根据初始化模式决定是否立即建立索引
	if initMode == "instant" {
		if err := vs.buildIndex(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to build vector index: %w", err)
		}
		vs.initialized = true
	}

	// 启动监听变更的 goroutine
	go vs.watchChanges()

	return vs, nil
}

// openOrCreateIndex 打开或创建 bleve 索引。
// 如果 partition 为空，打开默认索引。
func (vs *VectorSearch) openOrCreateIndex(partition string) error {
	path := vs.indexPath
	if partition != "" {
		// 分区索引存储在子目录中
		path = filepath.Join(vs.indexPath, "partition_"+partition)
	}

	// 尝试打开现有索引
	if index, err := bleve.Open(path); err == nil {
		if partition == "" {
			vs.index = index
		} else {
			vs.partitions[partition] = index
		}
		return nil
	}

	// 创建新的索引映射
	// 注意：bleve 的向量搜索功能需要启用 vectors 构建标签
	// 编译时使用: go build -tags vectors
	// 并且需要安装 FAISS 库
	//
	// 由于向量字段映射需要 vectors 构建标签，我们通过 JSON 配置来创建映射
	properties := map[string]interface{}{
		"_vector": map[string]interface{}{
			"type":           "vector",
			"store":          false,
			"index":          true,
			"include_in_all": false,
			"docvalues":      false,
			"skip_freq_norm": true,
			"dims":           vs.dimensions,
			"similarity":     vs.getSimilarityMetric(),
		},
	}

	// 添加元数据字段映射
	for _, field := range vs.metadataFields {
		properties[field] = map[string]interface{}{
			"type":  "text", // 默认使用 text，也可以根据需要支持 numeric 等
			"store": false,
			"index": true,
		}
	}

	indexMappingJSON := map[string]interface{}{
		"default_mapping": map[string]interface{}{
			"dynamic":    false,
			"properties": properties,
		},
	}

	jsonData, _ := json.Marshal(indexMappingJSON)
	indexMapping := bleve.NewIndexMapping()
	if err := json.Unmarshal(jsonData, indexMapping); err != nil {
		// 如果 JSON 配置失败，使用默认映射
		// 这会在没有 vectors 构建标签时发生
		indexMapping.DefaultMapping.Dynamic = false
	}

	// 创建索引目录
	if partition != "" {
		_ = os.MkdirAll(path, 0755)
	}

	// 创建索引
	index, err := bleve.New(path, indexMapping)
	if err != nil {
		return fmt.Errorf("failed to create bleve index at %s: %w", path, err)
	}

	if partition == "" {
		vs.index = index
	} else {
		vs.partitions[partition] = index
	}
	return nil
}

// getOrCreateIndex 获取或创建指定分区的索引。
func (vs *VectorSearch) getOrCreateIndex(partition string) (bleve.Index, error) {
	if partition == "" {
		if vs.index == nil {
			if err := vs.openOrCreateIndex(""); err != nil {
				return nil, err
			}
		}
		return vs.index, nil
	}

	if idx, ok := vs.partitions[partition]; ok {
		return idx, nil
	}

	if err := vs.openOrCreateIndex(partition); err != nil {
		return nil, err
	}
	return vs.partitions[partition], nil
}

// getEmbeddingWithCache 获取文档的嵌入向量，优先从缓存获取。
func (vs *VectorSearch) getEmbeddingWithCache(docID string, docData map[string]any) (Vector, error) {
	if docID != "" && !vs.idBloomFilter.Test(docID) {
		return nil, fmt.Errorf("embedding for document %s definitely does not exist", docID)
	}

	if vs.embeddingCache != nil {
		if val, ok := vs.embeddingCache.Get(docID); ok {
			return val, nil
		}
	}

	embedding, err := vs.docToEmbedding(docData)
	if err != nil {
		return nil, err
	}

	if vs.embeddingCache != nil {
		vs.embeddingCache.Add(docID, embedding)
	}
	return embedding, nil
}

// buildIndex 构建向量索引。
func (vs *VectorSearch) buildIndex(ctx context.Context) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 获取所有文档
	docs, err := vs.collection.All(ctx)
	if err != nil {
		return err
	}

	// 记录每个分区的批处理和计数
	type partitionInfo struct {
		batch *bleve.Batch
		index bleve.Index
		count int
	}
	pInfos := make(map[string]*partitionInfo)

	getPartitionInfo := func(p string) (*partitionInfo, error) {
		if info, ok := pInfos[p]; ok {
			return info, nil
		}
		idx, err := vs.getOrCreateIndex(p)
		if err != nil {
			return nil, err
		}
		pInfos[p] = &partitionInfo{
			batch: idx.NewBatch(),
			index: idx,
			count: 0,
		}
		return pInfos[p], nil
	}

	for _, doc := range docs {
		// 确定文档分区
		partition := ""
		if vs.partitionField != "" {
			if p, ok := doc.Data()[vs.partitionField].(string); ok {
				partition = p
			}
		}

		info, err := getPartitionInfo(partition)
		if err != nil {
			continue
		}

		// 更新分区布隆过滤器
		if partition != "" {
			if _, ok := vs.partitionBloomFilters[partition]; !ok {
				vs.partitionBloomFilters[partition] = NewBloomFilter(1000, 0.01)
			}
			vs.partitionBloomFilters[partition].Add(doc.ID())
		}
		// 更新全局 ID 布隆过滤器
		vs.idBloomFilter.Add(doc.ID())

		// 生成嵌入向量（使用缓存）
		embedding, err := vs.getEmbeddingWithCache(doc.ID(), doc.Data())
		if err != nil {
			continue // 跳过无法生成嵌入的文档
		}
		if len(embedding) != vs.dimensions {
			continue // 跳过维度不匹配的向量
		}

		// 转换为 float32（bleve 要求）
		vec32 := make([]float32, len(embedding))
		for i, v := range embedding {
			vec32[i] = float32(v)
		}

		// 创建 bleve 文档
		bleveDoc := map[string]interface{}{
			"_vector": vec32,
		}

		// 添加元数据字段
		for _, field := range vs.metadataFields {
			if val, ok := doc.Data()[field]; ok {
				bleveDoc[field] = val
			}
		}

		// 添加到批处理
		if err := info.batch.Index(doc.ID(), bleveDoc); err != nil {
			return fmt.Errorf("failed to index document %s: %w", doc.ID(), err)
		}

		info.count++
		if info.count >= vs.batchSize {
			// 提交批处理
			if err := info.index.Batch(info.batch); err != nil {
				return fmt.Errorf("failed to batch index: %w", err)
			}
			info.batch = info.index.NewBatch()
			info.count = 0
		}
	}

	// 提交所有剩余的批处理
	for _, info := range pInfos {
		if info.count > 0 {
			if err := info.index.Batch(info.batch); err != nil {
				return fmt.Errorf("failed to batch index: %w", err)
			}
		}
	}

	return nil
}

// watchChanges 监听集合变更并更新索引。
func (vs *VectorSearch) watchChanges() {
	changes := vs.collection.Changes()
	for {
		select {
		case <-vs.closeChan:
			return
		case event, ok := <-changes:
			if !ok {
				return
			}
			vs.handleChange(event)
		}
	}
}

// handleChange 处理变更事件。
func (vs *VectorSearch) handleChange(event ChangeEvent) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 确定文档分区
	partition := ""
	if vs.partitionField != "" {
		if p, ok := event.Doc[vs.partitionField].(string); ok {
			partition = p
		} else if event.Old != nil {
			if p, ok := event.Old[vs.partitionField].(string); ok {
				partition = p
			}
		}
	}

	idx, err := vs.getOrCreateIndex(partition)
	if err != nil {
		return
	}

	// 更新分区布隆过滤器
	if partition != "" {
		if _, ok := vs.partitionBloomFilters[partition]; !ok {
			vs.partitionBloomFilters[partition] = NewBloomFilter(1000, 0.01)
		}
		vs.partitionBloomFilters[partition].Add(event.ID)
	}
	// 更新全局 ID 布隆过滤器
	vs.idBloomFilter.Add(event.ID)

	switch event.Op {
	case OperationInsert, OperationUpdate:
		if event.Doc != nil {
			// 清除旧缓存
			if vs.embeddingCache != nil {
				vs.embeddingCache.Remove(event.ID)
			}

			embedding, err := vs.getEmbeddingWithCache(event.ID, event.Doc)
			if err != nil {
				return
			}
			if len(embedding) != vs.dimensions {
				return
			}

			// 转换为 float32
			vec32 := make([]float32, len(embedding))
			for i, v := range embedding {
				vec32[i] = float32(v)
			}

			bleveDoc := map[string]interface{}{
				"_vector": vec32,
			}

			// 添加元数据字段
			for _, field := range vs.metadataFields {
				if val, ok := event.Doc[field]; ok {
					bleveDoc[field] = val
				}
			}

			_ = idx.Index(event.ID, bleveDoc)
		}
	case OperationDelete:
		if vs.embeddingCache != nil {
			vs.embeddingCache.Remove(event.ID)
		}
		_ = idx.Delete(event.ID)

		// 标记布隆过滤器需要重建
		vs.idBloomNeedsRebuild = true
		if partition != "" {
			vs.partitionBloomNeedsRebuild[partition] = true
		}
	}
}

// ensureInitialized 确保索引已初始化。
func (vs *VectorSearch) ensureInitialized(ctx context.Context) error {
	if vs.initialized {
		return nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.initialized {
		return nil
	}

	if err := vs.buildIndex(ctx); err != nil {
		return err
	}
	vs.initialized = true
	return nil
}

// Search 执行向量相似性搜索。
// queryEmbedding 是查询向量，options 是搜索选项。
func (vs *VectorSearch) Search(ctx context.Context, queryEmbedding Vector, options ...VectorSearchOptions) ([]VectorSearchResult, error) {
	// 确保索引已初始化
	if err := vs.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// 解析选项
	var opts VectorSearchOptions
	if len(options) > 0 {
		opts = options[0]
	}

	// 选择索引（支持物理分区）
	var idx bleve.Index
	if vs.partitionField != "" && opts.Partition != "" {
		var ok bool
		idx, ok = vs.partitions[opts.Partition]
		if !ok {
			// 分区不存在，返回空结果
			return []VectorSearchResult{}, nil
		}
	} else {
		idx = vs.index
	}

	if idx == nil {
		return nil, fmt.Errorf("no index available for search")
	}

	// 验证查询向量维度
	if len(queryEmbedding) != vs.dimensions {
		return nil, fmt.Errorf("query embedding dimension mismatch: expected %d, got %d", vs.dimensions, len(queryEmbedding))
	}

	// 转换为 float32
	queryVec32 := make([]float32, len(queryEmbedding))
	for i, v := range queryEmbedding {
		queryVec32[i] = float32(v)
	}

	// 创建搜索请求
	// 如果有选择器，使用它作为基础查询，否则使用 MatchAllQuery
	baseQuery := selectorToBleveQuery(opts.Selector)

	// --- Heuristic Switching ---
	// 如果提供了选择器，检查匹配的文档数量。
	// 如果匹配数量非常少，使用暴力搜索可能更快。
	if len(opts.Selector) > 0 {
		countRequest := bleve.NewSearchRequest(baseQuery)
		countRequest.Size = 0 // 只需要计数
		res, err := idx.Search(countRequest)
		if err == nil && res.Total < 100 {
			// 策略切换：对于极少量的结果（< 100），执行前置过滤后的暴力搜索
			return vs.searchWithMetadataFilteredBruteForce(ctx, queryEmbedding, opts, res.Hits)
		}
	}

	searchRequest := bleve.NewSearchRequest(baseQuery)

	// 设置 kNN 参数
	k := int64(opts.Limit)
	if k <= 0 {
		k = 10 // 默认返回 10 个结果
	}

	// 添加 kNN 搜索
	if addKNN := getAddKNNMethod(searchRequest); addKNN != nil {
		addKNN("_vector", queryVec32, k, 1.0)
	} else {
		// 回退到全表扫描（如果没有向量搜索支持）
		return vs.searchWithoutKNN(ctx, queryEmbedding, opts)
	}

	// 执行搜索
	searchResult, err := idx.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("bleve vector search failed: %w", err)
	}

	// 转换结果
	var results []VectorSearchResult
	for _, hit := range searchResult.Hits {
		// 获取文档
		doc, err := vs.collection.FindByID(ctx, hit.ID)
		if err != nil {
			continue
		}

		// bleve 的 kNN 分数是 1 / (1 + squared_distance)
		// 我们需要计算实际距离
		distance := vs.scoreToDistance(hit.Score)

		// 应用最大距离过滤
		if opts.MaxDistance > 0 && distance > opts.MaxDistance {
			continue
		}

		// 计算相似度分数
		score := vs.distanceToScore(distance)

		// 应用最小分数过滤
		if opts.MinScore > 0 && score < opts.MinScore {
			continue
		}

		results = append(results, VectorSearchResult{
			Document: doc,
			Distance: distance,
			Score:    score,
		})
	}

	// 按距离排序（bleve 可能已经排序，但为了确保一致性）
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	// 应用限制
	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	return results, nil
}

// scoreToDistance 将 bleve 的分数转换为距离。
// bleve 的 kNN 分数是 1 / (1 + squared_distance)
func (vs *VectorSearch) scoreToDistance(score float64) float64 {
	if score <= 0 {
		return math.MaxFloat64
	}
	// score = 1 / (1 + squared_distance)
	// squared_distance = 1/score - 1
	squaredDistance := 1.0/score - 1.0
	if squaredDistance < 0 {
		return 0
	}
	return math.Sqrt(squaredDistance)
}

// distanceToScore 将距离转换为分数。
func (vs *VectorSearch) distanceToScore(distance float64) float64 {
	switch vs.distanceMetric {
	case "cosine":
		// 余弦距离范围 [0, 2]，转换为分数 [0, 1]
		return 1.0 - distance/2.0
	case "euclidean", "l2":
		// 欧几里得距离转换为分数，使用 sigmoid 函数
		return 1.0 / (1.0 + distance)
	case "dot", "dot_product":
		// 点积距离是负值，直接使用 sigmoid
		return 1.0 / (1.0 + math.Exp(distance))
	default:
		return 1.0 - distance
	}
}

// SearchByDocument 根据文档执行相似性搜索。
// 自动从文档生成嵌入向量并搜索相似文档。
func (vs *VectorSearch) SearchByDocument(ctx context.Context, doc map[string]any, options ...VectorSearchOptions) ([]VectorSearchResult, error) {
	docID := ""
	if vs.collection != nil {
		if id, err := vs.collection.extractPrimaryKey(doc); err == nil {
			docID = id
		}
	}

	embedding, err := vs.getEmbeddingWithCache(docID, doc)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}
	return vs.Search(ctx, embedding, options...)
}

// SearchByID 根据文档 ID 执行相似性搜索。
// 查找与指定文档相似的其他文档。
func (vs *VectorSearch) SearchByID(ctx context.Context, docID string, options ...VectorSearchOptions) ([]VectorSearchResult, error) {
	// 获取文档
	doc, err := vs.collection.FindByID(ctx, docID)
	if err != nil {
		return nil, fmt.Errorf("document %s not found: %w", docID, err)
	}

	// 使用带缓存的向量获取
	embedding, err := vs.getEmbeddingWithCache(docID, doc.Data())
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	results, err := vs.Search(ctx, embedding, options...)
	if err != nil {
		return nil, err
	}

	// 过滤掉查询文档本身
	filtered := make([]VectorSearchResult, 0, len(results))
	for _, r := range results {
		if r.Document.ID() != docID {
			filtered = append(filtered, r)
		}
	}

	return filtered, nil
}

// GetEmbedding 获取文档的嵌入向量。
// 注意：bleve 不直接存储原始向量，此方法需要重新生成。
func (vs *VectorSearch) GetEmbedding(docID string) (Vector, bool) {
	// 从集合获取文档
	doc, err := vs.collection.FindByID(context.Background(), docID)
	if err != nil {
		return nil, false
	}

	embedding, err := vs.docToEmbedding(doc.Data())
	if err != nil {
		return nil, false
	}

	return embedding, true
}

// SetEmbedding 手动设置文档的嵌入向量。
func (vs *VectorSearch) SetEmbedding(docID string, embedding Vector) error {
	if len(embedding) != vs.dimensions {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d", vs.dimensions, len(embedding))
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 转换为 float32
	vec32 := make([]float32, len(embedding))
	for i, v := range embedding {
		vec32[i] = float32(v)
	}

	bleveDoc := map[string]interface{}{
		"_vector": vec32,
	}

	return vs.index.Index(docID, bleveDoc)
}

// Reindex 重建向量索引。
func (vs *VectorSearch) Reindex(ctx context.Context) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 关闭旧索引
	if vs.index != nil {
		_ = vs.index.Close()
	}

	// 删除索引目录
	if err := os.RemoveAll(vs.indexPath); err != nil {
		return fmt.Errorf("failed to remove index directory: %w", err)
	}

	// 重新创建索引
	if err := vs.openOrCreateIndex(""); err != nil {
		return fmt.Errorf("failed to recreate index: %w", err)
	}

	return vs.buildIndex(ctx)
}

// Close 关闭向量搜索实例。
func (vs *VectorSearch) Close() {
	// 检查是否需要重建布隆过滤器
	vs.mu.Lock()
	needsRebuild := vs.idBloomNeedsRebuild
	if !needsRebuild {
		for _, needed := range vs.partitionBloomNeedsRebuild {
			if needed {
				needsRebuild = true
				break
			}
		}
	}
	vs.mu.Unlock()

	if needsRebuild {
		_ = vs.rebuildBloomFilters(context.Background())
	}

	// 保存布隆过滤器
	_ = vs.saveBloomFilters(context.Background())

	close(vs.closeChan)
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.index != nil {
		_ = vs.index.Close()
	}
}

// Count 返回已索引的文档数量。
func (vs *VectorSearch) Count() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	if vs.index == nil {
		return 0
	}
	docCount, _ := vs.index.DocCount()
	return int(docCount)
}

// Persist 持久化向量索引到存储。
// bleve 索引会自动持久化，此方法主要用于兼容性。
func (vs *VectorSearch) Persist(ctx context.Context) error {
	// bleve 索引已经持久化到磁盘，无需额外操作
	return nil
}

// Load 从存储加载持久化的向量索引。
// bleve 索引在打开时自动加载，此方法主要用于兼容性。
func (vs *VectorSearch) Load(ctx context.Context) error {
	// bleve 索引在 openOrCreateIndex 时已经加载
	vs.initialized = true
	return nil
}

// KNNSearch K 近邻搜索。
// 返回与查询向量最接近的 K 个文档。
func (vs *VectorSearch) KNNSearch(ctx context.Context, queryEmbedding Vector, k int) ([]VectorSearchResult, error) {
	return vs.Search(ctx, queryEmbedding, VectorSearchOptions{
		Limit: k,
	})
}

// RangeSearch 范围搜索。
// 返回与查询向量距离在指定范围内的所有文档。
func (vs *VectorSearch) RangeSearch(ctx context.Context, queryEmbedding Vector, maxDistance float64) ([]VectorSearchResult, error) {
	return vs.Search(ctx, queryEmbedding, VectorSearchOptions{
		MaxDistance: maxDistance,
	})
}

// BatchSearch 批量搜索。
// 对多个查询向量执行搜索。
func (vs *VectorSearch) BatchSearch(ctx context.Context, queryEmbeddings []Vector, options ...VectorSearchOptions) ([][]VectorSearchResult, error) {
	results := make([][]VectorSearchResult, len(queryEmbeddings))
	for i, query := range queryEmbeddings {
		result, err := vs.Search(ctx, query, options...)
		if err != nil {
			return nil, fmt.Errorf("failed to search for query %d: %w", i, err)
		}
		results[i] = result
	}
	return results, nil
}

// ComputeSimilarityMatrix 计算文档之间的相似度矩阵。
// 返回 docIDs x docIDs 的相似度矩阵。
func (vs *VectorSearch) ComputeSimilarityMatrix(docIDs []string) ([][]float64, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	n := len(docIDs)
	matrix := make([][]float64, n)
	for i := range matrix {
		matrix[i] = make([]float64, n)
	}

	// 获取所有文档的嵌入向量
	embeddings := make(map[string]Vector)
	for _, docID := range docIDs {
		doc, err := vs.collection.FindByID(context.Background(), docID)
		if err != nil {
			continue
		}
		embedding, err := vs.docToEmbedding(doc.Data())
		if err != nil {
			continue
		}
		embeddings[docID] = embedding
	}

	// 计算相似度矩阵
	for i, id1 := range docIDs {
		emb1, exists := embeddings[id1]
		if !exists {
			continue
		}
		for j, id2 := range docIDs {
			if i == j {
				matrix[i][j] = 1.0 // 自身相似度为 1
				continue
			}
			emb2, exists := embeddings[id2]
			if !exists {
				continue
			}
			distance := vs.calculateDistance(emb1, emb2)
			matrix[i][j] = vs.distanceToScore(distance)
		}
	}

	return matrix, nil
}

// calculateDistance 计算两个向量之间的距离。
func (vs *VectorSearch) calculateDistance(a, b Vector) float64 {
	switch vs.distanceMetric {
	case "euclidean", "l2":
		return EuclideanDistance(a, b)
	case "cosine":
		return CosineDistance(a, b)
	case "dot", "dot_product":
		return DotProductDistance(a, b)
	default:
		return CosineDistance(a, b)
	}
}

// EuclideanDistance 计算欧几里得距离。
func EuclideanDistance(a, b Vector) float64 {
	n := len(a)
	if n != len(b) {
		return math.MaxFloat64
	}

	var sum float64
	i := 0
	// 循环展开以辅助编译器自动向量化
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// CosineDistance 计算余弦距离（1 - 余弦相似度）。
func CosineDistance(a, b Vector) float64 {
	similarity := CosineSimilarity(a, b)
	return 1.0 - similarity
}

// CosineSimilarity 计算余弦相似度。
func CosineSimilarity(a, b Vector) float64 {
	n := len(a)
	if n != len(b) {
		return 0
	}

	var dotProduct, normA, normB float64
	i := 0
	// 循环展开以辅助编译器自动向量化
	for ; i <= n-4; i += 4 {
		dotProduct += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
		normA += a[i]*a[i] + a[i+1]*a[i+1] + a[i+2]*a[i+2] + a[i+3]*a[i+3]
		normB += b[i]*b[i] + b[i+1]*b[i+1] + b[i+2]*b[i+2] + b[i+3]*b[i+3]
	}
	for ; i < n; i++ {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// DotProductDistance 计算点积距离（负点积）。
func DotProductDistance(a, b Vector) float64 {
	n := len(a)
	if n != len(b) {
		return math.MaxFloat64
	}

	var dotProduct float64
	i := 0
	// 循环展开以辅助编译器自动向量化
	for ; i <= n-4; i += 4 {
		dotProduct += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	for ; i < n; i++ {
		dotProduct += a[i] * b[i]
	}
	return -dotProduct // 负值，使得更大的点积对应更小的距离
}

// NormalizeVector 归一化向量。
func NormalizeVector(v Vector) Vector {
	var norm float64
	for _, val := range v {
		norm += val * val
	}
	norm = math.Sqrt(norm)

	if norm == 0 {
		return v
	}

	normalized := make(Vector, len(v))
	for i, val := range v {
		normalized[i] = val / norm
	}
	return normalized
}

// getSimilarityMetric 获取相似度度量字符串
func (vs *VectorSearch) getSimilarityMetric() string {
	switch vs.distanceMetric {
	case "cosine":
		return "cosine"
	case "euclidean", "l2":
		return "l2_norm"
	case "dot", "dot_product":
		return "dot_product"
	default:
		return "cosine"
	}
}

// getAddKNNMethod 获取 AddKNN 方法（使用反射）
func getAddKNNMethod(req *bleve.SearchRequest) func(string, []float32, int64, float64) {
	v := reflect.ValueOf(req)
	method := v.MethodByName("AddKNN")
	if !method.IsValid() {
		return nil
	}

	return func(field string, vector []float32, k int64, boost float64) {
		method.Call([]reflect.Value{
			reflect.ValueOf(field),
			reflect.ValueOf(vector),
			reflect.ValueOf(k),
			reflect.ValueOf(boost),
		})
	}
}

// searchWithMetadataFilteredBruteForce 对已经通过元数据过滤的少量结果执行暴力向量搜索。
func (vs *VectorSearch) searchWithMetadataFilteredBruteForce(ctx context.Context, queryEmbedding Vector, opts VectorSearchOptions, initialHits []*search.DocumentMatch) ([]VectorSearchResult, error) {
	// 如果 initialHits 为空（Size=0 导致），重新获取匹配的 ID
	var hitIDs []string
	if len(initialHits) == 0 {
		baseQuery := selectorToBleveQuery(opts.Selector)
		req := bleve.NewSearchRequest(baseQuery)
		req.Size = 100 // 阈值内的最大值
		idx, _ := vs.getOrCreateIndex(opts.Partition)
		if idx == nil {
			return nil, fmt.Errorf("index not found for partition: %s", opts.Partition)
		}
		res, err := idx.Search(req)
		if err != nil {
			return nil, err
		}
		for _, hit := range res.Hits {
			hitIDs = append(hitIDs, hit.ID)
		}
	} else {
		for _, hit := range initialHits {
			hitIDs = append(hitIDs, hit.ID)
		}
	}

	var results []VectorSearchResult
	for _, docID := range hitIDs {
		doc, err := vs.collection.FindByID(ctx, docID)
		if err != nil {
			continue
		}

		embedding, err := vs.getEmbeddingWithCache(docID, doc.Data())
		if err != nil {
			continue
		}

		distance := vs.calculateDistance(queryEmbedding, embedding)
		if opts.MaxDistance > 0 && distance > opts.MaxDistance {
			continue
		}

		score := vs.distanceToScore(distance)
		if opts.MinScore > 0 && score < opts.MinScore {
			continue
		}

		results = append(results, VectorSearchResult{
			Document: doc,
			Distance: distance,
			Score:    score,
		})
	}

	// 排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	return results, nil
}

// searchWithoutKNN 在没有 kNN 支持时的回退搜索方法
func (vs *VectorSearch) searchWithoutKNN(ctx context.Context, queryEmbedding Vector, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	// 获取所有文档并计算距离
	docs, err := vs.collection.All(ctx)
	if err != nil {
		return nil, err
	}

	var results []VectorSearchResult
	for _, doc := range docs {
		// 如果有选择器，先过滤
		if len(opts.Selector) > 0 {
			q := vs.collection.Find(opts.Selector)
			if !q.match(doc.Data()) {
				continue
			}
		}

		embedding, err := vs.docToEmbedding(doc.Data())
		if err != nil {
			continue
		}
		if len(embedding) != vs.dimensions {
			continue
		}

		distance := vs.calculateDistance(queryEmbedding, embedding)
		if opts.MaxDistance > 0 && distance > opts.MaxDistance {
			continue
		}

		score := vs.distanceToScore(distance)
		if opts.MinScore > 0 && score < opts.MinScore {
			continue
		}

		results = append(results, VectorSearchResult{
			Document: doc,
			Distance: distance,
			Score:    score,
		})
	}

	// 按距离排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	// 应用限制
	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	return results, nil
}

// saveBloomFilters 将布隆过滤器保存到存储中。
func (vs *VectorSearch) saveBloomFilters(ctx context.Context) error {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// 保存全局 ID 布隆过滤器
	if data, err := vs.idBloomFilter.MarshalBinary(); err == nil {
		_ = vs.collection.store.Set(ctx, "_bloom", vs.identifier+"_ids", data)
	}

	// 保存分区布隆过滤器
	for p, bf := range vs.partitionBloomFilters {
		if data, err := bf.MarshalBinary(); err == nil {
			_ = vs.collection.store.Set(ctx, "_bloom", vs.identifier+"_partition_"+p, data)
		}
	}
	return nil
}

// loadBloomFilters 从存储中加载布隆过滤器。
func (vs *VectorSearch) loadBloomFilters(ctx context.Context) error {
	// 加载全局 ID 布隆过滤器
	if data, err := vs.collection.store.Get(ctx, "_bloom", vs.identifier+"_ids"); err == nil && data != nil {
		_ = vs.idBloomFilter.UnmarshalBinary(data)
	}

	// 加载分区布隆过滤器
	prefix := vs.identifier + "_partition_"
	err := vs.collection.store.Iterate(ctx, "_bloom", func(key, value []byte) error {
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			partition := strings.TrimPrefix(keyStr, prefix)
			bf := NewBloomFilter(1000, 0.01)
			if err := bf.UnmarshalBinary(value); err == nil {
				vs.partitionBloomFilters[partition] = bf
			}
		}
		return nil
	})
	return err
}

// rebuildBloomFilters 从索引中重新构建布隆过滤器。
func (vs *VectorSearch) rebuildBloomFilters(ctx context.Context) error {
	// 重置全局 ID 布隆过滤器
	vs.idBloomFilter.Clear()

	// 重置分区布隆过滤器
	for _, bf := range vs.partitionBloomFilters {
		bf.Clear()
	}

	// 遍历所有文档重新构建
	docs, err := vs.collection.All(ctx)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		vs.idBloomFilter.Add(doc.ID())

		if vs.partitionField != "" {
			if p, ok := doc.Data()[vs.partitionField].(string); ok {
				if bf, ok := vs.partitionBloomFilters[p]; ok {
					bf.Add(doc.ID())
				}
			}
		}
	}

	vs.idBloomNeedsRebuild = false
	for p := range vs.partitionBloomNeedsRebuild {
		vs.partitionBloomNeedsRebuild[p] = false
	}

	return nil
}
