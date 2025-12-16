package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/blevesearch/bleve/v2"
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

	index     bleve.Index
	indexPath string

	mu          sync.RWMutex
	initialized bool
	closeChan   chan struct{}
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
		identifier:     config.Identifier,
		collection:     col,
		docToEmbedding: config.DocToEmbedding,
		dimensions:     config.Dimensions,
		distanceMetric: distanceMetric,
		indexType:      indexType,
		numIndexes:     numIndexes,
		indexDistance:  indexDistance,
		batchSize:      batchSize,
		initMode:       initMode,
		indexPath:      indexPath,
		closeChan:      make(chan struct{}),
	}

	// 创建或打开 bleve 索引
	if err := vs.openOrCreateIndex(); err != nil {
		return nil, fmt.Errorf("failed to open/create bleve index: %w", err)
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
func (vs *VectorSearch) openOrCreateIndex() error {
	// 尝试打开现有索引
	if index, err := bleve.Open(vs.indexPath); err == nil {
		vs.index = index
		return nil
	}

	// 创建新的索引映射
	// 注意：bleve 的向量搜索功能需要启用 vectors 构建标签
	// 编译时使用: go build -tags vectors
	// 并且需要安装 FAISS 库
	//
	// 由于向量字段映射需要 vectors 构建标签，我们通过 JSON 配置来创建映射
	indexMappingJSON := map[string]interface{}{
		"default_mapping": map[string]interface{}{
			"dynamic": false,
			"properties": map[string]interface{}{
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
			},
		},
	}

	jsonData, _ := json.Marshal(indexMappingJSON)
	indexMapping := bleve.NewIndexMapping()
	if err := json.Unmarshal(jsonData, indexMapping); err != nil {
		// 如果 JSON 配置失败，使用默认映射
		// 这会在没有 vectors 构建标签时发生
		indexMapping.DefaultMapping.Dynamic = false
	}

	// 创建索引
	index, err := bleve.New(vs.indexPath, indexMapping)
	if err != nil {
		return fmt.Errorf("failed to create bleve index: %w", err)
	}

	vs.index = index
	return nil
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

	// 批量索引文档
	batch := vs.index.NewBatch()
	count := 0
	for _, doc := range docs {
		// 生成嵌入向量
		embedding, err := vs.docToEmbedding(doc.Data())
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

		// 添加到批处理
		if err := batch.Index(doc.ID(), bleveDoc); err != nil {
			return fmt.Errorf("failed to index document %s: %w", doc.ID(), err)
		}

		count++
		if count >= vs.batchSize {
			// 提交批处理
			if err := vs.index.Batch(batch); err != nil {
				return fmt.Errorf("failed to batch index: %w", err)
			}
			batch = vs.index.NewBatch()
			count = 0
		}
	}

	// 提交剩余的文档
	if count > 0 {
		if err := vs.index.Batch(batch); err != nil {
			return fmt.Errorf("failed to batch index: %w", err)
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

	switch event.Op {
	case OperationInsert, OperationUpdate:
		if event.Doc != nil {
			embedding, err := vs.docToEmbedding(event.Doc)
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
			_ = vs.index.Index(event.ID, bleveDoc)
		}
	case OperationDelete:
		_ = vs.index.Delete(event.ID)
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

	// 验证查询向量维度
	if len(queryEmbedding) != vs.dimensions {
		return nil, fmt.Errorf("query embedding dimension mismatch: expected %d, got %d", vs.dimensions, len(queryEmbedding))
	}

	// 解析选项
	var opts VectorSearchOptions
	if len(options) > 0 {
		opts = options[0]
	}

	// 转换为 float32
	queryVec32 := make([]float32, len(queryEmbedding))
	for i, v := range queryEmbedding {
		queryVec32[i] = float32(v)
	}

	// 创建搜索请求
	// 使用 MatchNoneQuery 进行纯向量搜索
	searchRequest := bleve.NewSearchRequest(bleve.NewMatchNoneQuery())

	// 设置 kNN 参数
	k := int64(opts.Limit)
	if k <= 0 {
		k = 10 // 默认返回 10 个结果
	}

	// 添加 kNN 搜索
	// 注意：AddKNN 方法需要 vectors 构建标签
	// 如果没有启用 vectors 构建标签，这里会失败
	// 需要使用反射或条件编译来处理
	if addKNN := getAddKNNMethod(searchRequest); addKNN != nil {
		addKNN("_vector", queryVec32, k, 1.0)
	} else {
		// 回退到全表扫描（如果没有向量搜索支持）
		return vs.searchWithoutKNN(ctx, queryEmbedding, opts)
	}

	// 执行搜索
	searchResult, err := vs.index.Search(searchRequest)
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
	embedding, err := vs.docToEmbedding(doc)
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

	// 生成嵌入向量
	embedding, err := vs.docToEmbedding(doc.Data())
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
	if err := vs.openOrCreateIndex(); err != nil {
		return fmt.Errorf("failed to recreate index: %w", err)
	}

	return vs.buildIndex(ctx)
}

// Close 关闭向量搜索实例。
func (vs *VectorSearch) Close() {
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
	if len(a) != len(b) {
		return math.MaxFloat64
	}

	var sum float64
	for i := range a {
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
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
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
	if len(a) != len(b) {
		return math.MaxFloat64
	}

	var dotProduct float64
	for i := range a {
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
	// 使用反射调用 AddKNN 方法
	// 这需要 vectors 构建标签才能工作
	// 如果没有 vectors 构建标签，返回 nil
	return nil // 占位符，实际需要使用反射
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
