package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
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
	IndexType string
	// NumIndexes 用于索引的采样向量数量（用于 IVF 索引）。
	// 默认为 5。
	NumIndexes int
	// IndexDistance 索引距离阈值（用于 IVF 查询优化）。
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
	DocsPerIndexSide int
	// UseFullScan 是否使用全表扫描（而不是索引）。
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

	// 存储
	embeddings map[string]Vector // docID -> embedding
	// IVF 索引相关
	sampleVectors []Vector          // 采样向量（用于 IVF）
	indexBuckets  []map[string]bool // 每个采样向量的文档桶

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
		embeddings:     make(map[string]Vector),
		sampleVectors:  make([]Vector, 0),
		indexBuckets:   make([]map[string]bool, 0),
		closeChan:      make(chan struct{}),
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

// buildIndex 构建向量索引。
func (vs *VectorSearch) buildIndex(ctx context.Context) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 获取所有文档
	docs, err := vs.collection.All(ctx)
	if err != nil {
		return err
	}

	// 为每个文档生成嵌入向量
	for _, doc := range docs {
		embedding, err := vs.docToEmbedding(doc.Data())
		if err != nil {
			continue // 跳过无法生成嵌入的文档
		}
		if len(embedding) != vs.dimensions {
			continue // 跳过维度不匹配的向量
		}
		vs.embeddings[doc.ID()] = embedding
	}

	// 如果使用 IVF 索引，构建采样向量和索引桶
	if vs.indexType == "ivf" && len(vs.embeddings) > 0 {
		vs.buildIVFIndex()
	}

	return nil
}

// buildIVFIndex 构建 IVF 索引。
func (vs *VectorSearch) buildIVFIndex() {
	// 随机选择采样向量
	allEmbeddings := make([]Vector, 0, len(vs.embeddings))
	allDocIDs := make([]string, 0, len(vs.embeddings))
	for docID, emb := range vs.embeddings {
		allEmbeddings = append(allEmbeddings, emb)
		allDocIDs = append(allDocIDs, docID)
	}

	// 使用简单的均匀采样
	step := len(allEmbeddings) / vs.numIndexes
	if step < 1 {
		step = 1
	}

	vs.sampleVectors = make([]Vector, 0, vs.numIndexes)
	for i := 0; i < vs.numIndexes && i*step < len(allEmbeddings); i++ {
		vs.sampleVectors = append(vs.sampleVectors, allEmbeddings[i*step])
	}

	// 创建索引桶
	vs.indexBuckets = make([]map[string]bool, len(vs.sampleVectors))
	for i := range vs.indexBuckets {
		vs.indexBuckets[i] = make(map[string]bool)
	}

	// 将每个文档分配到最近的采样向量桶
	for i, docID := range allDocIDs {
		nearestIdx := vs.findNearestSampleIndex(allEmbeddings[i])
		vs.indexBuckets[nearestIdx][docID] = true
	}
}

// findNearestSampleIndex 找到最近的采样向量索引。
func (vs *VectorSearch) findNearestSampleIndex(embedding Vector) int {
	if len(vs.sampleVectors) == 0 {
		return 0
	}

	minDist := math.MaxFloat64
	nearestIdx := 0

	for i, sample := range vs.sampleVectors {
		dist := vs.calculateDistance(embedding, sample)
		if dist < minDist {
			minDist = dist
			nearestIdx = i
		}
	}

	return nearestIdx
}

// calculateDistance 计算两个向量之间的距离。
func (vs *VectorSearch) calculateDistance(a, b Vector) float64 {
	switch vs.distanceMetric {
	case "euclidean":
		return EuclideanDistance(a, b)
	case "cosine":
		return CosineDistance(a, b)
	case "dot":
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

			// 从旧的索引桶中移除
			if vs.indexType == "ivf" && len(vs.embeddings[event.ID]) > 0 {
				oldIdx := vs.findNearestSampleIndex(vs.embeddings[event.ID])
				delete(vs.indexBuckets[oldIdx], event.ID)
			}

			// 添加新的嵌入
			vs.embeddings[event.ID] = embedding

			// 添加到新的索引桶
			if vs.indexType == "ivf" && len(vs.sampleVectors) > 0 {
				newIdx := vs.findNearestSampleIndex(embedding)
				vs.indexBuckets[newIdx][event.ID] = true
			}
		}
	case OperationDelete:
		// 从索引桶中移除
		if vs.indexType == "ivf" && len(vs.embeddings[event.ID]) > 0 {
			oldIdx := vs.findNearestSampleIndex(vs.embeddings[event.ID])
			if oldIdx < len(vs.indexBuckets) {
				delete(vs.indexBuckets[oldIdx], event.ID)
			}
		}
		delete(vs.embeddings, event.ID)
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

	// 收集候选文档
	var candidates []string
	if opts.UseFullScan || vs.indexType == "flat" {
		// 全表扫描
		candidates = make([]string, 0, len(vs.embeddings))
		for docID := range vs.embeddings {
			candidates = append(candidates, docID)
		}
	} else if vs.indexType == "ivf" {
		// 使用 IVF 索引
		candidates = vs.getCandidatesFromIVF(queryEmbedding, opts)
	}

	// 计算距离并排序
	type distanceResult struct {
		docID    string
		distance float64
	}
	var results []distanceResult

	for _, docID := range candidates {
		embedding, exists := vs.embeddings[docID]
		if !exists {
			continue
		}

		distance := vs.calculateDistance(queryEmbedding, embedding)

		// 应用最大距离过滤
		if opts.MaxDistance > 0 && distance > opts.MaxDistance {
			continue
		}

		// 计算分数并应用最小分数过滤
		score := vs.distanceToScore(distance)
		if opts.MinScore > 0 && score < opts.MinScore {
			continue
		}

		results = append(results, distanceResult{docID, distance})
	}

	// 按距离排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	// 应用限制
	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	// 获取文档并构建结果
	var searchResults []VectorSearchResult
	for _, r := range results {
		doc, err := vs.collection.FindByID(ctx, r.docID)
		if err != nil {
			continue
		}
		searchResults = append(searchResults, VectorSearchResult{
			Document: doc,
			Distance: r.distance,
			Score:    vs.distanceToScore(r.distance),
		})
	}

	return searchResults, nil
}

// getCandidatesFromIVF 从 IVF 索引获取候选文档。
func (vs *VectorSearch) getCandidatesFromIVF(queryEmbedding Vector, opts VectorSearchOptions) []string {
	if len(vs.sampleVectors) == 0 || len(vs.indexBuckets) == 0 {
		// 没有索引，返回所有文档
		candidates := make([]string, 0, len(vs.embeddings))
		for docID := range vs.embeddings {
			candidates = append(candidates, docID)
		}
		return candidates
	}

	// 计算查询向量到所有采样向量的距离
	type sampleDistance struct {
		index    int
		distance float64
	}
	sampleDistances := make([]sampleDistance, len(vs.sampleVectors))
	for i, sample := range vs.sampleVectors {
		sampleDistances[i] = sampleDistance{i, vs.calculateDistance(queryEmbedding, sample)}
	}

	// 按距离排序
	sort.Slice(sampleDistances, func(i, j int) bool {
		return sampleDistances[i].distance < sampleDistances[j].distance
	})

	// 从最近的桶收集候选
	candidateSet := make(map[string]bool)
	docsPerSide := opts.DocsPerIndexSide
	if docsPerSide <= 0 {
		docsPerSide = 100
	}

	// 至少访问前几个最近的桶，确保能返回结果
	minBucketsToVisit := vs.numIndexes / 2
	if minBucketsToVisit < 1 {
		minBucketsToVisit = 1
	}

	bucketsVisited := 0
	for _, sd := range sampleDistances {
		// 确保至少访问 minBucketsToVisit 个桶
		if bucketsVisited >= minBucketsToVisit {
			// 如果已访问足够的桶，检查距离阈值
			if vs.indexDistance > 0 && sd.distance > vs.indexDistance*float64(vs.numIndexes*10) {
				break
			}
		}

		if sd.index < len(vs.indexBuckets) {
			for docID := range vs.indexBuckets[sd.index] {
				candidateSet[docID] = true
			}
			bucketsVisited++
		}

		// 检查是否已收集足够的候选
		if len(candidateSet) >= docsPerSide*vs.numIndexes {
			break
		}
	}

	candidates := make([]string, 0, len(candidateSet))
	for docID := range candidateSet {
		candidates = append(candidates, docID)
	}
	return candidates
}

// distanceToScore 将距离转换为分数。
func (vs *VectorSearch) distanceToScore(distance float64) float64 {
	switch vs.distanceMetric {
	case "cosine":
		// 余弦距离范围 [0, 2]，转换为分数 [0, 1]
		return 1.0 - distance/2.0
	case "euclidean":
		// 欧几里得距离转换为分数，使用 sigmoid 函数
		return 1.0 / (1.0 + distance)
	case "dot":
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
	vs.mu.RLock()
	embedding, exists := vs.embeddings[docID]
	vs.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("document %s not found in vector index", docID)
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
func (vs *VectorSearch) GetEmbedding(docID string) (Vector, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	embedding, exists := vs.embeddings[docID]
	return embedding, exists
}

// SetEmbedding 手动设置文档的嵌入向量。
func (vs *VectorSearch) SetEmbedding(docID string, embedding Vector) error {
	if len(embedding) != vs.dimensions {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d", vs.dimensions, len(embedding))
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 从旧的索引桶中移除
	if vs.indexType == "ivf" && len(vs.embeddings[docID]) > 0 {
		oldIdx := vs.findNearestSampleIndex(vs.embeddings[docID])
		if oldIdx < len(vs.indexBuckets) {
			delete(vs.indexBuckets[oldIdx], docID)
		}
	}

	vs.embeddings[docID] = embedding

	// 添加到新的索引桶
	if vs.indexType == "ivf" && len(vs.sampleVectors) > 0 {
		newIdx := vs.findNearestSampleIndex(embedding)
		vs.indexBuckets[newIdx][docID] = true
	}

	return nil
}

// Reindex 重建向量索引。
func (vs *VectorSearch) Reindex(ctx context.Context) error {
	vs.mu.Lock()
	// 清空索引
	vs.embeddings = make(map[string]Vector)
	vs.sampleVectors = make([]Vector, 0)
	vs.indexBuckets = make([]map[string]bool, 0)
	vs.mu.Unlock()

	return vs.buildIndex(ctx)
}

// Close 关闭向量搜索实例。
func (vs *VectorSearch) Close() {
	close(vs.closeChan)
}

// Count 返回已索引的文档数量。
func (vs *VectorSearch) Count() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return len(vs.embeddings)
}

// Persist 持久化向量索引到存储。
func (vs *VectorSearch) Persist(ctx context.Context) error {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// 序列化索引
	indexData := struct {
		Embeddings    map[string]Vector `json:"embeddings"`
		SampleVectors []Vector          `json:"sample_vectors"`
		IndexBuckets  []map[string]bool `json:"index_buckets"`
	}{
		Embeddings:    vs.embeddings,
		SampleVectors: vs.sampleVectors,
		IndexBuckets:  vs.indexBuckets,
	}

	data, err := json.Marshal(indexData)
	if err != nil {
		return fmt.Errorf("failed to marshal vector index: %w", err)
	}

	// 存储到集合的元数据
	bucket := fmt.Sprintf("%s_vector", vs.collection.name)
	return vs.collection.store.Set(ctx, bucket, vs.identifier, data)
}

// Load 从存储加载持久化的向量索引。
func (vs *VectorSearch) Load(ctx context.Context) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	bucket := fmt.Sprintf("%s_vector", vs.collection.name)
	data, err := vs.collection.store.Get(ctx, bucket, vs.identifier)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // 没有持久化的索引
	}

	// 反序列化索引
	var indexData struct {
		Embeddings    map[string]Vector `json:"embeddings"`
		SampleVectors []Vector          `json:"sample_vectors"`
		IndexBuckets  []map[string]bool `json:"index_buckets"`
	}

	if err := json.Unmarshal(data, &indexData); err != nil {
		return fmt.Errorf("failed to unmarshal vector index: %w", err)
	}

	vs.embeddings = indexData.Embeddings
	vs.sampleVectors = indexData.SampleVectors
	vs.indexBuckets = indexData.IndexBuckets
	vs.initialized = true

	return nil
}

// KNNSearch K 近邻搜索。
// 返回与查询向量最接近的 K 个文档。
func (vs *VectorSearch) KNNSearch(ctx context.Context, queryEmbedding Vector, k int) ([]VectorSearchResult, error) {
	return vs.Search(ctx, queryEmbedding, VectorSearchOptions{
		Limit:       k,
		UseFullScan: true, // KNN 通常需要全表扫描以获得精确结果
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

	for i, id1 := range docIDs {
		emb1, exists := vs.embeddings[id1]
		if !exists {
			continue
		}
		for j, id2 := range docIDs {
			if i == j {
				matrix[i][j] = 1.0 // 自身相似度为 1
				continue
			}
			emb2, exists := vs.embeddings[id2]
			if !exists {
				continue
			}
			distance := vs.calculateDistance(emb1, emb2)
			matrix[i][j] = vs.distanceToScore(distance)
		}
	}

	return matrix, nil
}
