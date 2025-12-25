package rxdb

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/search/query"
	huichensego "github.com/huichen/sego"
	"github.com/mozhou-tech/rxdb-go/pkg/sego"
)

// FulltextSearchConfig 全文搜索配置。
// 参考 RxDB FlexSearch 插件的配置选项。
type FulltextSearchConfig struct {
	// Identifier 唯一标识符，用于存储元数据和在重启/重载时继续索引。
	Identifier string
	// DocToString 将文档转换为可搜索字符串的函数。
	// 可以返回单个字段值或连接多个字段。
	DocToString func(doc map[string]any) string
	// BatchSize 每次索引的文档数量（可选）。
	BatchSize int
	// Initialization 初始化模式："instant"（立即）或 "lazy"（懒加载）。
	// 默认为 "instant"。
	Initialization string
	// IndexOptions 索引选项（可选）。
	IndexOptions *FulltextIndexOptions
}

// FulltextIndexOptions 全文索引选项。
type FulltextIndexOptions struct {
	// Tokenize 分词模式："strict"（严格）、"forward"（前向）、"reverse"（反向）、"full"（完整）。
	Tokenize string
	// MinLength 最小搜索词长度。
	MinLength int
	// CaseSensitive 是否区分大小写。
	CaseSensitive bool
	// StopWords 停用词列表。
	StopWords []string
}

// FulltextSearchResult 全文搜索结果。
type FulltextSearchResult struct {
	Document Document
	Score    float64 // 相关性分数
}

// FulltextSearchOptions 全文搜索选项。
type FulltextSearchOptions struct {
	// Limit 返回结果数量限制。
	Limit int
	// Threshold 相关性阈值（0-1）。
	Threshold float64
	// Selector 元数据过滤选择器（Mango 语法）。
	// 如果提供，将在全文搜索时进行前置过滤。
	Selector map[string]any
}

// FulltextSearch 全文搜索实例。
// 参考 RxDB 的 RxFulltextSearch。
type FulltextSearch struct {
	identifier  string
	collection  *collection
	docToString func(doc map[string]any) string
	options     *FulltextIndexOptions
	index       bleve.Index
	indexPath   string
	mu          sync.RWMutex
	initialized bool
	initMode    string
	batchSize   int
	closeChan   chan struct{}
}

const (
	segoAnalyzerName  = "rxdb_sego"
	segoTokenizerName = "rxdb_sego_tokenizer"
)

var (
	registerSegoOnce sync.Once
)

// SetSegoDictionary 设置 sego 词典路径并初始化分词器。
// 注意：现在优先使用 pkg/sego 中内嵌的词典。
func SetSegoDictionary(path string) error {
	return nil
}

// getSegmenter 获取全局 sego 分词器。
func getSegmenter() *huichensego.Segmenter {
	segmenter, _ := sego.GetSegmenter()
	return segmenter
}

// registerSego 注册基于 sego 的 tokenizer 与 analyzer。
func registerSego() {
	registerSegoOnce.Do(func() {
		registry.RegisterTokenizer(segoTokenizerName, func(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
			return &segoTokenizer{seg: getSegmenter()}, nil
		})

		registry.RegisterAnalyzer(segoAnalyzerName, func(config map[string]interface{}, cache *registry.Cache) (analysis.Analyzer, error) {
			tokenizer, err := cache.TokenizerNamed(segoTokenizerName)
			if err != nil {
				return nil, err
			}
			lower, _ := cache.TokenFilterNamed(lowercase.Name)
			return &analysis.DefaultAnalyzer{
				Tokenizer:    tokenizer,
				TokenFilters: []analysis.TokenFilter{lower},
			}, nil
		})
	})
}

type segoTokenizer struct {
	seg *huichensego.Segmenter
}

func (t *segoTokenizer) Tokenize(input []byte) analysis.TokenStream {
	if t.seg == nil {
		return nil
	}

	segments := t.seg.Segment(input)
	stream := make(analysis.TokenStream, 0, len(segments))

	for i, seg := range segments {
		stream = append(stream, &analysis.Token{
			Term:     input[seg.Start():seg.End()],
			Start:    seg.Start(),
			End:      seg.End(),
			Position: i + 1,
		})
	}
	return stream
}

// AddFulltextSearch 在集合上创建全文搜索实例。
// 参考 RxDB 的 addFulltextSearch 函数。
func AddFulltextSearch(coll Collection, config FulltextSearchConfig) (*FulltextSearch, error) {
	col, ok := coll.(*collection)
	if !ok {
		return nil, fmt.Errorf("invalid collection type")
	}

	if config.Identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}
	if config.DocToString == nil {
		return nil, fmt.Errorf("docToString function is required")
	}

	initMode := config.Initialization
	if initMode == "" {
		initMode = "instant"
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	// 确定索引存储路径
	storePath := col.store.Path()
	var indexPath string
	if storePath != "" {
		// 使用数据库路径下的子目录存储 bleve 索引
		indexPath = filepath.Join(storePath, "fulltext", col.name, config.Identifier)
	} else {
		// 内存模式，使用临时目录
		indexPath = filepath.Join(os.TempDir(), "rxdb-fulltext", col.name, config.Identifier)
	}

	fts := &FulltextSearch{
		identifier:  config.Identifier,
		collection:  col,
		docToString: config.DocToString,
		options:     config.IndexOptions,
		indexPath:   indexPath,
		initMode:    initMode,
		batchSize:   batchSize,
		closeChan:   make(chan struct{}),
	}

	// 创建或打开 bleve 索引
	if err := fts.openOrCreateIndex(); err != nil {
		return nil, fmt.Errorf("failed to open/create bleve index: %w", err)
	}

	// 根据初始化模式决定是否立即建立索引
	if initMode == "instant" {
		if err := fts.buildIndex(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to build fulltext index: %w", err)
		}
		fts.initialized = true
	}

	// 启动监听变更的 goroutine
	go fts.watchChanges()

	return fts, nil
}

// openOrCreateIndex 打开或创建 bleve 索引。
func (fts *FulltextSearch) openOrCreateIndex() error {
	// 尝试打开现有索引
	if index, err := bleve.Open(fts.indexPath); err == nil {
		fts.index = index
		return nil
	}

	// 创建新的索引映射
	mapping := bleve.NewIndexMapping()

	// 配置文本字段映射
	textFieldMapping := bleve.NewTextFieldMapping()
	// 内存优化：不存储原始字段值，仅索引。因为我们可以通过 ID 在 RxDB 中查到原始数据。
	textFieldMapping.Store = false

	// 创建自定义分析器（如果需要）
	if fts.options != nil {
		// 中文分词：使用 sego + lowercase
		if strings.EqualFold(fts.options.Tokenize, "sego") {
			registerSego()
			if !fts.options.CaseSensitive {
				// 如果需要不区分大小写，我们需要创建一个组合了 sego tokenizer 和 lowercase filter 的分析器
				// 已经注册的 segoAnalyzerName 已经包含了 lowercase filter，所以直接使用它即可
				textFieldMapping.Analyzer = segoAnalyzerName
			} else {
				// 如果需要区分大小写，我们需要一个新的没有 lowercase filter 的分析器
				const segoCaseSensitiveAnalyzerName = "rxdb_sego_case_sensitive"
				err := mapping.AddCustomAnalyzer(segoCaseSensitiveAnalyzerName, map[string]interface{}{
					"type":      custom.Name,
					"tokenizer": segoTokenizerName,
					// 不包含 lowercase filter
				})
				if err == nil {
					textFieldMapping.Analyzer = segoCaseSensitiveAnalyzerName
				} else {
					// 降级使用默认的 sego 分析器（带小写转换）
					textFieldMapping.Analyzer = segoAnalyzerName
				}
			}
		} else if !fts.options.CaseSensitive {
			// 使用自定义分析器，包含小写转换
			err := mapping.AddCustomAnalyzer("rxdb_lowercase", map[string]interface{}{
				"type":      custom.Name,
				"tokenizer": unicode.Name,
				"token_filters": []string{
					lowercase.Name,
				},
			})
			if err == nil {
				textFieldMapping.Analyzer = "rxdb_lowercase"
			}
		}

		// 设置最小长度和停用词（通过自定义分析器）
		if fts.options.MinLength > 0 || len(fts.options.StopWords) > 0 {
			// 注意：bleve 的停用词和最小长度过滤需要自定义实现
			// 这里我们使用默认分析器，在搜索时进行过滤
		}
	}

	mapping.DefaultMapping.AddFieldMappingsAt("_content", textFieldMapping)

	// 启用动态映射以支持元数据过滤
	mapping.DefaultMapping.Dynamic = true

	// 创建索引，显式使用 scorch 存储引擎以优化内存和性能
	index, err := bleve.NewUsing(fts.indexPath, mapping, "scorch", "scorch", nil)
	if err != nil {
		return fmt.Errorf("failed to create bleve index: %w", err)
	}

	fts.index = index
	return nil
}

// buildIndex 构建全文索引。
func (fts *FulltextSearch) buildIndex(ctx context.Context) error {
	fts.mu.Lock()
	defer fts.mu.Unlock()

	// 获取所有文档
	docs, err := fts.collection.All(ctx)
	if err != nil {
		return err
	}

	// 批量索引文档
	batch := fts.index.NewBatch()
	count := 0
	for _, doc := range docs {
		// 将文档转换为可搜索字符串
		text := fts.docToString(doc.Data())
		if text == "" {
			continue
		}

		// 创建 bleve 文档
		bleveDoc := make(map[string]interface{})
		for k, v := range doc.Data() {
			bleveDoc[k] = v
		}
		bleveDoc["_content"] = text

		// 添加到批处理
		if err := batch.Index(doc.ID(), bleveDoc); err != nil {
			return fmt.Errorf("failed to index document %s: %w", doc.ID(), err)
		}

		count++
		if count >= fts.batchSize {
			// 提交批处理
			if err := fts.index.Batch(batch); err != nil {
				return fmt.Errorf("failed to batch index: %w", err)
			}
			batch = fts.index.NewBatch()
			count = 0
		}
	}

	// 提交剩余的文档
	if count > 0 {
		if err := fts.index.Batch(batch); err != nil {
			return fmt.Errorf("failed to batch index: %w", err)
		}
	}

	return nil
}

// watchChanges 监听集合变更并更新索引。
func (fts *FulltextSearch) watchChanges() {
	changes := fts.collection.Changes()
	for {
		select {
		case <-fts.closeChan:
			return
		case event, ok := <-changes:
			if !ok {
				return
			}
			fts.handleChange(event)
		}
	}
}

// handleChange 处理变更事件。
func (fts *FulltextSearch) handleChange(event ChangeEvent) {
	fts.mu.Lock()
	defer fts.mu.Unlock()

	switch event.Op {
	case OperationInsert, OperationUpdate:
		if event.Doc != nil {
			text := fts.docToString(event.Doc)
			if text != "" {
				bleveDoc := make(map[string]interface{})
				for k, v := range event.Doc {
					bleveDoc[k] = v
				}
				bleveDoc["_content"] = text
				_ = fts.index.Index(event.ID, bleveDoc)
			}
		}
	case OperationDelete:
		_ = fts.index.Delete(event.ID)
	}
}

// ensureInitialized 确保索引已初始化（用于懒加载模式）。
func (fts *FulltextSearch) ensureInitialized(ctx context.Context) error {
	if fts.initialized {
		return nil
	}

	fts.mu.Lock()
	defer fts.mu.Unlock()

	if fts.initialized {
		return nil
	}

	if err := fts.buildIndex(ctx); err != nil {
		return err
	}
	fts.initialized = true
	return nil
}

// Find 执行全文搜索。
// 返回匹配查询字符串的文档列表。
func (fts *FulltextSearch) Find(ctx context.Context, queryStr string, options ...FulltextSearchOptions) ([]Document, error) {
	results, err := fts.FindWithScores(ctx, queryStr, options...)
	if err != nil {
		return nil, err
	}

	docs := make([]Document, len(results))
	for i, r := range results {
		docs[i] = r.Document
	}
	return docs, nil
}

// FindWithScores 执行全文搜索并返回带分数的结果。
func (fts *FulltextSearch) FindWithScores(ctx context.Context, queryStr string, options ...FulltextSearchOptions) ([]FulltextSearchResult, error) {
	// 确保索引已初始化
	if err := fts.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	fts.mu.RLock()
	defer fts.mu.RUnlock()

	// 解析选项
	var opts FulltextSearchOptions
	if len(options) > 0 {
		opts = options[0]
	}

	// 处理查询字符串
	if queryStr == "" {
		return []FulltextSearchResult{}, nil
	}

	// 如果使用 sego 分词，需要手动分词查询字符串，然后使用 TermQuery 精确匹配
	// 这样可以确保查询词与索引中的词完全一致，避免模糊匹配
	var queryTerms []string
	caseSensitive := false
	if fts.options != nil {
		caseSensitive = fts.options.CaseSensitive
	}

	if fts.options != nil && strings.EqualFold(fts.options.Tokenize, "sego") {
		// 使用 sego 分词查询字符串
		segmenter := getSegmenter()
		queryBytes := unsafeS2B(queryStr)
		segments := segmenter.Segment(queryBytes)
		for _, seg := range segments {
			word := queryBytes[seg.Start():seg.End()]
			if len(word) == 0 {
				continue
			}
			wordStr := unsafeB2S(word)
			if !caseSensitive {
				wordStr = strings.ToLower(wordStr)
			}
			// 检查最小长度
			if fts.options.MinLength > 0 && len(wordStr) < fts.options.MinLength {
				continue
			}
			// 检查停用词
			isStopWord := false
			for _, stopWord := range fts.options.StopWords {
				if wordStr == stopWord {
					isStopWord = true
					break
				}
			}
			if !isStopWord {
				queryTerms = append(queryTerms, strings.Clone(wordStr))
			}
		}
	} else {
		// 使用空格分词（适用于英文）
		words := strings.Fields(queryStr)
		for _, word := range words {
			if word == "" {
				continue
			}
			wordStr := word
			if !caseSensitive {
				wordStr = strings.ToLower(wordStr)
			}
			if fts.options != nil {
				// 检查最小长度
				if fts.options.MinLength > 0 && len(wordStr) < fts.options.MinLength {
					continue
				}
				// 检查停用词
				isStopWord := false
				for _, stopWord := range fts.options.StopWords {
					if wordStr == stopWord {
						isStopWord = true
						break
					}
				}
				if isStopWord {
					continue
				}
			}
			queryTerms = append(queryTerms, wordStr)
		}
	}

	if len(queryTerms) == 0 {
		return []FulltextSearchResult{}, nil
	}

	// 创建 bleve 查询
	// 使用 MatchQuery，它会自动使用字段的分析器来分析查询字符串
	// 但我们需要确保查询字符串已经被正确分词，所以使用分词后的词重新组合
	// 这样 MatchQuery 会对每个词进行分析，然后匹配索引中的词
	// 如果索引中的词是"生态系统"，而查询词是"系统"，它们不会匹配（因为"生态系统"是一个完整的词）
	queryString := strings.Join(queryTerms, " ")
	mq := bleve.NewMatchQuery(queryString)
	mq.SetField("_content")
	var bleveQuery query.Query = mq

	// 如果有选择器，合并查询
	if len(opts.Selector) > 0 {
		filterQuery := selectorToBleveQuery(opts.Selector)
		bleveQuery = bleve.NewConjunctionQuery(bleveQuery, filterQuery)
	}

	// 创建搜索请求
	searchRequest := bleve.NewSearchRequest(bleveQuery)
	if opts.Limit > 0 {
		searchRequest.Size = opts.Limit
	} else {
		searchRequest.Size = 10 // 默认限制
	}

	// 执行搜索
	searchResult, err := fts.index.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("bleve search failed: %w", err)
	}

	// 转换结果
	var results []FulltextSearchResult
	for _, hit := range searchResult.Hits {
		// 应用阈值过滤
		if opts.Threshold > 0 {
			// bleve 的分数范围可能不同，需要归一化
			normalizedScore := hit.Score / searchResult.MaxScore
			if normalizedScore < opts.Threshold {
				continue
			}
		}

		// 获取文档
		doc, err := fts.collection.FindByID(ctx, hit.ID)
		if err != nil {
			continue
		}

		// 归一化分数到 0-1 范围（如果 MaxScore > 0）
		score := hit.Score
		if searchResult.MaxScore > 0 {
			score = hit.Score / searchResult.MaxScore
		}

		results = append(results, FulltextSearchResult{
			Document: doc,
			Score:    score,
		})
	}

	return results, nil
}

// Reindex 重建全文索引。
func (fts *FulltextSearch) Reindex(ctx context.Context) error {
	// 先关闭并重建索引，最后再重建数据，避免自旋死锁
	fts.mu.Lock()

	// 关闭旧索引
	if fts.index != nil {
		_ = fts.index.Close()
	}

	// 删除索引目录
	if err := os.RemoveAll(fts.indexPath); err != nil {
		fts.mu.Unlock()
		return fmt.Errorf("failed to remove index directory: %w", err)
	}

	// 重新创建索引
	if err := fts.openOrCreateIndex(); err != nil {
		fts.mu.Unlock()
		return fmt.Errorf("failed to recreate index: %w", err)
	}

	fts.mu.Unlock()

	// rebuild 时会再次获取 mu，避免在持锁状态下调用
	if err := fts.buildIndex(ctx); err != nil {
		return err
	}

	fts.mu.Lock()
	fts.initialized = true
	fts.mu.Unlock()

	return nil
}

// Close 关闭全文搜索实例。
func (fts *FulltextSearch) Close() {
	close(fts.closeChan)
	fts.mu.Lock()
	defer fts.mu.Unlock()
	if fts.index != nil {
		_ = fts.index.Close()
	}
}

// Count 返回已索引的文档数量。
func (fts *FulltextSearch) Count() int {
	fts.mu.RLock()
	defer fts.mu.RUnlock()
	if fts.index == nil {
		return 0
	}
	docCount, _ := fts.index.DocCount()
	return int(docCount)
}

// Persist 持久化索引到存储。
// bleve 索引会自动持久化，此方法主要用于兼容性。
func (fts *FulltextSearch) Persist(ctx context.Context) error {
	// bleve 索引已经持久化到磁盘，无需额外操作
	return nil
}

// Load 从存储加载持久化的索引。
// bleve 索引在打开时自动加载，此方法主要用于兼容性。
func (fts *FulltextSearch) Load(ctx context.Context) error {
	// bleve 索引在 openOrCreateIndex 时已经加载
	fts.initialized = true
	return nil
}

// selectorToBleveQuery 将 Mango 选择器转换为 Bleve 查询。
func selectorToBleveQuery(selector map[string]any) query.Query {
	if len(selector) == 0 {
		return bleve.NewMatchAllQuery()
	}

	var mustQueries []query.Query

	for key, value := range selector {
		if strings.HasPrefix(key, "$") {
			switch key {
			case "$and":
				if arr, ok := value.([]any); ok {
					var subQueries []query.Query
					for _, item := range arr {
						if m, ok := item.(map[string]any); ok {
							subQueries = append(subQueries, selectorToBleveQuery(m))
						}
					}
					mustQueries = append(mustQueries, bleve.NewConjunctionQuery(subQueries...))
				}
			case "$or":
				if arr, ok := value.([]any); ok {
					var subQueries []query.Query
					for _, item := range arr {
						if m, ok := item.(map[string]any); ok {
							subQueries = append(subQueries, selectorToBleveQuery(m))
						}
					}
					mustQueries = append(mustQueries, bleve.NewDisjunctionQuery(subQueries...))
				}
			}
			continue
		}

		// 字段匹配
		if ops, ok := value.(map[string]any); ok {
			for op, opVal := range ops {
				switch op {
				case "$eq":
					mustQueries = append(mustQueries, bleve.NewTermQuery(fmt.Sprintf("%v", opVal)))
				case "$in":
					if arr, ok := opVal.([]any); ok {
						var subQueries []query.Query
						for _, item := range arr {
							subQueries = append(subQueries, bleve.NewTermQuery(fmt.Sprintf("%v", item)))
						}
						mustQueries = append(mustQueries, bleve.NewDisjunctionQuery(subQueries...))
					} else if arr, ok := opVal.([]string); ok {
						var subQueries []query.Query
						for _, item := range arr {
							subQueries = append(subQueries, bleve.NewTermQuery(item))
						}
						mustQueries = append(mustQueries, bleve.NewDisjunctionQuery(subQueries...))
					}
				case "$nin":
					if arr, ok := opVal.([]any); ok {
						var subQueries []query.Query
						for _, item := range arr {
							subQueries = append(subQueries, bleve.NewTermQuery(fmt.Sprintf("%v", item)))
						}
						dq := bleve.NewDisjunctionQuery(subQueries...)
						bq := bleve.NewBooleanQuery()
						bq.AddMustNot(dq)
						mustQueries = append(mustQueries, bq)
					}
				case "$gt":
					min := toFloat64(opVal)
					q := bleve.NewNumericRangeQuery(&min, nil)
					falseVal := false
					q.InclusiveMin = &falseVal
					mustQueries = append(mustQueries, q)
				case "$gte":
					min := toFloat64(opVal)
					q := bleve.NewNumericRangeQuery(&min, nil)
					trueVal := true
					q.InclusiveMin = &trueVal
					mustQueries = append(mustQueries, q)
				case "$lt":
					max := toFloat64(opVal)
					q := bleve.NewNumericRangeQuery(nil, &max)
					falseVal := false
					q.InclusiveMax = &falseVal
					mustQueries = append(mustQueries, q)
				case "$lte":
					max := toFloat64(opVal)
					q := bleve.NewNumericRangeQuery(nil, &max)
					trueVal := true
					q.InclusiveMax = &trueVal
					mustQueries = append(mustQueries, q)
				}
			}
		} else {
			// 直接相等
			mustQueries = append(mustQueries, bleve.NewTermQuery(fmt.Sprintf("%v", value)))
		}
	}

	if len(mustQueries) == 0 {
		return bleve.NewMatchAllQuery()
	}
	if len(mustQueries) == 1 {
		return mustQueries[0]
	}
	return bleve.NewConjunctionQuery(mustQueries...)
}
