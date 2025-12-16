package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"
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
}

// FulltextSearch 全文搜索实例。
// 参考 RxDB 的 RxFulltextSearch。
type FulltextSearch struct {
	identifier  string
	collection  *collection
	docToString func(doc map[string]any) string
	options     *FulltextIndexOptions
	index       *fulltextIndex
	mu          sync.RWMutex
	initialized bool
	initMode    string
	batchSize   int
	closeChan   chan struct{}
}

// fulltextIndex 内部全文索引实现。
type fulltextIndex struct {
	// terms 存储词项到文档ID和位置的映射。
	// 格式：term -> docID -> positions
	terms map[string]map[string][]int
	// docTerms 存储文档ID到词项的映射（用于删除时快速查找）。
	docTerms map[string][]string
	// docCount 索引中的文档数量。
	docCount int
}

// newFulltextIndex 创建新的全文索引。
func newFulltextIndex() *fulltextIndex {
	return &fulltextIndex{
		terms:    make(map[string]map[string][]int),
		docTerms: make(map[string][]string),
	}
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

	fts := &FulltextSearch{
		identifier:  config.Identifier,
		collection:  col,
		docToString: config.DocToString,
		options:     config.IndexOptions,
		index:       newFulltextIndex(),
		initMode:    initMode,
		batchSize:   batchSize,
		closeChan:   make(chan struct{}),
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
	for _, doc := range docs {
		fts.indexDocumentLocked(doc.ID(), doc.Data())
	}

	return nil
}

// indexDocumentLocked 索引单个文档（需要已持有锁）。
func (fts *FulltextSearch) indexDocumentLocked(docID string, data map[string]any) {
	// 将文档转换为字符串
	text := fts.docToString(data)
	if text == "" {
		return
	}

	// 分词
	tokens := fts.tokenize(text)

	// 移除旧的索引（如果存在）
	fts.removeDocumentLocked(docID)

	// 添加新的索引
	docTerms := make([]string, 0, len(tokens))
	for pos, token := range tokens {
		if _, exists := fts.index.terms[token]; !exists {
			fts.index.terms[token] = make(map[string][]int)
		}
		fts.index.terms[token][docID] = append(fts.index.terms[token][docID], pos)
		docTerms = append(docTerms, token)
	}
	fts.index.docTerms[docID] = docTerms
	fts.index.docCount++
}

// removeDocumentLocked 从索引中移除文档（需要已持有锁）。
func (fts *FulltextSearch) removeDocumentLocked(docID string) {
	terms, exists := fts.index.docTerms[docID]
	if !exists {
		return
	}

	for _, term := range terms {
		if docMap, ok := fts.index.terms[term]; ok {
			delete(docMap, docID)
			if len(docMap) == 0 {
				delete(fts.index.terms, term)
			}
		}
	}
	delete(fts.index.docTerms, docID)
	fts.index.docCount--
}

// isCJK 检查字符是否为中日韩字符。
func isCJK(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul)
}

// generateNGrams 为中文文本生成 n-gram 分词。
// 例如 "并发编程" 会生成 ["并发", "发编", "编程", "并发编", "发编程", "并发编程"]
func generateNGrams(text string, minLen, maxLen int) []string {
	if minLen <= 0 {
		minLen = 1
	}
	if maxLen <= 0 {
		maxLen = len([]rune(text))
	}
	if maxLen > len([]rune(text)) {
		maxLen = len([]rune(text))
	}

	var ngrams []string
	runes := []rune(text)

	// 生成所有长度的 n-gram
	for n := minLen; n <= maxLen && n <= len(runes); n++ {
		for i := 0; i <= len(runes)-n; i++ {
			ngram := string(runes[i : i+n])
			ngrams = append(ngrams, ngram)
		}
	}

	return ngrams
}

// tokenize 对文本进行分词。
func (fts *FulltextSearch) tokenize(text string) []string {
	// 转为小写（如果不区分大小写）
	if fts.options == nil || !fts.options.CaseSensitive {
		text = strings.ToLower(text)
	}

	var tokens []string
	var current strings.Builder
	var cjkSegment strings.Builder // 用于收集连续的中日韩字符

	// 处理文本，分离中文和英文/数字
	for _, r := range text {
		if isCJK(r) {
			// 如果之前有非中文内容，先处理它
			if current.Len() > 0 {
				token := current.String()
				if fts.isValidToken(token) {
					tokens = append(tokens, token)
				}
				current.Reset()
			}
			cjkSegment.WriteRune(r)
		} else if unicode.IsLetter(r) || unicode.IsNumber(r) {
			// 如果之前有中文内容，先处理它
			if cjkSegment.Len() > 0 {
				cjkText := cjkSegment.String()
				// 对中文生成 n-gram（最小2个字符，最大不超过原长度）
				minLen := 2
				if fts.options != nil && fts.options.MinLength > 0 {
					minLen = fts.options.MinLength
				}
				ngrams := generateNGrams(cjkText, minLen, len([]rune(cjkText)))
				for _, ngram := range ngrams {
					if fts.isValidToken(ngram) {
						tokens = append(tokens, ngram)
					}
				}
				cjkSegment.Reset()
			}
			current.WriteRune(r)
		} else {
			// 分隔符：处理之前积累的内容
			if current.Len() > 0 {
				token := current.String()
				if fts.isValidToken(token) {
					tokens = append(tokens, token)
				}
				current.Reset()
			}
			if cjkSegment.Len() > 0 {
				cjkText := cjkSegment.String()
				minLen := 2
				if fts.options != nil && fts.options.MinLength > 0 {
					minLen = fts.options.MinLength
				}
				ngrams := generateNGrams(cjkText, minLen, len([]rune(cjkText)))
				for _, ngram := range ngrams {
					if fts.isValidToken(ngram) {
						tokens = append(tokens, ngram)
					}
				}
				cjkSegment.Reset()
			}
		}
	}

	// 处理最后剩余的内容
	if current.Len() > 0 {
		token := current.String()
		if fts.isValidToken(token) {
			tokens = append(tokens, token)
		}
	}
	if cjkSegment.Len() > 0 {
		cjkText := cjkSegment.String()
		minLen := 2
		if fts.options != nil && fts.options.MinLength > 0 {
			minLen = fts.options.MinLength
		}
		ngrams := generateNGrams(cjkText, minLen, len([]rune(cjkText)))
		for _, ngram := range ngrams {
			if fts.isValidToken(ngram) {
				tokens = append(tokens, ngram)
			}
		}
	}

	return tokens
}

// isValidToken 检查词项是否有效。
func (fts *FulltextSearch) isValidToken(token string) bool {
	if fts.options != nil {
		// 检查最小长度
		if fts.options.MinLength > 0 && len(token) < fts.options.MinLength {
			return false
		}
		// 检查停用词
		for _, stopWord := range fts.options.StopWords {
			if token == stopWord {
				return false
			}
		}
	}
	return true
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
			fts.indexDocumentLocked(event.ID, event.Doc)
		}
	case OperationDelete:
		fts.removeDocumentLocked(event.ID)
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
func (fts *FulltextSearch) Find(ctx context.Context, query string, options ...FulltextSearchOptions) ([]Document, error) {
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

	// 分词查询字符串
	queryTokens := fts.tokenize(query)
	if len(queryTokens) == 0 {
		return []Document{}, nil
	}

	// 计算每个文档的分数
	scores := make(map[string]float64)
	for _, token := range queryTokens {
		if docMap, exists := fts.index.terms[token]; exists {
			// TF-IDF 简化版本
			idf := 1.0
			if fts.index.docCount > 0 {
				idf = 1.0 + float64(fts.index.docCount)/float64(len(docMap)+1)
			}
			for docID, positions := range docMap {
				tf := float64(len(positions))
				scores[docID] += tf * idf
			}
		}
	}

	// 前缀匹配支持
	if fts.options != nil && (fts.options.Tokenize == "forward" || fts.options.Tokenize == "full") {
		for _, token := range queryTokens {
			for term, docMap := range fts.index.terms {
				if strings.HasPrefix(term, token) && term != token {
					idf := 1.0
					if fts.index.docCount > 0 {
						idf = 1.0 + float64(fts.index.docCount)/float64(len(docMap)+1)
					}
					for docID, positions := range docMap {
						tf := float64(len(positions)) * 0.5 // 前缀匹配权重较低
						scores[docID] += tf * idf
					}
				}
			}
		}
	}

	// 按分数排序
	type scoredDoc struct {
		docID string
		score float64
	}
	var sortedDocs []scoredDoc
	for docID, score := range scores {
		// 应用阈值过滤
		if opts.Threshold > 0 && score < opts.Threshold {
			continue
		}
		sortedDocs = append(sortedDocs, scoredDoc{docID, score})
	}

	sort.Slice(sortedDocs, func(i, j int) bool {
		return sortedDocs[i].score > sortedDocs[j].score
	})

	// 应用限制
	if opts.Limit > 0 && len(sortedDocs) > opts.Limit {
		sortedDocs = sortedDocs[:opts.Limit]
	}

	// 获取文档
	var results []Document
	for _, sd := range sortedDocs {
		doc, err := fts.collection.FindByID(ctx, sd.docID)
		if err != nil {
			continue
		}
		results = append(results, doc)
	}

	return results, nil
}

// FindWithScores 执行全文搜索并返回带分数的结果。
func (fts *FulltextSearch) FindWithScores(ctx context.Context, query string, options ...FulltextSearchOptions) ([]FulltextSearchResult, error) {
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

	// 分词查询字符串
	queryTokens := fts.tokenize(query)
	if len(queryTokens) == 0 {
		return []FulltextSearchResult{}, nil
	}

	// 计算每个文档的分数
	scores := make(map[string]float64)
	for _, token := range queryTokens {
		if docMap, exists := fts.index.terms[token]; exists {
			idf := 1.0
			if fts.index.docCount > 0 {
				idf = 1.0 + float64(fts.index.docCount)/float64(len(docMap)+1)
			}
			for docID, positions := range docMap {
				tf := float64(len(positions))
				scores[docID] += tf * idf
			}
		}
	}

	// 按分数排序
	type scoredDoc struct {
		docID string
		score float64
	}
	var sortedDocs []scoredDoc
	for docID, score := range scores {
		if opts.Threshold > 0 && score < opts.Threshold {
			continue
		}
		sortedDocs = append(sortedDocs, scoredDoc{docID, score})
	}

	sort.Slice(sortedDocs, func(i, j int) bool {
		return sortedDocs[i].score > sortedDocs[j].score
	})

	if opts.Limit > 0 && len(sortedDocs) > opts.Limit {
		sortedDocs = sortedDocs[:opts.Limit]
	}

	// 获取文档
	var results []FulltextSearchResult
	for _, sd := range sortedDocs {
		doc, err := fts.collection.FindByID(ctx, sd.docID)
		if err != nil {
			continue
		}
		results = append(results, FulltextSearchResult{
			Document: doc,
			Score:    sd.score,
		})
	}

	return results, nil
}

// Reindex 重建全文索引。
func (fts *FulltextSearch) Reindex(ctx context.Context) error {
	fts.mu.Lock()
	// 清空索引
	fts.index = newFulltextIndex()
	fts.mu.Unlock()

	return fts.buildIndex(ctx)
}

// Close 关闭全文搜索实例。
func (fts *FulltextSearch) Close() {
	close(fts.closeChan)
}

// Count 返回已索引的文档数量。
func (fts *FulltextSearch) Count() int {
	fts.mu.RLock()
	defer fts.mu.RUnlock()
	return fts.index.docCount
}

// Persist 持久化索引到存储。
func (fts *FulltextSearch) Persist(ctx context.Context) error {
	fts.mu.RLock()
	defer fts.mu.RUnlock()

	// 序列化索引
	indexData := struct {
		Terms    map[string]map[string][]int `json:"terms"`
		DocTerms map[string][]string         `json:"doc_terms"`
		DocCount int                         `json:"doc_count"`
	}{
		Terms:    fts.index.terms,
		DocTerms: fts.index.docTerms,
		DocCount: fts.index.docCount,
	}

	data, err := json.Marshal(indexData)
	if err != nil {
		return fmt.Errorf("failed to marshal fulltext index: %w", err)
	}

	// 存储到集合的元数据
	bucket := fmt.Sprintf("%s_fulltext", fts.collection.name)
	return fts.collection.store.Set(ctx, bucket, fts.identifier, data)
}

// Load 从存储加载持久化的索引。
func (fts *FulltextSearch) Load(ctx context.Context) error {
	fts.mu.Lock()
	defer fts.mu.Unlock()

	bucket := fmt.Sprintf("%s_fulltext", fts.collection.name)
	data, err := fts.collection.store.Get(ctx, bucket, fts.identifier)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // 没有持久化的索引
	}

	// 反序列化索引
	var indexData struct {
		Terms    map[string]map[string][]int `json:"terms"`
		DocTerms map[string][]string         `json:"doc_terms"`
		DocCount int                         `json:"doc_count"`
	}

	if err := json.Unmarshal(data, &indexData); err != nil {
		return fmt.Errorf("failed to unmarshal fulltext index: %w", err)
	}

	fts.index.terms = indexData.Terms
	fts.index.docTerms = indexData.DocTerms
	fts.index.docCount = indexData.DocCount
	fts.initialized = true

	return nil
}
