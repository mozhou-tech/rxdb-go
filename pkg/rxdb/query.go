package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	bstore "github.com/mozy/rxdb-go/pkg/storage/badger"
)

// Query 提供与 RxDB 兼容的查询 API。
// 支持 Mango Query 语法的子集。
type Query struct {
	collection *collection
	selector   map[string]any
	sortFields []SortField
	skip       int
	limit      int
}

// SortField 排序字段定义。
type SortField struct {
	Field string
	Desc  bool
}

// NewQuery 创建新的查询实例。
func (c *collection) Find(selector map[string]any) *Query {
	return &Query{
		collection: c,
		selector:   selector,
		limit:      -1,
	}
}

// FindOne 便捷方法：执行查询并返回首个匹配文档。
func (c *collection) FindOne(ctx context.Context, selector map[string]any) (Document, error) {
	return c.Find(selector).FindOne(ctx)
}

// Sort 设置排序。
// 参数格式: {"field": "asc"} 或 {"field": "desc"}
func (q *Query) Sort(sortDef map[string]string) *Query {
	for field, order := range sortDef {
		q.sortFields = append(q.sortFields, SortField{
			Field: field,
			Desc:  strings.ToLower(order) == "desc",
		})
	}
	return q
}

// Skip 设置跳过的文档数。
func (q *Query) Skip(n int) *Query {
	q.skip = n
	return q
}

// Limit 设置返回的最大文档数。
func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

// Where 开始链式查询构建，等同于 Find()。
func (c *collection) Where(field string) *Query {
	return &Query{
		collection: c,
		selector:   make(map[string]any),
		limit:      -1,
	}
}

// Equals 添加等于条件。
func (q *Query) Equals(field string, value any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	q.selector[field] = value
	return q
}

// Gt 添加大于条件。
func (q *Query) Gt(field string, value any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$gt"] = value
	return q
}

// Gte 添加大于等于条件。
func (q *Query) Gte(field string, value any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$gte"] = value
	return q
}

// Lt 添加小于条件。
func (q *Query) Lt(field string, value any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$lt"] = value
	return q
}

// Lte 添加小于等于条件。
func (q *Query) Lte(field string, value any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$lte"] = value
	return q
}

// In 添加在数组中的条件。
func (q *Query) In(field string, values []any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$in"] = values
	return q
}

// Nin 添加不在数组中的条件。
func (q *Query) Nin(field string, values []any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$nin"] = values
	return q
}

// Exists 添加字段存在条件。
func (q *Query) Exists(field string, exists bool) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$exists"] = exists
	return q
}

// Type 添加类型匹配条件。
func (q *Query) Type(field string, typeStr string) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$type"] = typeStr
	return q
}

// Regex 添加正则匹配条件。
func (q *Query) Regex(field string, pattern string) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	if _, ok := q.selector[field].(map[string]any); !ok {
		q.selector[field] = make(map[string]any)
	}
	q.selector[field].(map[string]any)["$regex"] = pattern
	return q
}

// Or 添加或条件。
func (q *Query) Or(conditions []map[string]any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	q.selector["$or"] = convertToAnySlice(conditions)
	return q
}

// And 添加与条件。
func (q *Query) And(conditions []map[string]any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	q.selector["$and"] = convertToAnySlice(conditions)
	return q
}

// Not 添加非条件。
func (q *Query) Not(condition map[string]any) *Query {
	if q.selector == nil {
		q.selector = make(map[string]any)
	}
	q.selector["$not"] = condition
	return q
}

// convertToAnySlice 将 []map[string]any 转换为 []any。
func convertToAnySlice(slice []map[string]any) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// tryUseIndex 尝试使用索引优化查询，返回匹配的文档ID列表和是否使用了索引。
func (q *Query) tryUseIndex(ctx context.Context) ([]string, bool) {
	if len(q.selector) == 0 {
		return nil, false
	}

	// 查找最佳索引
	bestIndex := q.findBestIndex()
	if bestIndex == nil {
		return nil, false
	}

	// 从索引中获取文档ID
	var docIDs []string
	indexName := bestIndex.Name
	if indexName == "" {
		indexName = strings.Join(bestIndex.Fields, "_")
	}
	bucketName := fmt.Sprintf("%s_idx_%s", q.collection.name, indexName)

	// 构建索引键
	indexKeyParts := make([]interface{}, 0, len(bestIndex.Fields))
	for _, field := range bestIndex.Fields {
		value := q.getSelectorValue(field)
		if value == nil {
			// 如果索引字段在查询中不存在，无法使用索引
			return nil, false
		}
		indexKeyParts = append(indexKeyParts, value)
	}

	indexKeyBytes, err := json.Marshal(indexKeyParts)
	if err != nil {
		return nil, false
	}
	indexKey := string(indexKeyBytes)

	// 从索引中获取文档ID列表
	data, err := q.collection.store.Get(ctx, bucketName, indexKey)
	if err != nil || data == nil {
		return nil, false
	}
	if err := json.Unmarshal(data, &docIDs); err != nil {
		return nil, false
	}

	if len(docIDs) == 0 {
		return nil, false
	}

	return docIDs, true
}

// findBestIndex 查找最适合当前查询的索引。
// 优先选择：
// 1. 所有字段都匹配的索引
// 2. 前缀匹配的索引（复合索引的前几个字段）
// 3. 字段数量最多的匹配索引
func (q *Query) findBestIndex() *Index {
	if len(q.selector) == 0 {
		return nil
	}

	// 提取查询中的字段列表（排除逻辑操作符）
	queryFields := q.extractQueryFields()
	if len(queryFields) == 0 {
		return nil
	}

	var bestIndex *Index
	maxMatchCount := 0

	for _, idx := range q.collection.schema.Indexes {
		matchCount := q.countIndexMatches(idx, queryFields)
		if matchCount > 0 && matchCount > maxMatchCount {
			// 检查是否所有索引字段都在查询中（完全匹配）
			if matchCount == len(idx.Fields) {
				bestIndex = &idx
				return bestIndex // 完全匹配的索引是最优的
			}
			// 前缀匹配（复合索引的前几个字段）
			if matchCount == len(queryFields) && matchCount <= len(idx.Fields) {
				// 检查是否是前缀匹配
				isPrefixMatch := true
				for i := 0; i < matchCount; i++ {
					if idx.Fields[i] != queryFields[i] {
						isPrefixMatch = false
						break
					}
				}
				if isPrefixMatch {
					bestIndex = &idx
					maxMatchCount = matchCount
				}
			}
		}
	}

	return bestIndex
}

// extractQueryFields 从查询选择器中提取字段列表（排除逻辑操作符）。
func (q *Query) extractQueryFields() []string {
	fields := make([]string, 0)
	if q.selector == nil {
		return fields
	}

	for key := range q.selector {
		if key != "$and" && key != "$or" && key != "$not" && key != "$nor" {
			fields = append(fields, key)
		}
	}

	return fields
}

// countIndexMatches 计算索引字段在查询中的匹配数量。
func (q *Query) countIndexMatches(idx Index, queryFields []string) int {
	count := 0
	queryFieldMap := make(map[string]bool)
	for _, f := range queryFields {
		queryFieldMap[f] = true
	}

	for _, field := range idx.Fields {
		if queryFieldMap[field] {
			count++
		} else {
			// 如果是复合索引，一旦遇到不匹配的字段就停止
			break
		}
	}

	return count
}

// getSelectorValue 从查询选择器中获取字段的值（支持简单相等查询）。
func (q *Query) getSelectorValue(field string) interface{} {
	if q.selector == nil {
		return nil
	}

	value, ok := q.selector[field]
	if !ok {
		return nil
	}

	// 如果是 map，可能是操作符（$eq, $gt 等）
	if opMap, ok := value.(map[string]any); ok {
		// 只支持 $eq 操作符用于索引
		if eqValue, ok := opMap["$eq"]; ok {
			return eqValue
		}
		// 不支持其他操作符用于索引查找
		return nil
	}

	// 直接值
	return value
}

// Exec 执行查询并返回结果。
func (q *Query) Exec(ctx context.Context) ([]Document, error) {
	logger := GetLogger()
	logger.Debug("Executing query: collection=%s", q.collection.name)

	q.collection.mu.RLock()
	defer q.collection.mu.RUnlock()

	if q.collection.closed {
		return nil, NewError(ErrorTypeClosed, "collection is closed", nil)
	}

	var results []map[string]any

	// 尝试使用索引优化查询
	indexedDocIDs, useIndex := q.tryUseIndex(ctx)
	if useIndex {
		logger.Debug("Query using index: collection=%s, indexedDocs=%d", q.collection.name, len(indexedDocIDs))
	} else {
		logger.Debug("Query using full scan: collection=%s", q.collection.name)
	}

	if useIndex && len(indexedDocIDs) > 0 {
		// 使用索引：只加载匹配的文档
		for _, docID := range indexedDocIDs {
			data, err := q.collection.store.Get(ctx, q.collection.name, docID)
			if err != nil || data == nil {
				continue
			}
			var doc map[string]any
			if err := json.Unmarshal(data, &doc); err != nil {
				continue
			}
			// 解密需要解密的字段
			if len(q.collection.schema.EncryptedFields) > 0 && q.collection.password != "" {
				if err := decryptDocumentFields(doc, q.collection.schema.EncryptedFields, q.collection.password); err != nil {
					// 解密失败时，继续处理文档
				}
			}
			// 仍然需要匹配，因为索引可能只覆盖部分查询条件
			if q.match(doc) {
				results = append(results, doc)
			}
		}
	} else {
		// 回退到全表扫描
		err := q.collection.store.Iterate(ctx, q.collection.name, func(k, v []byte) error {
			var doc map[string]any
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			// 解密需要解密的字段（在匹配前解密，以便查询可以正常工作）
			if len(q.collection.schema.EncryptedFields) > 0 && q.collection.password != "" {
				if err := decryptDocumentFields(doc, q.collection.schema.EncryptedFields, q.collection.password); err != nil {
					// 解密失败时，继续处理文档
				}
			}
			if q.match(doc) {
				results = append(results, doc)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// 排序
	if len(q.sortFields) > 0 {
		q.sortResults(results)
	}

	// Skip
	if q.skip > 0 && q.skip < len(results) {
		results = results[q.skip:]
	} else if q.skip >= len(results) {
		results = nil
	}

	// Limit
	if q.limit >= 0 && q.limit < len(results) {
		results = results[:q.limit]
	}

	// 转换为 Document
	docs := make([]Document, len(results))
	for i, r := range results {
		id, err := q.collection.extractPrimaryKey(r)
		if err != nil {
			return nil, fmt.Errorf("failed to extract primary key: %w", err)
		}
		docs[i] = &document{
			id:         id,
			data:       r,
			collection: q.collection,
			revField:   q.collection.schema.RevField,
		}
	}

	return docs, nil
}

// FindOne 执行查询并返回第一个结果。
func (q *Query) FindOne(ctx context.Context) (Document, error) {
	q.limit = 1
	docs, err := q.Exec(ctx)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

// Count 返回匹配的文档数量。
func (q *Query) Count(ctx context.Context) (int, error) {
	q.collection.mu.RLock()
	defer q.collection.mu.RUnlock()

	if q.collection.closed {
		return 0, fmt.Errorf("collection is closed")
	}

	var count int

	// 尝试使用索引优化查询
	indexedDocIDs, useIndex := q.tryUseIndex(ctx)

	if useIndex && len(indexedDocIDs) > 0 {
		// 使用索引：只检查匹配的文档
		for _, docID := range indexedDocIDs {
			data, err := q.collection.store.Get(ctx, q.collection.name, docID)
			if err != nil || data == nil {
				continue
			}
			var doc map[string]any
			if err := json.Unmarshal(data, &doc); err != nil {
				continue
			}
			// 解密需要解密的字段
			if len(q.collection.schema.EncryptedFields) > 0 && q.collection.password != "" {
				if err := decryptDocumentFields(doc, q.collection.schema.EncryptedFields, q.collection.password); err != nil {
					// 解密失败时，继续处理文档
				}
			}
			// 仍然需要匹配，因为索引可能只覆盖部分查询条件
			if q.match(doc) {
				count++
			}
		}
	} else {
		// 回退到全表扫描
		err := q.collection.store.Iterate(ctx, q.collection.name, func(k, v []byte) error {
			var doc map[string]any
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			// 解密需要解密的字段（在匹配前解密，以便查询可以正常工作）
			if len(q.collection.schema.EncryptedFields) > 0 && q.collection.password != "" {
				if err := decryptDocumentFields(doc, q.collection.schema.EncryptedFields, q.collection.password); err != nil {
					// 解密失败时，继续处理文档
				}
			}
			if q.match(doc) {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// match 检查文档是否匹配选择器。
// 支持 RxDB/Mango 查询操作符的子集。
func (q *Query) match(doc map[string]any) bool {
	if len(q.selector) == 0 {
		return true
	}
	return matchSelector(doc, q.selector)
}

func matchSelector(doc map[string]any, selector map[string]any) bool {
	for key, value := range selector {
		switch key {
		case "$and":
			if conditions, ok := value.([]any); ok {
				for _, cond := range conditions {
					if condMap, ok := cond.(map[string]any); ok {
						if !matchSelector(doc, condMap) {
							return false
						}
					}
				}
			}
		case "$or":
			if conditions, ok := value.([]any); ok {
				matched := false
				for _, cond := range conditions {
					if condMap, ok := cond.(map[string]any); ok {
						if matchSelector(doc, condMap) {
							matched = true
							break
						}
					}
				}
				if !matched {
					return false
				}
			}
		case "$not":
			if condMap, ok := value.(map[string]any); ok {
				if matchSelector(doc, condMap) {
					return false
				}
			}
		case "$nor":
			if conditions, ok := value.([]any); ok {
				for _, cond := range conditions {
					if condMap, ok := cond.(map[string]any); ok {
						if matchSelector(doc, condMap) {
							return false
						}
					}
				}
			}
		default:
			// 字段匹配
			docValue := getNestedValue(doc, key)
			fieldExists := fieldExistsInDoc(doc, key)
			if !matchFieldWithExistence(docValue, value, fieldExists) {
				return false
			}
		}
	}
	return true
}

// fieldExistsInDoc 检查字段是否存在于文档中（即使值为 nil）
func fieldExistsInDoc(doc map[string]any, path string) bool {
	parts := strings.Split(path, ".")
	current := doc
	for i, part := range parts {
		if current == nil {
			return false
		}
		if i == len(parts)-1 {
			_, exists := current[part]
			return exists
		}
		if next, ok := current[part]; ok {
			if nextMap, ok := next.(map[string]any); ok {
				current = nextMap
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return false
}

func matchField(docValue, selectorValue any) bool {
	return matchFieldWithExistence(docValue, selectorValue, true)
}

func matchFieldWithExistence(docValue, selectorValue any, fieldExists bool) bool {
	// 如果选择器值是 map，则包含操作符
	if ops, ok := selectorValue.(map[string]any); ok {
		for op, opValue := range ops {
			if !matchOperatorWithExistence(docValue, op, opValue, fieldExists) {
				return false
			}
		}
		return true
	}

	// 直接相等比较
	return compareEqual(docValue, selectorValue)
}

func matchOperator(docValue any, op string, opValue any) bool {
	return matchOperatorWithExistence(docValue, op, opValue, true)
}

func matchOperatorWithExistence(docValue any, op string, opValue any, fieldExists bool) bool {
	switch op {
	case "$eq":
		return compareEqual(docValue, opValue)
	case "$ne":
		return !compareEqual(docValue, opValue)
	case "$gt":
		return compareGreater(docValue, opValue)
	case "$gte":
		return compareGreater(docValue, opValue) || compareEqual(docValue, opValue)
	case "$lt":
		return compareLess(docValue, opValue)
	case "$lte":
		return compareLess(docValue, opValue) || compareEqual(docValue, opValue)
	case "$in":
		if arr, ok := opValue.([]any); ok {
			for _, v := range arr {
				if compareEqual(docValue, v) {
					return true
				}
			}
		}
		return false
	case "$nin":
		if arr, ok := opValue.([]any); ok {
			for _, v := range arr {
				if compareEqual(docValue, v) {
					return false
				}
			}
		}
		return true
	case "$exists":
		// 使用 fieldExists 参数来判断字段是否存在（区分字段不存在和值为 nil）
		if b, ok := opValue.(bool); ok {
			return fieldExists == b
		}
		return fieldExists
	case "$type":
		return matchType(docValue, opValue)
	case "$regex":
		if pattern, ok := opValue.(string); ok {
			if s, ok := docValue.(string); ok {
				re, err := regexp.Compile(pattern)
				if err != nil {
					return false
				}
				return re.MatchString(s)
			}
		}
		return false
	case "$elemMatch":
		if arr, ok := docValue.([]any); ok {
			if criteria, ok := opValue.(map[string]any); ok {
				for _, elem := range arr {
					if elemMap, ok := elem.(map[string]any); ok {
						if matchSelector(elemMap, criteria) {
							return true
						}
					}
				}
			}
		}
		return false
	case "$size":
		if arr, ok := docValue.([]any); ok {
			size := len(arr)
			switch v := opValue.(type) {
			case int:
				return size == v
			case float64:
				return size == int(v)
			}
		}
		return false
	case "$all":
		if arr, ok := docValue.([]any); ok {
			if required, ok := opValue.([]any); ok {
				for _, req := range required {
					found := false
					for _, elem := range arr {
						if compareEqual(elem, req) {
							found = true
							break
						}
					}
					if !found {
						return false
					}
				}
				return true
			}
		}
		return false
	case "$mod":
		if modArr, ok := opValue.([]any); ok && len(modArr) == 2 {
			divisor := toFloat64(modArr[0])
			remainder := toFloat64(modArr[1])
			docFloat := toFloat64(docValue)
			if divisor != 0 {
				return int(docFloat)%int(divisor) == int(remainder)
			}
		}
		return false
	}
	return false
}

func matchType(value any, typeValue any) bool {
	typeStr, ok := typeValue.(string)
	if !ok {
		return false
	}
	switch typeStr {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case int, int64, float64:
			return true
		}
		return false
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "null":
		return value == nil
	}
	return false
}

func compareEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// 对于数值类型，进行数值比较而不是严格类型匹配
	// 这样可以处理 JSON 序列化后的 float64 和原始 int 的比较
	if isNumeric(a) && isNumeric(b) {
		return toFloat64(a) == toFloat64(b)
	}
	return reflect.DeepEqual(a, b)
}

// isNumeric 检查值是否为数值类型
func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func compareGreater(a, b any) bool {
	// 字符串比较
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return as > bs
		}
	}
	// 数值比较
	if isNumeric(a) && isNumeric(b) {
		return toFloat64(a) > toFloat64(b)
	}
	return false
}

func compareLess(a, b any) bool {
	// 字符串比较
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return as < bs
		}
	}
	// 数值比较
	if isNumeric(a) && isNumeric(b) {
		return toFloat64(a) < toFloat64(b)
	}
	return false
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case string:
		return 0
	}
	return 0
}

func (q *Query) sortResults(results []map[string]any) {
	sort.Slice(results, func(i, j int) bool {
		for _, sf := range q.sortFields {
			vi := getNestedValue(results[i], sf.Field)
			vj := getNestedValue(results[j], sf.Field)

			cmp := compareValues(vi, vj)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func compareValues(a, b any) int {
	// 处理 nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 字符串比较
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return strings.Compare(as, bs)
		}
	}

	// 数值比较
	af := toFloat64(a)
	bf := toFloat64(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

// QueryCollection 为 Collection 添加查询方法的包装。
type QueryCollection interface {
	Collection
	Find(selector map[string]any) *Query
}

// AsQueryCollection 将 Collection 转换为支持查询的接口。
func AsQueryCollection(c Collection) QueryCollection {
	if qc, ok := c.(*collection); ok {
		return qc
	}
	return nil
}

// GetStore 返回底层存储（供内部使用）。
func (c *collection) GetStore() *bstore.Store {
	return c.store
}

// Remove 删除匹配查询的所有文档。
func (q *Query) Remove(ctx context.Context) (int, error) {
	q.collection.mu.Lock()

	if q.collection.closed {
		q.collection.mu.Unlock()
		return 0, fmt.Errorf("collection is closed")
	}

	var toRemove []string
	oldDocs := make(map[string]map[string]any)

	// 查找所有匹配的文档
	err := q.collection.store.Iterate(ctx, q.collection.name, func(k, v []byte) error {
		var doc map[string]any
		if err := json.Unmarshal(v, &doc); err != nil {
			return err
		}
		if q.match(doc) {
			id := string(k)
			toRemove = append(toRemove, id)
			oldDocs[id] = doc
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	if len(toRemove) == 0 {
		return 0, nil
	}

	// 批量删除
	for _, id := range toRemove {
		if err := q.collection.store.Delete(ctx, q.collection.name, id); err != nil {
			return 0, fmt.Errorf("failed to remove documents: %w", err)
		}
	}

	// 准备变更事件
	changeEvents := make([]ChangeEvent, 0, len(toRemove))
	for _, id := range toRemove {
		if oldDoc, exists := oldDocs[id]; exists {
			changeEvents = append(changeEvents, ChangeEvent{
				Collection: q.collection.name,
				ID:         id,
				Op:         OperationDelete,
				Doc:        nil,
				Old:        oldDoc,
				Meta:       nil,
			})
		}
	}

	// 释放锁后再发送变更事件，避免死锁
	q.collection.mu.Unlock()
	for _, event := range changeEvents {
		q.collection.emitChange(event)
	}

	return len(toRemove), nil
}

// Update 更新匹配查询的所有文档。
func (q *Query) Update(ctx context.Context, updates map[string]any) (int, error) {
	q.collection.mu.Lock()

	if q.collection.closed {
		q.collection.mu.Unlock()
		return 0, fmt.Errorf("collection is closed")
	}

	var toUpdate []string

	// 查找所有匹配的文档
	oldDocs := make(map[string]map[string]any)
	err := q.collection.store.Iterate(ctx, q.collection.name, func(k, v []byte) error {
		var doc map[string]any
		if err := json.Unmarshal(v, &doc); err != nil {
			return err
		}
		if q.match(doc) {
			id := string(k)
			toUpdate = append(toUpdate, id)
			// 深拷贝旧文档
			oldDocBytes, _ := json.Marshal(doc)
			oldDoc := make(map[string]any)
			json.Unmarshal(oldDocBytes, &oldDoc)
			oldDocs[id] = oldDoc
		}
		return nil
	})
	if err != nil {
		q.collection.mu.Unlock()
		return 0, err
	}

	if len(toUpdate) == 0 {
		q.collection.mu.Unlock()
		return 0, nil
	}

	// 读取所有需要更新的文档并应用更新
	updatedDocs := make(map[string]map[string]any)
	for _, id := range toUpdate {
		data, err := q.collection.store.Get(ctx, q.collection.name, id)
		if err != nil || data == nil {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			q.collection.mu.Unlock()
			return 0, err
		}
		// 应用更新（不允许更新主键）
		for k, v := range updates {
			if !q.collection.isPrimaryKeyField(k) {
				doc[k] = v
			}
		}
		// 更新修订号
		var oldRev string
		if rev, ok := doc[q.collection.schema.RevField]; ok {
			oldRev = fmt.Sprintf("%v", rev)
		}
		rev, err := q.collection.nextRevision(oldRev, doc)
		if err != nil {
			q.collection.mu.Unlock()
			return 0, fmt.Errorf("failed to generate revision: %w", err)
		}
		doc[q.collection.schema.RevField] = rev
		updatedDocs[id] = doc
	}

	// 批量写入更新后的文档
	for id, doc := range updatedDocs {
		data, err := json.Marshal(doc)
		if err != nil {
			q.collection.mu.Unlock()
			return 0, fmt.Errorf("failed to marshal document %s: %w", id, err)
		}
		if err := q.collection.store.Set(ctx, q.collection.name, id, data); err != nil {
			q.collection.mu.Unlock()
			return 0, fmt.Errorf("failed to update documents: %w", err)
		}
	}

	// 准备变更事件
	changeEvents := make([]ChangeEvent, 0, len(toUpdate))
	for _, id := range toUpdate {
		if updatedDoc, exists := updatedDocs[id]; exists {
			changeEvents = append(changeEvents, ChangeEvent{
				Collection: q.collection.name,
				ID:         id,
				Op:         OperationUpdate,
				Doc:        updatedDoc,
				Old:        oldDocs[id],
				Meta:       map[string]interface{}{"rev": updatedDoc[q.collection.schema.RevField]},
			})
		}
	}

	// 释放锁后再发送变更事件，避免死锁
	q.collection.mu.Unlock()
	for _, event := range changeEvents {
		q.collection.emitChange(event)
	}

	return len(toUpdate), nil
}

// Observe 观察查询结果的变化，返回一个 channel，当查询结果发生变化时会发送新的结果。
// 这相当于 RxDB 的 `$` 操作符。
func (q *Query) Observe(ctx context.Context) <-chan []Document {
	resultChan := make(chan []Document, 1)

	go func() {
		defer close(resultChan)

		// 初始执行查询
		initial, err := q.Exec(ctx)
		if err != nil {
			// 如果初始查询失败，发送空结果而不是直接返回
			select {
			case resultChan <- []Document{}:
			case <-ctx.Done():
				return
			}
			return
		}

		// 发送初始结果
		select {
		case resultChan <- initial:
		case <-ctx.Done():
			return
		}

		// 监听集合的变更事件
		changes := q.collection.Changes()
		lastResult := initial

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-changes:
				if !ok {
					return
				}
				// 当有变更时，重新执行查询
				// 检查变更是否可能影响查询结果
				if q.mightAffectQuery(event) {
					// 使用带超时的 context 执行查询，避免死锁
					queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					newResult, err := q.Exec(queryCtx)
					cancel()
					if err != nil {
						// 查询失败时继续，不阻塞变更事件处理
						continue
					}
					// 只有当结果真正改变时才发送
					if !resultsEqual(lastResult, newResult) {
						lastResult = newResult
						select {
						case resultChan <- newResult:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()

	return resultChan
}

// mightAffectQuery 检查变更事件是否可能影响查询结果。
func (q *Query) mightAffectQuery(event ChangeEvent) bool {
	// 如果查询选择器为空，所有变更都可能影响结果
	if len(q.selector) == 0 {
		return true
	}

	// 检查变更的文档是否匹配查询条件
	if event.Doc != nil {
		return q.match(event.Doc)
	}
	if event.Old != nil {
		return q.match(event.Old)
	}

	return true
}

// resultsEqual 比较两个查询结果是否相等。
func resultsEqual(a, b []Document) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID() != b[i].ID() {
			return false
		}
		// 可以进一步比较数据内容
	}
	return true
}
