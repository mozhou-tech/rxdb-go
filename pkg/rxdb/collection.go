package rxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mozy/rxdb-go/pkg/storage/bolt"
	bbolt "go.etcd.io/bbolt"
)

// HookFunc 定义钩子函数类型。
type HookFunc func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error

// getSchemaVersion 从 schema JSON 中获取版本号，默认为 0
func getSchemaVersion(schema Schema) int {
	if schema.JSON == nil {
		return 0
	}
	if version, ok := schema.JSON["version"]; ok {
		switch v := version.(type) {
		case int:
			return v
		case float64:
			return int(v)
		}
	}
	return 0
}

// collection 是 Collection 接口的默认实现。
type collection struct {
	name        string
	schema      Schema
	store       *bolt.Store
	changes     chan ChangeEvent
	mu          sync.RWMutex
	closed      bool
	closeChan   chan struct{}
	hashFn      func([]byte) string
	broadcaster *eventBroadcaster // 多实例事件广播器（如果启用）
	password    string            // 数据库密码（用于字段加密）

	// 钩子函数
	preInsert  []HookFunc
	postInsert []HookFunc
	preSave    []HookFunc
	postSave   []HookFunc
	preRemove  []HookFunc
	postRemove []HookFunc
	preCreate  []HookFunc
	postCreate []HookFunc
}

func newCollection(ctx context.Context, store *bolt.Store, name string, schema Schema, hashFn func([]byte) string, broadcaster *eventBroadcaster, password string) (*collection, error) {
	col := &collection{
		name:        name,
		schema:      schema,
		store:       store,
		changes:     make(chan ChangeEvent, 100),
		closeChan:   make(chan struct{}),
		hashFn:      hashFn,
		broadcaster: broadcaster,
		password:    password,
		preInsert:   make([]HookFunc, 0),
		postInsert:  make([]HookFunc, 0),
		preSave:     make([]HookFunc, 0),
		postSave:    make([]HookFunc, 0),
		preRemove:   make([]HookFunc, 0),
		postRemove:  make([]HookFunc, 0),
		preCreate:   make([]HookFunc, 0),
		postCreate:  make([]HookFunc, 0),
	}

	// 调用 preCreate 钩子
	for _, hook := range col.preCreate {
		if err := hook(ctx, nil, nil); err != nil {
			return nil, fmt.Errorf("preCreate hook failed: %w", err)
		}
	}

	// 创建集合对应的 bucket
	err := store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		// 为每个索引创建 bucket
		for _, idx := range schema.Indexes {
			indexName := idx.Name
			if indexName == "" {
				// 如果没有名称，使用字段名组合
				indexName = strings.Join(idx.Fields, "_")
			}
			bucketName := fmt.Sprintf("%s_idx_%s", name, indexName)
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return fmt.Errorf("failed to create index bucket %s: %w", bucketName, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create collection bucket: %w", err)
	}

	// 调用 postCreate 钩子
	for _, hook := range col.postCreate {
		if err := hook(ctx, nil, nil); err != nil {
			// 注意：postCreate 钩子失败不会回滚，但会记录错误
		}
	}

	// 检测版本变化并执行迁移
	currentVersion := getSchemaVersion(schema)
	if currentVersion > 0 && len(schema.MigrationStrategies) > 0 {
		// 获取存储的版本
		storedVersion := 0
		_ = store.WithView(ctx, func(tx *bbolt.Tx) error {
			metaBucket := tx.Bucket([]byte("_meta"))
			if metaBucket != nil {
				versionKey := fmt.Sprintf("%s_version", name)
				if data := metaBucket.Get([]byte(versionKey)); data != nil {
					_ = json.Unmarshal(data, &storedVersion)
				}
			}
			return nil
		})

		// 如果版本不同，执行迁移
		if storedVersion < currentVersion {
			if err := col.migrate(ctx, storedVersion, currentVersion); err != nil {
				return nil, fmt.Errorf("schema migration failed: %w", err)
			}
		}
	}

	return col, nil
}

func (c *collection) Name() string {
	return c.name
}

func (c *collection) Schema() Schema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.schema
}

func (c *collection) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.closeChan)
	close(c.changes)
}

func (c *collection) Changes() <-chan ChangeEvent {
	return c.changes
}

func (c *collection) emitChange(event ChangeEvent) {
	c.mu.RLock()
	closed := c.closed
	broadcaster := c.broadcaster
	c.mu.RUnlock()
	if closed {
		return
	}

	// 发送到本地通道
	select {
	case c.changes <- event:
	case <-c.closeChan:
		return
	default:
		// 通道满时丢弃，避免阻塞
	}

	// 如果启用多实例，也广播到全局广播器
	if broadcaster != nil {
		broadcaster.broadcast(event)
	}
}

// getPrimaryKeyFields 获取主键字段列表（支持单个和复合主键）。
func (c *collection) getPrimaryKeyFields() []string {
	switch pk := c.schema.PrimaryKey.(type) {
	case string:
		return []string{pk}
	case []string:
		return pk
	case []interface{}:
		// 处理 JSON 解析后的数组
		fields := make([]string, 0, len(pk))
		for _, f := range pk {
			if str, ok := f.(string); ok {
				fields = append(fields, str)
			}
		}
		return fields
	default:
		// 默认使用 "id"
		return []string{"id"}
	}
}

// extractPrimaryKey 从文档中提取主键值并生成字符串表示（用于存储）。
func (c *collection) extractPrimaryKey(doc map[string]any) (string, error) {
	fields := c.getPrimaryKeyFields()
	if len(fields) == 0 {
		return "", fmt.Errorf("no primary key fields defined")
	}

	if len(fields) == 1 {
		// 单个主键
		value, ok := doc[fields[0]]
		if !ok {
			return "", fmt.Errorf("document must have primary key field: %s", fields[0])
		}
		return fmt.Sprintf("%v", value), nil
	}

	// 复合主键：使用 JSON 编码确保唯一性
	keyParts := make([]interface{}, 0, len(fields))
	for _, field := range fields {
		value, ok := doc[field]
		if !ok {
			return "", fmt.Errorf("document must have primary key field: %s", field)
		}
		keyParts = append(keyParts, value)
	}
	// 使用 JSON 编码复合主键，确保唯一性
	keyBytes, err := json.Marshal(keyParts)
	if err != nil {
		return "", fmt.Errorf("failed to marshal composite primary key: %w", err)
	}
	return string(keyBytes), nil
}

// validatePrimaryKey 验证文档是否包含所有必需的主键字段。
func (c *collection) validatePrimaryKey(doc map[string]any) error {
	fields := c.getPrimaryKeyFields()
	for _, field := range fields {
		if _, ok := doc[field]; !ok {
			return fmt.Errorf("document must have primary key field: %s", field)
		}
	}
	return nil
}

// isPrimaryKeyField 检查字段是否是主键字段之一。
func (c *collection) isPrimaryKeyField(field string) bool {
	fields := c.getPrimaryKeyFields()
	for _, pkField := range fields {
		if field == pkField {
			return true
		}
	}
	return false
}

// updateIndexes 更新所有索引（插入/更新时调用）。
func (c *collection) updateIndexes(ctx context.Context, doc map[string]any, docID string, isDelete bool) error {
	if len(c.schema.Indexes) == 0 {
		return nil
	}

	return c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		for _, idx := range c.schema.Indexes {
			indexName := idx.Name
			if indexName == "" {
				indexName = strings.Join(idx.Fields, "_")
			}
			bucketName := fmt.Sprintf("%s_idx_%s", c.name, indexName)
			indexBucket := tx.Bucket([]byte(bucketName))
			if indexBucket == nil {
				continue
			}

			// 构建索引键
			indexKeyParts := make([]interface{}, 0, len(idx.Fields))
			for _, field := range idx.Fields {
				value := getNestedValue(doc, field)
				indexKeyParts = append(indexKeyParts, value)
			}
			indexKeyBytes, err := json.Marshal(indexKeyParts)
			if err != nil {
				continue
			}
			indexKey := string(indexKeyBytes)

			if isDelete {
				// 删除索引条目
				_ = indexBucket.Delete([]byte(indexKey))
			} else {
				// 更新索引：索引键 -> 文档ID列表（JSON数组）
				var docIDs []string
				existing := indexBucket.Get([]byte(indexKey))
				if existing != nil {
					_ = json.Unmarshal(existing, &docIDs)
				}
				// 检查是否已存在
				found := false
				for _, id := range docIDs {
					if id == docID {
						found = true
						break
					}
				}
				if !found {
					docIDs = append(docIDs, docID)
					docIDsBytes, _ := json.Marshal(docIDs)
					_ = indexBucket.Put([]byte(indexKey), docIDsBytes)
				}
			}
		}
		return nil
	})
}

// getNestedValue 获取嵌套字段值（用于索引）。
func getNestedValue(doc map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = doc
	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return nil
		}
	}
	return current
}

// nextRevision 计算新的修订号，支持自定义哈希函数；为空时回落到时间戳。
func (c *collection) nextRevision(oldRev string, doc map[string]any) (string, error) {
	version := 0
	if oldRev != "" {
		fmt.Sscanf(oldRev, "%d-", &version)
	}
	version++

	// 排除修订号字段，避免递归影响哈希
	payload := make(map[string]any, len(doc))
	for k, v := range doc {
		if k == c.schema.RevField {
			continue
		}
		payload[k] = v
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	if c.hashFn != nil {
		if hash := c.hashFn(data); hash != "" {
			suffix = hash
		}
	}

	return fmt.Sprintf("%d-%s", version, suffix), nil
}

func (c *collection) Insert(ctx context.Context, doc map[string]any) (Document, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	// 应用默认值
	ApplyDefaults(c.schema, doc)

	// 调用 preInsert 钩子
	for _, hook := range c.preInsert {
		if err := hook(ctx, doc, nil); err != nil {
			return nil, fmt.Errorf("preInsert hook failed: %w", err)
		}
	}

	// Schema 验证
	if err := ValidateDocument(c.schema, doc); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// 验证并提取主键
	if err := c.validatePrimaryKey(doc); err != nil {
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(doc)
	if err != nil {
		return nil, err
	}

	// 设置修订号
	rev, err := c.nextRevision("", doc)
	if err != nil {
		return nil, fmt.Errorf("failed to generate revision: %w", err)
	}
	doc[c.schema.RevField] = rev

	// 创建文档副本用于加密（变更事件中发送未加密的文档）
	docForStorage := make(map[string]any)
	docBytes, _ := json.Marshal(doc)
	json.Unmarshal(docBytes, &docForStorage)

	// 加密需要加密的字段
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	// 序列化文档（使用加密后的副本）
	data, err := json.Marshal(docForStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 检查文档是否已存在
	var exists bool
	err = c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		exists = bucket.Get([]byte(idStr)) != nil
		return nil
	})
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("document with id %s already exists", idStr)
	}

	// 写入文档
	err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return errors.New("collection bucket not found")
		}
		return bucket.Put([]byte(idStr), data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to insert document: %w", err)
	}

	// 更新索引
	_ = c.updateIndexes(ctx, doc, idStr, false)

	// 发送变更事件
	c.emitChange(ChangeEvent{
		Collection: c.name,
		ID:         idStr,
		Op:         OperationInsert,
		Doc:        doc,
		Old:        nil,
		Meta:       map[string]interface{}{"rev": rev},
	})

	result := &document{
		id:         idStr,
		data:       doc,
		collection: c,
		revField:   c.schema.RevField,
	}

	// 调用 postInsert 钩子
	for _, hook := range c.postInsert {
		if err := hook(ctx, doc, nil); err != nil {
			// 注意：postInsert 钩子失败不会回滚，但会记录错误
			// 在实际应用中可能需要日志记录
		}
	}

	return result, nil
}

func (c *collection) Upsert(ctx context.Context, doc map[string]any) (Document, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	// Schema 验证
	if err := ValidateDocument(c.schema, doc); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// 验证并提取主键
	if err := c.validatePrimaryKey(doc); err != nil {
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(doc)
	if err != nil {
		return nil, err
	}

	// 检查文档是否已存在
	var oldDoc map[string]any
	var oldRev string
	err = c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(idStr))
		if data != nil {
			oldDoc = make(map[string]any)
			if err := json.Unmarshal(data, &oldDoc); err != nil {
				return err
			}
			if rev, ok := oldDoc[c.schema.RevField]; ok {
				oldRev = fmt.Sprintf("%v", rev)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 验证 final 字段（如果文档已存在）
	if oldDoc != nil {
		if err := ValidateFinalFields(c.schema, oldDoc, doc); err != nil {
			return nil, fmt.Errorf("final field validation failed: %w", err)
		}
	} else {
		// 新文档应用默认值
		ApplyDefaults(c.schema, doc)
	}

	// 调用 preSave 钩子
	for _, hook := range c.preSave {
		if err := hook(ctx, doc, oldDoc); err != nil {
			return nil, fmt.Errorf("preSave hook failed: %w", err)
		}
	}

	// 计算新修订号
	rev, err := c.nextRevision(oldRev, doc)
	if err != nil {
		return nil, fmt.Errorf("failed to generate revision: %w", err)
	}
	doc[c.schema.RevField] = rev

	// 创建文档副本用于加密（变更事件中发送未加密的文档）
	docForStorage := make(map[string]any)
	docBytes, _ := json.Marshal(doc)
	json.Unmarshal(docBytes, &docForStorage)

	// 加密需要加密的字段
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	// 序列化文档（使用加密后的副本）
	data, err := json.Marshal(docForStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 写入文档
	err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return errors.New("collection bucket not found")
		}
		return bucket.Put([]byte(idStr), data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upsert document: %w", err)
	}

	// 更新索引（如果旧文档存在，先删除旧索引）
	if oldDoc != nil {
		_ = c.updateIndexes(ctx, oldDoc, idStr, true)
	}
	_ = c.updateIndexes(ctx, doc, idStr, false)

	// 发送变更事件
	op := OperationInsert
	if oldDoc != nil {
		op = OperationUpdate
	}
	c.emitChange(ChangeEvent{
		Collection: c.name,
		ID:         idStr,
		Op:         op,
		Doc:        doc,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	})

	result := &document{
		id:         idStr,
		data:       doc,
		collection: c,
		revField:   c.schema.RevField,
	}

	// 调用 postSave 钩子
	for _, hook := range c.postSave {
		if err := hook(ctx, doc, oldDoc); err != nil {
			// 注意：postSave 钩子失败不会回滚，但会记录错误
		}
	}

	return result, nil
}

// IncrementalUpsert 对已存在文档进行合并写入，不存在时插入。
func (c *collection) IncrementalUpsert(ctx context.Context, patch map[string]any) (Document, error) {
	if patch == nil {
		return nil, fmt.Errorf("patch cannot be nil")
	}

	// 验证并提取主键
	if err := c.validatePrimaryKey(patch); err != nil {
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(patch)
	if err != nil {
		return nil, err
	}

	// 如果不存在则按 Upsert 新建
	existing, err := c.FindByID(ctx, idStr)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		// 创建新文档，包含所有主键字段
		doc := make(map[string]any)
		fields := c.getPrimaryKeyFields()
		for _, field := range fields {
			if val, ok := patch[field]; ok {
				doc[field] = val
			}
		}
		// 添加其他字段
		for k, v := range patch {
			isPrimaryKey := false
			for _, field := range fields {
				if k == field {
					isPrimaryKey = true
					break
				}
			}
			if !isPrimaryKey {
				doc[k] = v
			}
		}
		return c.Upsert(ctx, doc)
	}

	// 存在则增量更新
	if err := existing.AtomicUpdate(ctx, func(doc map[string]any) error {
		fields := c.getPrimaryKeyFields()
		for k, v := range patch {
			isPrimaryKey := false
			for _, field := range fields {
				if k == field {
					isPrimaryKey = true
					break
				}
			}
			if !isPrimaryKey {
				doc[k] = v
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return existing, nil
}

// IncrementalModify 对指定文档应用修改函数。
func (c *collection) IncrementalModify(ctx context.Context, id string, modifier func(doc map[string]any) error) (Document, error) {
	doc, err := c.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, fmt.Errorf("document with id %s not found", id)
	}
	if err := doc.AtomicUpdate(ctx, modifier); err != nil {
		return nil, err
	}
	return doc, nil
}

func (c *collection) FindByID(ctx context.Context, id string) (Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var doc map[string]any
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(id))
		if data == nil {
			return nil
		}
		doc = make(map[string]any)
		return json.Unmarshal(data, &doc)
	})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	// 解密需要解密的字段
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		if err := decryptDocumentFields(doc, c.schema.EncryptedFields, c.password); err != nil {
			// 解密失败时，继续返回文档（可能包含未加密的值）
		}
	}

	return &document{
		id:         id,
		data:       doc,
		collection: c,
		revField:   c.schema.RevField,
	}, nil
}

func (c *collection) Remove(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 获取旧文档
	var oldDoc map[string]any
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(id))
		if data != nil {
			oldDoc = make(map[string]any)
			return json.Unmarshal(data, &oldDoc)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if oldDoc == nil {
		return fmt.Errorf("document with id %s not found", id)
	}

	// 调用 preRemove 钩子
	for _, hook := range c.preRemove {
		if err := hook(ctx, nil, oldDoc); err != nil {
			return fmt.Errorf("preRemove hook failed: %w", err)
		}
	}

	// 删除文档
	err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(id))
	})
	if err != nil {
		return fmt.Errorf("failed to remove document: %w", err)
	}

	// 更新索引（删除索引条目）
	_ = c.updateIndexes(ctx, oldDoc, id, true)

	// 发送变更事件
	c.emitChange(ChangeEvent{
		Collection: c.name,
		ID:         id,
		Op:         OperationDelete,
		Doc:        nil,
		Old:        oldDoc,
		Meta:       nil,
	})

	// 调用 postRemove 钩子
	for _, hook := range c.postRemove {
		if err := hook(ctx, nil, oldDoc); err != nil {
			// 注意：postRemove 钩子失败不会回滚，但会记录错误
		}
	}

	return nil
}

func (c *collection) All(ctx context.Context) ([]Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var docs []Document
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var doc map[string]any
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			// 解密需要解密的字段
			if len(c.schema.EncryptedFields) > 0 && c.password != "" {
				if err := decryptDocumentFields(doc, c.schema.EncryptedFields, c.password); err != nil {
					// 解密失败时，继续处理文档
				}
			}
			docs = append(docs, &document{
				id:         string(k),
				data:       doc,
				collection: c,
				revField:   c.schema.RevField,
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return docs, nil
}

// Count 返回集合中的文档总数。
func (c *collection) Count(ctx context.Context) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return 0, errors.New("collection is closed")
	}

	var count int
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	return count, err
}

// BulkInsert 批量插入文档。
func (c *collection) BulkInsert(ctx context.Context, docs []map[string]any) ([]Document, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	result := make([]Document, 0, len(docs))
	inserted := make(map[string]map[string]any)

	// 验证所有文档并准备数据
	for _, doc := range docs {
		// 验证并提取主键
		if err := c.validatePrimaryKey(doc); err != nil {
			return nil, err
		}
		idStr, err := c.extractPrimaryKey(doc)
		if err != nil {
			return nil, err
		}

		// 检查是否已存在
		var exists bool
		err = c.store.WithView(ctx, func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(c.name))
			if bucket == nil {
				return nil
			}
			exists = bucket.Get([]byte(idStr)) != nil
			return nil
		})
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("document with id %s already exists", idStr)
		}

		// 设置修订号
		rev, err := c.nextRevision("", doc)
		if err != nil {
			return nil, fmt.Errorf("failed to generate revision: %w", err)
		}
		doc[c.schema.RevField] = rev
		inserted[idStr] = doc
	}

	// 批量写入
	err := c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return errors.New("collection bucket not found")
		}
		for idStr, doc := range inserted {
			// 创建文档副本用于加密
			docForStorage := make(map[string]any)
			docBytes, _ := json.Marshal(doc)
			json.Unmarshal(docBytes, &docForStorage)

			// 加密需要加密的字段
			if len(c.schema.EncryptedFields) > 0 && c.password != "" {
				if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
					return fmt.Errorf("failed to encrypt fields for document %s: %w", idStr, err)
				}
			}

			data, err := json.Marshal(docForStorage)
			if err != nil {
				return fmt.Errorf("failed to marshal document %s: %w", idStr, err)
			}
			if err := bucket.Put([]byte(idStr), data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to bulk insert: %w", err)
	}

	// 创建 Document 对象并发送变更事件
	for idStr, doc := range inserted {
		result = append(result, &document{
			id:         idStr,
			data:       doc,
			collection: c,
			revField:   c.schema.RevField,
		})
		c.emitChange(ChangeEvent{
			Collection: c.name,
			ID:         idStr,
			Op:         OperationInsert,
			Doc:        doc,
			Old:        nil,
			Meta:       map[string]interface{}{"rev": doc[c.schema.RevField]},
		})
	}

	return result, nil
}

// BulkUpsert 批量更新或插入文档。
func (c *collection) BulkUpsert(ctx context.Context, docs []map[string]any) ([]Document, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	result := make([]Document, 0, len(docs))
	toUpsert := make(map[string]map[string]any)
	oldDocs := make(map[string]map[string]any)

	// 获取所有旧文档
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		for _, doc := range docs {
			// 验证并提取主键
			if err := c.validatePrimaryKey(doc); err != nil {
				return err
			}
			idStr, err := c.extractPrimaryKey(doc)
			if err != nil {
				return err
			}

			data := bucket.Get([]byte(idStr))
			if data != nil {
				oldDoc := make(map[string]any)
				if err := json.Unmarshal(data, &oldDoc); err != nil {
					return err
				}
				oldDocs[idStr] = oldDoc
			}

			toUpsert[idStr] = doc
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 计算修订号并准备数据
	for idStr, doc := range toUpsert {
		oldRev := ""
		if oldDoc, exists := oldDocs[idStr]; exists {
			if prev, ok := oldDoc[c.schema.RevField]; ok {
				oldRev = fmt.Sprintf("%v", prev)
			}
		}

		rev, err := c.nextRevision(oldRev, doc)
		if err != nil {
			return nil, fmt.Errorf("failed to generate revision: %w", err)
		}
		doc[c.schema.RevField] = rev
	}

	// 批量写入
	err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return errors.New("collection bucket not found")
		}
		for idStr, doc := range toUpsert {
			// 创建文档副本用于加密
			docForStorage := make(map[string]any)
			docBytes, _ := json.Marshal(doc)
			json.Unmarshal(docBytes, &docForStorage)

			// 加密需要加密的字段
			if len(c.schema.EncryptedFields) > 0 && c.password != "" {
				if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
					return fmt.Errorf("failed to encrypt fields for document %s: %w", idStr, err)
				}
			}

			data, err := json.Marshal(docForStorage)
			if err != nil {
				return fmt.Errorf("failed to marshal document %s: %w", idStr, err)
			}
			if err := bucket.Put([]byte(idStr), data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to bulk upsert: %w", err)
	}

	// 创建 Document 对象并发送变更事件
	for idStr, doc := range toUpsert {
		result = append(result, &document{
			id:         idStr,
			data:       doc,
			collection: c,
			revField:   c.schema.RevField,
		})

		op := OperationInsert
		if _, exists := oldDocs[idStr]; exists {
			op = OperationUpdate
		}
		c.emitChange(ChangeEvent{
			Collection: c.name,
			ID:         idStr,
			Op:         op,
			Doc:        doc,
			Old:        oldDocs[idStr],
			Meta:       map[string]interface{}{"rev": doc[c.schema.RevField]},
		})
	}

	return result, nil
}

// BulkRemove 批量删除文档。
func (c *collection) BulkRemove(ctx context.Context, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	oldDocs := make(map[string]map[string]any)

	// 获取所有旧文档
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		for _, id := range ids {
			data := bucket.Get([]byte(id))
			if data != nil {
				oldDoc := make(map[string]any)
				if err := json.Unmarshal(data, &oldDoc); err != nil {
					return err
				}
				oldDocs[id] = oldDoc
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 批量删除
	err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		for _, id := range ids {
			if err := bucket.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to bulk remove: %w", err)
	}

	// 发送变更事件
	for _, id := range ids {
		if oldDoc, exists := oldDocs[id]; exists {
			c.emitChange(ChangeEvent{
				Collection: c.name,
				ID:         id,
				Op:         OperationDelete,
				Doc:        nil,
				Old:        oldDoc,
				Meta:       nil,
			})
		}
	}

	return nil
}

// ExportJSON 导出集合的所有文档为 JSON 数组。
func (c *collection) ExportJSON(ctx context.Context) ([]map[string]any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var docs []map[string]any
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var doc map[string]any
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			docs = append(docs, doc)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to export collection: %w", err)
	}

	return docs, nil
}

// ImportJSON 从 JSON 数组导入文档到集合。
func (c *collection) ImportJSON(ctx context.Context, docs []map[string]any) error {
	if len(docs) == 0 {
		return nil
	}

	// 使用 BulkUpsert 来导入
	_, err := c.BulkUpsert(ctx, docs)
	return err
}

// PreInsert 注册插入前钩子。
func (c *collection) PreInsert(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preInsert = append(c.preInsert, hook)
}

// PostInsert 注册插入后钩子。
func (c *collection) PostInsert(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postInsert = append(c.postInsert, hook)
}

// PreSave 注册保存前钩子。
func (c *collection) PreSave(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preSave = append(c.preSave, hook)
}

// PostSave 注册保存后钩子。
func (c *collection) PostSave(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postSave = append(c.postSave, hook)
}

// PreRemove 注册删除前钩子。
func (c *collection) PreRemove(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preRemove = append(c.preRemove, hook)
}

// PostRemove 注册删除后钩子。
func (c *collection) PostRemove(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postRemove = append(c.postRemove, hook)
}

// PreCreate 注册创建前钩子。
func (c *collection) PreCreate(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preCreate = append(c.preCreate, hook)
}

// PostCreate 注册创建后钩子。
func (c *collection) PostCreate(hook HookFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postCreate = append(c.postCreate, hook)
}

// migrate 执行从旧版本到新版本的迁移
func (c *collection) migrate(ctx context.Context, fromVersion, toVersion int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 获取所有文档并迁移
	var docsToMigrate []map[string]any
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var doc map[string]any
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			docsToMigrate = append(docsToMigrate, doc)
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("failed to read documents for migration: %w", err)
	}

	// 执行迁移策略
	for version := fromVersion + 1; version <= toVersion; version++ {
		strategy, ok := c.schema.MigrationStrategies[version]
		if !ok {
			continue
		}

		// 迁移每个文档
		for _, doc := range docsToMigrate {
			migratedDoc, err := strategy(doc)
			if err != nil {
				return fmt.Errorf("migration strategy for version %d failed: %w", version, err)
			}
			// 更新文档
			id, err := c.extractPrimaryKey(migratedDoc)
			if err != nil {
				return fmt.Errorf("failed to extract primary key: %w", err)
			}

			// 保存迁移后的文档
			err = c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
				bucket := tx.Bucket([]byte(c.name))
				if bucket == nil {
					return errors.New("collection bucket not found")
				}
				data, err := json.Marshal(migratedDoc)
				if err != nil {
					return err
				}
				return bucket.Put([]byte(id), data)
			})
			if err != nil {
				return fmt.Errorf("failed to save migrated document: %w", err)
			}
		}
	}

	// 更新存储的版本号
	return c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		metaBucket, err := tx.CreateBucketIfNotExists([]byte("_meta"))
		if err != nil {
			return err
		}
		versionKey := fmt.Sprintf("%s_version", c.name)
		versionData, _ := json.Marshal(toVersion)
		return metaBucket.Put([]byte(versionKey), versionData)
	})
}

// Migrate 手动触发 Schema 迁移
func (c *collection) Migrate(ctx context.Context) error {
	currentVersion := getSchemaVersion(c.schema)
	if currentVersion == 0 {
		return nil // 没有版本信息，无需迁移
	}

	// 获取存储的版本
	storedVersion := 0
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte("_meta"))
		if metaBucket != nil {
			versionKey := fmt.Sprintf("%s_version", c.name)
			if data := metaBucket.Get([]byte(versionKey)); data != nil {
				_ = json.Unmarshal(data, &storedVersion)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read stored version: %w", err)
	}

	if storedVersion >= currentVersion {
		return nil // 已经是最新版本
	}

	return c.migrate(ctx, storedVersion, currentVersion)
}

// GetAttachment 获取文档的附件
func (c *collection) GetAttachment(ctx context.Context, docID, attachmentID string) (*Attachment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var attachment *Attachment
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		attachmentsBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachments", c.name)))
		if attachmentsBucket == nil {
			return fmt.Errorf("attachment %s not found for document %s", attachmentID, docID)
		}

		docAttachmentsBucket := attachmentsBucket.Bucket([]byte(docID))
		if docAttachmentsBucket == nil {
			return fmt.Errorf("attachment %s not found for document %s", attachmentID, docID)
		}

		data := docAttachmentsBucket.Get([]byte(attachmentID))
		if data == nil {
			return fmt.Errorf("attachment %s not found for document %s", attachmentID, docID)
		}

		attachment = &Attachment{}
		return json.Unmarshal(data, attachment)
	})

	return attachment, err
}

// PutAttachment 添加或更新文档的附件
func (c *collection) PutAttachment(ctx context.Context, docID string, attachment *Attachment) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 验证文档存在
	_, err := c.FindByID(ctx, docID)
	if err != nil {
		return fmt.Errorf("document %s not found: %w", docID, err)
	}

	// 设置时间戳
	now := time.Now().Unix()
	if attachment.Created == 0 {
		attachment.Created = now
	}
	attachment.Modified = now

	// 计算摘要（如果未提供）
	if attachment.Digest == "" && len(attachment.Data) > 0 {
		attachment.Digest = c.hashFn(attachment.Data)
	}

	return c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		attachmentsBucket, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s_attachments", c.name)))
		if err != nil {
			return err
		}

		docAttachmentsBucket, err := attachmentsBucket.CreateBucketIfNotExists([]byte(docID))
		if err != nil {
			return err
		}

		// 存储附件元数据（不包含数据，数据单独存储）
		attachmentMeta := *attachment
		attachmentMeta.Data = nil // 元数据中不包含数据

		metaData, err := json.Marshal(attachmentMeta)
		if err != nil {
			return err
		}

		if err := docAttachmentsBucket.Put([]byte(attachment.ID), metaData); err != nil {
			return err
		}

		// 存储附件数据到单独的 bucket
		dataBucket, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s_attachment_data", c.name)))
		if err != nil {
			return err
		}

		dataKey := fmt.Sprintf("%s_%s", docID, attachment.ID)
		return dataBucket.Put([]byte(dataKey), attachment.Data)
	})
}

// RemoveAttachment 删除文档的附件
func (c *collection) RemoveAttachment(ctx context.Context, docID, attachmentID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	return c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
		attachmentsBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachments", c.name)))
		if attachmentsBucket == nil {
			return nil // 附件不存在，直接返回
		}

		docAttachmentsBucket := attachmentsBucket.Bucket([]byte(docID))
		if docAttachmentsBucket == nil {
			return nil
		}

		if err := docAttachmentsBucket.Delete([]byte(attachmentID)); err != nil {
			return err
		}

		// 删除附件数据
		dataBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachment_data", c.name)))
		if dataBucket != nil {
			dataKey := fmt.Sprintf("%s_%s", docID, attachmentID)
			_ = dataBucket.Delete([]byte(dataKey))
		}

		return nil
	})
}

// GetAllAttachments 获取文档的所有附件
func (c *collection) GetAllAttachments(ctx context.Context, docID string) ([]*Attachment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var attachments []*Attachment
	err := c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		attachmentsBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachments", c.name)))
		if attachmentsBucket == nil {
			return nil
		}

		docAttachmentsBucket := attachmentsBucket.Bucket([]byte(docID))
		if docAttachmentsBucket == nil {
			return nil
		}

		dataBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachment_data", c.name)))

		return docAttachmentsBucket.ForEach(func(k, v []byte) error {
			var attachment Attachment
			if err := json.Unmarshal(v, &attachment); err != nil {
				return err
			}

			// 加载附件数据
			if dataBucket != nil {
				dataKey := fmt.Sprintf("%s_%s", docID, attachment.ID)
				if data := dataBucket.Get([]byte(dataKey)); data != nil {
					attachment.Data = data
				}
			}

			attachments = append(attachments, &attachment)
			return nil
		})
	})

	return attachments, err
}

// Dump 导出集合（包含文档和附件）
func (c *collection) Dump(ctx context.Context) (map[string]any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	dump := make(map[string]any)

	// 导出文档
	docs, err := c.ExportJSON(ctx)
	if err != nil {
		return nil, err
	}
	dump["documents"] = docs

	// 导出附件
	attachmentsMap := make(map[string]map[string]*Attachment)
	err = c.store.WithView(ctx, func(tx *bbolt.Tx) error {
		attachmentsBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachments", c.name)))
		if attachmentsBucket == nil {
			return nil
		}

		dataBucket := tx.Bucket([]byte(fmt.Sprintf("%s_attachment_data", c.name)))

		return attachmentsBucket.ForEach(func(docIDBytes, _ []byte) error {
			docID := string(docIDBytes)
			docAttachmentsBucket := attachmentsBucket.Bucket(docIDBytes)
			if docAttachmentsBucket == nil {
				return nil
			}

			docAttachments := make(map[string]*Attachment)
			if err := docAttachmentsBucket.ForEach(func(attIDBytes, metaBytes []byte) error {
				var attachment Attachment
				if err := json.Unmarshal(metaBytes, &attachment); err != nil {
					return err
				}

				// 加载附件数据
				if dataBucket != nil {
					dataKey := fmt.Sprintf("%s_%s", docID, attachment.ID)
					if data := dataBucket.Get([]byte(dataKey)); data != nil {
						attachment.Data = data
					}
				}

				docAttachments[attachment.ID] = &attachment
				return nil
			}); err != nil {
				return err
			}

			if len(docAttachments) > 0 {
				attachmentsMap[docID] = docAttachments
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	if len(attachmentsMap) > 0 {
		dump["attachments"] = attachmentsMap
	}

	dump["name"] = c.name
	return dump, nil
}

// ImportDump 导入集合（包含文档和附件）
func (c *collection) ImportDump(ctx context.Context, dump map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 导入文档
	if docsData, ok := dump["documents"].([]any); ok {
		docs := make([]map[string]any, 0, len(docsData))
		for _, doc := range docsData {
			if docMap, ok := doc.(map[string]any); ok {
				docs = append(docs, docMap)
			}
		}
		if err := c.ImportJSON(ctx, docs); err != nil {
			return fmt.Errorf("failed to import documents: %w", err)
		}
	}

	// 导入附件
	if attachmentsData, ok := dump["attachments"].(map[string]any); ok {
		return c.store.WithUpdate(ctx, func(tx *bbolt.Tx) error {
			attachmentsBucket, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s_attachments", c.name)))
			if err != nil {
				return err
			}

			dataBucket, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s_attachment_data", c.name)))
			if err != nil {
				return err
			}

			for docID, docAttachmentsData := range attachmentsData {
				docAttachmentsMap, ok := docAttachmentsData.(map[string]any)
				if !ok {
					continue
				}

				docAttachmentsBucket, err := attachmentsBucket.CreateBucketIfNotExists([]byte(docID))
				if err != nil {
					return err
				}

				for attID, attData := range docAttachmentsMap {
					attMap, ok := attData.(map[string]any)
					if !ok {
						continue
					}

					// 解析附件
					attachment := &Attachment{}
					if id, ok := attMap["id"].(string); ok {
						attachment.ID = id
					}
					if name, ok := attMap["name"].(string); ok {
						attachment.Name = name
					}
					if attType, ok := attMap["type"].(string); ok {
						attachment.Type = attType
					}
					if size, ok := attMap["size"].(float64); ok {
						attachment.Size = int64(size)
					}
					if data, ok := attMap["data"].([]byte); ok {
						attachment.Data = data
					} else if dataStr, ok := attMap["data"].(string); ok {
						// 处理 base64 编码的数据
						attachment.Data = []byte(dataStr)
					}

					// 存储附件元数据
					attachmentMeta := *attachment
					attachmentMeta.Data = nil
					metaData, err := json.Marshal(attachmentMeta)
					if err != nil {
						return err
					}

					if err := docAttachmentsBucket.Put([]byte(attID), metaData); err != nil {
						return err
					}

					// 存储附件数据
					if len(attachment.Data) > 0 {
						dataKey := fmt.Sprintf("%s_%s", docID, attID)
						if err := dataBucket.Put([]byte(dataKey), attachment.Data); err != nil {
							return err
						}
					}
				}
			}

			return nil
		})
	}

	return nil
}
