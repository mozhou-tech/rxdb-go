package rxdb

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	bstore "github.com/mozy/rxdb-go/pkg/storage/badger"
	"github.com/sirupsen/logrus"
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
	store       *bstore.Store
	changes     chan ChangeEvent
	mu          sync.RWMutex
	closed      bool
	closeChan   chan struct{}
	hashFn      func([]byte) string
	broadcaster *eventBroadcaster // 多实例事件广播器（如果启用）
	password    string            // 数据库密码（用于字段加密）

	// 订阅者管理
	subscribersMu   sync.RWMutex
	subscribers     map[uint64]chan ChangeEvent
	subscriberIDGen uint64

	// 数据库级别事件回调（用于向数据库发送变更事件）
	dbEventCallback func(event ChangeEvent)

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

func newCollection(ctx context.Context, store *bstore.Store, name string, schema Schema, hashFn func([]byte) string, broadcaster *eventBroadcaster, password string, dbEventCallback func(event ChangeEvent)) (*collection, error) {
	logrus.WithField("name", name).Debug("Creating collection")

	col := &collection{
		name:            name,
		schema:          schema,
		store:           store,
		changes:         make(chan ChangeEvent, 100),
		closeChan:       make(chan struct{}),
		hashFn:          hashFn,
		broadcaster:     broadcaster,
		password:        password,
		subscribers:     make(map[uint64]chan ChangeEvent),
		dbEventCallback: dbEventCallback,
		preInsert:       make([]HookFunc, 0),
		postInsert:      make([]HookFunc, 0),
		preSave:         make([]HookFunc, 0),
		postSave:        make([]HookFunc, 0),
		preRemove:       make([]HookFunc, 0),
		postRemove:      make([]HookFunc, 0),
		preCreate:       make([]HookFunc, 0),
		postCreate:      make([]HookFunc, 0),
	}

	// 调用 preCreate 钩子
	for _, hook := range col.preCreate {
		if err := hook(ctx, nil, nil); err != nil {
			return nil, fmt.Errorf("preCreate hook failed: %w", err)
		}
	}

	// Badger 不需要预创建 bucket，使用键前缀来区分集合
	logrus.WithFields(logrus.Fields{
		"name":    name,
		"indexes": len(schema.Indexes),
	}).Debug("Collection created successfully")

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
		versionKey := fmt.Sprintf("_meta:%s_version", name)
		data, _ := store.Get(ctx, "_meta", fmt.Sprintf("%s_version", name))
		if data != nil {
			_ = json.Unmarshal(data, &storedVersion)
		}
		_ = versionKey // 用于避免未使用警告

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

// Schema 返回集合的 schema。
// schema 在集合创建后不会改变，因此无需加锁。
func (c *collection) Schema() Schema {
	return c.schema
}

// getAttachmentDir 获取附件存储目录
func (c *collection) getAttachmentDir() (string, error) {
	dbPath := c.store.Path()
	if dbPath == "" {
		return "", errors.New("database path not available")
	}

	// 在数据库目录下创建 attachments 子目录
	attachmentDir := filepath.Join(dbPath, "attachments")
	if err := os.MkdirAll(attachmentDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create attachment directory: %w", err)
	}
	return attachmentDir, nil
}

// getAttachmentFilePath 生成附件的文件路径
func (c *collection) getAttachmentFilePath(docID, attachmentID, filename string) (string, error) {
	attachmentDir, err := c.getAttachmentDir()
	if err != nil {
		return "", err
	}

	// 使用 docID 和 attachmentID 生成唯一文件名
	// 格式: {docID}_{attachmentID}_{filename}
	// 如果 filename 为空，使用 attachmentID
	if filename == "" {
		filename = attachmentID
	}

	// 清理文件名，避免路径遍历攻击
	filename = filepath.Base(filename)
	safeFilename := fmt.Sprintf("%s_%s_%s", docID, attachmentID, filename)

	return filepath.Join(attachmentDir, safeFilename), nil
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

	// 关闭所有订阅者通道
	c.subscribersMu.Lock()
	for id, ch := range c.subscribers {
		close(ch)
		delete(c.subscribers, id)
	}
	c.subscribersMu.Unlock()
}

func (c *collection) Changes() <-chan ChangeEvent {
	return c.subscribe()
}

// subscribe 创建一个新的订阅通道，每个订阅者都会收到所有变更事件的独立副本。
func (c *collection) subscribe() <-chan ChangeEvent {
	c.subscribersMu.Lock()
	defer c.subscribersMu.Unlock()

	// 检查是否已关闭
	select {
	case <-c.closeChan:
		ch := make(chan ChangeEvent)
		close(ch)
		return ch
	default:
	}

	c.subscriberIDGen++
	id := c.subscriberIDGen
	ch := make(chan ChangeEvent, 100)
	c.subscribers[id] = ch

	return ch
}

func (c *collection) emitChange(event ChangeEvent) {
	// 注意：调用者应已持有锁或在释放锁后调用
	// 使用 closeChan 来安全地检测关闭状态，避免死锁
	select {
	case <-c.closeChan:
		return
	default:
	}

	// 向所有订阅者发送事件
	c.subscribersMu.RLock()
	subscribers := make([]chan ChangeEvent, 0, len(c.subscribers))
	for _, ch := range c.subscribers {
		subscribers = append(subscribers, ch)
	}
	c.subscribersMu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- event:
		case <-c.closeChan:
			return
		default:
			// 通道满时丢弃，避免阻塞
		}
	}

	// 向数据库级别发送事件
	if c.dbEventCallback != nil {
		c.dbEventCallback(event)
	}

	// 如果启用多实例，也广播到全局广播器
	if c.broadcaster != nil {
		c.broadcaster.broadcast(event)
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

// updateIndexesInTx 在现有事务中更新索引（用于批量操作优化）
func (c *collection) updateIndexesInTx(txn *badger.Txn, doc map[string]any, docID string, isDelete bool) error {
	if len(c.schema.Indexes) == 0 {
		return nil
	}

	for _, idx := range c.schema.Indexes {
		indexName := idx.Name
		if indexName == "" {
			indexName = strings.Join(idx.Fields, "_")
		}
		bucketName := fmt.Sprintf("%s_idx_%s", c.name, indexName)

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
		indexKey := bstore.BucketKey(bucketName, string(indexKeyBytes))

		if isDelete {
			// 从索引中移除文档ID
			item, err := txn.Get(indexKey)
			if err == nil {
				var existing []byte
				_ = item.Value(func(val []byte) error {
					existing = append([]byte{}, val...)
					return nil
				})
				if existing != nil {
					var docIDs []string
					if err := json.Unmarshal(existing, &docIDs); err == nil {
						// 从列表中移除该文档ID
						newDocIDs := make([]string, 0, len(docIDs))
						for _, id := range docIDs {
							if id != docID {
								newDocIDs = append(newDocIDs, id)
							}
						}
						// 如果列表为空，删除索引条目；否则更新列表
						if len(newDocIDs) == 0 {
							_ = txn.Delete(indexKey)
						} else {
							newDocIDsBytes, _ := json.Marshal(newDocIDs)
							_ = txn.Set(indexKey, newDocIDsBytes)
						}
					}
				}
			}
		} else {
			// 更新索引：索引键 -> 文档ID列表（JSON数组）
			var docIDs []string
			item, err := txn.Get(indexKey)
			if err == nil {
				_ = item.Value(func(val []byte) error {
					_ = json.Unmarshal(val, &docIDs)
					return nil
				})
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
				_ = txn.Set(indexKey, docIDsBytes)
			}
		}
	}
	return nil
}

// updateIndexes 更新所有索引（插入/更新时调用）。
func (c *collection) updateIndexes(ctx context.Context, doc map[string]any, docID string, isDelete bool) error {
	if len(c.schema.Indexes) == 0 {
		return nil
	}

	return c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
		return c.updateIndexesInTx(txn, doc, docID, isDelete)
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
	logrus.WithField("collection", c.name).Debug("Inserting document into collection")

	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	// 应用默认值
	ApplyDefaults(c.schema, doc)

	// 调用 preInsert 钩子
	for _, hook := range c.preInsert {
		if err := hook(ctx, doc, nil); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("preInsert hook failed: %w", err)
		}
	}

	// Schema 验证
	if err := ValidateDocument(c.schema, doc); err != nil {
		c.mu.Unlock()
		return nil, NewError(ErrorTypeValidation, "schema validation failed", err)
	}

	// 验证并提取主键
	if err := c.validatePrimaryKey(doc); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(doc)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// 调用 preSave 钩子（oldDoc 为 nil，因为是新插入）
	for _, hook := range c.preSave {
		if err := hook(ctx, doc, nil); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("preSave hook failed: %w", err)
		}
	}

	// 设置修订号
	rev, err := c.nextRevision("", doc)
	if err != nil {
		c.mu.Unlock()
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
			c.mu.Unlock()
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	// 序列化文档（使用加密后的副本）
	data, err := json.Marshal(docForStorage)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 检查文档是否已存在
	existingData, err := c.store.Get(ctx, c.name, idStr)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if existingData != nil {
		logrus.WithFields(logrus.Fields{
			"collection": c.name,
			"id":         idStr,
		}).Warn("Document already exists")
		c.mu.Unlock()
		return nil, NewError(ErrorTypeAlreadyExists, fmt.Sprintf("document with id %s already exists", idStr), nil).
			WithContext("document_id", idStr)
	}

	// 写入文档
	err = c.store.Set(ctx, c.name, idStr, data)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"collection": c.name,
			"id":         idStr,
		}).Error("Failed to insert document")
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to insert document: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"collection": c.name,
		"id":         idStr,
	}).Debug("Document inserted successfully")

	// 更新索引
	_ = c.updateIndexes(ctx, doc, idStr, false)

	// 返回的文档使用经过 JSON 序列化/反序列化后的数据
	// 确保数据类型与从存储读取时一致（数字变为 float64 等）
	// 对于非加密字段，使用 docForStorage；对于加密字段，需要返回未加密版本
	var returnData map[string]any
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		// 有加密字段时，使用原始 doc 的 JSON 序列化版本（未加密）
		returnData = make(map[string]any)
		json.Unmarshal(docBytes, &returnData)
	} else {
		// 无加密字段时，直接使用 docForStorage
		returnData = docForStorage
	}

	result := &document{
		id:         idStr,
		data:       returnData,
		collection: c,
		revField:   c.schema.RevField,
	}

	// 调用 postSave 钩子（oldDoc 为 nil，因为是新插入）
	for _, hook := range c.postSave {
		if err := hook(ctx, doc, nil); err != nil {
			// 注意：postSave 钩子失败不会回滚，但会记录错误
		}
	}

	// 调用 postInsert 钩子
	for _, hook := range c.postInsert {
		if err := hook(ctx, doc, nil); err != nil {
			// 注意：postInsert 钩子失败不会回滚，但会记录错误
			// 在实际应用中可能需要日志记录
		}
	}

	// 在释放锁之前准备变更事件
	changeEvent := ChangeEvent{
		Collection: c.name,
		ID:         idStr,
		Op:         OperationInsert,
		Doc:        doc,
		Old:        nil,
		Meta:       map[string]interface{}{"rev": rev},
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	c.emitChange(changeEvent)

	return result, nil
}

func (c *collection) Upsert(ctx context.Context, doc map[string]any) (Document, error) {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	// Schema 验证
	if err := ValidateDocument(c.schema, doc); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// 验证并提取主键
	if err := c.validatePrimaryKey(doc); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(doc)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// 检查文档是否已存在
	var oldDoc map[string]any
	var oldRev string
	existingData, err := c.store.Get(ctx, c.name, idStr)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if existingData != nil {
		oldDoc = make(map[string]any)
		if err := json.Unmarshal(existingData, &oldDoc); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if rev, ok := oldDoc[c.schema.RevField]; ok {
			oldRev = fmt.Sprintf("%v", rev)
		}
	}

	// 验证 final 字段（如果文档已存在）
	if oldDoc != nil {
		if err := ValidateFinalFields(c.schema, oldDoc, doc); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("final field validation failed: %w", err)
		}
	} else {
		// 新文档应用默认值
		ApplyDefaults(c.schema, doc)
	}

	// 调用 preSave 钩子
	for _, hook := range c.preSave {
		if err := hook(ctx, doc, oldDoc); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("preSave hook failed: %w", err)
		}
	}

	// 计算新修订号
	rev, err := c.nextRevision(oldRev, doc)
	if err != nil {
		c.mu.Unlock()
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
			c.mu.Unlock()
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	// 序列化文档（使用加密后的副本）
	data, err := json.Marshal(docForStorage)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 写入文档
	err = c.store.Set(ctx, c.name, idStr, data)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to upsert document: %w", err)
	}

	// 更新索引（如果旧文档存在，先删除旧索引）
	if oldDoc != nil {
		_ = c.updateIndexes(ctx, oldDoc, idStr, true)
	}
	_ = c.updateIndexes(ctx, doc, idStr, false)

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

	// 在释放锁之前准备变更事件
	op := OperationInsert
	if oldDoc != nil {
		op = OperationUpdate
	}
	changeEvent := ChangeEvent{
		Collection: c.name,
		ID:         idStr,
		Op:         op,
		Doc:        doc,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	c.emitChange(changeEvent)

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
		// 如果是未找到错误，则创建新文档
		if IsNotFoundError(err) {
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
		// FindByID 现在应该返回错误而不是 nil，但为了安全起见保留此检查
		return nil, NewError(ErrorTypeNotFound, fmt.Sprintf("document with id %s not found", id), nil)
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

	data, err := c.store.Get(ctx, c.name, id)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, NewError(ErrorTypeNotFound, fmt.Sprintf("document with id %s not found", id), nil)
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
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

	if c.closed {
		c.mu.Unlock()
		return errors.New("collection is closed")
	}

	// 获取旧文档
	var oldDoc map[string]any
	oldData, err := c.store.Get(ctx, c.name, id)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if oldData != nil {
		oldDoc = make(map[string]any)
		if err := json.Unmarshal(oldData, &oldDoc); err != nil {
			c.mu.Unlock()
			return err
		}
	}

	if oldDoc == nil {
		c.mu.Unlock()
		return NewError(ErrorTypeNotFound, fmt.Sprintf("document with id %s not found", id), nil)
	}

	// 调用 preRemove 钩子
	for _, hook := range c.preRemove {
		if err := hook(ctx, nil, oldDoc); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("preRemove hook failed: %w", err)
		}
	}

	// 删除文档
	err = c.store.Delete(ctx, c.name, id)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to remove document: %w", err)
	}

	// 删除该文档的所有附件
	bucket := fmt.Sprintf("%s_attachments", c.name)
	prefix := fmt.Sprintf("%s_", id)
	var attachmentKeysToDelete []string
	var attachmentsToDelete []*Attachment
	_ = c.store.Iterate(ctx, bucket, func(k, v []byte) error {
		keyStr := string(k)
		if strings.HasPrefix(keyStr, prefix) {
			attachmentKeysToDelete = append(attachmentKeysToDelete, keyStr)
			// 解析附件元数据以获取文件名
			var att Attachment
			if err := json.Unmarshal(v, &att); err == nil {
				attachmentsToDelete = append(attachmentsToDelete, &att)
			}
		}
		return nil
	})
	for i, key := range attachmentKeysToDelete {
		_ = c.store.Delete(ctx, bucket, key)
		// 删除文件系统中的附件文件
		if i < len(attachmentsToDelete) {
			att := attachmentsToDelete[i]
			filePath, err := c.getAttachmentFilePath(id, att.ID, att.Name)
			if err == nil {
				os.Remove(filePath) // 忽略删除文件的错误，可能文件已不存在
			}
		}
	}

	// 更新索引（删除索引条目）
	_ = c.updateIndexes(ctx, oldDoc, id, true)

	// 调用 postRemove 钩子
	for _, hook := range c.postRemove {
		if err := hook(ctx, nil, oldDoc); err != nil {
			// 注意：postRemove 钩子失败不会回滚，但会记录错误
		}
	}

	// 在释放锁之前准备变更事件
	changeEvent := ChangeEvent{
		Collection: c.name,
		ID:         id,
		Op:         OperationDelete,
		Doc:        nil,
		Old:        oldDoc,
		Meta:       nil,
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	c.emitChange(changeEvent)

	return nil
}

func (c *collection) All(ctx context.Context) ([]Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	var docs []Document
	err := c.store.Iterate(ctx, c.name, func(k, v []byte) error {
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
	err := c.store.Iterate(ctx, c.name, func(k, v []byte) error {
		count++
		return nil
	})
	return count, err
}

// BulkInsert 批量插入文档。
func (c *collection) BulkInsert(ctx context.Context, docs []map[string]any) ([]Document, error) {
	logrus.WithFields(logrus.Fields{
		"collection": c.name,
		"count":      len(docs),
	}).Debug("Bulk inserting documents")

	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return nil, NewError(ErrorTypeClosed, "collection is closed", nil)
	}

	if len(docs) == 0 {
		c.mu.Unlock()
		return []Document{}, nil
	}

	result := make([]Document, 0, len(docs))
	inserted := make(map[string]map[string]any)
	insertOrder := make([]string, 0, len(docs)) // 保持插入顺序

	// 验证所有文档并准备数据
	for _, doc := range docs {
		// 应用默认值
		ApplyDefaults(c.schema, doc)

		// 调用 preInsert 钩子
		for _, hook := range c.preInsert {
			if err := hook(ctx, doc, nil); err != nil {
				c.mu.Unlock()
				return nil, NewError(ErrorTypeValidation, "preInsert hook failed", err)
			}
		}

		// Schema 验证
		if err := ValidateDocument(c.schema, doc); err != nil {
			c.mu.Unlock()
			return nil, NewError(ErrorTypeValidation, "schema validation failed", err)
		}

		// 验证并提取主键
		if err := c.validatePrimaryKey(doc); err != nil {
			c.mu.Unlock()
			return nil, NewError(ErrorTypeValidation, "primary key validation failed", err)
		}
		idStr, err := c.extractPrimaryKey(doc)
		if err != nil {
			c.mu.Unlock()
			return nil, NewError(ErrorTypeValidation, "failed to extract primary key", err)
		}

		// 设置修订号
		rev, err := c.nextRevision("", doc)
		if err != nil {
			c.mu.Unlock()
			return nil, NewError(ErrorTypeUnknown, "failed to generate revision", err)
		}
		doc[c.schema.RevField] = rev
		inserted[idStr] = doc
		insertOrder = append(insertOrder, idStr) // 记录插入顺序
	}

	// 在单个事务中检查所有文档是否存在并批量写入
	err := c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
		// 检查所有文档是否已存在
		for idStr := range inserted {
			key := bstore.BucketKey(c.name, idStr)
			_, err := txn.Get(key)
			if err == nil {
				return NewError(ErrorTypeAlreadyExists, fmt.Sprintf("document with id %s already exists", idStr), nil).
					WithContext("document_id", idStr)
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}

		// 批量写入所有文档
		for idStr, doc := range inserted {
			// 创建文档副本用于加密
			docForStorage := make(map[string]any)
			docBytes, _ := json.Marshal(doc)
			json.Unmarshal(docBytes, &docForStorage)

			// 加密需要加密的字段
			if len(c.schema.EncryptedFields) > 0 && c.password != "" {
				if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
					return NewError(ErrorTypeEncryption, fmt.Sprintf("failed to encrypt fields for document %s", idStr), err)
				}
			}

			data, err := json.Marshal(docForStorage)
			if err != nil {
				return NewError(ErrorTypeIO, fmt.Sprintf("failed to marshal document %s", idStr), err)
			}
			key := bstore.BucketKey(c.name, idStr)
			if err := txn.Set(key, data); err != nil {
				return NewError(ErrorTypeIO, fmt.Sprintf("failed to write document %s", idStr), err)
			}
		}

		// 批量更新索引
		for idStr, doc := range inserted {
			if err := c.updateIndexesInTx(txn, doc, idStr, false); err != nil {
				return NewError(ErrorTypeIndex, fmt.Sprintf("failed to update indexes for document %s", idStr), err)
			}
		}

		return nil
	})
	if err != nil {
		c.mu.Unlock()
		logrus.WithError(err).WithField("collection", c.name).Error("Bulk insert failed")
		return nil, err
	}

	// 创建 Document 对象并准备变更事件（按原始顺序）
	changeEvents := make([]ChangeEvent, 0, len(inserted))
	for _, idStr := range insertOrder {
		doc := inserted[idStr]
		// 返回的文档使用经过 JSON 序列化/反序列化后的数据（保持类型一致性）
		var returnData map[string]any
		docBytes, _ := json.Marshal(doc)
		returnData = make(map[string]any)
		json.Unmarshal(docBytes, &returnData)

		result = append(result, &document{
			id:         idStr,
			data:       returnData,
			collection: c,
			revField:   c.schema.RevField,
		})
		changeEvents = append(changeEvents, ChangeEvent{
			Collection: c.name,
			ID:         idStr,
			Op:         OperationInsert,
			Doc:        returnData,
			Old:        nil,
			Meta:       map[string]interface{}{"rev": doc[c.schema.RevField]},
		})
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	for _, event := range changeEvents {
		c.emitChange(event)
	}

	logrus.WithFields(logrus.Fields{
		"collection": c.name,
		"count":      len(result),
	}).Info("Bulk insert completed")
	return result, nil
}

// BulkUpsert 批量更新或插入文档。
func (c *collection) BulkUpsert(ctx context.Context, docs []map[string]any) ([]Document, error) {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	result := make([]Document, 0, len(docs))
	toUpsert := make(map[string]map[string]any)
	oldDocs := make(map[string]map[string]any)

	// 获取所有旧文档
	for _, doc := range docs {
		// 验证并提取主键
		if err := c.validatePrimaryKey(doc); err != nil {
			return nil, err
		}
		idStr, err := c.extractPrimaryKey(doc)
		if err != nil {
			return nil, err
		}

		data, err := c.store.Get(ctx, c.name, idStr)
		if err != nil {
			return nil, err
		}
		if data != nil {
			oldDoc := make(map[string]any)
			if err := json.Unmarshal(data, &oldDoc); err != nil {
				return nil, err
			}
			oldDocs[idStr] = oldDoc
		}

		toUpsert[idStr] = doc
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
	err := c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
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
			key := bstore.BucketKey(c.name, idStr)
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to bulk upsert: %w", err)
	}

	// 创建 Document 对象并准备变更事件
	changeEvents := make([]ChangeEvent, 0, len(toUpsert))
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
		changeEvents = append(changeEvents, ChangeEvent{
			Collection: c.name,
			ID:         idStr,
			Op:         op,
			Doc:        doc,
			Old:        oldDocs[idStr],
			Meta:       map[string]interface{}{"rev": doc[c.schema.RevField]},
		})
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	for _, event := range changeEvents {
		c.emitChange(event)
	}

	return result, nil
}

// BulkRemove 批量删除文档。
func (c *collection) BulkRemove(ctx context.Context, ids []string) error {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return errors.New("collection is closed")
	}

	oldDocs := make(map[string]map[string]any)

	// 获取所有旧文档
	for _, id := range ids {
		data, err := c.store.Get(ctx, c.name, id)
		if err != nil {
			return err
		}
		if data != nil {
			oldDoc := make(map[string]any)
			if err := json.Unmarshal(data, &oldDoc); err != nil {
				return err
			}
			oldDocs[id] = oldDoc
		}
	}

	// 批量删除
	err := c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
		for _, id := range ids {
			key := bstore.BucketKey(c.name, id)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to bulk remove: %w", err)
	}

	// 准备变更事件
	changeEvents := make([]ChangeEvent, 0)
	for _, id := range ids {
		if oldDoc, exists := oldDocs[id]; exists {
			changeEvents = append(changeEvents, ChangeEvent{
				Collection: c.name,
				ID:         id,
				Op:         OperationDelete,
				Doc:        nil,
				Old:        oldDoc,
				Meta:       nil,
			})
		}
	}

	// 释放锁后再发送变更事件，避免死锁
	c.mu.Unlock()
	for _, event := range changeEvents {
		c.emitChange(event)
	}

	return nil
}

// ExportJSON 导出集合的所有文档为 JSON 数组。
func (c *collection) ExportJSON(ctx context.Context) ([]map[string]any, error) {
	// 检查 closed 状态
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errors.New("collection is closed")
	}
	collectionName := c.name
	c.mu.RUnlock()

	// store.Iterate 是线程安全的，不需要持有集合锁
	var docs []map[string]any
	err := c.store.Iterate(ctx, collectionName, func(k, v []byte) error {
		var doc map[string]any
		if err := json.Unmarshal(v, &doc); err != nil {
			return err
		}
		docs = append(docs, doc)
		return nil
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
	err := c.store.Iterate(ctx, c.name, func(k, v []byte) error {
		var doc map[string]any
		if err := json.Unmarshal(v, &doc); err != nil {
			return err
		}
		docsToMigrate = append(docsToMigrate, doc)
		return nil
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

		// 迁移每个文档，并更新 docsToMigrate 为迁移后的版本
		migratedDocs := make([]map[string]any, 0, len(docsToMigrate))
		for _, doc := range docsToMigrate {
			migratedDoc, err := strategy(doc)
			if err != nil {
				return fmt.Errorf("migration strategy for version %d failed: %w", version, err)
			}
			migratedDocs = append(migratedDocs, migratedDoc)

			// 更新文档
			id, err := c.extractPrimaryKey(migratedDoc)
			if err != nil {
				return fmt.Errorf("failed to extract primary key: %w", err)
			}

			// 保存迁移后的文档
			data, err := json.Marshal(migratedDoc)
			if err != nil {
				return fmt.Errorf("failed to marshal migrated document: %w", err)
			}
			if err := c.store.Set(ctx, c.name, id, data); err != nil {
				return fmt.Errorf("failed to save migrated document: %w", err)
			}
		}
		// 更新 docsToMigrate 为迁移后的文档，以便下一个版本的迁移使用
		docsToMigrate = migratedDocs
	}

	// 更新存储的版本号
	versionKey := fmt.Sprintf("%s_version", c.name)
	versionData, _ := json.Marshal(toVersion)
	return c.store.Set(ctx, "_meta", versionKey, versionData)
}

// Migrate 手动触发 Schema 迁移
func (c *collection) Migrate(ctx context.Context) error {
	currentVersion := getSchemaVersion(c.schema)
	if currentVersion == 0 {
		return nil // 没有版本信息，无需迁移
	}

	// 获取存储的版本
	storedVersion := 0
	versionKey := fmt.Sprintf("%s_version", c.name)
	data, err := c.store.Get(ctx, "_meta", versionKey)
	if err != nil {
		return fmt.Errorf("failed to read stored version: %w", err)
	}
	if data != nil {
		_ = json.Unmarshal(data, &storedVersion)
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

	// 获取附件元数据
	bucket := fmt.Sprintf("%s_attachments", c.name)
	key := fmt.Sprintf("%s_%s", docID, attachmentID)
	metaData, err := c.store.Get(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if metaData == nil {
		return nil, fmt.Errorf("attachment %s not found for document %s", attachmentID, docID)
	}

	var attachment Attachment
	if err := json.Unmarshal(metaData, &attachment); err != nil {
		return nil, err
	}

	// 从文件系统读取附件数据
	filePath, err := c.getAttachmentFilePath(docID, attachmentID, attachment.Name)
	if err != nil {
		return nil, err
	}

	attachmentData, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("attachment file not found: %s", filePath)
		}
		return nil, fmt.Errorf("failed to read attachment file: %w", err)
	}
	attachment.Data = attachmentData

	return &attachment, nil
}

// PutAttachment 添加或更新文档的附件
func (c *collection) PutAttachment(ctx context.Context, docID string, attachment *Attachment) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 设置时间戳
	now := time.Now().Unix()
	if attachment.Created == 0 {
		attachment.Created = now
	}
	attachment.Modified = now

	// 生成目标文件路径
	targetFilePath, err := c.getAttachmentFilePath(docID, attachment.ID, attachment.Name)
	if err != nil {
		return err
	}

	var attachmentData []byte
	var fileSize int64

	// 优先使用 FilePath，如果提供了文件路径，则从文件系统读取
	if attachment.FilePath != "" {
		// 检查源文件是否存在
		sourceFile, err := os.Open(attachment.FilePath)
		if err != nil {
			return fmt.Errorf("failed to open source file: %w", err)
		}
		defer sourceFile.Close()

		// 获取文件信息
		fileInfo, err := sourceFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to get file info: %w", err)
		}
		fileSize = fileInfo.Size()

		// 如果 Size 未设置，使用文件的实际大小
		if attachment.Size == 0 {
			attachment.Size = fileSize
		}

		// 读取文件数据用于计算哈希值
		attachmentData, err = io.ReadAll(sourceFile)
		if err != nil {
			return fmt.Errorf("failed to read source file: %w", err)
		}

		// 重新打开文件用于拷贝（因为 ReadAll 已经读取到末尾）
		sourceFile.Close()
		sourceFile, err = os.Open(attachment.FilePath)
		if err != nil {
			return fmt.Errorf("failed to reopen source file: %w", err)
		}
		defer sourceFile.Close()

		// 创建目标文件
		targetFile, err := os.Create(targetFilePath)
		if err != nil {
			return fmt.Errorf("failed to create target file: %w", err)
		}

		// 直接拷贝文件（更高效，特别是对于大文件）
		_, err = io.Copy(targetFile, sourceFile)
		targetFile.Close()
		if err != nil {
			os.Remove(targetFilePath) // 清理失败的文件
			return fmt.Errorf("failed to copy file: %w", err)
		}
	} else if len(attachment.Data) > 0 {
		// 如果没有提供 FilePath，使用 Data 字段
		attachmentData = attachment.Data
		fileSize = int64(len(attachmentData))

		// 如果 Size 未设置，使用数据长度
		if attachment.Size == 0 {
			attachment.Size = fileSize
		}

		// 将附件数据写入文件系统
		if err := os.WriteFile(targetFilePath, attachmentData, 0644); err != nil {
			return fmt.Errorf("failed to write attachment file: %w", err)
		}
	} else {
		return errors.New("either FilePath or Data must be provided")
	}

	// 自动计算哈希值
	if len(attachmentData) > 0 {
		// 计算 MD5 哈希值
		hash := md5.Sum(attachmentData)
		attachment.MD5 = hex.EncodeToString(hash[:])

		// 计算 SHA256 哈希值
		hash256 := sha256.Sum256(attachmentData)
		attachment.SHA256 = hex.EncodeToString(hash256[:])

		// 计算摘要（保留向后兼容）
		if c.hashFn != nil {
			attachment.Digest = c.hashFn(attachmentData)
		} else {
			// 如果没有自定义哈希函数，使用 SHA256 作为默认摘要
			attachment.Digest = attachment.SHA256
		}
	}

	// 存储附件元数据（不包含数据和文件路径）
	attachmentMeta := *attachment
	attachmentMeta.Data = nil    // 元数据中不包含数据
	attachmentMeta.FilePath = "" // 文件路径只是输入参数，不存储到元数据

	metaData, err := json.Marshal(attachmentMeta)
	if err != nil {
		// 如果序列化失败，删除已创建的文件
		os.Remove(targetFilePath)
		return err
	}

	bucket := fmt.Sprintf("%s_attachments", c.name)
	key := fmt.Sprintf("%s_%s", docID, attachment.ID)
	if err := c.store.Set(ctx, bucket, key, metaData); err != nil {
		// 如果存储失败，删除已创建的文件
		os.Remove(targetFilePath)
		return err
	}

	return nil
}

// RemoveAttachment 删除文档的附件
func (c *collection) RemoveAttachment(ctx context.Context, docID, attachmentID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("collection is closed")
	}

	// 先获取元数据以获取文件名
	bucket := fmt.Sprintf("%s_attachments", c.name)
	key := fmt.Sprintf("%s_%s", docID, attachmentID)
	metaData, err := c.store.Get(ctx, bucket, key)
	if err == nil && metaData != nil {
		var attachment Attachment
		if err := json.Unmarshal(metaData, &attachment); err == nil {
			// 删除文件系统中的文件
			filePath, err := c.getAttachmentFilePath(docID, attachmentID, attachment.Name)
			if err == nil {
				os.Remove(filePath) // 忽略删除文件的错误，可能文件已不存在
			}
		}
	}

	// 删除数据库中的元数据
	if err := c.store.Delete(ctx, bucket, key); err != nil {
		return err
	}

	return nil
}

// GetAllAttachments 获取文档的所有附件
func (c *collection) GetAllAttachments(ctx context.Context, docID string) ([]*Attachment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.New("collection is closed")
	}

	bucket := fmt.Sprintf("%s_attachments", c.name)
	prefix := docID + "_"

	var attachments []*Attachment
	err := c.store.Iterate(ctx, bucket, func(k, v []byte) error {
		keyStr := string(k)
		// 只处理属于该文档的附件
		if len(keyStr) <= len(prefix) || keyStr[:len(prefix)] != prefix {
			return nil
		}

		var attachment Attachment
		if err := json.Unmarshal(v, &attachment); err != nil {
			return err
		}

		// 从文件系统加载附件数据
		attachmentID := attachment.ID
		filePath, err := c.getAttachmentFilePath(docID, attachmentID, attachment.Name)
		if err == nil {
			attachmentData, err := os.ReadFile(filePath)
			if err == nil {
				attachment.Data = attachmentData
			}
			// 如果文件不存在，Data 保持为 nil
		}

		attachments = append(attachments, &attachment)
		return nil
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
	// 转换为 []any 以保持类型一致性
	docsAny := make([]any, len(docs))
	for i, doc := range docs {
		docsAny[i] = doc
	}
	dump["documents"] = docsAny

	// 导出附件
	attachmentsMap := make(map[string]map[string]*Attachment)
	bucket := fmt.Sprintf("%s_attachments", c.name)
	err = c.store.Iterate(ctx, bucket, func(k, v []byte) error {
		keyStr := string(k)
		// 从 key 中提取 docID 和 attachmentID (格式: docID_attachmentID)
		parts := strings.SplitN(keyStr, "_", 2)
		if len(parts) != 2 {
			return nil
		}
		docID := parts[0]

		var attachment Attachment
		if err := json.Unmarshal(v, &attachment); err != nil {
			return err
		}

		// 注意：不加载附件数据到内存中，只使用元数据
		// 附件数据存储在文件系统中，不会包含在导出的 JSON 中

		if _, exists := attachmentsMap[docID]; !exists {
			attachmentsMap[docID] = make(map[string]*Attachment)
		}
		attachmentsMap[docID][attachment.ID] = &attachment
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 转换附件为 map[string]any 以保持类型一致性
	// 注意：不包含附件数据，只包含元数据（附件数据存储在文件系统中）
	attachmentsAny := make(map[string]any)
	for docID, docAttachments := range attachmentsMap {
		docAttachmentsAny := make(map[string]any)
		for attID, att := range docAttachments {
			// 只导出元数据，不包含 Data 字段（附件数据存储在文件系统中）
			attMap := map[string]any{
				"id":     att.ID,
				"name":   att.Name,
				"type":   att.Type,
				"size":   att.Size,
				"md5":    att.MD5,
				"sha256": att.SHA256,
			}
			if att.Digest != "" {
				attMap["digest"] = att.Digest
			}
			if att.Created > 0 {
				attMap["created"] = att.Created
			}
			if att.Modified > 0 {
				attMap["modified"] = att.Modified
			}
			docAttachmentsAny[attID] = attMap
		}
		attachmentsAny[docID] = docAttachmentsAny
	}
	dump["attachments"] = attachmentsAny

	dump["name"] = c.name
	return dump, nil
}

// ImportDump 导入集合（包含文档和附件）
func (c *collection) ImportDump(ctx context.Context, dump map[string]any) error {
	// 检查 closed 状态
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("collection is closed")
	}
	c.mu.Unlock()

	// 导入文档
	if docsData, ok := dump["documents"].([]any); ok {
		docs := make([]map[string]any, 0, len(docsData))
		for _, doc := range docsData {
			if docMap, ok := doc.(map[string]any); ok {
				docs = append(docs, docMap)
			}
		}
		// ImportJSON 内部会获取锁，所以这里不需要持有锁
		if err := c.ImportJSON(ctx, docs); err != nil {
			return fmt.Errorf("failed to import documents: %w", err)
		}
	}

	// 导入附件
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("collection is closed")
	}
	bucket := fmt.Sprintf("%s_attachments", c.name)
	c.mu.Unlock()

	if attachmentsData, ok := dump["attachments"].(map[string]any); ok {

		for docID, docAttachmentsData := range attachmentsData {
			docAttachmentsMap, ok := docAttachmentsData.(map[string]any)
			if !ok {
				continue
			}

			for attID, attData := range docAttachmentsMap {
				attMap, ok := attData.(map[string]any)
				if !ok {
					continue
				}

				// 解析附件元数据
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
				if md5, ok := attMap["md5"].(string); ok {
					attachment.MD5 = md5
				}
				if sha256, ok := attMap["sha256"].(string); ok {
					attachment.SHA256 = sha256
				}
				if digest, ok := attMap["digest"].(string); ok {
					attachment.Digest = digest
				}
				if created, ok := attMap["created"].(float64); ok {
					attachment.Created = int64(created)
				}
				if modified, ok := attMap["modified"].(float64); ok {
					attachment.Modified = int64(modified)
				}

				// 注意：附件数据应该已经存在于文件系统中
				// 如果 dump 中包含了 data 字段（向后兼容），则从文件系统读取
				// 生成文件路径
				filePath, err := c.getAttachmentFilePath(docID, attID, attachment.Name)
				if err != nil {
					return err
				}

				// 尝试从文件系统读取附件数据
				attachmentData, err := os.ReadFile(filePath)
				if err != nil {
					// 如果文件不存在，检查是否有旧格式的数据（向后兼容）
					if data, ok := attMap["data"].([]byte); ok {
						attachmentData = data
					} else if dataStr, ok := attMap["data"].(string); ok {
						// 处理 base64 编码的数据（向后兼容）
						if decoded, err := base64.StdEncoding.DecodeString(dataStr); err == nil {
							attachmentData = decoded
						} else {
							// 如果不是有效的 base64，作为普通字符串处理
							attachmentData = []byte(dataStr)
						}
					}
					// 如果有数据，写入文件系统
					if len(attachmentData) > 0 {
						if err := os.WriteFile(filePath, attachmentData, 0644); err != nil {
							return fmt.Errorf("failed to write attachment file: %w", err)
						}
						// 重新计算哈希值
						hash := md5.Sum(attachmentData)
						attachment.MD5 = hex.EncodeToString(hash[:])
						hash256 := sha256.Sum256(attachmentData)
						attachment.SHA256 = hex.EncodeToString(hash256[:])
					} else {
						// 文件不存在且没有数据，跳过这个附件
						continue
					}
				} else {
					// 文件存在，验证哈希值（如果提供了）
					if attachment.MD5 != "" || attachment.SHA256 != "" {
						hash := md5.Sum(attachmentData)
						calculatedMD5 := hex.EncodeToString(hash[:])
						hash256 := sha256.Sum256(attachmentData)
						calculatedSHA256 := hex.EncodeToString(hash256[:])

						// 如果哈希值不匹配，使用计算出的值
						if attachment.MD5 != "" && attachment.MD5 != calculatedMD5 {
							attachment.MD5 = calculatedMD5
						}
						if attachment.SHA256 != "" && attachment.SHA256 != calculatedSHA256 {
							attachment.SHA256 = calculatedSHA256
						}
					}
				}

				// 存储附件元数据
				attachmentMeta := *attachment
				attachmentMeta.Data = nil
				metaData, err := json.Marshal(attachmentMeta)
				if err != nil {
					// 如果序列化失败，删除已创建的文件
					os.Remove(filePath)
					return err
				}

				key := fmt.Sprintf("%s_%s", docID, attID)
				if err := c.store.Set(ctx, bucket, key, metaData); err != nil {
					// 如果存储失败，删除已创建的文件
					os.Remove(filePath)
					return err
				}
			}
		}
	}

	return nil
}

// CreateIndex 创建新索引。
func (c *collection) CreateIndex(ctx context.Context, index Index) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("collection is closed")
	}

	// 验证索引
	if len(index.Fields) == 0 {
		return fmt.Errorf("index must have at least one field")
	}

	// 检查索引是否已存在
	indexName := index.Name
	if indexName == "" {
		indexName = strings.Join(index.Fields, "_")
	}

	for _, existingIdx := range c.schema.Indexes {
		existingName := existingIdx.Name
		if existingName == "" {
			existingName = strings.Join(existingIdx.Fields, "_")
		}
		// 检查名称重复
		if existingName == indexName {
			return fmt.Errorf("index %s already exists", indexName)
		}
		// 检查字段重复（即使名称不同，相同字段的索引也不允许）
		if len(existingIdx.Fields) == len(index.Fields) {
			fieldsMatch := true
			for i, field := range index.Fields {
				if existingIdx.Fields[i] != field {
					fieldsMatch = false
					break
				}
			}
			if fieldsMatch {
				return fmt.Errorf("index with fields %v already exists", index.Fields)
			}
		}
	}

	// 构建索引：遍历所有文档并建立索引
	bucketName := fmt.Sprintf("%s_idx_%s", c.name, indexName)
	err := c.store.Iterate(ctx, c.name, func(k, v []byte) error {
		var doc map[string]any
		if err := json.Unmarshal(v, &doc); err != nil {
			return nil // 跳过无效文档
		}

		// 解密字段（如果需要）
		if len(c.schema.EncryptedFields) > 0 && c.password != "" {
			if err := decryptDocumentFields(doc, c.schema.EncryptedFields, c.password); err != nil {
				// 解密失败时继续
			}
		}

		// 构建索引键
		indexKeyParts := make([]interface{}, 0, len(index.Fields))
		for _, field := range index.Fields {
			value := getNestedValue(doc, field)
			indexKeyParts = append(indexKeyParts, value)
		}
		indexKeyBytes, err := json.Marshal(indexKeyParts)
		if err != nil {
			return nil // 跳过无法序列化的文档
		}
		indexKey := string(indexKeyBytes)

		// 更新索引：索引键 -> 文档ID列表
		var docIDs []string
		existing, _ := c.store.Get(ctx, bucketName, indexKey)
		if existing != nil {
			_ = json.Unmarshal(existing, &docIDs)
		}

		docID := string(k)
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
			_ = c.store.Set(ctx, bucketName, indexKey, docIDsBytes)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to build index: %w", err)
	}

	// 将索引添加到 schema
	c.schema.Indexes = append(c.schema.Indexes, index)

	return nil
}

// DropIndex 删除索引。
func (c *collection) DropIndex(ctx context.Context, indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("collection is closed")
	}

	// 查找索引
	var indexToRemove *Index
	var indexIndex int = -1
	for i, idx := range c.schema.Indexes {
		name := idx.Name
		if name == "" {
			name = strings.Join(idx.Fields, "_")
		}
		if name == indexName {
			indexToRemove = &idx
			indexIndex = i
			break
		}
	}

	if indexToRemove == nil {
		return fmt.Errorf("index %s not found", indexName)
	}

	// 删除索引数据（通过迭代删除所有以该索引前缀开头的键）
	bucketName := fmt.Sprintf("%s_idx_%s", c.name, indexName)
	// 收集要删除的键
	var keysToDelete []string
	err := c.store.Iterate(ctx, bucketName, func(k, v []byte) error {
		keysToDelete = append(keysToDelete, string(k))
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to iterate index bucket: %w", err)
	}
	// 删除所有键
	for _, key := range keysToDelete {
		if err := c.store.Delete(ctx, bucketName, key); err != nil {
			return fmt.Errorf("failed to delete index key: %w", err)
		}
	}

	// 从 schema 中移除索引
	c.schema.Indexes = append(c.schema.Indexes[:indexIndex], c.schema.Indexes[indexIndex+1:]...)

	return nil
}

// ListIndexes 返回所有索引列表。
// 注意：这里返回的是 schema.Indexes 的副本，schema 在集合创建后不会改变，
// 但 CreateIndex/DropIndex 会修改 schema.Indexes，所以仍需要锁保护。
func (c *collection) ListIndexes() []Index {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 返回索引的副本
	indexes := make([]Index, len(c.schema.Indexes))
	copy(indexes, c.schema.Indexes)
	return indexes
}
