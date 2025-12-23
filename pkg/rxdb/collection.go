package rxdb

import (
	"bytes"
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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	bstore "github.com/mozhou-tech/rxdb-go/pkg/storage/badger"
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
	db          Database // 关联的数据库
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

	// 操作计数回调（用于 RequestIdle）
	beginOp func(ctx context.Context) error
	endOp   func()

	// 钩子函数
	preInsert  []HookFunc
	postInsert []HookFunc
	preSave    []HookFunc
	postSave   []HookFunc
	preRemove  []HookFunc
	postRemove []HookFunc
	preCreate  []HookFunc
	postCreate []HookFunc

	// 同步处理
	resyncHandlers     []func(ctx context.Context, docID string) error
	syncStatusHandlers []func() bool
}

func newCollection(ctx context.Context, db Database, store *bstore.Store, name string, schema Schema, hashFn func([]byte) string, broadcaster *eventBroadcaster, password string, dbEventCallback func(event ChangeEvent), beginOp func(ctx context.Context) error, endOp func()) (*collection, error) {
	logrus.WithField("name", name).Debug("Creating collection")

	col := &collection{
		name:            name,
		schema:          schema,
		store:           store,
		db:              db,
		changes:         make(chan ChangeEvent, 100),
		closeChan:       make(chan struct{}),
		hashFn:          hashFn,
		broadcaster:     broadcaster,
		password:        password,
		subscribers:     make(map[uint64]chan ChangeEvent),
		dbEventCallback: dbEventCallback,
		beginOp:         beginOp,
		endOp:           endOp,
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
// RegisterResyncHandler 注册重新同步处理函数。
func (c *collection) RegisterResyncHandler(handler func(ctx context.Context, docID string) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resyncHandlers = append(c.resyncHandlers, handler)
}

// RegisterSyncStatusHandler 注册同步状态处理函数。
func (c *collection) RegisterSyncStatusHandler(handler func() bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncStatusHandlers = append(c.syncStatusHandlers, handler)
}

// Synced 返回一个通道，该通道会定期发送当前的同步状态（true 表示已同步）。
func (c *collection) Synced(ctx context.Context) <-chan bool {
	ch := make(chan bool, 1)

	go func() {
		defer close(ch)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			c.mu.RLock()
			handlers := make([]func() bool, len(c.syncStatusHandlers))
			copy(handlers, c.syncStatusHandlers)
			c.mu.RUnlock()

			synced := true
			if len(handlers) == 0 {
				synced = true // 如果没有同步器，默认视为已同步
			} else {
				for _, h := range handlers {
					if !h() {
						synced = false
						break
					}
				}
			}

			select {
			case ch <- synced:
			default:
				// 通道满时跳过
			}

			select {
			case <-ctx.Done():
				return
			case <-c.closeChan:
				return
			case <-ticker.C:
			}
		}
	}()

	return ch
}

// getRefCollection 从 Schema 中获取指定字段的关联集合名称。
func (c *collection) getRefCollection(field string) string {
	if c.schema.JSON == nil {
		return ""
	}

	properties, ok := c.schema.JSON["properties"].(map[string]any)
	if !ok {
		return ""
	}

	fieldSchema, ok := properties[field].(map[string]any)
	if !ok {
		return ""
	}

	ref, ok := fieldSchema["ref"].(string)
	if !ok {
		return ""
	}

	return ref
}

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
		// 使用新的编码方式：{values}\0{docID}，避免序列化开销并支持前缀扫描
		indexKey := bstore.BucketKey(bucketName, string(encodeIndexKey(indexKeyParts, docID)))

		if isDelete {
			_ = txn.Delete(indexKey)
		} else {
			_ = txn.Set(indexKey, nil)
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
	if !strings.Contains(path, ".") {
		return doc[path]
	}
	parts := strings.Split(path, ".")
	return getNestedValueByParts(doc, parts)
}

// getNestedValueByParts 使用预拆分路径获取嵌套字段值（高性能版）。
func getNestedValueByParts(doc map[string]any, parts []string) any {
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

// Insert 向集合中插入一个新文档。
func (c *collection) Insert(ctx context.Context, doc map[string]any) (Document, error) {
	if doc == nil {
		return nil, errors.New("document cannot be nil")
	}

	// 1. 无需锁的准备阶段：应用默认值和基础验证
	ApplyDefaults(c.schema, doc)
	if err := ValidateDocument(c.schema, doc); err != nil {
		return nil, NewError(ErrorTypeValidation, "schema validation failed", err)
	}
	if err := c.validatePrimaryKey(doc); err != nil {
		return nil, err
	}
	idStr, err := c.extractPrimaryKey(doc)
	if err != nil {
		return nil, err
	}

	// 1.2 轻量级预检：避免后续无效的加密/克隆
	existing, _ := c.store.Get(ctx, c.name, idStr)
	if existing != nil {
		return nil, NewError(ErrorTypeAlreadyExists, fmt.Sprintf("document with id %s already exists", idStr), nil).
			WithContext("document_id", idStr)
	}

	if err := c.beginOp(ctx); err != nil {
		return nil, err
	}
	defer c.endOp()

	logrus.WithField("collection", c.name).Debug("Inserting document into collection")

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	// 2. 调用前置钩子
	for _, hook := range c.preInsert {
		if err := hook(ctx, doc, nil); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("preInsert hook failed: %w", err)
		}
	}

	// 调用 preSave 钩子
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
	c.mu.Unlock()

	// 3. 计算密集型操作：克隆、加密、序列化（在锁外进行）
	docForStorage := DeepCloneMap(doc)

	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	data, err := json.Marshal(docForStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 4. 写入阶段：重新加锁执行存储写入和索引更新
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	// 检查文档是否已存在并写入
	existingData, err := c.store.Get(ctx, c.name, idStr)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if existingData != nil {
		c.mu.Unlock()
		return nil, NewError(ErrorTypeAlreadyExists, fmt.Sprintf("document with id %s already exists", idStr), nil).
			WithContext("document_id", idStr)
	}

	err = c.store.Set(ctx, c.name, idStr, data)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to insert document: %w", err)
	}

	// 更新索引
	_ = c.updateIndexes(ctx, doc, idStr, false)

	// 准备返回数据
	var returnData map[string]any
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		returnData = DeepCloneMap(doc)
	} else {
		returnData = docForStorage
	}

	result := acquireDocument(idStr, returnData, c)

	// 准备变更事件
	changeEvent := ChangeEvent{
		Collection: c.name,
		ID:         idStr,
		Op:         OperationInsert,
		Doc:        doc,
		Old:        nil,
		Meta:       map[string]interface{}{"rev": rev},
	}

	c.mu.Unlock()

	// 5. 后置处理：在释放锁后调用钩子和发送事件
	for _, hook := range c.postSave {
		_ = hook(ctx, doc, nil)
	}
	for _, hook := range c.postInsert {
		_ = hook(ctx, doc, nil)
	}
	c.emitChange(changeEvent)

	return result, nil
}

func (c *collection) Upsert(ctx context.Context, doc map[string]any) (Document, error) {
	if err := c.beginOp(ctx); err != nil {
		return nil, err
	}
	defer c.endOp()

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
	c.mu.Unlock()

	// 2. 计算密集型操作（锁外进行）
	docForStorage := DeepCloneMap(doc)
	if len(c.schema.EncryptedFields) > 0 && c.password != "" {
		if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
			return nil, fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	data, err := json.Marshal(docForStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// 3. 写入阶段
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

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

	result := acquireDocument(idStr, doc, c)

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
	if err := c.beginOp(ctx); err != nil {
		return nil, err
	}
	defer c.endOp()

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

	return acquireDocument(id, doc, c), nil
}

func (c *collection) Remove(ctx context.Context, id string) error {
	if err := c.beginOp(ctx); err != nil {
		return err
	}
	defer c.endOp()

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
	if err := c.beginOp(ctx); err != nil {
		return nil, err
	}
	defer c.endOp()

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
		docs = append(docs, acquireDocument(string(k), doc, c))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return docs, nil
}

// Count 返回集合中的文档总数。
func (c *collection) Count(ctx context.Context) (int, error) {
	if err := c.beginOp(ctx); err != nil {
		return 0, err
	}
	defer c.endOp()

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

	if len(docs) == 0 {
		return []Document{}, nil
	}

	// 1. 并发预处理阶段 (锁外进行)：应用默认值、验证、提取主键、生成修订号
	// 这些操作是 CPU 密集型的，并行化可以显著提高大批量性能
	type preppedResult struct {
		idStr string
		doc   map[string]any
		err   error
	}

	preppedResults := make([]preppedResult, len(docs))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(docs) {
		numWorkers = len(docs)
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < len(docs); j += numWorkers {
				doc := docs[j]
				ApplyDefaults(c.schema, doc)
				if err := ValidateDocument(c.schema, doc); err != nil {
					preppedResults[j].err = NewError(ErrorTypeValidation, "schema validation failed", err)
					continue
				}
				if err := c.validatePrimaryKey(doc); err != nil {
					preppedResults[j].err = NewError(ErrorTypeValidation, "primary key validation failed", err)
					continue
				}
				idStr, err := c.extractPrimaryKey(doc)
				if err != nil {
					preppedResults[j].err = NewError(ErrorTypeValidation, "failed to extract primary key", err)
					continue
				}
				rev, err := c.nextRevision("", doc)
				if err != nil {
					preppedResults[j].err = NewError(ErrorTypeUnknown, "failed to generate revision", err)
					continue
				}
				doc[c.schema.RevField] = rev
				preppedResults[j].idStr = idStr
				preppedResults[j].doc = doc
			}
		}(i)
	}
	wg.Wait()

	// 检查预处理错误
	for _, res := range preppedResults {
		if res.err != nil {
			return nil, res.err
		}
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, NewError(ErrorTypeClosed, "collection is closed", nil)
	}

	// 2. 调用钩子（锁内进行，保证钩子执行的顺序性和安全性）
	for _, res := range preppedResults {
		for _, hook := range c.preInsert {
			if err := hook(ctx, res.doc, nil); err != nil {
				c.mu.Unlock()
				return nil, NewError(ErrorTypeValidation, "preInsert hook failed", err)
			}
		}
	}
	c.mu.Unlock()

	// 3. 并发计算密集型操作 (锁外进行)：克隆、加密、JSON 序列化
	type writeResult struct {
		idStr string
		data  []byte
		doc   map[string]any
		err   error
	}
	writeResults := make([]writeResult, len(preppedResults))
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < len(preppedResults); j += numWorkers {
				res := preppedResults[j]
				docForStorage := DeepCloneMap(res.doc)
				if len(c.schema.EncryptedFields) > 0 && c.password != "" {
					if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
						writeResults[j].err = NewError(ErrorTypeEncryption, fmt.Sprintf("failed to encrypt fields for document %s", res.idStr), err)
						continue
					}
				}
				data, err := json.Marshal(docForStorage)
				if err != nil {
					writeResults[j].err = NewError(ErrorTypeIO, fmt.Sprintf("failed to marshal document %s", res.idStr), err)
					continue
				}
				writeResults[j] = writeResult{idStr: res.idStr, data: data, doc: res.doc}
			}
		}(i)
	}
	wg.Wait()

	for _, res := range writeResults {
		if res.err != nil {
			return nil, res.err
		}
	}

	// 4. 存储写入阶段 (使用 Badger 事务保证原子性)
	err := c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
		// 批量检查和写入
		for _, item := range writeResults {
			key := bstore.BucketKey(c.name, item.idStr)
			if _, err := txn.Get(key); err == nil {
				return NewError(ErrorTypeAlreadyExists, fmt.Sprintf("document with id %s already exists", item.idStr), nil).
					WithContext("document_id", item.idStr)
			}
			if err := txn.Set(key, item.data); err != nil {
				return NewError(ErrorTypeIO, fmt.Sprintf("failed to write document %s", item.idStr), err)
			}
			// 批量更新索引
			if err := c.updateIndexesInTx(txn, item.doc, item.idStr, false); err != nil {
				return NewError(ErrorTypeIndex, fmt.Sprintf("failed to update indexes for document %s", item.idStr), err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 5. 准备返回结果和变更事件
	result := make([]Document, len(writeResults))
	changeEvents := make([]ChangeEvent, len(writeResults))
	for i, res := range writeResults {
		returnData := DeepCloneMap(res.doc)
		result[i] = acquireDocument(res.idStr, returnData, c)
		changeEvents[i] = ChangeEvent{
			Collection: c.name,
			ID:         res.idStr,
			Op:         OperationInsert,
			Doc:        returnData,
			Old:        nil,
			Meta:       map[string]interface{}{"rev": res.doc[c.schema.RevField]},
		}
	}

	// 6. 后置处理 (锁外发送事件)
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
	if len(docs) == 0 {
		return []Document{}, nil
	}

	// 1. 并发处理：验证、提取主键、获取旧文档
	type upsertItem struct {
		idStr  string
		doc    map[string]any
		oldDoc map[string]any
		err    error
	}

	items := make([]upsertItem, len(docs))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(docs) {
		numWorkers = len(docs)
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < len(docs); j += numWorkers {
				doc := docs[j]
				// 验证并提取主键
				if err := c.validatePrimaryKey(doc); err != nil {
					items[j].err = err
					continue
				}
				idStr, err := c.extractPrimaryKey(doc)
				if err != nil {
					items[j].err = err
					continue
				}

				// 获取旧文档 (这里是磁盘 I/O，可以并行)
				data, err := c.store.Get(ctx, c.name, idStr)
				if err != nil {
					items[j].err = err
					continue
				}

				var oldDoc map[string]any
				if data != nil {
					oldDoc = make(map[string]any)
					if err := json.Unmarshal(data, &oldDoc); err != nil {
						items[j].err = err
						continue
					}
				}

				items[j] = upsertItem{idStr: idStr, doc: doc, oldDoc: oldDoc}
			}
		}(i)
	}
	wg.Wait()

	// 检查第一阶段错误
	for _, item := range items {
		if item.err != nil {
			return nil, item.err
		}
	}

	// 2. 准备数据和修订号
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("collection is closed")
	}

	for i := range items {
		item := &items[i]
		// Schema 验证
		if err := ValidateDocument(c.schema, item.doc); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("schema validation failed for doc %s: %w", item.idStr, err)
		}

		oldRev := ""
		if item.oldDoc != nil {
			// 验证 final 字段
			if err := ValidateFinalFields(c.schema, item.oldDoc, item.doc); err != nil {
				c.mu.Unlock()
				return nil, fmt.Errorf("final field validation failed for doc %s: %w", item.idStr, err)
			}
			if prev, ok := item.oldDoc[c.schema.RevField]; ok {
				oldRev = fmt.Sprintf("%v", prev)
			}
		} else {
			// 新文档应用默认值
			ApplyDefaults(c.schema, item.doc)
		}

		// 调用 preSave 钩子
		for _, hook := range c.preSave {
			if err := hook(ctx, item.doc, item.oldDoc); err != nil {
				c.mu.Unlock()
				return nil, fmt.Errorf("preSave hook failed: %w", err)
			}
		}

		rev, err := c.nextRevision(oldRev, item.doc)
		if err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("failed to generate revision: %w", err)
		}
		item.doc[c.schema.RevField] = rev
	}
	c.mu.Unlock()

	// 3. 并发准备写入数据 (克隆、加密、序列化)
	type writeData struct {
		idStr  string
		data   []byte
		doc    map[string]any
		oldDoc map[string]any
		err    error
	}
	toWrite := make([]writeData, len(items))
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < len(items); j += numWorkers {
				item := items[j]
				docForStorage := DeepCloneMap(item.doc)
				if len(c.schema.EncryptedFields) > 0 && c.password != "" {
					if err := encryptDocumentFields(docForStorage, c.schema.EncryptedFields, c.password); err != nil {
						toWrite[j].err = fmt.Errorf("failed to encrypt fields for document %s: %w", item.idStr, err)
						continue
					}
				}

				data, err := json.Marshal(docForStorage)
				if err != nil {
					toWrite[j].err = fmt.Errorf("failed to marshal document %s: %w", item.idStr, err)
					continue
				}
				toWrite[j] = writeData{idStr: item.idStr, data: data, doc: item.doc, oldDoc: item.oldDoc}
			}
		}(i)
	}
	wg.Wait()

	for _, w := range toWrite {
		if w.err != nil {
			return nil, w.err
		}
	}

	// 4. 执行批量写入
	err := c.store.WithUpdate(ctx, func(txn *badger.Txn) error {
		for _, item := range toWrite {
			key := bstore.BucketKey(c.name, item.idStr)
			if err := txn.Set(key, item.data); err != nil {
				return err
			}
			// 更新索引
			if item.oldDoc != nil {
				if err := c.updateIndexesInTx(txn, item.oldDoc, item.idStr, true); err != nil {
					return err
				}
			}
			if err := c.updateIndexesInTx(txn, item.doc, item.idStr, false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to bulk upsert: %w", err)
	}

	// 5. 准备结果和变更事件
	result := make([]Document, len(toWrite))
	changeEvents := make([]ChangeEvent, len(toWrite))
	for i, item := range toWrite {
		result[i] = acquireDocument(item.idStr, item.doc, c)

		op := OperationInsert
		if item.oldDoc != nil {
			op = OperationUpdate
		}
		changeEvents[i] = ChangeEvent{
			Collection: c.name,
			ID:         item.idStr,
			Op:         op,
			Doc:        item.doc,
			Old:        item.oldDoc,
			Meta:       map[string]interface{}{"rev": item.doc[c.schema.RevField]},
		}
	}

	// 6. 发送变更事件
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

	// 取消解密逻辑，直接返回原始数据，压榨 CPU
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

	// 确保目标目录存在
	if err := os.MkdirAll(filepath.Dir(targetFilePath), 0755); err != nil {
		return fmt.Errorf("failed to create attachment directory: %w", err)
	}

	var source io.Reader
	var closer io.Closer

	if attachment.FilePath != "" {
		// 如果提供了文件路径，打开文件进行流式读取
		f, err := os.Open(attachment.FilePath)
		if err != nil {
			return fmt.Errorf("failed to open source file: %w", err)
		}
		source = f
		closer = f

		// 获取文件大小
		if info, err := f.Stat(); err == nil && attachment.Size == 0 {
			attachment.Size = info.Size()
		}
	} else if len(attachment.Data) > 0 {
		// 如果提供了内存数据，使用 bytes.Reader
		source = bytes.NewReader(attachment.Data)
		if attachment.Size == 0 {
			attachment.Size = int64(len(attachment.Data))
		}
	} else {
		return errors.New("either FilePath or Data must be provided")
	}

	if closer != nil {
		defer closer.Close()
	}

	// 创建目标文件
	targetFile, err := os.Create(targetFilePath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	// 初始化哈希计算器，实现流式哈希计算
	md5Hash := md5.New()
	sha256Hash := sha256.New()

	// 使用 MultiWriter 同时向文件写入数据并计算哈希，避免多次读取
	mw := io.MultiWriter(targetFile, md5Hash, sha256Hash)

	written, err := io.Copy(mw, source)
	if err != nil {
		os.Remove(targetFilePath)
		return fmt.Errorf("failed to write attachment: %w", err)
	}

	// 更新元数据
	attachment.Size = written
	attachment.MD5 = hex.EncodeToString(md5Hash.Sum(nil))
	attachment.SHA256 = hex.EncodeToString(sha256Hash.Sum(nil))

	// 设置摘要（优先使用 SHA256）
	if c.hashFn != nil && len(attachment.Data) > 0 {
		attachment.Digest = c.hashFn(attachment.Data)
	} else {
		attachment.Digest = attachment.SHA256
	}

	// 存储附件元数据（不包含数据内容，实现内存压榨）
	attachmentMeta := *attachment
	attachmentMeta.Data = nil    // 元数据中不包含大数据内容
	attachmentMeta.FilePath = "" // 仅作为输入参数，不持久化

	metaData, err := json.Marshal(attachmentMeta)
	if err != nil {
		os.Remove(targetFilePath)
		return err
	}

	bucket := fmt.Sprintf("%s_attachments", c.name)
	key := fmt.Sprintf("%s_%s", docID, attachment.ID)
	if err := c.store.Set(ctx, bucket, key, metaData); err != nil {
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
	// 包含附件数据（base64 编码）以便 dump 是自包含的
	attachmentsAny := make(map[string]any)
	for docID, docAttachments := range attachmentsMap {
		docAttachmentsAny := make(map[string]any)
		for attID, att := range docAttachments {
			// 读取附件数据
			filePath, err := c.getAttachmentFilePath(docID, att.ID, att.Name)
			var attachmentData []byte
			if err == nil {
				// 尝试从文件系统读取附件数据
				if data, readErr := os.ReadFile(filePath); readErr == nil {
					attachmentData = data
				}
			}

			// 导出元数据和数据
			attMap := map[string]any{
				"id":     att.ID,
				"name":   att.Name,
				"type":   att.Type,
				"size":   att.Size,
				"md5":    att.MD5,
				"sha256": att.SHA256,
			}
			// 如果成功读取了附件数据，将其 base64 编码后包含在 dump 中
			if len(attachmentData) > 0 {
				attMap["data"] = base64.StdEncoding.EncodeToString(attachmentData)
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

		docID := string(k)
		indexKey := encodeIndexKey(indexKeyParts, docID)

		// 直接设置索引键，无需读取旧列表
		_ = c.store.Set(ctx, bucketName, string(indexKey), nil)

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
