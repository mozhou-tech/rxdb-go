package rxdb

import (
	"context"
	"encoding/json"
	"fmt"
)

// document 是 Document 接口的默认实现。
type document struct {
	id         string
	data       map[string]any
	collection *collection
	revField   string
	changes    chan ChangeEvent
}

func (d *document) ID() string {
	return d.id
}

func (d *document) Data() map[string]any {
	return d.data
}

// Get 获取指定字段的值。
func (d *document) Get(field string) any {
	return d.data[field]
}

// GetString 获取字符串类型字段。
func (d *document) GetString(field string) string {
	if v, ok := d.data[field].(string); ok {
		return v
	}
	return ""
}

// GetInt 获取整数类型字段。
func (d *document) GetInt(field string) int {
	switch v := d.data[field].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

// GetFloat 获取浮点数类型字段。
func (d *document) GetFloat(field string) float64 {
	switch v := d.data[field].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return 0
}

// GetBool 获取布尔类型字段。
func (d *document) GetBool(field string) bool {
	if v, ok := d.data[field].(bool); ok {
		return v
	}
	return false
}

// GetArray 获取数组类型字段。
func (d *document) GetArray(field string) []any {
	if v, ok := d.data[field].([]any); ok {
		return v
	}
	return nil
}

// GetObject 获取对象类型字段。
func (d *document) GetObject(field string) map[string]any {
	if v, ok := d.data[field].(map[string]any); ok {
		return v
	}
	return nil
}

// Set 设置字段值（不保存到数据库）。
func (d *document) Set(ctx context.Context, field string, value any) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}
	d.data[field] = value
	return nil
}

// Update 更新文档的多个字段并保存到数据库。
func (d *document) Update(ctx context.Context, updates map[string]any) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}

	// 合并更新到当前数据
	for k, v := range updates {
		// 不允许更新主键
		if d.collection.isPrimaryKeyField(k) {
			continue
		}
		d.data[k] = v
	}

	// 保存更新
	return d.Save(ctx)
}

// Remove 删除文档。
func (d *document) Remove(ctx context.Context) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.Remove(ctx, d.id)
}

// Save 保存文档到数据库。
func (d *document) Save(ctx context.Context) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}

	d.collection.mu.Lock()

	if d.collection.closed {
		d.collection.mu.Unlock()
		return fmt.Errorf("collection is closed")
	}

	// 获取旧文档用于变更事件和 final 字段验证
	var oldDoc map[string]any
	var oldDocForIndex map[string]any // 用于索引更新（需要解密）
	oldData, err := d.collection.store.Get(ctx, d.collection.name, d.id)
	if err != nil {
		d.collection.mu.Unlock()
		return err
	}
	if oldData != nil {
		oldDoc = make(map[string]any)
		if err := json.Unmarshal(oldData, &oldDoc); err != nil {
			d.collection.mu.Unlock()
			return err
		}
		// 创建用于索引更新的副本（需要解密）
		oldDocForIndex = make(map[string]any)
		oldDocBytes, _ := json.Marshal(oldDoc)
		json.Unmarshal(oldDocBytes, &oldDocForIndex)
		if len(d.collection.schema.EncryptedFields) > 0 && d.collection.password != "" {
			if err := decryptDocumentFields(oldDocForIndex, d.collection.schema.EncryptedFields, d.collection.password); err != nil {
				// 解密失败时继续，使用原始数据
			}
		}
	}

	// 验证 final 字段（如果文档已存在）
	if oldDoc != nil {
		if err := ValidateFinalFields(d.collection.schema, oldDoc, d.data); err != nil {
			d.collection.mu.Unlock()
			return fmt.Errorf("final field validation failed: %w", err)
		}
	}

	// 调用 preSave 钩子
	for _, hook := range d.collection.preSave {
		if err := hook(ctx, d.data, oldDoc); err != nil {
			d.collection.mu.Unlock()
			return fmt.Errorf("preSave hook failed: %w", err)
		}
	}

	// 获取当前修订号
	var oldRev string
	if oldDoc != nil {
		if rev, ok := oldDoc[d.revField]; ok {
			oldRev = fmt.Sprintf("%v", rev)
		}
	}

	// 计算新修订号
	rev, err := d.collection.nextRevision(oldRev, d.data)
	if err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("failed to generate revision: %w", err)
	}
	d.data[d.revField] = rev
	d.collection.mu.Unlock()

	// 3. 计算密集型操作（锁外进行）
	docForStorage := DeepCloneMap(d.data)
	if len(d.collection.schema.EncryptedFields) > 0 && d.collection.password != "" {
		if err := encryptDocumentFields(docForStorage, d.collection.schema.EncryptedFields, d.collection.password); err != nil {
			return fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	data, err := json.Marshal(docForStorage)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	// 4. 写入阶段
	d.collection.mu.Lock()
	if d.collection.closed {
		d.collection.mu.Unlock()
		return NewError(ErrorTypeClosed, "collection is closed", nil)
	}

	err = d.collection.store.Set(ctx, d.collection.name, d.id, data)
	if err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("failed to save document: %w", err)
	}

	// 更新索引（如果旧文档存在，先删除旧索引）
	if oldDoc != nil {
		// 解密旧文档的加密字段（如果需要），以便正确构建旧索引键
		oldDocForIndex := DeepCloneMap(oldDoc)
		if len(d.collection.schema.EncryptedFields) > 0 && d.collection.password != "" {
			if err := decryptDocumentFields(oldDocForIndex, d.collection.schema.EncryptedFields, d.collection.password); err != nil {
				// 解密失败时继续，使用原始数据
			}
		}
		_ = d.collection.updateIndexes(ctx, oldDocForIndex, d.id, true)
	}
	_ = d.collection.updateIndexes(ctx, d.data, d.id, false)

	// 调用 postSave 钩子
	for _, hook := range d.collection.postSave {
		if err := hook(ctx, d.data, oldDoc); err != nil {
			// 注意：postSave 钩子失败不会回滚，但会记录错误
		}
	}

	// 在释放锁之前准备变更事件
	op := OperationInsert
	if oldDoc != nil {
		op = OperationUpdate
	}
	changeEvent := ChangeEvent{
		Collection: d.collection.name,
		ID:         d.id,
		Op:         op,
		Doc:        d.data,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	}

	// 释放锁后再发送变更事件，避免死锁
	d.collection.mu.Unlock()
	d.collection.emitChange(changeEvent)

	return nil
}

// Changes 返回文档的变更事件通道。
func (d *document) Changes() <-chan ChangeEvent {
	if d.collection == nil {
		// 返回一个已关闭的空通道
		ch := make(chan ChangeEvent)
		close(ch)
		return ch
	}
	return d.collection.Changes()
}

// ToJSON 将文档转换为 JSON 字节数组。
func (d *document) ToJSON() ([]byte, error) {
	return json.Marshal(d.data)
}

// ToMutableJSON 返回文档数据的深拷贝，便于安全修改。
func (d *document) ToMutableJSON() (map[string]any, error) {
	return DeepCloneMap(d.data), nil
}

// Deleted 检查文档是否已删除。
func (d *document) Deleted(ctx context.Context) (bool, error) {
	if d.collection == nil {
		return false, fmt.Errorf("document is not associated with a collection")
	}

	d.collection.mu.RLock()
	defer d.collection.mu.RUnlock()

	if d.collection.closed {
		return false, fmt.Errorf("collection is closed")
	}

	data, err := d.collection.store.Get(ctx, d.collection.name, d.id)
	if err != nil {
		return false, err
	}

	return data == nil, nil
}

// AtomicUpdate 原子更新文档，使用更新函数。
func (d *document) AtomicUpdate(ctx context.Context, updateFn func(doc map[string]any) error) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}

	d.collection.mu.Lock()

	if d.collection.closed {
		d.collection.mu.Unlock()
		return fmt.Errorf("collection is closed")
	}

	// 读取当前文档
	currentData, err := d.collection.store.Get(ctx, d.collection.name, d.id)
	if err != nil {
		d.collection.mu.Unlock()
		return err
	}
	if currentData == nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("document with id %s not found", d.id)
	}
	currentDoc := make(map[string]any)
	if err := json.Unmarshal(currentData, &currentDoc); err != nil {
		d.collection.mu.Unlock()
		return err
	}

	// 应用更新函数
	if err := updateFn(currentDoc); err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("update function failed: %w", err)
	}

	// 不允许更新主键：确保所有主键字段保持不变
	fields := d.collection.getPrimaryKeyFields()
	for _, field := range fields {
		// 从原始数据恢复主键值（如果被修改了）
		if originalValue, ok := d.data[field]; ok {
			currentDoc[field] = originalValue
		}
	}

	// 更新修订号
	var oldRev string
	if rev, ok := currentDoc[d.revField]; ok {
		oldRev = fmt.Sprintf("%v", rev)
	}
	rev, err := d.collection.nextRevision(oldRev, currentDoc)
	if err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("failed to generate revision: %w", err)
	}
	currentDoc[d.revField] = rev

	// 保存旧文档用于变更事件
	oldDoc := make(map[string]any)
	oldDocBytes, _ := json.Marshal(d.data)
	json.Unmarshal(oldDocBytes, &oldDoc)

	// 原子写入 - 再次检查文档是否存在（乐观锁）
	existingData, err := d.collection.store.Get(ctx, d.collection.name, d.id)
	if err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("failed to atomic update document: %w", err)
	}
	if existingData == nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("document with id %s was deleted", d.id)
	}

	// 检查修订号是否匹配
	var existingDoc map[string]any
	if err := json.Unmarshal(existingData, &existingDoc); err != nil {
		d.collection.mu.Unlock()
		return err
	}

	// 读取存储中的旧文档（用于索引更新，需要解密）
	oldDocForIndex := make(map[string]any)
	oldDocBytesFromStorage, _ := json.Marshal(existingDoc)
	json.Unmarshal(oldDocBytesFromStorage, &oldDocForIndex)
	if len(d.collection.schema.EncryptedFields) > 0 && d.collection.password != "" {
		if err := decryptDocumentFields(oldDocForIndex, d.collection.schema.EncryptedFields, d.collection.password); err != nil {
			// 解密失败时继续，使用原始数据
		}
	}
	if existingRev, ok := existingDoc[d.revField]; ok {
		if fmt.Sprintf("%v", existingRev) != oldRev {
			d.collection.mu.Unlock()
			return fmt.Errorf("document revision mismatch: expected %s, got %v", oldRev, existingRev)
		}
	}

	// 创建文档副本用于加密
	docForStorage := make(map[string]any)
	docBytes, _ := json.Marshal(currentDoc)
	json.Unmarshal(docBytes, &docForStorage)

	// 加密需要加密的字段
	if len(d.collection.schema.EncryptedFields) > 0 && d.collection.password != "" {
		if err := encryptDocumentFields(docForStorage, d.collection.schema.EncryptedFields, d.collection.password); err != nil {
			d.collection.mu.Unlock()
			return fmt.Errorf("failed to encrypt fields: %w", err)
		}
	}

	// 写入更新后的文档（使用加密后的副本）
	newData, err := json.Marshal(docForStorage)
	if err != nil {
		d.collection.mu.Unlock()
		return err
	}
	err = d.collection.store.Set(ctx, d.collection.name, d.id, newData)
	if err != nil {
		d.collection.mu.Unlock()
		return fmt.Errorf("failed to atomic update document: %w", err)
	}

	// 更新本地数据
	d.data = currentDoc

	// 更新索引（先删除旧索引，再添加新索引）
	_ = d.collection.updateIndexes(ctx, oldDocForIndex, d.id, true)
	_ = d.collection.updateIndexes(ctx, currentDoc, d.id, false)

	// 在释放锁之前准备变更事件
	changeEvent := ChangeEvent{
		Collection: d.collection.name,
		ID:         d.id,
		Op:         OperationUpdate,
		Doc:        currentDoc,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	}

	// 释放锁后再发送变更事件，避免死锁
	d.collection.mu.Unlock()
	d.collection.emitChange(changeEvent)

	return nil
}

// AtomicPatch 原子补丁更新文档，合并补丁数据。
func (d *document) AtomicPatch(ctx context.Context, patch map[string]any) error {
	return d.AtomicUpdate(ctx, func(doc map[string]any) error {
		// 深度合并补丁数据
		deepMerge(doc, patch, d.collection)
		return nil
	})
}

// deepMerge 深度合并两个 map，src 合并到 dst 中
func deepMerge(dst, src map[string]any, col *collection) {
	for k, v := range src {
		// 不允许更新主键
		if col != nil && col.isPrimaryKeyField(k) {
			continue
		}

		// 如果值是 map，尝试深度合并
		if srcMap, ok := v.(map[string]any); ok {
			if dstMap, ok := dst[k].(map[string]any); ok {
				// 两边都是 map，深度合并
				deepMerge(dstMap, srcMap, nil)
				continue
			}
		}

		// 其他情况直接替换
		dst[k] = v
	}
}

// IncrementalModify 以增量方式修改文档，内部复用原子更新。
func (d *document) IncrementalModify(ctx context.Context, modifier func(doc map[string]any) error) error {
	return d.AtomicUpdate(ctx, modifier)
}

// IncrementalPatch 以增量补丁方式修改文档。
func (d *document) IncrementalPatch(ctx context.Context, patch map[string]any) error {
	return d.AtomicPatch(ctx, patch)
}

// GetFieldChanges 返回指定字段的变更事件通道。
func (d *document) GetFieldChanges(ctx context.Context, field string) <-chan FieldChangeEvent {
	if d.collection == nil {
		// 返回一个已关闭的空通道
		ch := make(chan FieldChangeEvent)
		close(ch)
		return ch
	}

	fieldChanges := make(chan FieldChangeEvent, 10)

	go func() {
		defer close(fieldChanges)
		for event := range d.collection.Changes() {
			// 只关注当前文档的变更
			if event.ID != d.id {
				continue
			}

			// 检查字段是否发生变化
			var oldVal, newVal interface{}
			if event.Old != nil {
				oldVal = event.Old[field]
			}
			if event.Doc != nil {
				newVal = event.Doc[field]
			}

			// 如果值发生变化，发送事件
			if oldVal != newVal {
				select {
				case fieldChanges <- FieldChangeEvent{
					Field: field,
					Old:   oldVal,
					New:   newVal,
				}:
				case <-ctx.Done():
					return
				case <-d.collection.closeChan:
					return
				}
			}
		}
	}()

	return fieldChanges
}

// GetAttachment 获取文档的附件
func (d *document) GetAttachment(ctx context.Context, attachmentID string) (*Attachment, error) {
	if d.collection == nil {
		return nil, fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.GetAttachment(ctx, d.id, attachmentID)
}

// PutAttachment 添加或更新文档的附件
func (d *document) PutAttachment(ctx context.Context, attachment *Attachment) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.PutAttachment(ctx, d.id, attachment)
}

// RemoveAttachment 删除文档的附件
func (d *document) RemoveAttachment(ctx context.Context, attachmentID string) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.RemoveAttachment(ctx, d.id, attachmentID)
}

// Synced 返回文档的同步状态通道。
func (d *document) Synced(ctx context.Context) <-chan bool {
	if d.collection == nil {
		ch := make(chan bool, 1)
		ch <- true
		close(ch)
		return ch
	}
	return d.collection.Synced(ctx)
}

// Resync 重新从同步源拉取当前文档。
func (d *document) Resync(ctx context.Context) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}

	d.collection.mu.RLock()
	handlers := make([]func(context.Context, string) error, len(d.collection.resyncHandlers))
	copy(handlers, d.collection.resyncHandlers)
	d.collection.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler(ctx, d.id); err != nil {
			return err
		}
	}

	return nil
}

// Populate 填充关联文档。
func (d *document) Populate(ctx context.Context, field string) (Document, error) {
	if d.collection == nil || d.collection.db == nil {
		return nil, fmt.Errorf("document is not associated with a database")
	}

	// 获取关联文档 ID
	refID := d.GetString(field)
	if refID == "" {
		return nil, nil
	}

	// 从 Schema 中查找关联集合
	refCollectionName := d.collection.getRefCollection(field)
	if refCollectionName == "" {
		return nil, fmt.Errorf("field %s is not a reference", field)
	}

	// 获取关联集合
	// 注意：这里需要知道关联集合的 schema，或者如果已经创建过则复用
	// 在 RxDB 中，Populate 通常在关联集合已存在时工作
	refCol, err := d.collection.db.Collection(ctx, refCollectionName, Schema{})
	if err != nil {
		return nil, fmt.Errorf("failed to get reference collection %s: %w", refCollectionName, err)
	}

	// 查找关联文档
	return refCol.FindByID(ctx, refID)
}

// GetAllAttachments 获取文档的所有附件
func (d *document) GetAllAttachments(ctx context.Context) ([]*Attachment, error) {
	if d.collection == nil {
		return nil, fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.GetAllAttachments(ctx, d.id)
}
