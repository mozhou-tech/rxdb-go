package rxdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mozy/rxdb-go/pkg/storage/bolt"
	bbolt "go.etcd.io/bbolt"
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
	defer d.collection.mu.Unlock()

	if d.collection.closed {
		return fmt.Errorf("collection is closed")
	}

	// 获取旧文档用于变更事件和 final 字段验证
	var oldDoc map[string]any
	err := d.collection.store.WithView(ctx, func(tx interface{}) error {
		bboltTx := tx.(*bbolt.Tx)
		bucket := bboltTx.Bucket([]byte(d.collection.name))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(d.id))
		if data != nil {
			oldDoc = make(map[string]any)
			if err := json.Unmarshal(data, &oldDoc); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 验证 final 字段（如果文档已存在）
	if oldDoc != nil {
		if err := ValidateFinalFields(d.collection.schema, oldDoc, d.data); err != nil {
			return fmt.Errorf("final field validation failed: %w", err)
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
		return fmt.Errorf("failed to generate revision: %w", err)
	}
	d.data[d.revField] = rev

	// 序列化文档
	data, err := json.Marshal(d.data)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	// 写入文档
	err = d.collection.store.WithUpdate(ctx, func(tx interface{}) error {
		bboltTx := tx.(*bbolt.Tx)
		bucket := bboltTx.Bucket([]byte(d.collection.name))
		if bucket == nil {
			return fmt.Errorf("collection bucket not found")
		}
		return bucket.Put([]byte(d.id), data)
	})
	if err != nil {
		return fmt.Errorf("failed to save document: %w", err)
	}

	// 发送变更事件
	op := OperationInsert
	if oldDoc != nil {
		op = OperationUpdate
	}
	d.collection.emitChange(ChangeEvent{
		Collection: d.collection.name,
		ID:         d.id,
		Op:         op,
		Doc:        d.data,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	})

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
	cloned := make(map[string]any)
	bytes, err := json.Marshal(d.data)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &cloned); err != nil {
		return nil, err
	}
	return cloned, nil
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

	var exists bool
	err := d.collection.store.WithView(ctx, func(tx interface{}) error {
		bboltTx := tx.(*bbolt.Tx)
		bucket := bboltTx.Bucket([]byte(d.collection.name))
		if bucket == nil {
			return nil
		}
		exists = bucket.Get([]byte(d.id)) != nil
		return nil
	})
	if err != nil {
		return false, err
	}

	return !exists, nil
}

// AtomicUpdate 原子更新文档，使用更新函数。
func (d *document) AtomicUpdate(ctx context.Context, updateFn func(doc map[string]any) error) error {
	if d.collection == nil {
		return fmt.Errorf("document is not associated with a collection")
	}

	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()

	if d.collection.closed {
		return fmt.Errorf("collection is closed")
	}

	// 读取当前文档
	var currentDoc map[string]any
	err := d.collection.store.WithView(ctx, func(tx interface{}) error {
		bboltTx := tx.(*bbolt.Tx)
		bucket := bboltTx.Bucket([]byte(d.collection.name))
		if bucket == nil {
			return fmt.Errorf("collection bucket not found")
		}
		data := bucket.Get([]byte(d.id))
		if data == nil {
			return fmt.Errorf("document with id %s not found", d.id)
		}
		currentDoc = make(map[string]any)
		return json.Unmarshal(data, &currentDoc)
	})
	if err != nil {
		return err
	}

	// 应用更新函数
	if err := updateFn(currentDoc); err != nil {
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
		return fmt.Errorf("failed to generate revision: %w", err)
	}
	currentDoc[d.revField] = rev

	// 保存旧文档用于变更事件
	oldDoc := make(map[string]any)
	oldDocBytes, _ := json.Marshal(d.data)
	json.Unmarshal(oldDocBytes, &oldDoc)

	// 原子写入
	err = d.collection.store.WithUpdate(ctx, func(tx interface{}) error {
		bboltTx := tx.(*bbolt.Tx)
		bucket := bboltTx.Bucket([]byte(d.collection.name))
		if bucket == nil {
			return fmt.Errorf("collection bucket not found")
		}

		// 再次检查文档是否存在（乐观锁）
		data := bucket.Get([]byte(d.id))
		if data == nil {
			return fmt.Errorf("document with id %s was deleted", d.id)
		}

		// 检查修订号是否匹配
		var existingDoc map[string]any
		if err := json.Unmarshal(data, &existingDoc); err != nil {
			return err
		}
		if existingRev, ok := existingDoc[d.revField]; ok {
			if fmt.Sprintf("%v", existingRev) != oldRev {
				return fmt.Errorf("document revision mismatch: expected %s, got %v", oldRev, existingRev)
			}
		}

		// 写入更新后的文档
		newData, err := json.Marshal(currentDoc)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(d.id), newData)
	})
	if err != nil {
		return fmt.Errorf("failed to atomic update document: %w", err)
	}

	// 更新本地数据
	d.data = currentDoc

	// 发送变更事件
	d.collection.emitChange(ChangeEvent{
		Collection: d.collection.name,
		ID:         d.id,
		Op:         OperationUpdate,
		Doc:        currentDoc,
		Old:        oldDoc,
		Meta:       map[string]interface{}{"rev": rev},
	})

	return nil
}

// AtomicPatch 原子补丁更新文档，合并补丁数据。
func (d *document) AtomicPatch(ctx context.Context, patch map[string]any) error {
	return d.AtomicUpdate(ctx, func(doc map[string]any) error {
		// 合并补丁数据
		for k, v := range patch {
			// 不允许更新主键
			if !d.collection.isPrimaryKeyField(k) {
				doc[k] = v
			}
		}
		return nil
	})
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

// GetAllAttachments 获取文档的所有附件
func (d *document) GetAllAttachments(ctx context.Context) ([]*Attachment, error) {
	if d.collection == nil {
		return nil, fmt.Errorf("document is not associated with a collection")
	}
	return d.collection.GetAllAttachments(ctx, d.id)
}
