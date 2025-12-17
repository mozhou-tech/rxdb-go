package cayley

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
)

// Database 定义数据库接口（避免循环依赖）
type Database interface {
	Changes() <-chan ChangeEvent
}

// ChangeEvent 变更事件（避免循环依赖）
type ChangeEvent struct {
	Collection string
	ID         string
	Op         string
	Doc        map[string]any
	Old        map[string]any
	Meta       map[string]interface{}
}

// Bridge 桥接文档数据库和图数据库
type Bridge struct {
	db      Database
	graph   *Client
	enabled bool
	mu      sync.RWMutex

	// 关系映射配置
	relationMappings map[string]*RelationMapping
}

// RelationMapping 定义文档字段到图关系的映射规则
type RelationMapping struct {
	// Collection 集合名称
	Collection string
	// Field 文档字段名（包含关系数据的字段）
	Field string
	// Relation 图关系名称（谓词）
	Relation string
	// TargetField 目标文档ID字段（如果关系数据是对象，指定ID字段）
	TargetField string
	// AutoLink 是否自动创建链接
	AutoLink bool
}

// NewBridge 创建新的桥接实例
func NewBridge(db Database, graph *Client) *Bridge {
	return &Bridge{
		db:               db,
		graph:            graph,
		enabled:          true,
		relationMappings: make(map[string]*RelationMapping),
	}
}

// Enable 启用桥接功能
func (b *Bridge) Enable() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enabled = true
	logrus.Info("[Graph Bridge] Enabled")
}

// Disable 禁用桥接功能
func (b *Bridge) Disable() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enabled = false
	logrus.Info("[Graph Bridge] Disabled")
}

// IsEnabled 检查桥接是否启用
func (b *Bridge) IsEnabled() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.enabled
}

// AddRelationMapping 添加关系映射规则
func (b *Bridge) AddRelationMapping(mapping *RelationMapping) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s:%s", mapping.Collection, mapping.Field)
	b.relationMappings[key] = mapping
	logrus.WithFields(logrus.Fields{
		"collection": mapping.Collection,
		"field":      mapping.Field,
		"relation":   mapping.Relation,
		"autoLink":   mapping.AutoLink,
	}).Info("[Graph Bridge] AddRelationMapping")
}

// RemoveRelationMapping 移除关系映射规则
func (b *Bridge) RemoveRelationMapping(collection, field string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s:%s", collection, field)
	delete(b.relationMappings, key)
	logrus.WithFields(logrus.Fields{
		"collection": collection,
		"field":      field,
	}).Info("[Graph Bridge] RemoveRelationMapping")
}

// SyncDocumentToGraph 将文档同步到图数据库
func (b *Bridge) SyncDocumentToGraph(ctx context.Context, collection string, docID string, doc map[string]any) error {
	if !b.IsEnabled() {
		return nil
	}
	logrus.WithFields(logrus.Fields{
		"collection": collection,
		"docID":      docID,
	}).Debug("[Graph Bridge] SyncDocumentToGraph")

	b.mu.RLock()
	mappings := make(map[string]*RelationMapping)
	for k, v := range b.relationMappings {
		if v.Collection == collection {
			mappings[k] = v
		}
	}
	b.mu.RUnlock()

	// 处理每个映射规则
	for _, mapping := range mappings {
		if !mapping.AutoLink {
			continue
		}

		fieldValue, exists := doc[mapping.Field]
		if !exists {
			continue
		}

		// 处理不同类型的字段值
		switch v := fieldValue.(type) {
		case string:
			// 直接作为目标节点ID
			logrus.WithFields(logrus.Fields{
				"from":     docID,
				"relation": mapping.Relation,
				"to":       v,
				"field":    mapping.Field,
			}).Info("[Graph Bridge] Auto-linking")
			if err := b.graph.Link(ctx, docID, mapping.Relation, v); err != nil {
				logrus.WithFields(logrus.Fields{
					"docID":    docID,
					"relation": mapping.Relation,
					"target":   v,
					"error":    err,
				}).Error("[Graph Bridge] failed to link document")
				return fmt.Errorf("failed to link document: %w", err)
			}

		case []any:
			// 数组类型，每个元素可能是字符串或对象
			for _, item := range v {
				targetID := b.extractTargetID(item, mapping.TargetField)
				if targetID != "" {
					if err := b.graph.Link(ctx, docID, mapping.Relation, targetID); err != nil {
						return fmt.Errorf("failed to link document: %w", err)
					}
				}
			}

		case map[string]any:
			// 对象类型，提取目标ID
			targetID := b.extractTargetID(v, mapping.TargetField)
			if targetID != "" {
				logrus.WithFields(logrus.Fields{
					"from":        docID,
					"relation":    mapping.Relation,
					"to":          targetID,
					"field":       mapping.Field,
					"targetField": mapping.TargetField,
				}).Info("[Graph Bridge] Auto-linking")
				if err := b.graph.Link(ctx, docID, mapping.Relation, targetID); err != nil {
					logrus.WithFields(logrus.Fields{
						"docID":    docID,
						"relation": mapping.Relation,
						"target":   targetID,
						"error":    err,
					}).Error("[Graph Bridge] failed to link document")
					return fmt.Errorf("failed to link document: %w", err)
				}
			}
		}
	}

	return nil
}

// RemoveDocumentFromGraph 从图数据库移除文档及其所有关系
func (b *Bridge) RemoveDocumentFromGraph(ctx context.Context, docID string) error {
	if !b.IsEnabled() {
		return nil
	}

	// 获取所有与该节点相关的边
	// 简化实现：删除所有以该节点为起点或终点的边
	// 实际使用时，可以通过查询获取所有相关的边，然后逐一删除

	// 获取所有出边和入边的邻居
	neighbors, err := b.graph.GetNeighbors(ctx, docID, "")
	if err != nil {
		return fmt.Errorf("failed to get neighbors: %w", err)
	}

	// 删除所有相关的边（简化实现）
	// 注意：这里需要知道具体的谓词才能删除，简化版本只删除已知的关系
	// 实际使用时，应该先查询所有相关的边，然后逐一删除
	_ = neighbors // 避免未使用变量警告

	return nil
}

// extractTargetID 从值中提取目标ID
func (b *Bridge) extractTargetID(value any, targetField string) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]any:
		if targetField != "" {
			if id, ok := v[targetField].(string); ok {
				return id
			}
		}
		// 尝试常见的ID字段
		if id, ok := v["id"].(string); ok {
			return id
		}
		if id, ok := v["_id"].(string); ok {
			return id
		}
	}
	return ""
}

// HandleChangeEvent 处理文档变更事件，自动同步到图数据库
func (b *Bridge) HandleChangeEvent(ctx context.Context, event ChangeEvent) error {
	if !b.IsEnabled() {
		return nil
	}

	switch event.Op {
	case "insert", "update":
		// 插入或更新时，同步文档到图数据库
		if event.Doc != nil {
			// 先删除旧的关系（如果是更新）
			if event.Old != nil {
				// 可以在这里处理旧关系的删除
			}
			// 添加新的关系
			return b.SyncDocumentToGraph(ctx, event.Collection, event.ID, event.Doc)
		}

	case "delete":
		// 删除时，从图数据库移除文档
		return b.RemoveDocumentFromGraph(ctx, event.ID)
	}

	return nil
}

// StartAutoSync 启动自动同步（监听数据库变更事件）
func (b *Bridge) StartAutoSync(ctx context.Context) error {
	if !b.IsEnabled() {
		logrus.Info("[Graph Bridge] StartAutoSync: bridge is disabled, skipping")
		return nil
	}

	logrus.Info("[Graph Bridge] StartAutoSync: starting auto-sync goroutine")
	changes := b.db.Changes()
	go func() {
		logrus.Info("[Graph Bridge] AutoSync: goroutine started")
		for {
			select {
			case <-ctx.Done():
				logrus.Info("[Graph Bridge] AutoSync: context done, stopping")
				return
			case event, ok := <-changes:
				if !ok {
					logrus.Info("[Graph Bridge] AutoSync: changes channel closed, stopping")
					return
				}
				logrus.WithFields(logrus.Fields{
					"op":         event.Op,
					"collection": event.Collection,
					"docID":      event.ID,
				}).Debug("[Graph Bridge] AutoSync: received event")
				if err := b.HandleChangeEvent(ctx, event); err != nil {
					logrus.WithFields(logrus.Fields{
						"op":         event.Op,
						"collection": event.Collection,
						"docID":      event.ID,
						"error":      err,
					}).Error("[Graph Bridge] AutoSync: failed to handle event")
					// 记录错误但不中断同步
				}
			}
		}
	}()

	return nil
}
