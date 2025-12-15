package rxdb

import (
	"context"
)

// Operation 表示文档变更类型。
type Operation string

const (
	OperationInsert Operation = "insert"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
)

// ChangeEvent 与 RxDB 变更事件概念对齐，用于本地事件流与同步。
type ChangeEvent struct {
	Collection string                 // 发生变更的集合名
	ID         string                 // 主键值
	Op         Operation              // 操作类型
	Doc        map[string]any         // 新文档数据（delete 时可为空）
	Old        map[string]any         // 旧文档数据（insert 时可为空）
	Meta       map[string]interface{} // 额外元数据（修订号等）
}

// FieldChangeEvent 表示字段级别的变更事件。
type FieldChangeEvent struct {
	Field string      // 字段名
	Old   interface{} // 旧值
	New   interface{} // 新值
}

// MigrationStrategy 定义版本迁移策略函数
// 参数：oldDoc 是旧版本的文档数据，返回迁移后的新文档数据
type MigrationStrategy func(oldDoc map[string]any) (map[string]any, error)

// Schema 采用 RxDB JSON schema 的子集，后续根据需要扩展。
type Schema struct {
	JSON               map[string]any            // 原始 JSON Schema
	PrimaryKey         interface{}               // 主键字段名（字符串）或复合主键（字符串数组）
	RevField           string                    // 修订号字段名，默认可使用 _rev
	Indexes            []Index                   // 索引定义（用于查询优化）
	MigrationStrategies map[int]MigrationStrategy // 版本迁移策略，key 为目标版本号
}

// Index 定义索引结构。
type Index struct {
	Fields []string // 索引字段列表（支持复合索引）
	Name   string   // 索引名称（可选，用于唯一标识）
}

// Database 接口对齐 RxDB 概念。
type Database interface {
	Name() string
	Close(ctx context.Context) error
	Destroy(ctx context.Context) error
	Collection(ctx context.Context, name string, schema Schema) (Collection, error)
	Changes() <-chan ChangeEvent
	ExportJSON(ctx context.Context) (map[string]any, error)
	ImportJSON(ctx context.Context, data map[string]any) error
	Backup(ctx context.Context, backupPath string) error
	WaitForLeadership(ctx context.Context) error
	RequestIdle(ctx context.Context) error
	Password() string
	MultiInstance() bool
}

// Collection 接口对齐 RxCollection 常用能力，后续再扩充。
type Collection interface {
	Name() string
	Schema() Schema
	Insert(ctx context.Context, doc map[string]any) (Document, error)
	Upsert(ctx context.Context, doc map[string]any) (Document, error)
	IncrementalUpsert(ctx context.Context, patch map[string]any) (Document, error)
	IncrementalModify(ctx context.Context, id string, modifier func(doc map[string]any) error) (Document, error)
	Find(selector map[string]any) *Query
	FindOne(ctx context.Context, selector map[string]any) (Document, error)
	FindByID(ctx context.Context, id string) (Document, error)
	Remove(ctx context.Context, id string) error
	All(ctx context.Context) ([]Document, error)
	Count(ctx context.Context) (int, error)
	BulkInsert(ctx context.Context, docs []map[string]any) ([]Document, error)
	BulkUpsert(ctx context.Context, docs []map[string]any) ([]Document, error)
	BulkRemove(ctx context.Context, ids []string) error
	ExportJSON(ctx context.Context) ([]map[string]any, error)
	ImportJSON(ctx context.Context, docs []map[string]any) error
	Migrate(ctx context.Context) error
	Changes() <-chan ChangeEvent
}

// Document 接口对齐 RxDocument。
type Document interface {
	ID() string
	Data() map[string]any
	Get(field string) any
	GetString(field string) string
	GetInt(field string) int
	GetFloat(field string) float64
	GetBool(field string) bool
	GetArray(field string) []any
	GetObject(field string) map[string]any
	Set(ctx context.Context, field string, value any) error
	Update(ctx context.Context, updates map[string]any) error
	Remove(ctx context.Context) error
	Save(ctx context.Context) error
	Changes() <-chan ChangeEvent
	ToJSON() ([]byte, error)
	ToMutableJSON() (map[string]any, error)
	Deleted(ctx context.Context) (bool, error)
	AtomicUpdate(ctx context.Context, updateFn func(doc map[string]any) error) error
	AtomicPatch(ctx context.Context, patch map[string]any) error
	IncrementalModify(ctx context.Context, modifier func(doc map[string]any) error) error
	IncrementalPatch(ctx context.Context, patch map[string]any) error
	GetFieldChanges(ctx context.Context, field string) <-chan FieldChangeEvent
	// TODO: 支持 reactive/getter/setter、同步状态观察等扩展
}

