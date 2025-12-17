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

// Attachment 表示文档附件
type Attachment struct {
	ID       string // 附件 ID
	Name     string // 附件名称
	Type     string // MIME 类型
	Size     int64  // 附件大小（字节）
	Data     []byte // 附件数据（如果提供了 FilePath，则优先使用 FilePath）
	FilePath string // 文件系统路径（如果提供，将直接从该路径拷贝文件）
	Digest   string // 附件摘要（用于验证，保留向后兼容）
	MD5      string // MD5 哈希值
	SHA256   string // SHA256 哈希值
	Created  int64  // 创建时间戳
	Modified int64  // 修改时间戳
}

// MigrationStrategy 定义版本迁移策略函数
// 参数：oldDoc 是旧版本的文档数据，返回迁移后的新文档数据
type MigrationStrategy func(oldDoc map[string]any) (map[string]any, error)

// Schema 采用 RxDB JSON schema 的子集，后续根据需要扩展。
type Schema struct {
	JSON                map[string]any            // 原始 JSON Schema
	PrimaryKey          interface{}               // 主键字段名（字符串）或复合主键（字符串数组）
	RevField            string                    // 修订号字段名，默认可使用 _rev
	Indexes             []Index                   // 索引定义（用于查询优化）
	MigrationStrategies map[int]MigrationStrategy // 版本迁移策略，key 为目标版本号
	EncryptedFields     []string                  // 需要加密的字段列表
}

// Index 定义索引结构。
type Index struct {
	Fields []string // 索引字段列表（支持复合索引）
	Name   string   // 索引名称（可选，用于唯一标识）
}

// GraphDatabase 图数据库接口
type GraphDatabase interface {
	// Link 创建两个节点之间的链接
	Link(ctx context.Context, from, relation, to string) error
	// Unlink 删除两个节点之间的链接
	Unlink(ctx context.Context, from, relation, to string) error
	// GetNeighbors 获取节点的所有邻居节点
	GetNeighbors(ctx context.Context, nodeID string, relation string) ([]string, error)
	// FindPath 查找两个节点之间的路径
	FindPath(ctx context.Context, from, to string, maxDepth int, relations ...string) ([][]string, error)
	// Query 创建查询对象
	Query() GraphQuery
	// Close 关闭图数据库
	Close() error
}

// GraphQuery 图查询接口（使用指针类型避免值复制）
type GraphQuery interface {
	// V 从指定节点开始查询
	V(nodes ...string) *GraphQueryImpl
	// Out 沿着指定谓词向外查询
	Out(predicates ...string) *GraphQueryImpl
	// In 沿着指定谓词向内查询
	In(predicates ...string) *GraphQueryImpl
	// Both 双向查询
	Both(predicates ...string) *GraphQueryImpl
	// Has 过滤具有指定谓词和对象的节点
	Has(predicate, object string) *GraphQueryImpl
	// Limit 限制返回结果数量
	Limit(n int) *GraphQueryImpl
	// All 执行查询并返回所有结果
	All(ctx context.Context) ([]GraphQueryResult, error)
	// AllNodes 返回所有节点值
	AllNodes(ctx context.Context) ([]string, error)
	// Count 返回结果数量
	Count(ctx context.Context) (int64, error)
	// First 返回第一个结果
	First(ctx context.Context) (*GraphQueryResult, error)
}

// GraphQueryImpl 图查询实现（在 graph.go 中定义）
type GraphQueryImpl struct {
	// query 字段在 graph.go 中定义为 *cayley.Query
	// 使用 interface{} 避免循环依赖，实际类型在 graph.go 中处理
	query interface{}
}

// GraphQueryResult 图查询结果
type GraphQueryResult struct {
	Subject   string
	Predicate string
	Object    string
	Label     string
}

// GraphOptions 图数据库配置选项
type GraphOptions struct {
	// Enabled 是否启用图数据库
	Enabled bool
	// Backend 存储后端类型：bolt, leveldb, badger, memory
	Backend string
	// Path 存储路径
	Path string
	// AutoSync 是否自动同步文档变更到图数据库
	AutoSync bool
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
	// Graph 返回图数据库实例（如果已启用）
	Graph() GraphDatabase
	// GraphBridge 返回图数据库桥接实例（如果已启用）
	GraphBridge() GraphBridge
}

// GraphBridge 图数据库桥接接口
type GraphBridge interface {
	Enable()
	Disable()
	IsEnabled() bool
	AddRelationMapping(mapping *GraphRelationMapping)
	RemoveRelationMapping(collection, field string)
	StartAutoSync(ctx context.Context) error
}

// GraphRelationMapping 图关系映射配置
type GraphRelationMapping struct {
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
	GetAttachment(ctx context.Context, docID, attachmentID string) (*Attachment, error)
	PutAttachment(ctx context.Context, docID string, attachment *Attachment) error
	RemoveAttachment(ctx context.Context, docID, attachmentID string) error
	GetAllAttachments(ctx context.Context, docID string) ([]*Attachment, error)
	Dump(ctx context.Context) (map[string]any, error)
	ImportDump(ctx context.Context, dump map[string]any) error
	Changes() <-chan ChangeEvent
	CreateIndex(ctx context.Context, index Index) error
	DropIndex(ctx context.Context, indexName string) error
	ListIndexes() []Index
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
	GetAttachment(ctx context.Context, attachmentID string) (*Attachment, error)
	PutAttachment(ctx context.Context, attachment *Attachment) error
	RemoveAttachment(ctx context.Context, attachmentID string) error
	GetAllAttachments(ctx context.Context) ([]*Attachment, error)
	// TODO: 支持 reactive/getter/setter、同步状态观察等扩展
}
