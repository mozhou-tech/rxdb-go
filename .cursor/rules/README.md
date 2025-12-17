# RxDB-Go Cursor Rules

## 项目概述

这是一个 Golang 版本的 RxDB，提供与 RxDB JavaScript 版本兼容的 API。项目使用 Badger 作为存储后端，支持 Supabase 同步、图数据库查询、全文搜索和向量搜索等功能。

## 核心设计原则

### 1. API 兼容性
- **与 RxDB JavaScript 版本对齐**：所有核心 API（Database、Collection、Document、Query）必须与 RxDB JavaScript 版本保持概念一致
- **使用 `map[string]any` 表示文档数据**：这是 Go 中表示动态 JSON 数据的标准方式
- **使用 context 和 channel 替代 Promise**：遵循 Go 的并发模型，使用 context 传递取消信号和超时，使用 channel 处理异步事件流

### 2. 存储后端
- **使用 Badger 作为持久化存储**：所有数据操作必须通过 `pkg/storage/badger` 包
- **事务性操作**：确保数据一致性的操作必须使用事务
- **路径管理**：数据库路径使用 `filepath.Join` 和 `filepath.Clean` 处理，确保跨平台兼容

### 3. 并发安全
- **所有公共 API 必须是线程安全的**：使用 `sync.RWMutex` 或 `sync.Mutex` 保护共享状态
- **变更事件通道**：使用带缓冲的 channel（建议容量 100），避免阻塞
- **多实例支持**：通过事件广播器实现多实例间的事件共享

### 4. 错误处理
- **使用 Go 标准错误处理**：返回 `error` 类型，使用 `errors.New` 或 `fmt.Errorf` 创建错误
- **错误包装**：使用 `fmt.Errorf` 和 `%w` 动词包装底层错误，保留错误链
- **上下文取消**：所有长时间运行的操作必须检查 `ctx.Done()` 并返回适当的错误

### 5. 日志处理
- **统一使用 logrus**：所有日志输出必须使用 `github.com/sirupsen/logrus`
- **日志级别**：使用适当的日志级别（Debug、Info、Warn、Error）
- **结构化日志**：使用 `logrus.WithFields()` 添加上下文信息

```go
import "github.com/sirupsen/logrus"

logrus.WithFields(logrus.Fields{
    "database": db.Name(),
    "collection": collection.Name(),
    "docID": docID,
}).Error("Failed to insert document")
```

## 代码风格

### 1. 命名规范
- **接口命名**：使用大驼峰，如 `Database`、`Collection`、`Document`
- **实现类型**：使用小写开头的驼峰，如 `database`、`collection`、`document`
- **常量**：使用大写下划线分隔，如 `OperationInsert`、`OperationUpdate`
- **私有函数**：使用小写开头的驼峰，如 `newDatabase`、`validateSchema`

### 2. 文件组织
- **核心 API**：`pkg/rxdb/` 目录下
  - `database.go` - Database 接口和实现
  - `collection.go` - Collection 接口和实现
  - `document.go` - Document 接口和实现
  - `query.go` - Query 接口和实现
  - `types.go` - 类型定义
- **存储实现**：`pkg/storage/badger/`
- **同步实现**：`pkg/replication/supabase/`
- **图数据库**：`pkg/graph/cayley/`

### 3. 函数签名
- **第一个参数必须是 `context.Context`**：所有公共 API 方法必须接受 context
- **返回错误**：所有可能失败的操作必须返回 `error` 作为最后一个返回值
- **链式查询**：Query 方法返回 `*Query` 以支持链式调用

```go
// 正确示例
func (c *collection) Find(selector map[string]any) *Query
func (c *collection) Insert(ctx context.Context, doc map[string]any) (Document, error)

// 错误示例
func (c *collection) Find(ctx context.Context, selector map[string]any) (*Query, error) // 不应该返回 error
```

## 功能实现规范

### 1. Schema 和验证
- **Schema 结构**：使用 `Schema` 类型定义集合结构
- **主键支持**：支持字符串主键和复合主键（字符串数组）
- **修订号字段**：使用 `_rev` 字段跟踪文档版本
- **索引定义**：通过 `Schema.Indexes` 定义索引，支持复合索引

### 2. 查询 API
- **Mango Query 语法**：支持以下操作符的子集
  - 比较：`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
  - 数组：`$in`, `$nin`
  - 字符串：`$regex`
  - 存在性：`$exists`, `$type`
  - 逻辑：`$and`, `$or`, `$not`, `$nor`
- **链式查询**：支持 `Find().Sort().Skip().Limit().Exec()`
- **查询优化**：利用索引优化查询性能

### 3. 变更事件
- **ChangeEvent 结构**：包含 Collection、ID、Op、Doc、Old、Meta 字段
- **事件通道**：通过 `Changes()` 方法返回 `<-chan ChangeEvent`
- **事件广播**：多实例间通过事件广播器共享变更事件

### 4. Supabase 同步
- **双向同步**：支持本地到远程和远程到本地的数据同步
- **REST API**：使用 Supabase REST API 进行数据拉取和推送
- **Realtime**：使用 Supabase Realtime 监听远程变更
- **冲突处理**：支持可配置的冲突解决策略

### 5. 图数据库
- **Cayley 集成**：使用 Cayley 作为图数据库后端
- **自动同步**：支持文档变更自动同步到图数据库
- **关系映射**：通过 `GraphRelationMapping` 配置文档字段到图关系的映射
- **查询接口**：提供 Gremlin 风格的查询 API（V、Out、In、Both、Has 等）

### 6. 全文搜索
- **索引支持**：支持为集合创建全文搜索索引
- **查询接口**：提供全文搜索查询方法

### 7. 向量搜索
- **向量索引**：支持向量字段的索引和搜索
- **相似度查询**：支持基于向量相似度的查询

## 测试要求

### 1. 测试文件
- **每个源文件对应一个测试文件**：如 `database.go` 对应 `database_test.go`
- **测试函数命名**：使用 `TestXxx` 格式
- **表驱动测试**：对于多个测试用例，使用表驱动测试模式

### 2. 测试覆盖
- **核心功能**：Database、Collection、Document 的所有公共方法必须有测试
- **边界情况**：测试空值、nil、边界条件
- **并发测试**：测试并发访问的安全性
- **集成测试**：在 `integration_test.go` 中测试完整流程

### 3. 测试数据
- **使用临时目录**：测试时使用 `os.TempDir()` 创建临时数据库
- **清理资源**：测试结束后必须清理临时文件和数据库实例

```go
func TestExample(t *testing.T) {
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")
    defer os.RemoveAll(tmpDir)
    
    db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
        Name: "test",
        Path: dbPath,
    })
    // ... 测试代码
    defer db.Close(ctx)
}
```

## 性能优化

### 1. 批量操作
- **提供批量 API**：`BulkInsert`、`BulkUpsert`、`BulkRemove`
- **事务批处理**：批量操作应在单个事务中完成

### 2. 索引优化
- **创建索引**：为常用查询字段创建索引
- **复合索引**：支持多字段复合索引

### 3. 内存管理
- **及时释放资源**：使用 `defer` 确保资源释放
- **避免内存泄漏**：确保 channel 和 goroutine 正确关闭

## 文档要求

### 1. 代码注释
- **公共 API**：所有公共类型、函数、方法必须有注释
- **注释格式**：使用 Go 标准注释格式，注释以类型/函数名开头

```go
// Database 接口对齐 RxDB 概念，提供数据库级别的操作。
type Database interface {
    // Name 返回数据库名称。
    Name() string
    
    // Close 关闭数据库并释放所有资源。
    Close(ctx context.Context) error
}
```

### 2. 示例代码
- **examples 目录**：在 `examples/` 目录下提供使用示例
- **README 更新**：重要功能变更时更新 README.md

## 注意事项

### 1. 与 RxDB JavaScript 的差异
- **静态类型**：Go 是静态类型语言，API 使用 `map[string]any` 表示文档
- **异步模型**：使用 context 和 channel 替代 Promise
- **存储后端**：使用 Badger 替代 IndexedDB/LocalStorage
- **同步机制**：直接使用 Supabase REST API 和 Realtime

### 2. 向后兼容性
- **API 变更**：重大 API 变更需要版本号升级
- **数据迁移**：通过 `MigrationStrategy` 支持数据版本迁移

### 3. 安全性
- **字段加密**：支持通过 `Schema.EncryptedFields` 指定需要加密的字段
- **密码保护**：数据库支持密码保护

## 开发工作流

1. **创建功能分支**：从 main 分支创建功能分支
2. **编写测试**：先编写测试，再实现功能（TDD）
3. **实现功能**：遵循上述规范和原则
4. **运行测试**：确保所有测试通过
5. **代码审查**：提交 PR 前进行自我审查
6. **更新文档**：更新相关文档和示例
