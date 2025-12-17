# 图数据库集成

本模块提供了图数据库功能，支持在 rxdb-go 中存储和查询图数据。

## 功能特性

- ✅ **图数据存储**：支持三元组（subject-predicate-object）存储
- ✅ **图查询 API**：提供类似 Gremlin 的查询接口
- ✅ **自动同步**：支持文档变更自动同步到图数据库
- ✅ **关系映射**：支持自定义文档字段到图关系的映射规则
- ✅ **路径查询**：支持查找两个节点之间的路径

## 快速开始

### 1. 创建带图功能的数据库

```go
db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
    Name: "mydb",
    Path: "./mydb.db",
    GraphOptions: &rxdb.GraphOptions{
        Enabled:  true,
        Backend:  "memory", // 或 "bolt", "leveldb"
        Path:     "./mydb.db/graph", // 图数据存储在数据库目录下的 graph 子目录
        AutoSync: true, // 启用自动同步
    },
})
```

### 2. 创建图关系

```go
graphDB := db.Graph()

// 创建链接
graphDB.Link(ctx, "user1", "follows", "user2")
graphDB.Link(ctx, "user2", "follows", "user3")
```

### 3. 查询图数据

```go
// 获取邻居节点
neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")

// 查找路径
paths, err := graphDB.FindPath(ctx, "user1", "user3", 5, "follows")

// 使用查询 API
query := graphDB.Query()
results, err := query.V("user1").Out("follows").All(ctx)
```

### 4. 配置自动关系映射

```go
bridge := db.GraphBridge()
if bridge != nil {
    bridge.AddRelationMapping(&rxdb.GraphRelationMapping{
        Collection:  "users",
        Field:       "follows",  // 文档字段
        Relation:    "follows",   // 图关系名称
        TargetField: "id",        // 目标文档ID字段
        AutoLink:    true,        // 自动创建链接
    })
}
```

## API 参考

### GraphDatabase 接口

- `Link(ctx, from, relation, to)` - 创建链接
- `Unlink(ctx, from, relation, to)` - 删除链接
- `GetNeighbors(ctx, nodeID, relation)` - 获取邻居节点
- `FindPath(ctx, from, to, maxDepth, relations...)` - 查找路径
- `Query()` - 创建查询对象

### GraphQuery 接口

- `V(nodes...)` - 从指定节点开始查询
- `Out(predicates...)` - 沿着指定谓词向外查询
- `In(predicates...)` - 沿着指定谓词向内查询
- `Both(predicates...)` - 双向查询
- `Has(predicate, object)` - 过滤节点
- `Limit(n)` - 限制结果数量
- `All(ctx)` - 执行查询并返回所有结果
- `AllNodes(ctx)` - 返回所有节点值
- `Count(ctx)` - 返回结果数量
- `First(ctx)` - 返回第一个结果

## 存储后端

当前支持以下存储后端：

- **memory** - 内存模式（默认，适合测试）
- **bolt** - BoltDB（适合单机应用）
- **leveldb** - LevelDB（适合高性能场景）

## 注意事项

1. **性能**：图查询可能较慢，建议对常用查询进行优化
2. **数据一致性**：文档删除时，相关图关系需要手动或通过桥接自动删除
3. **存储分离**：图数据与文档数据分开存储，便于管理

## 后续扩展

当前实现提供了基础的图数据库功能。后续可以：

1. 集成 Cayley HTTP API 客户端（如果使用独立的 Cayley 服务）
2. 支持更复杂的图查询语言（如 Gremlin、SPARQL）
3. 添加图数据持久化到文件系统
4. 支持分布式图数据库

## 示例

查看 `examples/graph/main.go` 获取完整示例代码。

