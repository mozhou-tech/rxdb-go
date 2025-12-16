# rxdb-go

Golang 版本的 RxDB，提供与 RxDB JavaScript 版本兼容的 API，底层使用 Badger 存储，支持 Supabase 同步。

## 功能特性

- ✅ **核心 API**：与 RxDB 对齐的 Database、Collection、Document 接口
- ✅ **Badger 存储**：基于 `github.com/dgraph-io/badger/v4` 的持久化存储
- ✅ **变更流**：支持监听文档的 insert/update/delete 事件
- ✅ **查询 API**：支持 Mango Query 语法的子集（$eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $regex, $exists, $type 等）
- ✅ **排序和分页**：支持 Sort、Skip、Limit
- ✅ **Supabase 同步**：支持与 Supabase 的双向数据同步（REST API + Realtime）
- ✅ **冲突处理**：可配置的冲突解决策略

## 安装

```bash
go get github.com/mozy/rxdb-go
```

## 快速开始

### 基本使用

```go
package main

import (
    "context"
    "github.com/mozy/rxdb-go/pkg/rxdb"
)

func main() {
    ctx := context.Background()
    
    // 创建数据库
    db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
        Name: "mydb",
        Path: "./mydb.db",
    })
    if err != nil {
        panic(err)
    }
    defer db.Close(ctx)
    
    // 定义 schema
    schema := rxdb.Schema{
        PrimaryKey: "id",
        RevField:   "_rev",
    }
    
    // 创建集合
    collection, err := db.Collection(ctx, "heroes", schema)
    if err != nil {
        panic(err)
    }
    
    // 插入文档
    doc, err := collection.Insert(ctx, map[string]any{
        "id":   "hero-001",
        "name": "Superman",
    })
    
    // 查询文档
    found, err := collection.FindByID(ctx, "hero-001")
    
    // 使用查询 API
    results, err := collection.Find(map[string]any{
        "name": "Superman",
    }).Exec(ctx)
    first, err := collection.FindOne(ctx, map[string]any{
        "name": "Superman",
    })
}
```

### Supabase 同步

```go
import (
    "github.com/mozy/rxdb-go/pkg/replication/supabase"
)

// 创建同步客户端
replication, err := supabase.NewReplication(collection, supabase.ReplicationOptions{
    SupabaseURL:    "https://your-project.supabase.co",
    SupabaseKey:    "your-anon-key",
    Table:          "todos",
    PrimaryKey:     "id",
    UpdatedAtField: "updated_at",
    PullInterval:   10 * time.Second,
    PushOnChange:   true,
})

// 启动同步
replication.Start(ctx)
defer replication.Stop()
```

## API 文档

### Database

- `CreateDatabase(ctx, opts)` - 创建数据库实例
- `Name()` - 获取数据库名称
- `Close(ctx)` - 关闭数据库
- `Collection(ctx, name, schema)` - 创建或获取集合
- `RequestIdle(ctx)` - 等待数据库级操作空闲（不含集合内细粒度操作）

### Collection

- `Name()` - 获取集合名称
- `Schema()` - 获取集合的 schema
- `Insert(ctx, doc)` - 插入文档
- `Upsert(ctx, doc)` - 更新或插入文档
- `Find(selector)` - 创建查询（返回链式查询对象）
- `FindOne(ctx, selector)` - 查找第一个匹配的文档
- `FindByID(ctx, id)` - 按 ID 查找文档
- `Remove(ctx, id)` - 删除文档
- `All(ctx)` - 获取所有文档
- `Count(ctx)` - 获取文档总数
- `Changes()` - 返回变更事件通道

### Query

- `Find(selector)` - 创建查询
- `Sort(sortDef)` - 设置排序
- `Skip(n)` - 跳过文档数
- `Limit(n)` - 限制返回数
- `Exec(ctx)` - 执行查询
- `FindOne(ctx)` - 返回第一个结果
- `Count(ctx)` - 返回匹配数量

### 查询操作符

支持以下 Mango Query 操作符：

- `$eq` - 等于
- `$ne` - 不等于
- `$gt` - 大于
- `$gte` - 大于等于
- `$lt` - 小于
- `$lte` - 小于等于
- `$in` - 在数组中
- `$nin` - 不在数组中
- `$regex` - 正则匹配
- `$exists` - 字段存在
- `$type` - 类型匹配
- `$and` - 逻辑与
- `$or` - 逻辑或
- `$not` - 逻辑非
- `$nor` - 逻辑或非

## 示例

查看 `examples/` 目录：

- `examples/basic/` - 基本 CRUD 操作示例
- `examples/supabase-sync/` - Supabase 同步示例

## 测试

```bash
go test ./...
```

## 项目结构

```
rxdb-go/
├── pkg/
│   ├── rxdb/           # 核心 API
│   │   ├── database.go
│   │   ├── collection.go
│   │   ├── document.go
│   │   ├── query.go
│   │   └── types.go
│   ├── storage/
│   │   └── badger/      # Badger 存储实现
│   └── replication/
│       └── supabase/    # Supabase 同步
├── examples/            # 示例代码
└── README.md
```

## 与 RxDB 的差异

1. **语言特性**：Go 是静态类型语言，API 使用 `map[string]any` 表示文档数据
2. **异步模型**：使用 context 和 channel 替代 Promise
3. **存储后端**：使用 Badger 替代 IndexedDB/LocalStorage
4. **同步机制**：直接使用 Supabase REST API 和 Realtime，而非 RxDB 插件系统

## 许可证

MIT

## 贡献

欢迎提交 Issue 和 Pull Request！
