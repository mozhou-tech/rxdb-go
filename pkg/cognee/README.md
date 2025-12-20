# Cognee 风格的 AI 记忆系统

基于 RxDB-Go 实现的类似 Cognee 的 AI 记忆系统，提供完整的 API 用于构建、管理和查询知识图谱。

## 功能特性

- 📝 **数据摄取**: 支持文本、文档、结构化数据等多种格式
- 🧠 **知识处理**: 自动提取实体和关系，构建知识图谱
- 🔍 **多模式搜索**: 
  - 全文搜索（基于 Bleve，支持中文分词）
  - 向量搜索（语义相似度）
  - 图搜索（关系查询）
  - 混合搜索（结合全文和向量）
- 📊 **数据集管理**: 支持多数据集管理
- 🎨 **可视化**: 提供知识图谱可视化接口
- 🔌 **多种嵌入模型**: 支持 Simple、OpenAI、HuggingFace 等嵌入生成器

## 快速开始

### 1. 创建数据库和记忆服务

```go
package main

import (
    "context"
    "path/filepath"
    "github.com/mozhou-tech/rxdb-go/pkg/cognee"
    "github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

func main() {
    ctx := context.Background()
    
    // 创建数据库（启用图数据库功能）
    db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
        Name: "cognee-memory",
        Path: "./data/cognee-memory",
        GraphOptions: &rxdb.GraphOptions{
            Enabled:  true,
            Backend:  "badger",
            Path:     filepath.Join("./data/cognee-memory", "graph"),
            AutoSync: true,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close(ctx)
    
    // 创建嵌入生成器（使用简单嵌入生成器作为示例）
    embedder := cognee.NewSimpleEmbedder(384)
    
    // 创建记忆服务
    service, err := cognee.NewMemoryService(ctx, db, cognee.MemoryServiceOptions{
        Embedder: embedder,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 使用服务...
}
```

### 2. 基本使用

```go
// 添加记忆
memory, err := service.AddMemory(ctx, 
    "AI 正在改变我们的工作和生活方式。", 
    "text", 
    "main_dataset", 
    nil,
)

// 处理记忆（提取实体和关系）
err := service.ProcessMemory(ctx, memory.ID)

// 搜索
results, err := service.Search(ctx, "AI", "HYBRID", 10)
for _, result := range results {
    fmt.Printf("ID: %s, Score: %.2f, Source: %s\n", 
        result.ID, result.Score, result.Source)
}

// 获取记忆
memory, err := service.GetMemory(ctx, memoryID)

// 删除记忆
err := service.DeleteMemory(ctx, memoryID)
```

## 核心接口

### MemoryService

`MemoryService` 是主要的接口，提供所有记忆管理功能。

#### 数据管理

```go
// 添加记忆
// AddMemory(ctx, content, memoryType, dataset, metadata)
memory, err := service.AddMemory(ctx, 
    "文本内容", 
    "text",           // 类型: text, code, url 等
    "main_dataset",   // 数据集名称
    map[string]interface{}{"author": "张三"}, // 可选元数据
)

// 获取记忆
memory, err := service.GetMemory(ctx, memoryID)

// 删除记忆
err := service.DeleteMemory(ctx, memoryID)

// 删除数据集（删除数据集中的所有记忆）
err := service.DeleteDataset(ctx, datasetID)
```

#### 知识处理

```go
// 处理单个记忆（提取实体和关系）
err := service.ProcessMemory(ctx, memoryID)

// 处理整个数据集
count, err := service.ProcessDataset(ctx, datasetID)
// count 返回成功处理的记忆数量
```

#### 搜索

```go
// 搜索记忆
// Search(ctx, query, searchType, limit)
results, err := service.Search(ctx, "查询内容", "HYBRID", 10)
```

**搜索类型 (searchType)**:
- `"FULLTEXT"` 或 `"CHUNKS"`: 全文搜索
- `"VECTOR"` 或 `"SEMANTIC"`: 向量语义搜索
- `"GRAPH"` 或 `"INSIGHTS"`: 图关系搜索
- `"HYBRID"`: 混合搜索（默认，结合全文和向量）

**搜索结果 (SearchResult)**:
```go
type SearchResult struct {
    ID       string  // 记忆 ID
    Content  string  // 内容
    Type     string  // 类型
    Score    float64 // 相关性分数
    Distance float64 // 向量距离（仅向量搜索）
    Source   string  // 来源: fulltext, vector, graph, hybrid
}
```

#### 数据集管理

```go
// 列出所有数据集
datasets, err := service.ListDatasets(ctx)
// 返回 []*Dataset

// 获取数据集数据
data, err := service.GetDatasetData(ctx, datasetID)
// 返回 []map[string]interface{}

// 获取数据集状态
status, err := service.GetDatasetStatus(ctx, datasetID)
// 返回 *DatasetStatus
// DatasetStatus 包含: Dataset, Status, Total, Processed, Pending
```

#### 可视化

```go
// 获取图谱数据用于可视化
graphData, err := service.GetGraphData(ctx)
// 返回 *GraphData
// GraphData 包含:
//   - Nodes: []GraphNode (ID, Name, Type)
//   - Edges: []GraphEdge (From, To, Type)
```

#### 健康检查

```go
// 获取健康状态
health, err := service.Health(ctx)
// 返回 *HealthStatus
// HealthStatus 包含:
//   - Status: "healthy"
//   - Stats: HealthStats (Memories, Entities, Relations 数量)
```

## 实现细节

### 全文搜索

使用 Bleve 搜索引擎，支持中文分词（基于 gojieba）。默认配置：
- Tokenize: "jieba"
- CaseSensitive: false

### 向量搜索

使用 Bleve 的向量搜索功能，支持余弦相似度、欧几里得距离等多种距离度量。默认使用余弦相似度。

### 图数据库

使用 Cayley 图数据库，支持实体关系的自动同步和查询。

### 嵌入生成器

支持多种嵌入生成器，可以通过 `CreateEmbedder` 工厂函数或直接创建：

#### 1. SimpleEmbedder（简单嵌入生成器，仅用于演示）

```go
// 直接创建
embedder := cognee.NewSimpleEmbedder(384)

// 或使用工厂函数
embedder, err := cognee.CreateEmbedder("simple", map[string]interface{}{
    "dimensions": 384,
})
```

#### 2. OpenAIEmbedder（OpenAI 嵌入生成器）

```go
// 使用工厂函数创建
embedder, err := cognee.CreateEmbedder("openai", map[string]interface{}{
    "api_key": "your-openai-api-key",  // 必需
    "model":   "text-embedding-ada-002", // 可选，默认 text-embedding-ada-002
    "base_url": "https://api.openai.com/v1", // 可选，支持 OpenAI 兼容 API
    "dimensions": 1536, // 可选，根据模型自动确定
})

// 或直接创建
embedder, err := cognee.NewOpenAIEmbedder(map[string]interface{}{
    "api_key": "your-openai-api-key",
    "model":   "text-embedding-3-small", // 支持 text-embedding-3-small, text-embedding-3-large
})
```

**支持的模型**:
- `text-embedding-ada-002` (默认，1536 维)
- `text-embedding-3-small` (1536 维)
- `text-embedding-3-large` (3072 维)

#### 3. HuggingFaceEmbedder（HuggingFace 嵌入生成器）

```go
// 使用工厂函数创建
embedder, err := cognee.CreateEmbedder("huggingface", map[string]interface{}{
    "api_key": "your-huggingface-api-key", // 必需
    "model":   "sentence-transformers/all-MiniLM-L6-v2", // 可选
    "base_url": "https://api-inference.huggingface.co", // 可选
    "dimensions": 384, // 可选，根据模型自动确定
})

// 或直接创建
embedder, err := cognee.NewHuggingFaceEmbedder(map[string]interface{}{
    "api_key": "your-huggingface-api-key",
    "model":   "sentence-transformers/all-mpnet-base-v2", // 768 维
})
```

**支持的模型**:
- `sentence-transformers/all-MiniLM-L6-v2` (默认，384 维)
- `sentence-transformers/all-MiniLM-L12-v2` (384 维)
- `sentence-transformers/all-mpnet-base-v2` (768 维)
- 其他 HuggingFace 模型

#### 自定义嵌入生成器

实现 `Embedder` 接口：

```go
type MyEmbedder struct {
    // 你的嵌入模型配置
}

func (e *MyEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
    // 调用真实的嵌入模型
    // 返回向量嵌入
    return embedding, nil
}

func (e *MyEmbedder) Dimensions() int {
    return 1536 // 返回向量维度
}

// 使用自定义嵌入生成器
service, err := cognee.NewMemoryService(ctx, db, cognee.MemoryServiceOptions{
    Embedder: &MyEmbedder{},
})
```

## 完整工作流示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "path/filepath"
    
    "github.com/mozhou-tech/rxdb-go/pkg/cognee"
    "github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

func main() {
    ctx := context.Background()
    
    // 1. 创建数据库
    dbPath := "./data/cognee-memory"
    db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
        Name: "cognee-memory",
        Path: dbPath,
        GraphOptions: &rxdb.GraphOptions{
            Enabled:  true,
            Backend:  "badger",
            Path:     filepath.Join(dbPath, "graph"),
            AutoSync: true,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close(ctx)
    
    // 2. 创建嵌入生成器
    // 选项 1: 使用简单嵌入生成器（演示用）
    embedder := cognee.NewSimpleEmbedder(384)
    
    // 选项 2: 使用 OpenAI（需要 API 密钥）
    // embedder, err := cognee.CreateEmbedder("openai", map[string]interface{}{
    //     "api_key": os.Getenv("OPENAI_API_KEY"),
    //     "model":   "text-embedding-ada-002",
    // })
    
    // 选项 3: 使用 HuggingFace（需要 API 密钥）
    // embedder, err := cognee.CreateEmbedder("huggingface", map[string]interface{}{
    //     "api_key": os.Getenv("HUGGINGFACE_API_KEY"),
    //     "model":   "sentence-transformers/all-MiniLM-L6-v2",
    // })
    
    // 3. 创建记忆服务
    service, err := cognee.NewMemoryService(ctx, db, cognee.MemoryServiceOptions{
        Embedder: embedder,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 4. 添加数据
    memory, err := service.AddMemory(ctx, 
        "AI 正在改变我们的工作和生活方式。人工智能技术正在各个领域产生深远影响。", 
        "text", 
        "main_dataset", 
        map[string]interface{}{
            "author": "示例作者",
            "source": "示例来源",
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("✅ 添加记忆: %s\n", memory.ID)
    
    // 5. 处理知识图谱（提取实体和关系）
    if err := service.ProcessMemory(ctx, memory.ID); err != nil {
        log.Printf("⚠️  处理失败: %v", err)
    } else {
        fmt.Println("✅ 记忆处理完成")
    }
    
    // 6. 搜索
    results, err := service.Search(ctx, "AI", "HYBRID", 10)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("🔍 找到 %d 个结果:\n", len(results))
    for _, result := range results {
        fmt.Printf("  - ID: %s, Score: %.2f, Source: %s\n", 
            result.ID, result.Score, result.Source)
    }
    
    // 7. 获取数据集列表
    datasets, err := service.ListDatasets(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("📊 数据集数量: %d\n", len(datasets))
    
    // 8. 获取图谱数据
    graphData, err := service.GetGraphData(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("🕸️  图谱节点: %d, 边: %d\n", 
        len(graphData.Nodes), len(graphData.Edges))
    
    // 9. 健康检查
    health, err := service.Health(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("💚 健康状态: %s\n", health.Status)
    fmt.Printf("   记忆: %d, 实体: %d, 关系: %d\n", 
        health.Stats.Memories, 
        health.Stats.Entities, 
        health.Stats.Relations)
}
```

## 数据结构

### Memory（记忆）

```go
type Memory struct {
    ID          string                 // 唯一标识符
    Content     string                 // 内容
    Type        string                 // 类型: text, code, url 等
    Dataset     string                 // 所属数据集
    Metadata    map[string]interface{} // 元数据
    CreatedAt   int64                  // 创建时间（Unix 时间戳）
    ProcessedAt int64                  // 处理时间（Unix 时间戳）
    Chunks      []string               // 关联的文本块 ID
}
```

### Entity（实体）

```go
type Entity struct {
    ID        string                 // 唯一标识符
    Name      string                 // 名称
    Type      string                 // 类型: person, organization, concept 等
    Metadata  map[string]interface{} // 元数据
    CreatedAt int64                  // 创建时间
}
```

### Relation（关系）

```go
type Relation struct {
    ID        string                 // 唯一标识符
    From      string                 // 源实体 ID
    To        string                 // 目标实体 ID
    Type      string                 // 关系类型
    Metadata  map[string]interface{} // 元数据
    CreatedAt int64                  // 创建时间
}
```

### Dataset（数据集）

```go
type Dataset struct {
    ID          string                 // 唯一标识符
    Name        string                 // 名称
    Description string                 // 描述
    Metadata    map[string]interface{} // 元数据
    CreatedAt   int64                  // 创建时间
    Status      string                 // 状态: pending, processing, completed, error
}
```

## 运行示例

```bash
cd examples/cognee
go run main.go
```

## 配置选项

### MemoryServiceOptions

```go
type MemoryServiceOptions struct {
    Embedder            Embedder                    // 嵌入生成器（必需）
    FulltextIndexOptions *rxdb.FulltextIndexOptions // 全文搜索选项（可选）
    VectorSearchOptions  *VectorSearchOptions        // 向量搜索选项（可选）
}

type VectorSearchOptions struct {
    DistanceMetric string // 距离度量: cosine, euclidean, dot
    IndexType      string // 索引类型: flat, ivf
}
```

### FulltextIndexOptions

```go
type FulltextIndexOptions struct {
    Tokenize      string // 分词器: "jieba"（中文）, "standard"（英文）
    CaseSensitive bool   // 是否区分大小写
}
```

## 注意事项

1. **嵌入生成器**: 默认的 `SimpleEmbedder` 仅用于演示，生产环境应使用真实的嵌入模型（OpenAI、HuggingFace 等）。

2. **图数据库**: 需要启用图数据库功能才能使用图搜索和可视化功能。

3. **实体和关系提取**: 当前实现使用简单的关键词提取，实际应用中应集成 NLP 模型进行更准确的实体和关系提取。

4. **API 密钥**: 使用 OpenAI 或 HuggingFace 嵌入生成器时，需要设置相应的 API 密钥。

5. **向量维度**: 不同嵌入模型的向量维度不同，确保在创建向量搜索时使用正确的维度。

## 参考

- [Cognee 项目](https://github.com/topoteretes/cognee) - 原始 Python 实现
- [RxDB-Go 文档](../rxdb/README.md) - 底层数据库文档
