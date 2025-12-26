# RxDB-Go 附件存储指南

在 `rxdb-go` 中，附件存储采用了“元数据在数据库，数据在文件系统”的架构方案。这种设计能够极大地优化内存使用，并支持超大文件的存储。

## 附件存储机制（基于文件系统拷贝）

当你通过 `FilePath` 方式添加附件时，系统会执行以下操作：

1.  **流式拷贝**：系统会自动打开源文件，并使用 `io.Copy` 将内容流式传输到数据库目录下的 `attachments` 子目录中。
2.  **自动重命名**：为了防止冲突，文件会按照 `{docID}_{attachmentID}_{filename}` 的格式进行重命名。
3.  **哈希计算**：在拷贝过程中，系统会利用 `io.MultiWriter` 同时计算 MD5 和 SHA256 哈希值，确保数据完整性且不增加额外的 IO 开销。
4.  **内存压榨**：数据库中仅存储附件的元数据（如 ID、MIME 类型、大小、哈希值等），`Data` 字段在持久化时会被置空，实际数据保存在磁盘。

## 代码示例

### 添加附件（基于文件路径）

```go
package main

import (
	"context"
	"fmt"
	"log"
	"github.com/rxdb-go/rxdb-go/pkg/rxdb"
)

func main() {
	ctx := context.Background()

	// 1. 初始化数据库
	db, _ := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "mydb",
		Path: "./data/mydb.db", // 附件将存储在 ./data/mydb.db/attachments/ 目录下
	})

	collection, _ := db.Collection(ctx, "docs", rxdb.Schema{PrimaryKey: "id"})

	// 2. 获取或插入文档
	doc, _ := collection.Insert(ctx, map[string]any{"id": "doc-1"})

	// 3. 准备附件
	attachment := &rxdb.Attachment{
		ID:       "att-1",
		Name:     "manual.pdf",
		Type:     "application/pdf",
		FilePath: "/path/to/source/manual.pdf", // 指定源文件路径
	}

	// 4. 存储附件
	// 系统会自动执行文件拷贝并计算哈希
	err := doc.PutAttachment(ctx, attachment)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("附件已存储，SHA256: %s, 大小: %d 字节\n", attachment.SHA256, attachment.Size)
}
```

### 读取附件

读取附件时，系统会从文件系统加载数据到 `Data` 字段：

```go
att, err := collection.GetAttachment(ctx, "doc-1", "att-1")
if err != nil {
    log.Fatal(err)
}
// att.Data 包含了文件内容
fmt.Printf("读取到附件: %s, 长度: %d\n", att.Name, len(att.Data))
```

## 关键技术细节

| 特性 | 说明 |
| :--- | :--- |
| **存储路径** | 默认位于 `DatabaseOptions.Path` 下的 `attachments` 文件夹。 |
| **流式处理** | 使用 `io.Reader` 接口进行处理，支持 GB 级别的大文件而不会导致 OOM。 |
| **优先级** | 在 `PutAttachment` 时，若设置了 `FilePath`，系统将优先从路径拷贝；若未设置且 `Data` 不为空，则从内存写入。 |
| **数据校验** | 提供 MD5 和 SHA256 校验，方便后续同步或完整性检查。 |
| **路径安全** | 系统会自动对文件名进行 `filepath.Base` 处理，防止路径遍历攻击。 |

