# 发布指南

本文档说明如何发布 `rxdb-go` 模块，使其可以通过 `go get` 被其他项目使用。

## 前提条件

1. 确保代码已推送到 GitHub 仓库：`github.com/mozhou-tech/rxdb-go`
2. 确保 `go.mod` 中的模块路径正确：`module github.com/mozhou-tech/rxdb-go`
3. 确保所有代码已提交并推送到远程仓库

## 发布步骤

### 1. 确保代码已提交并推送

```bash
# 检查状态
git status

# 提交所有更改（如果有）
git add .
git commit -m "准备发布版本"

# 推送到远程仓库
git push origin main
```

### 2. 创建版本标签

Go 模块使用 Git 标签来标识版本。遵循语义化版本控制（Semantic Versioning）：

```bash
# 创建并推送版本标签（例如 v1.0.0）
git tag v1.0.0
git push origin v1.0.0

# 或者使用 Makefile 命令
make tag VERSION=v1.0.0
```

版本格式：
- `v1.0.0` - 主版本号.次版本号.修订号
- `v1.0.1` - 修复 bug
- `v1.1.0` - 新功能
- `v2.0.0` - 重大变更（需要更新 go.mod 中的模块路径）

### 3. 验证发布

在另一个项目中测试安装：

```bash
# 安装最新版本
go get github.com/mozhou-tech/rxdb-go@latest

# 安装特定版本
go get github.com/mozhou-tech/rxdb-go@v1.0.0

# 安装主分支（开发版本）
go get github.com/mozhou-tech/rxdb-go@main
```

### 4. 使用示例

其他项目可以通过以下方式使用：

```go
package main

import (
    "context"
    "github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

func main() {
    ctx := context.Background()
    
    db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
        Name: "mydb",
        Path: "./mydb.db",
    })
    if err != nil {
        panic(err)
    }
    defer db.Close(ctx)
    
    // ... 使用数据库
}
```

## 版本管理最佳实践

1. **主版本号（Major）**：不兼容的 API 变更
   - 如果发布 v2.0.0，需要更新模块路径为 `github.com/mozhou-tech/rxdb-go/v2`

2. **次版本号（Minor）**：向后兼容的功能新增
   - v1.0.0 → v1.1.0

3. **修订号（Patch）**：向后兼容的问题修复
   - v1.0.0 → v1.0.1

4. **预发布版本**：
   - `v1.0.0-alpha.1` - Alpha 版本
   - `v1.0.0-beta.1` - Beta 版本
   - `v1.0.0-rc.1` - 候选版本

## 常见问题

### Q: 如何更新已发布的版本？

A: 创建新的版本标签并推送即可。Go 会自动识别所有已发布的版本。

### Q: 如何删除错误的标签？

```bash
# 删除本地标签
git tag -d v1.0.0

# 删除远程标签
git push origin :refs/tags/v1.0.0
```

### Q: 如何查看所有已发布的版本？

```bash
git tag -l
```

### Q: 模块路径必须是 GitHub 吗？

A: 不一定。可以是任何 Git 托管服务（GitLab、Bitbucket 等），只要：
- 可以通过 HTTPS 访问
- 支持 Git 标签
- 符合 Go 模块路径规范

## 自动化发布

可以使用 Makefile 中的命令简化发布流程：

```bash
# 创建并推送版本标签
make tag VERSION=v1.0.0

# 验证构建
make test

# 清理并准备发布
make clean
```

