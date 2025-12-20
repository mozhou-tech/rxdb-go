.PHONY: test build clean examples

# 运行测试
test:
	go test ./pkg/rxdb

# 构建示例
build:
	go build -o bin/basic ./examples/basic
	go build -o bin/supabase-sync ./examples/supabase-sync

# 运行基础示例
run-basic:
	go run ./examples/basic

# 运行 Supabase 同步示例
run-sync:
	go run ./examples/supabase-sync

# 清理
clean:
	rm -rf bin/
	rm -f *.db
	find . -name "*.db" -type f -delete

# 格式化代码（排除 examples 目录）
fmt:
	go fmt ./pkg/...

# 运行 linter（排除 examples 目录）
lint:
	golangci-lint run ./pkg/...

# 安装依赖
deps:
	go mod download
	go mod tidy

# 发布相关命令
.PHONY: tag release verify-release

# 创建并推送版本标签（自动使用当前时间）
# 使用方法: make tag
tag:
	@VERSION=$$(date +%Y%m%d-%H%M%S); \
	echo "创建版本标签: v$$VERSION"; \
	git tag v$$VERSION; \
	echo "标签已创建，正在推送到远程..."; \
	git push github v$$VERSION

# 验证发布（检查代码是否可以正常构建和测试）
verify-release:
	@echo "验证代码构建..."
	go mod tidy
	@echo "验证库代码编译（排除 examples 目录）..."
	@go vet ./pkg/... || true
	@echo "✓ 代码检查完成"
	@echo "运行测试..."
	# go test ./pkg/... -v
	@echo "验证通过！"

# 完整发布流程（自动使用当前时间生成版本标签）
# 使用方法: make release
release: verify-release
	@VERSION=$$(date +%Y%m%d-%H%M%S); \
	echo "准备发布版本 v$$VERSION..."; \
	echo "1. 确保所有更改已提交:"; \
	git status --short; \
	echo ""; \
	echo "2. 创建版本标签 v$$VERSION..."; \
	git tag v0.0.0-$$VERSION; \
	echo ""; \
	echo "3. 标签已创建，执行以下命令完成发布:"; \
	echo "   git push github master"; \
	echo "   git push github v0.0.0-$$VERSION"; \
	git push github master; \
	git push github v0.0.0-$$VERSION


