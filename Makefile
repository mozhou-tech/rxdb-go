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

# 格式化代码
fmt:
	go fmt ./...

# 运行 linter
lint:
	golangci-lint run ./...

# 安装依赖
deps:
	go mod download
	go mod tidy

