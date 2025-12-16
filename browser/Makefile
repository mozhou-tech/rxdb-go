.PHONY: install dev build api frontend seed

# 安装前端依赖
install:
	pnpm install
	cd api && go mod download

# 启动开发服务器（前端和后端）
dev:
	@echo "启动后端 API 服务器..."
	@cd api && go run main.go &
	@sleep 2
	@echo "启动前端开发服务器..."
	@pnpm dev

# 仅启动后端
api:
	cd api && go run main.go

# 仅启动前端
frontend:
	pnpm dev

# 构建前端
build:
	pnpm build

# 构建后端
build-api:
	cd api && go build -o browser-api main.go

# 生成示例数据
seed:
	@echo "🌱 生成示例数据..."
	@cd api && go run seed.go

# 生成大量数据用于性能测试（1万条）
largeseed:
	@echo "🚀 生成大量数据用于性能测试（1万条）..."
	@cd api && go run largeseed.go

