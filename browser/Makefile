.PHONY: install dev build api frontend

# 安装前端依赖
install:
	cd browser && pnpm install
	cd browser/api && go mod download

# 启动开发服务器（前端和后端）
dev:
	@echo "启动后端 API 服务器..."
	@cd browser/api && go run main.go &
	@sleep 2
	@echo "启动前端开发服务器..."
	@cd browser && pnpm dev

# 仅启动后端
api:
	cd browser/api && go run main.go

# 仅启动前端
frontend:
	cd browser && pnpm dev

# 构建前端
build:
	cd browser && pnpm build

# 构建后端
build-api:
	cd browser/api && go build -o browser-api main.go

