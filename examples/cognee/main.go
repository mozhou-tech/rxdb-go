package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/mozy/rxdb-go/pkg/cognee"
	"github.com/mozy/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

func main() {
	// 从环境变量读取配置
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/cognee-memory"
	}

	ctx := context.Background()

	// 确保数据目录存在
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		logrus.WithError(err).Fatal("Failed to create data directory")
	}

	// 创建数据库（启用图数据库功能）
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
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer db.Close(ctx)

	// 创建简单的嵌入生成器
	embedder := cognee.NewSimpleEmbedder(384)

	// 创建记忆服务
	service, err := cognee.NewMemoryService(ctx, db, cognee.MemoryServiceOptions{
		Embedder: embedder,
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create memory service")
	}

	// 示例：添加记忆
	memory, err := service.AddMemory(ctx, "AI 正在改变我们的工作和生活方式。", "text", "main_dataset", nil)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to add memory")
	}
	logrus.WithField("id", memory.ID).Info("Added memory")

	// 示例：处理记忆
	if err := service.ProcessMemory(ctx, memory.ID); err != nil {
		logrus.WithError(err).Error("Failed to process memory")
	}

	// 示例：搜索
	results, err := service.Search(ctx, "AI", "HYBRID", 10)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to search")
	}
	logrus.WithField("count", len(results)).Info("Found results")
	for _, result := range results {
		logrus.WithFields(logrus.Fields{
			"id":    result.ID,
			"score": result.Score,
		}).Info("  - Result")
	}

	// 示例：健康检查
	health, err := service.Health(ctx)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get health")
	}
	logrus.WithFields(logrus.Fields{
		"status":   health.Status,
		"memory":   health.Stats.Memories,
		"entity":   health.Stats.Entities,
		"relation": health.Stats.Relations,
	}).Info("Health check")
}
