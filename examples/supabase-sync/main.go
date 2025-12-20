package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/replication/supabase"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "syncdb",
		Path: "./syncdb.db",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer db.Close(ctx)

	// 定义 schema
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "todo",
			"description": "todo item",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "string",
				},
				"title": map[string]any{
					"type": "string",
				},
				"completed": map[string]any{
					"type": "boolean",
				},
				"updated_at": map[string]any{
					"type":   "string",
					"format": "date-time",
				},
			},
			"required": []string{"id", "title"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "todos", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 配置 Supabase 同步
	// 注意：需要设置实际的 Supabase URL 和 API Key
	supabaseURL := os.Getenv("SUPABASE_URL")
	supabaseKey := os.Getenv("SUPABASE_KEY")

	if supabaseURL == "" || supabaseKey == "" {
		logrus.Warn("SUPABASE_URL and SUPABASE_KEY not set, skipping sync")
		logrus.Info("Set environment variables to enable Supabase sync:")
		logrus.Info("  export SUPABASE_URL=https://your-project.supabase.co")
		logrus.Info("  export SUPABASE_KEY=your-anon-key")

		// 演示本地操作
		demoLocal(ctx, collection)
		return
	}

	// 创建同步客户端
	replication, err := supabase.NewReplication(collection, supabase.ReplicationOptions{
		SupabaseURL:    supabaseURL,
		SupabaseKey:    supabaseKey,
		Table:          "todos",
		PrimaryKey:     "id",
		UpdatedAtField: "updated_at",
		PullInterval:   10 * time.Second,
		PushOnChange:   true,
		ConflictHandler: func(local, remote map[string]any) map[string]any {
			// 简单的冲突处理：使用更新的时间戳
			localTime, _ := time.Parse(time.RFC3339, fmt.Sprintf("%v", local["updated_at"]))
			remoteTime, _ := time.Parse(time.RFC3339, fmt.Sprintf("%v", remote["updated_at"]))
			if remoteTime.After(localTime) {
				return remote
			}
			return local
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create replication")
	}

	// 启动同步
	if err := replication.Start(ctx); err != nil {
		logrus.WithError(err).Fatal("Failed to start replication")
	}
	defer replication.Stop()

	// 监听错误
	go func() {
		for err := range replication.Errors() {
			logrus.WithError(err).Error("Replication error")
		}
	}()

	// 监听同步状态
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := replication.State()
				fmt.Printf("Replication state: %s\n", state)
			}
		}
	}()

	// 演示本地操作（会自动同步到 Supabase）
	fmt.Println("Creating local todos (will sync to Supabase)...")
	collection.Insert(ctx, map[string]any{
		"id":         "todo-001",
		"title":      "Learn RxDB Go",
		"completed":  false,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	collection.Insert(ctx, map[string]any{
		"id":         "todo-002",
		"title":      "Implement Supabase sync",
		"completed":  true,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	// 等待同步
	time.Sleep(2 * time.Second)

	// 手动触发一次拉取
	fmt.Println("Pulling from Supabase...")
	if err := replication.PullOnce(ctx); err != nil {
		logrus.WithError(err).Error("Pull error")
	}

	// 监听本地变更
	go func() {
		for event := range collection.Changes() {
			fmt.Printf("Local change: %s %s\n", event.Op, event.ID)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
}

func demoLocal(ctx context.Context, collection rxdb.Collection) {
	fmt.Println("Running in local-only mode (no Supabase sync)")

	// 插入一些数据
	collection.Insert(ctx, map[string]any{
		"id":         "todo-001",
		"title":      "Local todo 1",
		"completed":  false,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	collection.Insert(ctx, map[string]any{
		"id":         "todo-002",
		"title":      "Local todo 2",
		"completed":  true,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	// 查询
	all, _ := collection.All(ctx)
	fmt.Printf("Total todos: %d\n", len(all))
	for _, doc := range all {
		fmt.Printf("  - %s: %s (completed: %v)\n",
			doc.ID(),
			doc.Data()["title"],
			doc.Data()["completed"])
	}

	// 清理
	os.Remove("./syncdb.db")
}
