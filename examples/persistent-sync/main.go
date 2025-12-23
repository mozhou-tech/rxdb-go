package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/replication/supabase"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "persistent-sync-db",
		Path: "./persistent-sync.db",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	// 定义 schema
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"title":     map[string]any{"type": "string"},
				"completed": map[string]any{"type": "boolean"},
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
		log.Fatal(err)
	}

	// 配置 Supabase 同步（从环境变量读取）
	supabaseURL := os.Getenv("SUPABASE_URL")
	supabaseKey := os.Getenv("SUPABASE_KEY")

	if supabaseURL == "" || supabaseKey == "" {
		log.Println("⚠️  SUPABASE_URL 和 SUPABASE_KEY 未设置，使用演示模式")
		log.Println("设置环境变量以启用 Supabase 同步：")
		log.Println("  export SUPABASE_URL=https://your-project.supabase.co")
		log.Println("  export SUPABASE_KEY=your-anon-key")

		// 演示本地操作
		demoLocal(ctx, collection)
		return
	}

	// 创建持久化同步实例
	persistentRep, err := supabase.NewPersistentReplication(collection, supabase.PersistentReplicationOptions{
		ReplicationOptions: supabase.ReplicationOptions{
			SupabaseURL:    supabaseURL,
			SupabaseKey:    supabaseKey,
			Table:          "todos",
			PrimaryKey:     "id",
			UpdatedAtField: "updated_at",
			PullInterval:   10 * time.Second,
			PushOnChange:   true,
			ConflictHandler: func(local, remote map[string]any) map[string]any {
				// 时间戳优先策略
				localTime, _ := time.Parse(time.RFC3339, fmt.Sprintf("%v", local["updated_at"]))
				remoteTime, _ := time.Parse(time.RFC3339, fmt.Sprintf("%v", remote["updated_at"]))
				if remoteTime.After(localTime) {
					return remote
				}
				return local
			},
		},
		StatePath:            "./.rxdb-sync-state.json",
		MaxRetries:           10,
		RetryInterval:        5 * time.Second,
		QueueProcessInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 启动持久化同步
	if err := persistentRep.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer persistentRep.Stop()

	// 监听错误
	go func() {
		for err := range persistentRep.Errors() {
			log.Printf("❌ 同步错误: %v", err)
		}
	}()

	// 监听同步状态和队列大小
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := persistentRep.State()
				queueSize := persistentRep.GetPendingQueueSize()
				fmt.Printf("📊 同步状态: %s, 待推送队列: %d 条\n", state, queueSize)
			}
		}
	}()

	// 演示操作
	fmt.Println("✅ 持久化同步已启动")
	fmt.Println("📝 创建一些待办事项...")

	// 插入文档（会自动同步，失败会加入队列）
	collection.Insert(ctx, map[string]any{
		"id":         "todo-001",
		"title":      "学习 RxDB Go 持久化同步",
		"completed":  false,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	collection.Insert(ctx, map[string]any{
		"id":         "todo-002",
		"title":      "实现待推送队列持久化",
		"completed":  true,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	// 修改文档
	doc, _ := collection.FindByID(ctx, "todo-001")
	if doc != nil {
		doc.Update(ctx, map[string]any{
			"completed":  true,
			"updated_at": time.Now().Format(time.RFC3339),
		})
	}

	fmt.Println("✅ 操作完成，数据会自动同步（失败会加入持久化队列）")

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 正在停止同步并保存状态...")
}

func demoLocal(ctx context.Context, collection rxdb.Collection) {
	fmt.Println("运行在本地模式（无 Supabase 同步）")

	collection.Insert(ctx, map[string]any{
		"id":         "todo-001",
		"title":      "本地待办 1",
		"completed":  false,
		"updated_at": time.Now().Format(time.RFC3339),
	})

	all, _ := collection.All(ctx)
	fmt.Printf("总文档数: %d\n", len(all))
}
