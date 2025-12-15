package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mozy/rxdb-go/pkg/rxdb"
)

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "mydb",
		Path: "./mydb.db",
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	// 定义 schema
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "hero",
			"description": "describes a simple hero",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":      "string",
					"maxLength": 100,
				},
				"name": map[string]any{
					"type": "string",
				},
				"color": map[string]any{
					"type": "string",
				},
			},
			"required": []string{"id", "name"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "heroes", schema)
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// 监听变更（在后台）
	go func() {
		for event := range collection.Changes() {
			fmt.Printf("Change event: %s %s\n", event.Op, event.ID)
		}
	}()

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":    "hero-001",
		"name":  "Superman",
		"color": "blue",
	})
	if err != nil {
		log.Fatalf("Failed to insert: %v", err)
	}
	fmt.Printf("Inserted: %s - %v\n", doc.ID(), doc.Data())

	// 插入更多文档
	collection.Insert(ctx, map[string]any{
		"id":    "hero-002",
		"name":  "Batman",
		"color": "black",
	})
	collection.Insert(ctx, map[string]any{
		"id":    "hero-003",
		"name":  "Wonder Woman",
		"color": "gold",
	})

	// 按 ID 查找
	found, err := collection.FindByID(ctx, "hero-001")
	if err != nil {
		log.Fatalf("Failed to find: %v", err)
	}
	if found != nil {
		fmt.Printf("Found by ID: %v\n", found.Data())
	}

	// 使用查询 API
	qc := rxdb.AsQueryCollection(collection)
	if qc != nil {
		// 查询所有
		all, _ := qc.Find(nil).Exec(ctx)
		fmt.Printf("All heroes: %d\n", len(all))

		// 条件查询
		results, _ := qc.Find(map[string]any{
			"color": "blue",
		}).Exec(ctx)
		fmt.Printf("Blue heroes: %d\n", len(results))

		// 使用操作符查询
		results, _ = qc.Find(map[string]any{
			"name": map[string]any{
				"$regex": "man$",
			},
		}).Exec(ctx)
		fmt.Printf("Heroes ending with 'man': %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  - %s\n", r.Data()["name"])
		}

		// 排序和分页
		results, _ = qc.Find(nil).
			Sort(map[string]string{"name": "asc"}).
			Limit(2).
			Exec(ctx)
		fmt.Printf("First 2 heroes (sorted by name):\n")
		for _, r := range results {
			fmt.Printf("  - %s\n", r.Data()["name"])
		}

		// 统计
		count, _ := qc.Find(nil).Count(ctx)
		fmt.Printf("Total heroes: %d\n", count)
	}

	// Upsert（更新或插入）
	updated, err := collection.Upsert(ctx, map[string]any{
		"id":    "hero-001",
		"name":  "Superman",
		"color": "red", // 更新颜色
	})
	if err != nil {
		log.Fatalf("Failed to upsert: %v", err)
	}
	fmt.Printf("Upserted: %v\n", updated.Data())

	// 删除文档
	err = collection.Remove(ctx, "hero-002")
	if err != nil {
		log.Fatalf("Failed to remove: %v", err)
	}
	fmt.Println("Removed hero-002")

	// 获取所有文档
	allDocs, _ := collection.All(ctx)
	fmt.Printf("Remaining heroes: %d\n", len(allDocs))
	for _, d := range allDocs {
		fmt.Printf("  - %s: %s\n", d.ID(), d.Data()["name"])
	}

	// 清理
	os.Remove("./mydb.db")
	fmt.Println("Done!")
}

