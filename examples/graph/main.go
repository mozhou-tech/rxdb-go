package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()

	// 创建带图功能的数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "graph-db",
		Path: "./graph-db.db",
		GraphOptions: &rxdb.GraphOptions{
			Enabled:  true,
			Backend:  "memory", // 使用内存模式（生产环境可使用 bolt 或 leveldb）
			Path:     filepath.Join("./graph-db.db", "graph"),
			AutoSync: true, // 启用自动同步
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer db.Close(ctx)

	// 获取图数据库实例
	graphDB := db.Graph()
	if graphDB == nil {
		logrus.Fatal("Graph database not available")
	}

	// 创建文档集合
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	users, err := db.Collection(ctx, "users", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 插入用户文档
	user1, err := users.Insert(ctx, map[string]any{
		"id":   "user1",
		"name": "Alice",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to insert user1")
	}
	fmt.Printf("Inserted user1: %s\n", user1.ID())

	user2, err := users.Insert(ctx, map[string]any{
		"id":   "user2",
		"name": "Bob",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to insert user2")
	}
	fmt.Printf("Inserted user2: %s\n", user2.ID())

	user3, err := users.Insert(ctx, map[string]any{
		"id":   "user3",
		"name": "Charlie",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to insert user3")
	}
	fmt.Printf("Inserted user3: %s\n", user3.ID())

	// 创建图关系：user1 关注 user2，user2 关注 user3
	fmt.Println("\n创建图关系...")
	if err := graphDB.Link(ctx, "user1", "follows", "user2"); err != nil {
		logrus.WithError(err).Fatal("Failed to link user1 -> user2")
	}
	fmt.Println("user1 follows user2")

	if err := graphDB.Link(ctx, "user2", "follows", "user3"); err != nil {
		logrus.WithError(err).Fatal("Failed to link user2 -> user3")
	}
	fmt.Println("user2 follows user3")

	// 查询：查找 user1 关注的所有人
	fmt.Println("\n查询 user1 关注的所有人...")
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get neighbors")
	}
	fmt.Printf("user1 follows: %v\n", neighbors)

	// 使用查询 API
	fmt.Println("\n使用查询 API...")
	query := graphDB.Query()
	if query != nil {
		queryImpl := query.V("user1").Out("follows")
		results, err := queryImpl.All(ctx)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to query")
		}
		fmt.Printf("Query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  %s --%s--> %s\n", r.Subject, r.Predicate, r.Object)
		}
	}

	// 查找路径：user1 到 user3 的路径
	fmt.Println("\n查找 user1 到 user3 的路径...")
	paths, err := graphDB.FindPath(ctx, "user1", "user3", 5, "follows")
	if err != nil {
		logrus.WithError(err).Fatal("Failed to find path")
	}
	fmt.Printf("Found %d paths:\n", len(paths))
	for i, path := range paths {
		fmt.Printf("  Path %d: %v\n", i+1, path)
	}

	// 配置自动关系映射（示例）
	bridge := db.GraphBridge()
	if bridge != nil {
		bridge.AddRelationMapping(&rxdb.GraphRelationMapping{
			Collection:  "users",
			Field:       "follows",
			Relation:    "follows",
			TargetField: "id",
			AutoLink:    true,
		})
		fmt.Println("\n已配置自动关系映射")
	}

	fmt.Println("\n示例完成！")
}
