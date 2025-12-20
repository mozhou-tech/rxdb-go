package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()

	// 创建数据库
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "sort-demo",
		Path: "./data/sort-demo.db",
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}
	defer func() {
		db.Close(ctx)
		os.RemoveAll("./data/sort-demo.db")
	}()

	// 定义学生集合的 schema
	schema := rxdb.Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"title":       "student",
			"description": "学生集合",
			"version":     0,
			"type":        "object",
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"name":      map[string]any{"type": "string"},
				"age":       map[string]any{"type": "integer"},
				"score":     map[string]any{"type": "number"},
				"grade":     map[string]any{"type": "string"},
				"createdAt": map[string]any{"type": "string"},
			},
			"required": []string{"id", "name", "age", "score"},
		},
	}

	// 创建集合
	collection, err := db.Collection(ctx, "students", schema)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create collection")
	}

	// 插入示例学生数据
	students := []map[string]any{
		{
			"id":        "student-001",
			"name":      "张三",
			"age":       20,
			"score":     85.5,
			"grade":     "A",
			"createdAt": "2024-01-15",
		},
		{
			"id":        "student-002",
			"name":      "李四",
			"age":       22,
			"score":     92.0,
			"grade":     "A",
			"createdAt": "2024-01-10",
		},
		{
			"id":        "student-003",
			"name":      "王五",
			"age":       19,
			"score":     78.5,
			"grade":     "B",
			"createdAt": "2024-01-20",
		},
		{
			"id":        "student-004",
			"name":      "赵六",
			"age":       21,
			"score":     88.0,
			"grade":     "A",
			"createdAt": "2024-01-12",
		},
		{
			"id":        "student-005",
			"name":      "钱七",
			"age":       20,
			"score":     95.5,
			"grade":     "A",
			"createdAt": "2024-01-08",
		},
		{
			"id":        "student-006",
			"name":      "孙八",
			"age":       23,
			"score":     72.0,
			"grade":     "C",
			"createdAt": "2024-01-25",
		},
	}

	fmt.Println("📚 插入示例学生数据...")
	for i, student := range students {
		_, err := collection.Insert(ctx, student)
		if err != nil {
			logrus.WithError(err).WithField("student_id", student["id"]).Error("Failed to insert student")
		} else {
			fmt.Printf("  ✅ [%d/%d] %s - %s (年龄: %d, 分数: %.1f)\n",
				i+1, len(students), student["id"], student["name"], student["age"], student["score"])
		}
	}
	fmt.Printf("✅ 已插入 %d 个学生\n\n", len(students))

	// 获取查询集合
	qc := rxdb.AsQueryCollection(collection)
	if qc == nil {
		log.Fatal("Failed to get QueryCollection")
	}

	// ========================================
	// 排序示例
	// ========================================

	// 示例 1: 按分数升序排序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 1: 按分数升序排序 (score: asc)")
	fmt.Println("===========================================")
	results, err := qc.Find(nil).
		Sort(map[string]string{"score": "asc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f, 年龄: %v\n",
			i+1, r.ID(), data["name"], data["score"], data["age"])
	}
	fmt.Println()

	// 示例 2: 按分数降序排序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 2: 按分数降序排序 (score: desc)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{"score": "desc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f, 年龄: %v\n",
			i+1, r.ID(), data["name"], data["score"], data["age"])
	}
	fmt.Println()

	// 示例 3: 按年龄升序排序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 3: 按年龄升序排序 (age: asc)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{"age": "asc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 年龄: %v, 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["age"], data["score"])
	}
	fmt.Println()

	// 示例 4: 按姓名升序排序（字符串排序）
	fmt.Println("===========================================")
	fmt.Println("📊 示例 4: 按姓名升序排序 (name: asc)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{"name": "asc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["score"])
	}
	fmt.Println()

	// 示例 5: 多字段排序 - 先按年级，再按分数降序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 5: 多字段排序 (grade: asc, score: desc)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{
			"grade": "asc",  // 先按年级升序
			"score": "desc", // 再按分数降序
		}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 年级: %s, 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["grade"], data["score"])
	}
	fmt.Println()

	// 示例 6: 多字段排序 - 先按年龄，再按分数
	fmt.Println("===========================================")
	fmt.Println("📊 示例 6: 多字段排序 (age: asc, score: desc)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{
			"age":   "asc",  // 先按年龄升序
			"score": "desc", // 再按分数降序
		}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 年龄: %v, 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["age"], data["score"])
	}
	fmt.Println()

	// 示例 7: 排序 + 限制数量（Top N）
	fmt.Println("===========================================")
	fmt.Println("📊 示例 7: 排序 + 限制数量 (Top 3 学生)")
	fmt.Println("===========================================")
	results, err = qc.Find(nil).
		Sort(map[string]string{"score": "desc"}).
		Limit(3).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Top 3 学生:\n")
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["score"])
	}
	fmt.Println()

	// 示例 8: 排序 + 跳过 + 限制（分页）
	fmt.Println("===========================================")
	fmt.Println("📊 示例 8: 排序 + 分页 (第 2 页，每页 2 条)")
	fmt.Println("===========================================")
	pageSize := 2
	page := 2
	results, err = qc.Find(nil).
		Sort(map[string]string{"score": "desc"}).
		Skip((page - 1) * pageSize).
		Limit(pageSize).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("第 %d 页 (每页 %d 条):\n", page, pageSize)
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f\n",
			(page-1)*pageSize+i+1, r.ID(), data["name"], data["score"])
	}
	fmt.Println()

	// 示例 9: 条件查询 + 排序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 9: 条件查询 + 排序 (年级为 A 的学生，按分数降序)")
	fmt.Println("===========================================")
	results, err = qc.Find(map[string]any{
		"grade": "A",
	}).
		Sort(map[string]string{"score": "desc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个 A 级学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["score"])
	}
	fmt.Println()

	// 示例 10: 范围查询 + 排序
	fmt.Println("===========================================")
	fmt.Println("📊 示例 10: 范围查询 + 排序 (分数 >= 80，按年龄升序)")
	fmt.Println("===========================================")
	results, err = qc.Find(map[string]any{
		"score": map[string]any{
			"$gte": 80.0,
		},
	}).
		Sort(map[string]string{"age": "asc"}).
		Exec(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("找到 %d 个分数 >= 80 的学生:\n", len(results))
	for i, r := range results {
		data := r.Data()
		fmt.Printf("  %d. [%s] %s - 年龄: %v, 分数: %.1f\n",
			i+1, r.ID(), data["name"], data["age"], data["score"])
	}
	fmt.Println()

	fmt.Println("🎉 排序功能演示完成!")
}
