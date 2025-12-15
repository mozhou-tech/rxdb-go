package rxdb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestQuery_Find(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_query.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_query.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	collection.Insert(ctx, map[string]any{
		"id":    "1",
		"name":  "Alice",
		"age":   30,
		"color": "blue",
	})
	collection.Insert(ctx, map[string]any{
		"id":    "2",
		"name":  "Bob",
		"age":   25,
		"color": "red",
	})
	collection.Insert(ctx, map[string]any{
		"id":    "3",
		"name":  "Charlie",
		"age":   35,
		"color": "blue",
	})

	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	// 测试简单查询
	results, err := qc.Find(map[string]any{
		"color": "blue",
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 测试操作符查询
	results, err = qc.Find(map[string]any{
		"age": map[string]any{
			"$gte": 30,
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results with age >= 30, got %d", len(results))
	}

	// 测试 $in 操作符
	results, err = qc.Find(map[string]any{
		"name": map[string]any{
			"$in": []any{"Alice", "Bob"},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results with name in [Alice, Bob], got %d", len(results))
	}

	// 测试 $regex 操作符
	results, err = qc.Find(map[string]any{
		"name": map[string]any{
			"$regex": "^A",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result with name starting with 'A', got %d", len(results))
	}
}

func TestCollection_FindAndFindOne(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_collection_query.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_collection_query.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "heroes", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	_, _ = collection.Insert(ctx, map[string]any{
		"id":    "1",
		"name":  "Alice",
		"color": "blue",
	})
	_, _ = collection.Insert(ctx, map[string]any{
		"id":    "2",
		"name":  "Bob",
		"color": "red",
	})

	results, err := collection.Find(map[string]any{
		"color": "blue",
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute collection.Find: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result from collection.Find, got %d", len(results))
	}

	first, err := collection.FindOne(ctx, map[string]any{
		"name": "Bob",
	})
	if err != nil {
		t.Fatalf("Failed to execute collection.FindOne: %v", err)
	}
	if first == nil || first.GetString("id") != "2" {
		t.Fatalf("Expected to find document with id 2, got %+v", first)
	}
}

func TestQuery_Sort(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_sort.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_sort.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	collection.Insert(ctx, map[string]any{
		"id":   "1",
		"name": "Charlie",
		"age":  35,
	})
	collection.Insert(ctx, map[string]any{
		"id":   "2",
		"name": "Alice",
		"age":  30,
	})
	collection.Insert(ctx, map[string]any{
		"id":   "3",
		"name": "Bob",
		"age":  25,
	})

	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	// 测试排序
	results, err := qc.Find(nil).
		Sort(map[string]string{"age": "asc"}).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// 验证排序
	firstAge := results[0].Data()["age"].(float64)
	lastAge := results[2].Data()["age"].(float64)

	if firstAge >= lastAge {
		t.Error("Results should be sorted by age ascending")
	}
}

func TestQuery_LimitSkip(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_limit.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_limit.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入多个文档
	for i := 1; i <= 10; i++ {
		collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("Document %d", i),
		})
	}

	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	// 测试 Limit
	results, err := qc.Find(nil).
		Limit(5).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// 测试 Skip
	results, err = qc.Find(nil).
		Skip(3).
		Limit(5).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results after skip, got %d", len(results))
	}
}

func TestQuery_Count(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_count.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_count.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	for i := 1; i <= 5; i++ {
		collection.Insert(ctx, map[string]any{
			"id":   string(rune('0' + i)),
			"name": "Document " + string(rune('0'+i)),
			"age":  i * 10,
		})
	}

	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	// 测试 Count
	count, err := qc.Find(nil).Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected count 5, got %d", count)
	}

	// 测试带条件的 Count
	count, err = qc.Find(map[string]any{
		"age": map[string]any{
			"$gte": 30,
		},
	}).Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3 with age >= 30, got %d", count)
	}
}

func TestQuery_FindOne(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_findone.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_findone.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	collection.Insert(ctx, map[string]any{
		"id":   "1",
		"name": "Alice",
	})

	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	// 测试 FindOne
	doc, err := qc.Find(map[string]any{
		"name": "Alice",
	}).FindOne(ctx)
	if err != nil {
		t.Fatalf("Failed to find one: %v", err)
	}

	if doc == nil {
		t.Fatal("Expected to find document")
	}

	if doc.Data()["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got '%v'", doc.Data()["name"])
	}

	// 测试不存在的文档
	doc, err = qc.Find(map[string]any{
		"name": "Bob",
	}).FindOne(ctx)
	if err != nil {
		t.Fatalf("Failed to find one: %v", err)
	}

	if doc != nil {
		t.Error("Expected nil for non-existent document")
	}
}

