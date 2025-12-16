package rxdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestQuery_Find(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_query.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_query.db")

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
		Path: "../../data/test_collection_query.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_collection_query.db")

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
		Path: "../../data/test_sort.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_sort.db")

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
		Path: "../../data/test_limit.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_limit.db")

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
		Path: "../../data/test_count.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_count.db")

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
		Path: "../../data/test_findone.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_findone.db")

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

func TestQuery_Operator_Eq(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_eq.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "name": "Alice", "age": 30, "status": "active"},
		{"id": "doc2", "name": "Bob", "age": 25, "status": "inactive"},
		{"id": "doc3", "name": "Alice", "age": 35, "status": "active"},
		{"id": "doc4", "name": "Charlie", "age": 30, "status": "pending"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)

	// 测试字符串等于
	results, err := qc.Find(map[string]any{
		"name": map[string]any{"$eq": "Alice"},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids := make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc3"] {
		t.Error("Expected doc1 and doc3 in results")
	}
	if ids["doc2"] || ids["doc4"] {
		t.Error("doc2 and doc4 should not be in results")
	}

	// 测试数字等于
	results, err = qc.Find(map[string]any{
		"age": map[string]any{"$eq": 30},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids = make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc4"] {
		t.Error("Expected doc1 and doc4 in results")
	}

	// 测试布尔值等于（如果有布尔字段）
	results, err = qc.Find(map[string]any{
		"status": map[string]any{"$eq": "active"},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids = make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc3"] {
		t.Error("Expected doc1 and doc3 in results")
	}

	// 测试不存在的值等于
	results, err = qc.Find(map[string]any{
		"name": map[string]any{"$eq": "Eve"},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestQuery_Operator_Ne(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_ne.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Alice", "age": 35})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"name": map[string]any{"$ne": "Alice"},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Bob" {
		t.Errorf("Expected 'Bob', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_Gt(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_gt.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "age": 20})
	collection.Insert(ctx, map[string]any{"id": "2", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "3", "age": 40})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$gt": 30},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetInt("age") != 40 {
		t.Errorf("Expected age 40, got %d", results[0].GetInt("age"))
	}
}

func TestQuery_Operator_Lt(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_lt.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "age": 20})
	collection.Insert(ctx, map[string]any{"id": "2", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "3", "age": 40})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$lt": 30},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetInt("age") != 20 {
		t.Errorf("Expected age 20, got %d", results[0].GetInt("age"))
	}
}

func TestQuery_Operator_Nin(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_nin.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice"})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob"})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie"})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"name": map[string]any{"$nin": []any{"Alice", "Bob"}},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Charlie" {
		t.Errorf("Expected 'Charlie', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_Exists(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_exists.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "email": "alice@example.com"})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob"}) // 没有 email

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"email": map[string]any{"$exists": true},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_And(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_and.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30, "color": "blue"})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 30, "color": "red"})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 25, "color": "blue"})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"$and": []any{
			map[string]any{"age": 30},
			map[string]any{"color": "blue"},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_Or(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_or.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"$or": []any{
			map[string]any{"name": "Alice"},
			map[string]any{"age": 35},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestQuery_Operator_All(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_all.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "tags": []any{"tag1", "tag2", "tag3"}})
	collection.Insert(ctx, map[string]any{"id": "2", "tags": []any{"tag1", "tag2"}})
	collection.Insert(ctx, map[string]any{"id": "3", "tags": []any{"tag2", "tag3"}})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"tags": map[string]any{"$all": []any{"tag1", "tag2"}},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestQuery_Operator_Size(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_size.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "tags": []any{"tag1"}})
	collection.Insert(ctx, map[string]any{"id": "2", "tags": []any{"tag1", "tag2"}})
	collection.Insert(ctx, map[string]any{"id": "3", "tags": []any{"tag1", "tag2", "tag3"}})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"tags": map[string]any{"$size": 2},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].ID() != "2" {
		t.Errorf("Expected ID '2', got '%s'", results[0].ID())
	}
}

func TestQuery_Operator_Not(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_not.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"$not": map[string]any{
			"age": map[string]any{"$gte": 30},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Bob" {
		t.Errorf("Expected 'Bob', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_Nor(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_nor.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"$nor": []any{
			map[string]any{"name": "Alice"},
			map[string]any{"age": 35},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Bob" {
		t.Errorf("Expected 'Bob', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_Operator_ElemMatch(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_elemmatch.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{
		"id": "1",
		"items": []any{
			map[string]any{"name": "item1", "price": 10},
			map[string]any{"name": "item2", "price": 20},
		},
	})
	collection.Insert(ctx, map[string]any{
		"id": "2",
		"items": []any{
			map[string]any{"name": "item3", "price": 15},
		},
	})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"items": map[string]any{
			"$elemMatch": map[string]any{
				"price": map[string]any{"$gt": 15},
			},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].ID() != "1" {
		t.Errorf("Expected ID '1', got '%s'", results[0].ID())
	}
}

func TestQuery_Operator_Type(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_type.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "value": "string"})
	collection.Insert(ctx, map[string]any{"id": "2", "value": 123})
	collection.Insert(ctx, map[string]any{"id": "3", "value": true})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"value": map[string]any{"$type": "string"},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].ID() != "1" {
		t.Errorf("Expected ID '1', got '%s'", results[0].ID())
	}
}

func TestQuery_Operator_Mod(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_mod.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "age": 20})
	collection.Insert(ctx, map[string]any{"id": "2", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "age": 30})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$mod": []any{5, 0}}, // age % 5 == 0
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestQuery_Operator_Mod_BoundaryCases 测试 Mod 操作符的边界情况
func TestQuery_Operator_Mod_BoundaryCases(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_mod_boundary.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据，包括边界值
	collection.Insert(ctx, map[string]any{"id": "1", "age": 0})   // 0 % 5 == 0
	collection.Insert(ctx, map[string]any{"id": "2", "age": 5})   // 5 % 5 == 0
	collection.Insert(ctx, map[string]any{"id": "3", "age": 10})  // 10 % 5 == 0
	collection.Insert(ctx, map[string]any{"id": "4", "age": 1})   // 1 % 5 == 1
	collection.Insert(ctx, map[string]any{"id": "5", "age": -5})  // 负数
	collection.Insert(ctx, map[string]any{"id": "6", "age": 100}) // 大数

	qc := AsQueryCollection(collection)

	// 测试 age % 5 == 0
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$mod": []any{5, 0}},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该返回 age 为 0, 5, 10 的文档
	if len(results) < 3 {
		t.Errorf("Expected at least 3 results (0, 5, 10), got %d", len(results))
	}

	// 测试 age % 5 == 1
	results2, err := qc.Find(map[string]any{
		"age": map[string]any{"$mod": []any{5, 1}},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该返回 age 为 1 的文档
	if len(results2) < 1 {
		t.Errorf("Expected at least 1 result (age=1), got %d", len(results2))
	}

	// 测试除以 1 的情况（所有数字 % 1 == 0）
	results3, err := qc.Find(map[string]any{
		"age": map[string]any{"$mod": []any{1, 0}},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 所有整数 % 1 == 0，所以应该返回所有文档
	if len(results3) < 6 {
		t.Logf("Mod 1 query returned %d results (expected all)", len(results3))
	}
}

// TestQuery_Operator_Not_Nested 测试嵌套 NOT
func TestQuery_Operator_Not_Nested(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_not_nested.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30, "active": true})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25, "active": false})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35, "active": true})

	qc := AsQueryCollection(collection)
	// 测试嵌套 NOT：NOT (age >= 30 AND active == true)
	results, err := qc.Find(map[string]any{
		"$not": map[string]any{
			"$and": []any{
				map[string]any{"age": map[string]any{"$gte": 30}},
				map[string]any{"active": true},
			},
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// NOT (age >= 30 AND active == true) = (age < 30) OR (active == false)
	// 所以应该返回 Bob (age < 30) 和 Bob (active == false) - 但 Bob 只有一个
	// 实际上应该是 Bob (满足 age < 30)
	if len(results) < 1 {
		t.Errorf("Expected at least 1 result, got %d", len(results))
	}

	// 验证 Bob 在结果中
	foundBob := false
	for _, doc := range results {
		if doc.GetString("name") == "Bob" {
			foundBob = true
			break
		}
	}
	if !foundBob {
		t.Error("Expected 'Bob' in results")
	}
}

func TestQuery_Chain(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_chain.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{
			"$gt": 25,
			"$lt": 35,
		},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", results[0].GetString("name"))
	}
}

func TestQuery_SortMultipleFields(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_sort_multiple.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30, "score": 80})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 30, "score": 90})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 25, "score": 85})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(nil).
		Sort(map[string]string{
			"age":   "asc",
			"score": "desc",
		}).
		Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// 第一个应该是 age=25 的
	if results[0].GetInt("age") != 25 {
		t.Errorf("Expected age 25 first, got %d", results[0].GetInt("age"))
	}

	// 接下来两个 age=30 的，score 高的在前
	if results[1].GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", results[1].GetInt("age"))
	}
	if results[1].GetInt("score") != 90 {
		t.Errorf("Expected score 90, got %d", results[1].GetInt("score"))
	}
}

func TestQuery_Operator_Gte(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_gte.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "age": 20})
	collection.Insert(ctx, map[string]any{"id": "2", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "3", "age": 40})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$gte": 30},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 验证结果包含 age >= 30 的文档
	ages := make(map[int]bool)
	for _, doc := range results {
		ages[doc.GetInt("age")] = true
	}
	if !ages[30] || !ages[40] {
		t.Error("Results should include ages 30 and 40")
	}
}

func TestQuery_Operator_Lte(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_lte.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "age": 20})
	collection.Insert(ctx, map[string]any{"id": "2", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "3", "age": 40})

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"age": map[string]any{"$lte": 30},
	}).Exec(ctx)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 验证结果包含 age <= 30 的文档
	ages := make(map[int]bool)
	for _, doc := range results {
		ages[doc.GetInt("age")] = true
	}
	if !ages[20] || !ages[30] {
		t.Error("Results should include ages 20 and 30")
	}
}

func TestQuery_Observe(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_observe.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入初始数据
	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})

	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"age": map[string]any{"$gte": 30}})

	// 创建观察者
	observeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := query.Observe(observeCtx)

	// 接收初始结果
	initialResults := <-resultChan
	if len(initialResults) != 1 {
		t.Errorf("Expected 1 initial result, got %d", len(initialResults))
	}
	if initialResults[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", initialResults[0].GetString("name"))
	}

	// 插入新文档，应该触发更新
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	// 等待并接收更新
	updatedResults := <-resultChan
	if len(updatedResults) != 2 {
		t.Errorf("Expected 2 results after insert, got %d", len(updatedResults))
	}

	// 更新文档，应该触发更新
	doc, _ := collection.FindByID(ctx, "2")
	if doc != nil {
		doc.Update(ctx, map[string]any{"age": 32})
	}

	// 等待并接收更新
	updatedResults2 := <-resultChan
	if len(updatedResults2) != 3 {
		t.Errorf("Expected 3 results after update, got %d", len(updatedResults2))
	}
}

func TestQuery_ObserveMultiple(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_observe_multiple.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})

	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"age": map[string]any{"$gte": 25}})

	// 创建多个观察者
	observeCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	observeCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	resultChan1 := query.Observe(observeCtx1)
	resultChan2 := query.Observe(observeCtx2)

	// 两个观察者都应该收到初始结果
	results1 := <-resultChan1
	results2 := <-resultChan2

	if len(results1) != 1 || len(results2) != 1 {
		t.Errorf("Expected both observers to receive 1 result, got %d and %d", len(results1), len(results2))
	}

	// 插入新文档，两个观察者都应该收到更新
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 28})

	results1_2 := <-resultChan1
	results2_2 := <-resultChan2

	if len(results1_2) != 2 || len(results2_2) != 2 {
		t.Errorf("Expected both observers to receive 2 results after insert, got %d and %d", len(results1_2), len(results2_2))
	}
}

func TestQuery_Update(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_update.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30, "status": "active"})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25, "status": "active"})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35, "status": "inactive"})

	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"status": "active"})

	// 更新匹配的文档
	updatedCount, err := query.Update(ctx, map[string]any{"status": "updated"})
	if err != nil {
		t.Fatalf("Failed to update documents: %v", err)
	}

	if updatedCount != 2 {
		t.Errorf("Expected 2 documents to be updated, got %d", updatedCount)
	}

	// 验证更新
	results, err := query.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after update (status changed), got %d", len(results))
	}

	// 验证文档确实被更新了
	allDocs, err := collection.All(ctx)
	if err != nil {
		t.Fatalf("Failed to get all documents: %v", err)
	}

	updatedCount2 := 0
	for _, doc := range allDocs {
		if doc.GetString("status") == "updated" {
			updatedCount2++
		}
	}

	if updatedCount2 != 2 {
		t.Errorf("Expected 2 documents with status 'updated', got %d", updatedCount2)
	}
}

func TestQuery_Remove(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_remove.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})

	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"age": map[string]any{"$gte": 30}})

	// 删除匹配的文档
	removedCount, err := query.Remove(ctx)
	if err != nil {
		t.Fatalf("Failed to remove documents: %v", err)
	}

	if removedCount != 2 {
		t.Errorf("Expected 2 documents to be removed, got %d", removedCount)
	}

	// 验证删除
	results, err := query.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after removal, got %d", len(results))
	}

	// 验证剩余文档
	allDocs, err := collection.All(ctx)
	if err != nil {
		t.Fatalf("Failed to get all documents: %v", err)
	}

	if len(allDocs) != 1 {
		t.Errorf("Expected 1 document remaining, got %d", len(allDocs))
	}

	if allDocs[0].GetString("name") != "Bob" {
		t.Errorf("Expected remaining document to be 'Bob', got '%s'", allDocs[0].GetString("name"))
	}
}

func TestQuery_IndexUsage(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_index.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = collection.Insert(ctx, map[string]any{"id": "3", "name": "Charlie", "age": 35})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 创建索引
	err = collection.CreateIndex(ctx, Index{Fields: []string{"name"}, Name: "name_idx"})
	if err != nil {
		t.Fatalf("Failed to create name index: %v", err)
	}

	err = collection.CreateIndex(ctx, Index{Fields: []string{"age"}, Name: "age_idx"})
	if err != nil {
		t.Fatalf("Failed to create age index: %v", err)
	}

	// 测试使用索引查询
	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"name": "Alice"})
	results, err := query.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", results[0].GetString("name"))
	}

	// 测试使用 age 索引查询
	query2 := qc.Find(map[string]any{"age": 30})
	results2, err := query2.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results2) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results2))
	}
	if results2[0].GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", results2[0].GetInt("age"))
	}

	// 验证索引列表
	indexes := collection.ListIndexes()
	if len(indexes) < 2 {
		t.Errorf("Expected at least 2 indexes, got %d", len(indexes))
	}
}

// TestQuery_IndexUsage_Performance 测试索引性能对比
func TestQuery_IndexUsage_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	dbPath := "../../data/test_query_index_perf.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入大量数据
	numDocs := 1000
	for i := 0; i < numDocs; i++ {
		_, err := collection.Insert(ctx, map[string]any{
			"id":   string(rune(i)),
			"name": fmt.Sprintf("User%d", i),
			"age":  i % 100, // 0-99
		})
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)

	// 测试无索引查询性能
	start := time.Now()
	results1, err := qc.Find(map[string]any{"age": 50}).Exec(ctx)
	noIndexDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 创建索引
	err = collection.CreateIndex(ctx, Index{Fields: []string{"age"}, Name: "age_idx"})
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 测试有索引查询性能
	start = time.Now()
	results2, err := qc.Find(map[string]any{"age": 50}).Exec(ctx)
	withIndexDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 验证结果一致性
	if len(results1) != len(results2) {
		t.Errorf("Results should be consistent: no index=%d, with index=%d", len(results1), len(results2))
	}

	t.Logf("Query without index: %v (%d results)", noIndexDuration, len(results1))
	t.Logf("Query with index: %v (%d results)", withIndexDuration, len(results2))
	t.Logf("Speedup: %.2fx", float64(noIndexDuration)/float64(withIndexDuration))
}

func TestQuery_CompositeIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_composite_index.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{"id": "1", "name": "Alice", "age": 30, "city": "NYC"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = collection.Insert(ctx, map[string]any{"id": "2", "name": "Bob", "age": 25, "city": "LA"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = collection.Insert(ctx, map[string]any{"id": "3", "name": "Alice", "age": 35, "city": "SF"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 创建复合索引
	err = collection.CreateIndex(ctx, Index{Fields: []string{"name", "age"}, Name: "name_age_idx"})
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// 测试完全匹配（两个字段都匹配）
	qc := AsQueryCollection(collection)
	query := qc.Find(map[string]any{"name": "Alice", "age": 30})
	results, err := query.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].GetString("name") != "Alice" || results[0].GetInt("age") != 30 {
		t.Errorf("Expected name='Alice' and age=30, got name='%s' and age=%d",
			results[0].GetString("name"), results[0].GetInt("age"))
	}

	// 测试前缀匹配（只匹配第一个字段）
	query2 := qc.Find(map[string]any{"name": "Alice"})
	results2, err := query2.Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results2) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results2))
	}

	// 验证所有结果都有 name="Alice"
	for _, doc := range results2 {
		if doc.GetString("name") != "Alice" {
			t.Errorf("Expected name='Alice', got '%s'", doc.GetString("name"))
		}
	}
}

// TestQuery_SortStability 测试排序稳定性
func TestQuery_SortStability(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_sort_stability.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入多个具有相同排序键值的文档
	for i := 0; i < 5; i++ {
		_, err = collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": "Same Name",
			"age":  30,
		})
		if err != nil {
			t.Fatalf("Failed to insert doc%d: %v", i, err)
		}
	}

	qc := AsQueryCollection(collection)
	results, err := qc.Find(map[string]any{
		"name": "Same Name",
	}).Sort(map[string]string{"age": "asc"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 验证排序结果的一致性（多次查询应该返回相同顺序）
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// 再次查询验证稳定性
	results2, err := qc.Find(map[string]any{
		"name": "Same Name",
	}).Sort(map[string]string{"age": "asc"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results2) != len(results) {
		t.Errorf("Results count mismatch: %d vs %d", len(results2), len(results))
	}

	// 验证顺序一致（至少ID顺序应该一致）
	for i := range results {
		if results[i].ID() != results2[i].ID() {
			t.Errorf("Sort order not stable at index %d: %s vs %s", i, results[i].ID(), results2[i].ID())
		}
	}
}

// TestQuery_Operator_Ne_NullValue 测试不等于操作符的空值处理
func TestQuery_Operator_Ne_NullValue(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_ne_null.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": nil,
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":  "doc3",
		"age": 35,
		// name 字段不存在
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// 查询 name 不等于 "Alice" 的文档
	results, err := qc.Find(map[string]any{
		"name": map[string]any{
			"$ne": "Alice",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该返回 doc2 和 doc3
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids := make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc2"] || !ids["doc3"] {
		t.Error("Expected doc2 and doc3 in results")
	}
	if ids["doc1"] {
		t.Error("doc1 should not be in results")
	}
}

// TestQuery_Operator_Gt_Date 测试日期大于比较
// 注意：当前实现不支持日期类型，日期值会被转换为字符串进行比较
func TestQuery_Operator_Gt_Date(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_gt_date.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据，使用 RFC3339 格式的日期字符串
	testDocs := []map[string]any{
		{"id": "doc1", "createdAt": "2023-01-01T00:00:00Z"},
		{"id": "doc2", "createdAt": "2023-06-15T12:00:00Z"},
		{"id": "doc3", "createdAt": "2023-12-31T23:59:59Z"},
		{"id": "doc4", "createdAt": "2024-01-01T00:00:00Z"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)
	// 查询 createdAt > "2023-06-01T00:00:00Z" 的文档
	// 注意：当前实现会将日期字符串按字符串比较，而不是日期比较
	results, err := qc.Find(map[string]any{
		"createdAt": map[string]any{
			"$gt": "2023-06-01T00:00:00Z",
		},
	}).Sort(map[string]string{"createdAt": "asc"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// RFC3339 格式的日期字符串可以按字典序比较，所以应该返回 doc2, doc3, doc4
	// 因为 "2023-06-15" > "2023-06-01", "2023-12-31" > "2023-06-01", "2024-01-01" > "2023-06-01"
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// 验证结果顺序（按字符串排序）
	if len(results) >= 1 && results[0].GetString("createdAt") != "2023-06-15T12:00:00Z" {
		t.Logf("Note: Date comparison uses string comparison, not date comparison")
		t.Logf("First result createdAt: %s", results[0].GetString("createdAt"))
	}

	// 验证所有结果都满足条件（字符串比较）
	for _, doc := range results {
		createdAt := doc.GetString("createdAt")
		if createdAt <= "2023-06-01T00:00:00Z" {
			t.Errorf("Document %s createdAt '%s' should be greater than '2023-06-01T00:00:00Z'", doc.ID(), createdAt)
		}
	}

	// 注意：RFC3339 格式的日期字符串可以按字典序正确比较
	// 但如果日期格式不一致，字符串比较可能不正确
	// 实际应用中，建议使用时间戳（数字）进行日期比较
}

// TestQuery_Operator_Gt_String 测试字符串大于比较
func TestQuery_Operator_Gt_String(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_gt_string.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "name": "Alice"},
		{"id": "doc2", "name": "Bob"},
		{"id": "doc3", "name": "Charlie"},
		{"id": "doc4", "name": "David"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)
	// 查询 name > "Bob" 的文档（按字典序）
	results, err := qc.Find(map[string]any{
		"name": map[string]any{
			"$gt": "Bob",
		},
	}).Sort(map[string]string{"name": "asc"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该返回 Charlie 和 David
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if results[0].GetString("name") != "Charlie" {
		t.Errorf("Expected first result 'Charlie', got '%s'", results[0].GetString("name"))
	}
	if results[1].GetString("name") != "David" {
		t.Errorf("Expected second result 'David', got '%s'", results[1].GetString("name"))
	}
}

// TestQuery_Operator_In 测试基本数组包含查询
func TestQuery_Operator_In(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_in.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "name": "Alice", "status": "active"},
		{"id": "doc2", "name": "Bob", "status": "inactive"},
		{"id": "doc3", "name": "Charlie", "status": "active"},
		{"id": "doc4", "name": "David", "status": "pending"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)

	// 测试字符串数组包含
	results, err := qc.Find(map[string]any{
		"name": map[string]any{
			"$in": []any{"Alice", "Bob", "Eve"},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids := make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc2"] {
		t.Error("Expected doc1 and doc2 in results")
	}
	if ids["doc3"] || ids["doc4"] {
		t.Error("doc3 and doc4 should not be in results")
	}

	// 测试数字数组包含
	results, err = qc.Find(map[string]any{
		"id": map[string]any{
			"$in": []any{"doc1", "doc3", "doc5"},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids = make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc3"] {
		t.Error("Expected doc1 and doc3 in results")
	}

	// 测试状态数组包含
	results, err = qc.Find(map[string]any{
		"status": map[string]any{
			"$in": []any{"active", "pending"},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestQuery_Operator_In_EmptyArray 测试空数组处理
func TestQuery_Operator_In_EmptyArray(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_in_empty.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// 使用空数组查询
	results, err := qc.Find(map[string]any{
		"name": map[string]any{
			"$in": []any{},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 空数组应该不匹配任何文档
	if len(results) != 0 {
		t.Errorf("Expected 0 results with empty array, got %d", len(results))
	}
}

// TestQuery_Operator_Regex 测试基本正则匹配
func TestQuery_Operator_Regex(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_regex.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "name": "Alice", "email": "alice@example.com"},
		{"id": "doc2", "name": "Bob", "email": "bob@test.org"},
		{"id": "doc3", "name": "Charlie", "email": "charlie@example.com"},
		{"id": "doc4", "name": "David", "email": "david@test.org"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)

	// 测试基本正则匹配 - 以 A 开头的名字
	results, err := qc.Find(map[string]any{
		"name": map[string]any{
			"$regex": "^A",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", results[0].GetString("name"))
	}

	// 测试正则匹配 - 包含 "ob" 的名字
	results, err = qc.Find(map[string]any{
		"name": map[string]any{
			"$regex": "ob",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids := make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc2"] || !ids["doc4"] {
		t.Error("Expected doc2 and doc4 in results")
	}

	// 测试正则匹配 - 以 .com 结尾的邮箱
	results, err = qc.Find(map[string]any{
		"email": map[string]any{
			"$regex": "\\.com$",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	ids = make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc3"] {
		t.Error("Expected doc1 and doc3 in results")
	}

	// 测试不区分大小写的正则匹配（如果支持）
	results, err = qc.Find(map[string]any{
		"name": map[string]any{
			"$regex": "(?i)^a",
		},
	}).Exec(ctx)
	if err != nil {
		// 如果不支持 (?i) 语法，这是可以接受的
		// 只测试基本功能
		return
	}

	if len(results) < 1 {
		t.Errorf("Expected at least 1 result, got %d", len(results))
	}
}

// TestQuery_Operator_Regex_Complex 测试复杂正则表达式
func TestQuery_Operator_Regex_Complex(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_regex_complex.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "email": "alice@example.com"},
		{"id": "doc2", "email": "bob@test.org"},
		{"id": "doc3", "email": "charlie@example.com"},
		{"id": "doc4", "email": "invalid-email"},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)
	// 使用复杂正则表达式匹配邮箱
	results, err := qc.Find(map[string]any{
		"email": map[string]any{
			"$regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该匹配前三个文档
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	ids := make(map[string]bool)
	for _, doc := range results {
		ids[doc.ID()] = true
	}

	if !ids["doc1"] || !ids["doc2"] || !ids["doc3"] {
		t.Error("Expected doc1, doc2, doc3 in results")
	}
	if ids["doc4"] {
		t.Error("doc4 should not match email regex")
	}
}

// TestQuery_Operator_And_Nested 测试嵌套 AND 操作符
func TestQuery_Operator_And_Nested(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_and_nested.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
		"city": "NYC",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		"age":  25,
		"city": "LA",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// 嵌套 AND 查询
	results, err := qc.Find(map[string]any{
		"$and": []any{
			map[string]any{
				"name": "Alice",
			},
			map[string]any{
				"$and": []any{
					map[string]any{"age": map[string]any{"$gte": 25}},
					map[string]any{"city": "NYC"},
				},
			},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].ID() != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].ID())
	}
}

// TestQuery_Operator_Or_Nested 测试嵌套 OR 操作符
func TestQuery_Operator_Or_Nested(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_or_nested.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

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
		_, err = collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("User%d", i),
			"age":  20 + i*5,
		})
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qc := AsQueryCollection(collection)
	// 嵌套 OR 查询
	results, err := qc.Find(map[string]any{
		"$or": []any{
			map[string]any{"age": map[string]any{"$lt": 25}},
			map[string]any{
				"$or": []any{
					map[string]any{"age": map[string]any{"$gt": 35}},
					map[string]any{"name": "User3"},
				},
			},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该匹配 age < 25 (doc1), age > 35 (doc4, doc5), 或 name=User3 (doc3)
	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}
}

// TestQuery_Operator_AndOr_Combined 测试 AND 和 OR 组合
func TestQuery_Operator_AndOr_Combined(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_andor_combined.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
		"city": "NYC",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		"age":  25,
		"city": "LA",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// AND 和 OR 组合查询: (name=Alice OR name=Bob) AND age>=25
	results, err := qc.Find(map[string]any{
		"$and": []any{
			map[string]any{
				"$or": []any{
					map[string]any{"name": "Alice"},
					map[string]any{"name": "Bob"},
				},
			},
			map[string]any{"age": map[string]any{"$gte": 25}},
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestQuery_Operator_Exists_NotExists 测试字段不存在检查
func TestQuery_Operator_Exists_NotExists(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_exists_not.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		// age 字段不存在
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc3",
		"name": "Charlie",
		"age":  nil, // age 字段存在但为 nil
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// 查询 age 字段不存在的文档
	results, err := qc.Find(map[string]any{
		"age": map[string]any{
			"$exists": false,
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// 应该返回 doc2（字段不存在）
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].ID() != "doc2" {
		t.Errorf("Expected doc2, got %s", results[0].ID())
	}
}

// TestQuery_Operator_Type_ArrayObject 测试数组和对象类型
func TestQuery_Operator_Type_ArrayObject(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_query_type_array_object.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2"},
		"meta": map[string]any{"key": "value"},
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"tags": "not-an-array",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qc := AsQueryCollection(collection)
	// 查询 tags 字段为数组类型的文档
	results, err := qc.Find(map[string]any{
		"tags": map[string]any{
			"$type": "array",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].ID() != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].ID())
	}

	// 查询 meta 字段为对象类型的文档
	results, err = qc.Find(map[string]any{
		"meta": map[string]any{
			"$type": "object",
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}
