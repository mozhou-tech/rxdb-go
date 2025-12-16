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

func TestQuery_Operator_Ne(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_query_ne.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_gt.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_lt.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_nin.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_exists.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_and.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_or.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_all.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_size.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_not.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_nor.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_elemmatch.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_type.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_mod.db"
	defer os.Remove(dbPath)

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

func TestQuery_Chain(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_query_chain.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_sort_multiple.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_gte.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_lte.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_observe.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_observe_multiple.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_update.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_remove.db"
	defer os.Remove(dbPath)

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
	dbPath := "./test_query_index.db"
	defer os.Remove(dbPath)

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

func TestQuery_CompositeIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_query_composite_index.db"
	defer os.Remove(dbPath)

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
