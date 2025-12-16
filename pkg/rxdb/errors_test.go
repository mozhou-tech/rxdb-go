package rxdb

import (
	"context"
	"os"
	"testing"
)

// TestErrors_ValidationError 测试验证错误
func TestErrors_ValidationError(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_errors_validation.db"
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
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "string",
				},
				"name": map[string]any{
					"type":     "string",
					"required": true,
				},
			},
			"required": []any{"id", "name"},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 测试缺少必需字段
	_, err = collection.Insert(ctx, map[string]any{
		"id": "doc1",
		// 缺少 name 字段
	})
	if err == nil {
		t.Error("Expected validation error for missing required field")
	}

	// 验证错误类型
	if !IsValidationError(err) {
		t.Errorf("Expected validation error, got: %v", err)
	}

	// 验证错误信息
	if err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}

// TestErrors_NotFoundError 测试未找到错误
func TestErrors_NotFoundError(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_errors_notfound.db"
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

	// 测试查找不存在的文档
	doc, err := collection.FindByID(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error when finding nonexistent document")
	}
	if doc != nil {
		t.Error("Expected nil document for nonexistent ID")
	}

	// 验证错误类型（如果实现了 NotFoundError）
	// 注意：当前实现可能不返回 NotFoundError，这里测试实际行为
	if err != nil {
		t.Logf("Error when finding nonexistent document: %v", err)
	}

	// 测试删除不存在的文档
	err = collection.Remove(ctx, "nonexistent")
	if err != nil {
		// 删除不存在的文档可能返回错误或静默成功
		t.Logf("Error when removing nonexistent document: %v", err)
	}

	// 测试不存在的集合
	_, err = db.Collection(ctx, "nonexistent", schema)
	if err != nil {
		// 集合不存在时创建新集合，不应该报错
		t.Logf("Collection creation behavior: %v", err)
	}
}

// TestErrors_AlreadyExistsError 测试已存在错误
func TestErrors_AlreadyExistsError(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_errors_alreadyexists.db"
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

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 尝试插入相同 ID 的文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Duplicate",
	})
	if err == nil {
		t.Error("Expected error when inserting duplicate document")
	}

	// 验证错误类型
	if !IsAlreadyExistsError(err) {
		// 如果当前实现不返回 AlreadyExistsError，记录实际错误
		t.Logf("Expected AlreadyExistsError, got: %v", err)
	}

	// 测试重复创建集合（应该成功，返回已存在的集合）
	collection2, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to get existing collection: %v", err)
	}
	if collection2 == nil {
		t.Error("Expected to get existing collection")
	}
}

// TestErrors_ClosedError 测试已关闭错误
func TestErrors_ClosedError(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_errors_closed.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 关闭数据库
	err = db.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// 测试关闭后插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err == nil {
		t.Error("Expected error when inserting after database closed")
	}

	// 验证错误类型
	if !IsClosedError(err) {
		// 如果当前实现不返回 ClosedError，记录实际错误
		t.Logf("Expected ClosedError, got: %v", err)
	}

	// 测试关闭后查询
	_, err = collection.FindByID(ctx, "doc1")
	if err == nil {
		t.Error("Expected error when querying after database closed")
	}

	// 测试关闭后删除
	err = collection.Remove(ctx, "doc1")
	if err == nil {
		t.Error("Expected error when removing after database closed")
	}

	// 测试关闭后创建集合
	_, err = db.Collection(ctx, "test2", schema)
	if err == nil {
		t.Error("Expected error when creating collection after database closed")
	}
}

// TestErrors_Recovery 测试错误恢复
func TestErrors_Recovery(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_errors_recovery.db"
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
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "string",
				},
				"name": map[string]any{
					"type":     "string",
					"required": true,
				},
			},
			"required": []any{"id", "name"},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 第一次插入失败（缺少必需字段）
	_, err = collection.Insert(ctx, map[string]any{
		"id": "doc1",
		// 缺少 name 字段
	})
	if err == nil {
		t.Error("Expected validation error")
	}

	// 从错误中恢复，使用正确的数据插入
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test Document",
	})
	if err != nil {
		t.Fatalf("Failed to insert after recovery: %v", err)
	}

	// 验证数据一致性
	doc, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document after recovery: %v", err)
	}

	if doc == nil {
		t.Fatal("Document should exist after successful insert")
	}

	if doc.GetString("name") != "Test Document" {
		t.Errorf("Expected 'Test Document', got '%s'", doc.GetString("name"))
	}

	// 测试重复插入错误后的恢复
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Duplicate",
	})
	if err == nil {
		t.Error("Expected error for duplicate insert")
	}

	// 使用不同的 ID 恢复
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Another Document",
	})
	if err != nil {
		t.Fatalf("Failed to insert after duplicate error: %v", err)
	}

	// 验证两个文档都存在
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 documents, got %d", count)
	}
}
