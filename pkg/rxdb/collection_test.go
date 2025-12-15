package rxdb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestCollection_Insert(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_insert.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_insert.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test Document",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if doc.ID() != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%s'", doc.ID())
	}

	if doc.Data()["name"] != "Test Document" {
		t.Errorf("Expected name 'Test Document', got '%v'", doc.Data()["name"])
	}

	// 验证文档已保存
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found == nil {
		t.Fatal("Document not found after insert")
	}

	if found.Data()["name"] != "Test Document" {
		t.Errorf("Expected name 'Test Document', got '%v'", found.Data()["name"])
	}
}

func TestCollection_Upsert(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_upsert.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_upsert.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 首次插入
	doc1, err := collection.Upsert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	rev1 := doc1.Data()["_rev"].(string)

	// 更新
	doc2, err := collection.Upsert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to upsert update: %v", err)
	}

	rev2 := doc2.Data()["_rev"].(string)

	if rev1 == rev2 {
		t.Error("Revision should change after update")
	}

	if doc2.Data()["name"] != "Updated" {
		t.Errorf("Expected name 'Updated', got '%v'", doc2.Data()["name"])
	}
}

func TestCollection_Remove(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_remove.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_remove.db")

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
		t.Fatalf("Failed to insert: %v", err)
	}

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove: %v", err)
	}

	// 验证文档已删除
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found != nil {
		t.Error("Document should be deleted")
	}
}

func TestCollection_All(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_all.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_all.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入多个文档
	for i := 1; i <= 5; i++ {
		_, err = collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("Document %d", i),
		})
		if err != nil {
			t.Fatalf("Failed to insert doc%d: %v", i, err)
		}
	}

	// 获取所有文档
	all, err := collection.All(ctx)
	if err != nil {
		t.Fatalf("Failed to get all: %v", err)
	}

	if len(all) != 5 {
		t.Errorf("Expected 5 documents, got %d", len(all))
	}
}

func TestCollection_Changes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "./test_changes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_changes.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	changes := collection.Changes()

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 接收变更事件
	event := <-changes
	if event.Op != OperationInsert {
		t.Errorf("Expected OperationInsert, got %s", event.Op)
	}
	if event.ID != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%s'", event.ID)
	}

	// 更新文档
	_, err = collection.Upsert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	event = <-changes
	if event.Op != OperationUpdate {
		t.Errorf("Expected OperationUpdate, got %s", event.Op)
	}

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove: %v", err)
	}

	event = <-changes
	if event.Op != OperationDelete {
		t.Errorf("Expected OperationDelete, got %s", event.Op)
	}
}

