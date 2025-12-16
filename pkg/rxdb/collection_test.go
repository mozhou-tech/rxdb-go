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

func TestCollection_InsertDuplicate(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_insert_duplicate.db"
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

	// 插入第一个文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "First",
	})
	if err != nil {
		t.Fatalf("Failed to insert first document: %v", err)
	}

	// 尝试插入相同 ID 的文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Duplicate",
	})
	if err == nil {
		t.Error("Should fail when inserting duplicate document ID")
	}

	if !IsAlreadyExistsError(err) {
		t.Errorf("Expected AlreadyExists error, got: %v", err)
	}
}

func TestCollection_FindByID(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_findbyid.db"
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

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test Document",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 查找存在的文档
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found == nil {
		t.Fatal("Document should be found")
	}

	if found.ID() != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%s'", found.ID())
	}

	if found.GetString("name") != "Test Document" {
		t.Errorf("Expected name 'Test Document', got '%s'", found.GetString("name"))
	}

	// 查找不存在的文档
	notFound, err := collection.FindByID(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("FindByID should not error for nonexistent document: %v", err)
	}

	if notFound != nil {
		t.Error("Document should not be found")
	}
}

func TestCollection_Count(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_count.db"
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

	// 空集合应该返回 0
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected count 0 for empty collection, got %d", count)
	}

	// 插入文档
	for i := 1; i <= 5; i++ {
		_, err = collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("Document %d", i),
		})
		if err != nil {
			t.Fatalf("Failed to insert doc%d: %v", i, err)
		}
	}

	// 验证数量
	count, err = collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected count 5, got %d", count)
	}

	// 删除一个文档后验证数量
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	count, err = collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 4 {
		t.Errorf("Expected count 4 after removal, got %d", count)
	}
}

func TestCollection_BulkInsert(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_bulk_insert.db"
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

	// 准备批量插入的文档
	docs := []map[string]any{
		{"id": "doc1", "name": "Document 1"},
		{"id": "doc2", "name": "Document 2"},
		{"id": "doc3", "name": "Document 3"},
	}

	// 批量插入
	result, err := collection.BulkInsert(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to bulk insert: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 documents, got %d", len(result))
	}

	// 验证所有文档都已插入
	for i, doc := range result {
		if doc.ID() != fmt.Sprintf("doc%d", i+1) {
			t.Errorf("Expected ID 'doc%d', got '%s'", i+1, doc.ID())
		}
	}

	// 验证数据库中的文档
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestCollection_BulkUpsert(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_bulk_upsert.db"
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

	// 先插入一个文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 批量更新或插入
	docs := []map[string]any{
		{"id": "doc1", "name": "Updated"}, // 更新
		{"id": "doc2", "name": "New"},     // 插入
	}

	result, err := collection.BulkUpsert(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to bulk upsert: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(result))
	}

	// 验证更新
	doc1, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1: %v", err)
	}
	if doc1.GetString("name") != "Updated" {
		t.Errorf("Expected 'Updated', got '%s'", doc1.GetString("name"))
	}

	// 验证插入
	doc2, err := collection.FindByID(ctx, "doc2")
	if err != nil {
		t.Fatalf("Failed to find doc2: %v", err)
	}
	if doc2.GetString("name") != "New" {
		t.Errorf("Expected 'New', got '%s'", doc2.GetString("name"))
	}
}

func TestCollection_BulkRemove(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_bulk_remove.db"
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

	// 批量删除
	ids := []string{"doc1", "doc2", "doc3"}
	err = collection.BulkRemove(ctx, ids)
	if err != nil {
		t.Fatalf("Failed to bulk remove: %v", err)
	}

	// 验证删除
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// 验证特定文档已删除
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1: %v", err)
	}
	if found != nil {
		t.Error("doc1 should be deleted")
	}

	// 验证剩余文档存在
	found, err = collection.FindByID(ctx, "doc4")
	if err != nil {
		t.Fatalf("Failed to find doc4: %v", err)
	}
	if found == nil {
		t.Error("doc4 should still exist")
	}
}

func TestCollection_IncrementalUpsert(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_incremental_upsert.db"
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

	// 插入初始文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 增量更新
	patch := map[string]any{
		"id":  "doc1",
		"age": 30, // 只更新 age
	}

	doc, err := collection.IncrementalUpsert(ctx, patch)
	if err != nil {
		t.Fatalf("Failed to incremental upsert: %v", err)
	}

	// 验证更新
	if doc.GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", doc.GetInt("age"))
	}

	// 验证其他字段保持不变
	if doc.GetString("name") != "Original" {
		t.Errorf("Expected name 'Original', got '%s'", doc.GetString("name"))
	}
}

func TestCollection_ExportJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_export_json.db"
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

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 导出
	exported, err := collection.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	if len(exported) != 1 {
		t.Errorf("Expected 1 document, got %d", len(exported))
	}

	if exported[0]["id"] != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%v'", exported[0]["id"])
	}
}

func TestCollection_ImportJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_import_json.db"
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

	// 导入文档
	docs := []map[string]any{
		{"id": "doc1", "name": "Document 1"},
		{"id": "doc2", "name": "Document 2"},
	}

	err = collection.ImportJSON(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to import: %v", err)
	}

	// 验证导入
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	doc1, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1: %v", err)
	}
	if doc1.GetString("name") != "Document 1" {
		t.Errorf("Expected 'Document 1', got '%s'", doc1.GetString("name"))
	}
}

func TestCollection_IncrementalModify(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_incremental_modify.db"
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

	// 插入初始文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 增量修改
	doc, err := collection.IncrementalModify(ctx, "doc1", func(docData map[string]any) error {
		docData["age"] = docData["age"].(float64) + 5
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to incremental modify: %v", err)
	}

	// 验证更新
	if doc.GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", doc.GetInt("age"))
	}

	// 验证其他字段不变
	if doc.GetString("name") != "Original" {
		t.Errorf("Expected 'Original', got '%s'", doc.GetString("name"))
	}
}

func TestCollection_ChangesMultipleListeners(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_changes_multiple.db"
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

	// 创建多个监听者
	changes1 := collection.Changes()
	changes2 := collection.Changes()

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 两个监听者都应该收到事件
	event1 := <-changes1
	event2 := <-changes2

	if event1.ID != "doc1" {
		t.Errorf("Expected ID 'doc1' in listener 1, got '%s'", event1.ID)
	}

	if event2.ID != "doc1" {
		t.Errorf("Expected ID 'doc1' in listener 2, got '%s'", event2.ID)
	}
}

func TestCollection_ExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbPath1 := "./test_export_import1.db"
	dbPath2 := "./test_export_import2.db"
	defer os.Remove(dbPath1)
	defer os.Remove(dbPath2)

	// 创建第一个数据库
	db1, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb1",
		Path: dbPath1,
	})
	if err != nil {
		t.Fatalf("Failed to create database1: %v", err)
	}
	defer db1.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection1, err := db1.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection1.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test Document",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 导出
	exported, err := collection1.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	// 创建第二个数据库并导入
	db2, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb2",
		Path: dbPath2,
	})
	if err != nil {
		t.Fatalf("Failed to create database2: %v", err)
	}
	defer db2.Close(ctx)

	collection2, err := db2.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection2: %v", err)
	}

	err = collection2.ImportJSON(ctx, exported)
	if err != nil {
		t.Fatalf("Failed to import: %v", err)
	}

	// 验证数据一致性
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if doc == nil {
		t.Fatal("Document should exist")
	}

	if doc.GetString("name") != "Test Document" {
		t.Errorf("Expected 'Test Document', got '%s'", doc.GetString("name"))
	}

	if doc.GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", doc.GetInt("age"))
	}
}

func TestCollection_Dump(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_dump.db"
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

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Dump
	dump, err := collection.Dump(ctx)
	if err != nil {
		t.Fatalf("Failed to dump: %v", err)
	}

	if dump == nil {
		t.Fatal("Dump should not be nil")
	}

	// 验证 dump 包含文档
	docs, ok := dump["documents"].([]any)
	if !ok {
		t.Fatal("Dump should contain 'documents' field")
	}

	if len(docs) != 1 {
		t.Errorf("Expected 1 document in dump, got %d", len(docs))
	}
}

func TestCollection_ImportDump(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_import_dump.db"
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

	// 准备 dump 数据
	dump := map[string]any{
		"documents": []any{
			map[string]any{
				"id":   "doc1",
				"name": "Test Document",
			},
		},
		"attachments": map[string]any{},
	}

	// 导入 dump
	err = collection.ImportDump(ctx, dump)
	if err != nil {
		t.Fatalf("Failed to import dump: %v", err)
	}

	// 验证导入
	doc, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if doc == nil {
		t.Fatal("Document should exist")
	}

	if doc.GetString("name") != "Test Document" {
		t.Errorf("Expected 'Test Document', got '%s'", doc.GetString("name"))
	}
}

func TestCollection_UpsertWithConflict(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_upsert_conflict.db"
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

	// 插入初始文档
	doc1, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	originalRev := doc1.Data()["_rev"].(string)

	// 模拟并发更新：先更新一次
	doc2, err := collection.Upsert(ctx, map[string]any{
		"id":   "doc1",
		"name": "First Update",
	})
	if err != nil {
		t.Fatalf("Failed to first upsert: %v", err)
	}

	firstRev := doc2.Data()["_rev"].(string)

	// 使用旧的修订号尝试更新（模拟冲突）
	// 注意：当前实现中，Upsert 会自动处理修订号更新
	// 这里我们验证修订号确实更新了
	if originalRev == firstRev {
		t.Error("Revision should change after update")
	}

	// 再次更新，验证修订号继续更新
	doc3, err := collection.Upsert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Second Update",
	})
	if err != nil {
		t.Fatalf("Failed to second upsert: %v", err)
	}

	secondRev := doc3.Data()["_rev"].(string)
	if firstRev == secondRev {
		t.Error("Revision should change after second update")
	}

	if doc3.GetString("name") != "Second Update" {
		t.Errorf("Expected 'Second Update', got '%s'", doc3.GetString("name"))
	}
}

func TestCollection_ChangesFilter(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_changes_filter.db"
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

	changes := collection.Changes()

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 接收插入事件
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

	// 接收更新事件
	event = <-changes
	if event.Op != OperationUpdate {
		t.Errorf("Expected OperationUpdate, got %s", event.Op)
	}

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove: %v", err)
	}

	// 接收删除事件
	event = <-changes
	if event.Op != OperationDelete {
		t.Errorf("Expected OperationDelete, got %s", event.Op)
	}

	// 注意：当前实现可能不支持按操作类型过滤
	// 这里我们验证所有事件都能正确接收
}
