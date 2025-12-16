package rxdb

import (
	"context"
	"os"
	"testing"
)

func TestDocument_ID(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_id.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc-123",
		"name": "Test Document",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if doc.ID() != "doc-123" {
		t.Errorf("Expected ID 'doc-123', got '%s'", doc.ID())
	}
}

func TestDocument_Data(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_data.db"
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

	docData := map[string]any{
		"id":    "doc1",
		"name":  "Test Document",
		"age":   30,
		"email": "test@example.com",
	}

	doc, err := collection.Insert(ctx, docData)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	data := doc.Data()
	if data == nil {
		t.Fatal("Document data should not be nil")
	}

	if data["name"] != "Test Document" {
		t.Errorf("Expected name 'Test Document', got '%v'", data["name"])
	}

	if data["age"] != float64(30) {
		t.Errorf("Expected age 30, got '%v'", data["age"])
	}
}

func TestDocument_Get(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_get.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":     "doc1",
		"name":   "Test",
		"age":    25,
		"active": true,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 测试获取存在的字段
	if doc.Get("name") != "Test" {
		t.Errorf("Expected 'Test', got '%v'", doc.Get("name"))
	}

	if doc.Get("age") != float64(25) {
		t.Errorf("Expected 25, got '%v'", doc.Get("age"))
	}

	if doc.Get("active") != true {
		t.Errorf("Expected true, got '%v'", doc.Get("active"))
	}

	// 测试获取不存在的字段
	if doc.Get("nonexistent") != nil {
		t.Error("Expected nil for nonexistent field")
	}
}

func TestDocument_GetString(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getstring.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test String",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if doc.GetString("name") != "Test String" {
		t.Errorf("Expected 'Test String', got '%s'", doc.GetString("name"))
	}

	// 测试不存在的字段
	if doc.GetString("nonexistent") != "" {
		t.Error("Expected empty string for nonexistent field")
	}

	// 测试非字符串类型（应该返回空字符串）
	if doc.GetString("id") != "doc1" {
		t.Errorf("Expected 'doc1', got '%s'", doc.GetString("id"))
	}
}

func TestDocument_GetInt(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getint.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":  "doc1",
		"age": 30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if doc.GetInt("age") != 30 {
		t.Errorf("Expected 30, got %d", doc.GetInt("age"))
	}

	// 测试不存在的字段
	if doc.GetInt("nonexistent") != 0 {
		t.Error("Expected 0 for nonexistent field")
	}
}

func TestDocument_GetFloat(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getfloat.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"price": 99.99,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	price := doc.GetFloat("price")
	if price != 99.99 {
		t.Errorf("Expected 99.99, got %f", price)
	}

	// 测试不存在的字段
	if doc.GetFloat("nonexistent") != 0.0 {
		t.Error("Expected 0.0 for nonexistent field")
	}
}

func TestDocument_GetBool(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getbool.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":     "doc1",
		"active": true,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if !doc.GetBool("active") {
		t.Error("Expected true, got false")
	}

	// 测试不存在的字段
	if doc.GetBool("nonexistent") {
		t.Error("Expected false for nonexistent field")
	}
}

func TestDocument_GetArray(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getarray.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2", "tag3"},
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	tags := doc.GetArray("tags")
	if tags == nil {
		t.Fatal("Expected tags array, got nil")
	}

	if len(tags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(tags))
	}

	if tags[0] != "tag1" {
		t.Errorf("Expected 'tag1', got '%v'", tags[0])
	}
}

func TestDocument_GetObject(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_getobject.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id": "doc1",
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "New York",
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	address := doc.GetObject("address")
	if address == nil {
		t.Fatal("Expected address object, got nil")
	}

	if address["street"] != "123 Main St" {
		t.Errorf("Expected '123 Main St', got '%v'", address["street"])
	}

	if address["city"] != "New York" {
		t.Errorf("Expected 'New York', got '%v'", address["city"])
	}
}

func TestDocument_Set(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_set.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 设置字段值（不保存）
	err = doc.Set(ctx, "name", "Modified")
	if err != nil {
		t.Fatalf("Failed to set field: %v", err)
	}

	if doc.GetString("name") != "Modified" {
		t.Errorf("Expected 'Modified', got '%s'", doc.GetString("name"))
	}

	// 验证未保存到数据库
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found.GetString("name") != "Original" {
		t.Errorf("Expected 'Original' in database, got '%s'", found.GetString("name"))
	}
}

func TestDocument_Update(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_update.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	oldRev := doc.Data()["_rev"].(string)

	// 更新文档
	err = doc.Update(ctx, map[string]any{
		"name": "Updated",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// 验证更新成功
	if doc.GetString("name") != "Updated" {
		t.Errorf("Expected 'Updated', got '%s'", doc.GetString("name"))
	}

	if doc.GetInt("age") != 30 {
		t.Errorf("Expected 30, got %d", doc.GetInt("age"))
	}

	// 验证修订号更新
	newRev := doc.Data()["_rev"].(string)
	if newRev == oldRev {
		t.Error("Revision should be updated after update")
	}

	// 验证数据库中的更新
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found.GetString("name") != "Updated" {
		t.Errorf("Expected 'Updated' in database, got '%s'", found.GetString("name"))
	}
}

func TestDocument_Save(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_save.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 修改字段
	doc.Set(ctx, "name", "Modified")

	// 保存
	err = doc.Save(ctx)
	if err != nil {
		t.Fatalf("Failed to save document: %v", err)
	}

	// 验证保存成功
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found.GetString("name") != "Modified" {
		t.Errorf("Expected 'Modified', got '%s'", found.GetString("name"))
	}
}

func TestDocument_Remove(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_remove.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 删除文档
	err = doc.Remove(ctx)
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
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

func TestDocument_ToJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_tojson.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	jsonData, err := doc.ToJSON()
	if err != nil {
		t.Fatalf("Failed to convert to JSON: %v", err)
	}

	if len(jsonData) == 0 {
		t.Error("JSON data should not be empty")
	}

	// 验证 JSON 包含必要字段
	jsonStr := string(jsonData)
	if !contains(jsonStr, "doc1") {
		t.Error("JSON should contain document ID")
	}
	if !contains(jsonStr, "Test") {
		t.Error("JSON should contain document name")
	}
}

func TestDocument_ToMutableJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_mutablejson.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	mutableJSON, err := doc.ToMutableJSON()
	if err != nil {
		t.Fatalf("Failed to get mutable JSON: %v", err)
	}
	if mutableJSON == nil {
		t.Fatal("Mutable JSON should not be nil")
	}

	if mutableJSON["id"] != "doc1" {
		t.Errorf("Expected 'doc1', got '%v'", mutableJSON["id"])
	}

	// 验证可以修改
	mutableJSON["name"] = "Modified"
	if doc.GetString("name") != "Test" {
		t.Error("Original document should not be affected")
	}
}

func TestDocument_Deleted(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_deleted.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 检查未删除状态
	deleted, err := doc.Deleted(ctx)
	if err != nil {
		t.Fatalf("Failed to check deleted status: %v", err)
	}

	if deleted {
		t.Error("Document should not be deleted")
	}

	// 删除文档
	err = doc.Remove(ctx)
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 检查删除状态
	deleted, err = doc.Deleted(ctx)
	if err != nil {
		t.Fatalf("Failed to check deleted status: %v", err)
	}

	if !deleted {
		t.Error("Document should be deleted")
	}
}

func TestDocument_Changes(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_changes.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	changes := doc.Changes()

	// 更新文档
	err = doc.Update(ctx, map[string]any{
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// 接收变更事件
	event := <-changes
	if event.ID != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%s'", event.ID)
	}
	if event.Op != OperationUpdate {
		t.Errorf("Expected OperationUpdate, got %s", event.Op)
	}
}

func TestDocument_AtomicUpdate(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_atomic_update.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 原子更新
	err = doc.AtomicUpdate(ctx, func(docData map[string]any) error {
		docData["name"] = "Updated"
		docData["age"] = 30
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to atomic update: %v", err)
	}

	// 验证更新
	if doc.GetString("name") != "Updated" {
		t.Errorf("Expected 'Updated', got '%s'", doc.GetString("name"))
	}

	if doc.GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", doc.GetInt("age"))
	}

	// 验证数据库中的更新
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found.GetString("name") != "Updated" {
		t.Errorf("Expected 'Updated' in database, got '%s'", found.GetString("name"))
	}
}

func TestDocument_AtomicPatch(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_atomic_patch.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 原子补丁
	patch := map[string]any{
		"age": 30,
	}

	err = doc.AtomicPatch(ctx, patch)
	if err != nil {
		t.Fatalf("Failed to atomic patch: %v", err)
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

func TestDocument_IncrementalModify(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_incremental_modify.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 增量修改
	err = doc.IncrementalModify(ctx, func(docData map[string]any) error {
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
}

func TestDocument_IncrementalPatch(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_incremental_patch.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id": "doc1",
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "New York",
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 增量补丁（嵌套字段）
	patch := map[string]any{
		"address": map[string]any{
			"city": "Boston",
		},
	}

	err = doc.IncrementalPatch(ctx, patch)
	if err != nil {
		t.Fatalf("Failed to incremental patch: %v", err)
	}

	// 验证更新
	address := doc.GetObject("address")
	if address == nil {
		t.Fatal("Address should exist")
	}

	if address["city"] != "Boston" {
		t.Errorf("Expected 'Boston', got '%v'", address["city"])
	}

	// 验证其他字段保持不变
	if address["street"] != "123 Main St" {
		t.Errorf("Expected '123 Main St', got '%v'", address["street"])
	}
}

func TestDocument_GetFieldChanges(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_document_field_changes.db"
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

	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Original",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	fieldChanges := doc.GetFieldChanges(ctx, "name")

	// 更新字段
	err = doc.Update(ctx, map[string]any{
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// 接收字段变更事件
	change := <-fieldChanges
	if change.Field != "name" {
		t.Errorf("Expected field 'name', got '%s'", change.Field)
	}

	if change.Old != "Original" {
		t.Errorf("Expected old value 'Original', got '%v'", change.Old)
	}

	if change.New != "Updated" {
		t.Errorf("Expected new value 'Updated', got '%v'", change.New)
	}
}

// 辅助函数
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			containsMiddle(s, substr))))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
