package rxdb

import (
	"context"
	"os"
	"testing"
)

func TestDatabase_CreateDatabase(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_create.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	if db == nil {
		t.Fatal("Database instance should not be nil")
	}

	if db.Name() != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", db.Name())
	}

	// 验证数据库文件已创建
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file should be created")
	}
}

func TestDatabase_CreateDatabaseWithPassword(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_password.db"
	defer os.Remove(dbPath)

	password := "test-password-123"
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name:     "testdb",
		Path:     dbPath,
		Password: password,
	})
	if err != nil {
		t.Fatalf("Failed to create database with password: %v", err)
	}
	defer db.Close(ctx)

	if db.Password() != password {
		t.Errorf("Expected password '%s', got '%s'", password, db.Password())
	}

	// 验证数据库可以正常使用
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档测试
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if doc.ID() != "doc1" {
		t.Errorf("Expected document ID 'doc1', got '%s'", doc.ID())
	}
}

func TestDatabase_Name(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_name.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "mydb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	if db.Name() != "mydb" {
		t.Errorf("Expected database name 'mydb', got '%s'", db.Name())
	}
}

func TestDatabase_Close(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_close.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// 关闭数据库
	err = db.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// 再次关闭应该不报错
	err = db.Close(ctx)
	if err != nil {
		t.Errorf("Closing already closed database should not error: %v", err)
	}

	// 关闭后无法执行操作
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	_, err = db.Collection(ctx, "test", schema)
	if err == nil {
		t.Error("Should not be able to create collection after database is closed")
	}
}

func TestDatabase_Destroy(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_destroy.db"

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// 插入一些数据
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 销毁数据库
	err = db.Destroy(ctx)
	if err != nil {
		t.Fatalf("Failed to destroy database: %v", err)
	}

	// 验证数据库文件已删除
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("Database file should be deleted after destroy")
	}

	// 销毁后无法使用
	_, err = db.Collection(ctx, "test", schema)
	if err == nil {
		t.Error("Should not be able to use database after destroy")
	}
}

func TestDatabase_Collection(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_collection.db"
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

	if collection == nil {
		t.Fatal("Collection should not be nil")
	}

	if collection.Name() != "test" {
		t.Errorf("Expected collection name 'test', got '%s'", collection.Name())
	}

	// 验证 Schema 正确应用
	collectionSchema := collection.Schema()
	if collectionSchema.PrimaryKey != schema.PrimaryKey {
		t.Errorf("Expected primary key 'id', got '%v'", collectionSchema.PrimaryKey)
	}
}

func TestDatabase_MultipleCollections(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_multiple_collections.db"
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

	// 创建多个集合
	collection1, err := db.Collection(ctx, "collection1", schema)
	if err != nil {
		t.Fatalf("Failed to create collection1: %v", err)
	}

	collection2, err := db.Collection(ctx, "collection2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection2: %v", err)
	}

	if collection1.Name() != "collection1" {
		t.Errorf("Expected collection name 'collection1', got '%s'", collection1.Name())
	}

	if collection2.Name() != "collection2" {
		t.Errorf("Expected collection name 'collection2', got '%s'", collection2.Name())
	}

	// 验证集合之间隔离
	_, err = collection1.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Collection1 Doc",
	})
	if err != nil {
		t.Fatalf("Failed to insert into collection1: %v", err)
	}

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Collection2 Doc",
	})
	if err != nil {
		t.Fatalf("Failed to insert into collection2: %v", err)
	}

	// 验证数据隔离
	doc1, err := collection1.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1 in collection1: %v", err)
	}
	if doc1.GetString("name") != "Collection1 Doc" {
		t.Errorf("Expected 'Collection1 Doc', got '%s'", doc1.GetString("name"))
	}

	doc2, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1 in collection2: %v", err)
	}
	if doc2.GetString("name") != "Collection2 Doc" {
		t.Errorf("Expected 'Collection2 Doc', got '%s'", doc2.GetString("name"))
	}
}

func TestDatabase_ExportJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_export.db"
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

	// 创建集合并插入数据
	collection1, err := db.Collection(ctx, "collection1", schema)
	if err != nil {
		t.Fatalf("Failed to create collection1: %v", err)
	}

	collection2, err := db.Collection(ctx, "collection2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection2: %v", err)
	}

	_, err = collection1.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Doc1",
	})
	if err != nil {
		t.Fatalf("Failed to insert into collection1: %v", err)
	}

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Doc2",
	})
	if err != nil {
		t.Fatalf("Failed to insert into collection2: %v", err)
	}

	// 导出数据库
	exported, err := db.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("Failed to export database: %v", err)
	}

	if exported == nil {
		t.Fatal("Exported data should not be nil")
	}

	if exported["name"] != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%v'", exported["name"])
	}

	collections, ok := exported["collections"].(map[string]any)
	if !ok {
		t.Fatal("Exported data should contain 'collections' map")
	}

	if len(collections) != 2 {
		t.Errorf("Expected 2 collections, got %d", len(collections))
	}

	// 验证集合数据
	col1Docs, ok := collections["collection1"].([]any)
	if !ok || len(col1Docs) != 1 {
		t.Errorf("Expected collection1 to have 1 document, got %d", len(col1Docs))
	}

	col2Docs, ok := collections["collection2"].([]any)
	if !ok || len(col2Docs) != 1 {
		t.Errorf("Expected collection2 to have 1 document, got %d", len(col2Docs))
	}
}

func TestDatabase_ImportJSON(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_import.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	// 准备导入数据
	importData := map[string]any{
		"name": "testdb",
		"collections": map[string]any{
			"collection1": []any{
				map[string]any{
					"id":   "doc1",
					"name": "Doc1",
				},
			},
			"collection2": []any{
				map[string]any{
					"id":   "doc2",
					"name": "Doc2",
				},
			},
		},
	}

	// 导入数据
	err = db.ImportJSON(ctx, importData)
	if err != nil {
		t.Fatalf("Failed to import database: %v", err)
	}

	// 验证数据导入成功
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection1, err := db.Collection(ctx, "collection1", schema)
	if err != nil {
		t.Fatalf("Failed to get collection1: %v", err)
	}

	doc1, err := collection1.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find doc1: %v", err)
	}

	if doc1 == nil {
		t.Fatal("Document doc1 should exist")
	}

	if doc1.GetString("name") != "Doc1" {
		t.Errorf("Expected 'Doc1', got '%s'", doc1.GetString("name"))
	}

	collection2, err := db.Collection(ctx, "collection2", schema)
	if err != nil {
		t.Fatalf("Failed to get collection2: %v", err)
	}

	doc2, err := collection2.FindByID(ctx, "doc2")
	if err != nil {
		t.Fatalf("Failed to find doc2: %v", err)
	}

	if doc2 == nil {
		t.Fatal("Document doc2 should exist")
	}

	if doc2.GetString("name") != "Doc2" {
		t.Errorf("Expected 'Doc2', got '%s'", doc2.GetString("name"))
	}
}

func TestDatabase_ExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbPath1 := "./test_roundtrip1.db"
	dbPath2 := "./test_roundtrip2.db"
	defer os.Remove(dbPath1)
	defer os.Remove(dbPath2)

	// 创建第一个数据库并插入数据
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

	_, err = collection1.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test Doc",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 导出数据
	exported, err := db1.ExportJSON(ctx)
	if err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	// 创建第二个数据库并导入数据
	db2, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb2",
		Path: dbPath2,
	})
	if err != nil {
		t.Fatalf("Failed to create database2: %v", err)
	}
	defer db2.Close(ctx)

	err = db2.ImportJSON(ctx, exported)
	if err != nil {
		t.Fatalf("Failed to import: %v", err)
	}

	// 验证数据一致性
	collection2, err := db2.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}

	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if doc == nil {
		t.Fatal("Document should exist")
	}

	if doc.GetString("name") != "Test Doc" {
		t.Errorf("Expected 'Test Doc', got '%s'", doc.GetString("name"))
	}

	if doc.GetInt("age") != 30 {
		t.Errorf("Expected age 30, got %d", doc.GetInt("age"))
	}
}

func TestDatabase_Backup(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_backup.db"
	backupPath := "./test_backup_file.db"
	defer os.Remove(dbPath)
	defer os.Remove(backupPath)

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

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 备份数据库
	err = db.Backup(ctx, backupPath)
	if err != nil {
		t.Fatalf("Failed to backup database: %v", err)
	}

	// 验证备份文件存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Error("Backup file should be created")
	}
}

func TestDatabase_Changes(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_changes.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	changes := db.Changes()

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

	// 接收变更事件
	event := <-changes
	if event.Collection != "test" {
		t.Errorf("Expected collection 'test', got '%s'", event.Collection)
	}
	if event.ID != "doc1" {
		t.Errorf("Expected ID 'doc1', got '%s'", event.ID)
	}
	if event.Op != OperationInsert {
		t.Errorf("Expected OperationInsert, got %s", event.Op)
	}
}

func TestDatabase_WaitForLeadership(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_leadership.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	// 单实例下应该立即返回
	err = db.WaitForLeadership(ctx)
	if err != nil {
		t.Errorf("WaitForLeadership should succeed in single instance: %v", err)
	}
}

func TestDatabase_RequestIdle(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_idle.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	// 请求空闲应该成功
	err = db.RequestIdle(ctx)
	if err != nil {
		t.Errorf("RequestIdle should succeed: %v", err)
	}
}

func TestIsRxDatabase(t *testing.T) {
	ctx := context.Background()
	dbPath := "./test_isrxdb.db"
	defer os.Remove(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	if !IsRxDatabase(db) {
		t.Error("IsRxDatabase should return true for database instance")
	}

	if IsRxDatabase(nil) {
		t.Error("IsRxDatabase should return false for nil")
	}

	if IsRxDatabase("not a database") {
		t.Error("IsRxDatabase should return false for non-database type")
	}
}
