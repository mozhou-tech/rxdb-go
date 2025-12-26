package rxdb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestMigration_SchemaVersion(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_version.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_version.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 验证版本
	currentSchema := collection.Schema()
	version := getSchemaVersion(currentSchema)
	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}

	// 创建版本 2 的 schema
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
	}

	collection2, err := db.Collection(ctx, "test2", schemaV2)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	currentSchema2 := collection2.Schema()
	version2 := getSchemaVersion(currentSchema2)
	if version2 != 2 {
		t.Errorf("Expected version 2, got %d", version2)
	}
}

func TestMigration_MigrationStrategy(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_strategy.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_strategy.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema（无迁移策略）
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入一些旧版本的数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"name":  "Old Name",
		"value": 100,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 2 的 schema，带迁移策略
	migrationStrategy := func(oldDoc map[string]any) (map[string]any, error) {
		// 添加新字段
		oldDoc["newField"] = "migrated"
		// 重命名字段
		if name, ok := oldDoc["name"]; ok {
			oldDoc["fullName"] = name
			delete(oldDoc, "name")
		}
		// 转换字段类型
		if value, ok := oldDoc["value"].(float64); ok {
			oldDoc["value"] = int(value) * 2
		}
		return oldDoc, nil
	}

	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: migrationStrategy,
		},
	}

	// 创建新集合（会自动迁移）
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to create collection with migration: %v", err)
	}

	// 验证数据已迁移
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if doc == nil {
		t.Fatal("Expected document to exist after migration")
	}

	// 验证迁移后的字段
	if doc.Get("fullName") != "Old Name" {
		t.Errorf("Expected fullName 'Old Name', got '%v'", doc.Get("fullName"))
	}
	if doc.Get("name") != nil {
		t.Error("Expected 'name' field to be removed")
	}
	if doc.Get("newField") != "migrated" {
		t.Errorf("Expected newField 'migrated', got '%v'", doc.Get("newField"))
	}
	if doc.GetInt("value") != 200 {
		t.Errorf("Expected value 200, got %d", doc.GetInt("value"))
	}
}

func TestMigration_MultipleVersions(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_multiple.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_multiple.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"data": "v1",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 3 的 schema，带多个迁移策略
	migrationV2 := func(oldDoc map[string]any) (map[string]any, error) {
		oldDoc["data"] = "v2"
		oldDoc["step"] = 2
		return oldDoc, nil
	}

	migrationV3 := func(oldDoc map[string]any) (map[string]any, error) {
		oldDoc["data"] = "v3"
		oldDoc["step"] = 3
		return oldDoc, nil
	}

	schemaV3 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 3,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: migrationV2,
			3: migrationV3,
		},
	}

	// 创建新集合（应该执行版本 1->2 和 2->3 的迁移）
	collection2, err := db.Collection(ctx, "test", schemaV3)
	if err != nil {
		t.Fatalf("Failed to create collection with migration: %v", err)
	}

	// 验证数据已迁移到版本 3
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if doc.Get("data") != "v3" {
		t.Errorf("Expected data 'v3', got '%v'", doc.Get("data"))
	}
	if doc.GetInt("step") != 3 {
		t.Errorf("Expected step 3, got %d", doc.GetInt("step"))
	}
}

func TestMigration_AutoMigration(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_auto.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_auto.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 2 的 schema，带迁移策略（应该自动迁移）
	migrationStrategy := func(oldDoc map[string]any) (map[string]any, error) {
		oldDoc["migrated"] = true
		return oldDoc, nil
	}

	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: migrationStrategy,
		},
	}

	// 创建新集合（应该自动检测版本差异并迁移）
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to create collection with auto migration: %v", err)
	}

	// 验证数据已自动迁移
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if doc.Get("migrated") != true {
		t.Error("Expected document to be migrated automatically")
	}
}

func TestMigration_ManualMigration(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_manual.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_manual.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 2 的 schema，带迁移策略
	migrationStrategy := func(oldDoc map[string]any) (map[string]any, error) {
		oldDoc["migrated"] = true
		return oldDoc, nil
	}

	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: migrationStrategy,
		},
	}

	// 创建新集合
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 手动触发迁移
	err = collection2.Migrate(ctx)
	if err != nil {
		t.Fatalf("Failed to manually migrate: %v", err)
	}

	// 验证数据已迁移
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if doc.Get("migrated") != true {
		t.Error("Expected document to be migrated")
	}
}

func TestMigration_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_error.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_error.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 2 的 schema，带会失败的迁移策略
	failingMigrationStrategy := func(oldDoc map[string]any) (map[string]any, error) {
		return nil, fmt.Errorf("migration failed")
	}

	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: failingMigrationStrategy,
		},
	}

	// 尝试创建新集合（应该失败）
	_, err = db.Collection(ctx, "test", schemaV2)
	if err == nil {
		t.Error("Expected migration to fail, but it succeeded")
	}

	// 验证原始数据未被修改
	doc, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if doc.Get("migrated") != nil {
		t.Error("Expected document to not be migrated after failure")
	}
}

func TestMigration_NoVersion(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_noversion.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_noversion.db")
	defer db.Close(ctx)

	// 创建无版本的 schema
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 手动迁移应该不执行任何操作（无版本信息）
	err = collection.Migrate(ctx)
	if err != nil {
		t.Fatalf("Expected no error when migrating collection without version, got: %v", err)
	}
}

func TestMigration_SkipVersions(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_migration_skip.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_migration_skip.db")
	defer db.Close(ctx)

	// 创建版本 1 的 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"data": "v1",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建版本 3 的 schema，但只有版本 3 的迁移策略（跳过版本 2）
	migrationV3 := func(oldDoc map[string]any) (map[string]any, error) {
		oldDoc["data"] = "v3"
		return oldDoc, nil
	}

	schemaV3 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 3,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			3: migrationV3,
		},
	}

	// 创建新集合（应该只执行版本 3 的迁移）
	collection2, err := db.Collection(ctx, "test", schemaV3)
	if err != nil {
		t.Fatalf("Failed to create collection with migration: %v", err)
	}

	// 验证数据已迁移
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if doc.Get("data") != "v3" {
		t.Errorf("Expected data 'v3', got '%v'", doc.Get("data"))
	}
}

// TestSchemaModify_Indexes 测试修改 schema 的索引
func TestSchemaModify_Indexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_modify_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_modify_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，带一个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证初始索引
	indexes := collection.ListIndexes()
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indexes))
	}

	// 修改 schema，添加新索引
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
			{Fields: []string{"name", "age"}, Name: "name_age_idx"},
		},
	}

	// 使用新 schema 获取集合（应该更新 schema）
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to modify collection schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(updatedSchema.Indexes))
	}

	// 验证新索引可以用于查询
	docs, err := collection2.Find(map[string]any{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
}

// TestSchemaModify_RevField 测试修改 schema 的修订号字段
func TestSchemaModify_RevField(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_modify_revfield.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_modify_revfield.db")
	defer db.Close(ctx)

	// 创建初始 schema，使用默认 _rev 字段
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证使用 _rev 字段
	if doc.Get("_rev") == nil {
		t.Error("Expected document to have _rev field")
	}

	// 修改 schema，使用自定义修订号字段
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "revision",
		JSON: map[string]any{
			"version": 2,
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: func(oldDoc map[string]any) (map[string]any, error) {
				// 迁移策略：将 _rev 重命名为 revision
				if rev, ok := oldDoc["_rev"]; ok {
					oldDoc["revision"] = rev
					delete(oldDoc, "_rev")
				}
				return oldDoc, nil
			},
		},
	}

	// 使用新 schema 获取集合
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to modify collection schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if updatedSchema.RevField != "revision" {
		t.Errorf("Expected RevField 'revision', got '%s'", updatedSchema.RevField)
	}

	// 验证迁移后的文档使用新字段
	migratedDoc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find migrated document: %v", err)
	}

	if migratedDoc.Get("revision") == nil {
		t.Error("Expected document to have 'revision' field after migration")
	}
	if migratedDoc.Get("_rev") != nil {
		t.Error("Expected '_rev' field to be removed after migration")
	}
}

// TestSchemaModify_EncryptedFields 测试修改 schema 的加密字段
func TestSchemaModify_EncryptedFields(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name:     "testdb",
		Path:     "../../data/test_schema_modify_encrypted.db",
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_modify_encrypted.db")
	defer db.Close(ctx)

	// 创建初始 schema，带一个加密字段
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		EncryptedFields: []string{"secret"},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":     "doc1",
		"name":   "Alice",
		"secret": "sensitive-data",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证数据已加密存储
	doc, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc.GetString("secret") != "sensitive-data" {
		t.Errorf("Expected secret 'sensitive-data', got '%s'", doc.GetString("secret"))
	}

	// 修改 schema，添加更多加密字段
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		EncryptedFields: []string{"secret", "password", "token"},
		MigrationStrategies: map[int]MigrationStrategy{
			2: func(oldDoc map[string]any) (map[string]any, error) {
				// 迁移策略：添加新字段
				oldDoc["password"] = "default-password"
				oldDoc["token"] = "default-token"
				return oldDoc, nil
			},
		},
	}

	// 使用新 schema 获取集合
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to modify collection schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.EncryptedFields) != 3 {
		t.Errorf("Expected 3 encrypted fields, got %d", len(updatedSchema.EncryptedFields))
	}

	// 验证新插入的文档使用新的加密字段
	newDoc, err := collection2.Insert(ctx, map[string]any{
		"id":       "doc2",
		"name":     "Bob",
		"secret":   "new-secret",
		"password": "new-password",
		"token":    "new-token",
	})
	if err != nil {
		t.Fatalf("Failed to insert new document: %v", err)
	}

	// 验证新文档的字段可以正确解密
	if newDoc.GetString("secret") != "new-secret" {
		t.Errorf("Expected secret 'new-secret', got '%s'", newDoc.GetString("secret"))
	}
	if newDoc.GetString("password") != "new-password" {
		t.Errorf("Expected password 'new-password', got '%s'", newDoc.GetString("password"))
	}
	if newDoc.GetString("token") != "new-token" {
		t.Errorf("Expected token 'new-token', got '%s'", newDoc.GetString("token"))
	}
}

// TestSchemaModify_WithoutVersionChange 测试在不改变版本的情况下修改 schema
func TestSchemaModify_WithoutVersionChange(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_modify_noversion.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_modify_noversion.db")
	defer db.Close(ctx)

	// 创建初始 schema
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但不同索引的 schema（不触发迁移）
	schemaV1Modified := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
		},
	}

	// 使用新 schema 获取集合（版本相同，不会触发迁移）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新（即使版本相同）
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(updatedSchema.Indexes))
	}

	// 验证数据仍然存在
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected document to still exist")
	}
}

// TestSchemaModify_AddIndexes 测试添加索引到现有 schema
func TestSchemaModify_AddIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_add_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_add_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，无索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc2",
		"name":  "Bob",
		"email": "bob@example.com",
		"age":   25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 修改 schema，添加索引
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		Indexes: []Index{
			{Fields: []string{"email"}, Name: "email_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: func(oldDoc map[string]any) (map[string]any, error) {
				// 无需修改数据，只更新索引
				return oldDoc, nil
			},
		},
	}

	// 使用新 schema 获取集合
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to modify collection schema: %v", err)
	}

	// 验证索引已添加
	indexes := collection2.ListIndexes()
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// 验证可以使用新索引进行查询
	docs, err := collection2.Find(map[string]any{"email": "alice@example.com"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
	if docs[0].ID() != "doc1" {
		t.Errorf("Expected document ID 'doc1', got '%s'", docs[0].ID())
	}
}

// TestSchemaModify_RemoveIndexes 测试从 schema 中移除索引
func TestSchemaModify_RemoveIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_remove_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_remove_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，带多个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"email"}, Name: "email_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 验证初始索引
	indexes := collection.ListIndexes()
	if len(indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(indexes))
	}

	// 修改 schema，移除部分索引
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
		MigrationStrategies: map[int]MigrationStrategy{
			2: func(oldDoc map[string]any) (map[string]any, error) {
				return oldDoc, nil
			},
		},
	}

	// 使用新 schema 获取集合
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to modify collection schema: %v", err)
	}

	// 验证索引已更新
	updatedIndexes := collection2.ListIndexes()
	if len(updatedIndexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(updatedIndexes))
	}

	// 验证 schema 中的索引已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 1 {
		t.Errorf("Expected 1 index in schema, got %d", len(updatedSchema.Indexes))
	}
}

// TestSchemaModify_SameVersion_AddIndexes 测试相同版本添加索引并验证索引被自动构建
func TestSchemaModify_SameVersion_AddIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_same_version_add_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_same_version_add_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，带一个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
		"city": "Beijing",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		"age":  25,
		"city": "Shanghai",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但添加新索引的 schema
	schemaV1Modified := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
			{Fields: []string{"city"}, Name: "city_idx"},
		},
	}

	// 使用新 schema 获取集合（应该自动构建新索引）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(updatedSchema.Indexes))
	}

	// 验证新索引已被构建（可以通过查询验证）
	docs, err := collection2.Find(map[string]any{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
	if docs[0].ID() != "doc1" {
		t.Errorf("Expected document ID 'doc1', got '%s'", docs[0].ID())
	}

	// 验证另一个新索引
	docs2, err := collection2.Find(map[string]any{"city": "Shanghai"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs2) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs2))
	}
	if docs2[0].ID() != "doc2" {
		t.Errorf("Expected document ID 'doc2', got '%s'", docs2[0].ID())
	}
}

// TestSchemaModify_SameVersion_RemoveIndexes 测试相同版本删除索引
func TestSchemaModify_SameVersion_RemoveIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_same_version_remove_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_same_version_remove_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，带多个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
			{Fields: []string{"email"}, Name: "email_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"name":  "Alice",
		"age":   30,
		"email": "alice@example.com",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但删除部分索引的 schema
	schemaV1Modified := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	// 使用新 schema 获取集合（应该自动删除旧索引）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(updatedSchema.Indexes))
	}

	// 验证索引列表
	indexes := collection2.ListIndexes()
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index in list, got %d", len(indexes))
	}
	if indexes[0].Name != "name_idx" {
		t.Errorf("Expected index name 'name_idx', got '%s'", indexes[0].Name)
	}
}

// TestSchemaModify_SameVersion_ChangeIndexFields 测试相同版本修改索引字段
func TestSchemaModify_SameVersion_ChangeIndexFields(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_same_version_change_index_fields.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_same_version_change_index_fields.db")
	defer db.Close(ctx)

	// 创建初始 schema，带一个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但修改索引字段的 schema（从 name 改为 age）
	schemaV1Modified := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
		Indexes: []Index{
			{Fields: []string{"age"}, Name: "name_idx"}, // 保持名称相同但字段不同
		},
	}

	// 使用新 schema 获取集合（应该重建索引）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(updatedSchema.Indexes))
	}
	if len(updatedSchema.Indexes[0].Fields) != 1 || updatedSchema.Indexes[0].Fields[0] != "age" {
		t.Errorf("Expected index field 'age', got %v", updatedSchema.Indexes[0].Fields)
	}

	// 验证新索引已被构建（可以通过查询验证）
	docs, err := collection2.Find(map[string]any{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
}

// TestSchemaModify_SameVersion_ChangeEncryptedFields 测试相同版本修改加密字段
func TestSchemaModify_SameVersion_ChangeEncryptedFields(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name:     "testdb",
		Path:     "../../data/test_schema_same_version_change_encrypted_fields.db",
		Password: "testpassword",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_same_version_change_encrypted_fields.db")
	defer db.Close(ctx)

	// 创建初始 schema，带加密字段
	schemaV1 := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"email"},
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"name":  "Alice",
		"email": "alice@example.com",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但修改加密字段的 schema
	schemaV1Modified := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"email", "phone"}, // 添加新字段
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
	}

	// 使用新 schema 获取集合（应该更新加密字段配置）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.EncryptedFields) != 2 {
		t.Errorf("Expected 2 encrypted fields, got %d", len(updatedSchema.EncryptedFields))
	}

	// 验证数据仍然可以访问
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected document to still exist")
	}
	if doc.Get("email") != "alice@example.com" {
		t.Errorf("Expected email 'alice@example.com', got '%v'", doc.Get("email"))
	}
}

// TestSchemaModify_SameVersion_ChangeKeyCompression 测试相同版本修改键压缩设置
func TestSchemaModify_SameVersion_ChangeKeyCompression(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_same_version_change_key_compression.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_same_version_change_key_compression.db")
	defer db.Close(ctx)

	// 创建初始 schema，禁用键压缩
	compressionDisabled := false
	schemaV1 := Schema{
		PrimaryKey:     "id",
		RevField:       "_rev",
		KeyCompression: &compressionDisabled,
		JSON: map[string]any{
			"version": 1,
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用相同版本但启用键压缩的 schema
	compressionEnabled := true
	schemaV1Modified := Schema{
		PrimaryKey:     "id",
		RevField:       "_rev",
		KeyCompression: &compressionEnabled,
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
	}

	// 使用新 schema 获取集合（应该更新键压缩设置）
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if updatedSchema.KeyCompression == nil || !*updatedSchema.KeyCompression {
		t.Error("Expected key compression to be enabled")
	}

	// 验证数据仍然可以访问
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected document to still exist")
	}
}

// TestSchemaModify_LowerVersion 测试版本号降低的情况
func TestSchemaModify_LowerVersion(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_lower_version.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_lower_version.db")
	defer db.Close(ctx)

	// 创建版本 2 的 schema
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 2,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用版本 1 的 schema（版本降低）
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"version": 1, // 版本降低
		},
		Indexes: []Index{
			{Fields: []string{"age"}, Name: "age_idx"},
		},
	}

	// 使用新 schema 获取集合（应该允许更新，可能是回滚场景）
	collection2, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to get collection with lower version schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(updatedSchema.Indexes))
	}
	if updatedSchema.Indexes[0].Name != "age_idx" {
		t.Errorf("Expected index name 'age_idx', got '%s'", updatedSchema.Indexes[0].Name)
	}

	// 验证数据仍然存在
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected document to still exist")
	}
}

// TestSchemaModify_NoVersion_WithIndexes 测试无版本号但索引变化的情况
func TestSchemaModify_NoVersion_WithIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_schema_no_version_with_indexes.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_no_version_with_indexes.db")
	defer db.Close(ctx)

	// 创建初始 schema，无版本号，带一个索引
	schemaV1 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用无版本号但添加索引的 schema
	schemaV2 := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"},
		},
	}

	// 使用新 schema 获取集合（应该更新索引）
	collection2, err := db.Collection(ctx, "test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证 schema 已更新
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(updatedSchema.Indexes))
	}

	// 验证新索引已被构建
	docs, err := collection2.Find(map[string]any{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
}

// TestSchemaModify_ComplexChanges 测试同时修改多个属性的情况
func TestSchemaModify_ComplexChanges(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name:     "testdb",
		Path:     "../../data/test_schema_complex_changes.db",
		Password: "testpassword",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_schema_complex_changes.db")
	defer db.Close(ctx)

	// 创建初始 schema
	compressionDisabled := false
	schemaV1 := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"email"},
		KeyCompression:  &compressionDisabled,
		JSON: map[string]any{
			"version": 1,
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
		},
	}

	collection, err := db.Collection(ctx, "test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":    "doc1",
		"name":  "Alice",
		"age":   30,
		"email": "alice@example.com",
		"phone": "1234567890",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 同时修改多个属性：添加索引、修改加密字段、启用键压缩
	compressionEnabled := true
	schemaV1Modified := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"email", "phone"}, // 添加 phone
		KeyCompression:  &compressionEnabled,         // 启用压缩
		JSON: map[string]any{
			"version": 1, // 相同版本
		},
		Indexes: []Index{
			{Fields: []string{"name"}, Name: "name_idx"},
			{Fields: []string{"age"}, Name: "age_idx"}, // 添加新索引
		},
	}

	// 使用新 schema 获取集合
	collection2, err := db.Collection(ctx, "test", schemaV1Modified)
	if err != nil {
		t.Fatalf("Failed to get collection with modified schema: %v", err)
	}

	// 验证所有更改都已应用
	updatedSchema := collection2.Schema()
	if len(updatedSchema.Indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(updatedSchema.Indexes))
	}
	if len(updatedSchema.EncryptedFields) != 2 {
		t.Errorf("Expected 2 encrypted fields, got %d", len(updatedSchema.EncryptedFields))
	}
	if updatedSchema.KeyCompression == nil || !*updatedSchema.KeyCompression {
		t.Error("Expected key compression to be enabled")
	}

	// 验证数据仍然可以访问
	doc, err := collection2.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected document to still exist")
	}

	// 验证新索引可用
	docs, err := collection2.Find(map[string]any{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with new index: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}
}
