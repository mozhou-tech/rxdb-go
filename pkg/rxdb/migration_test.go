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
		Path: "./test_migration_version.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_version.db")

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
		Path: "./test_migration_strategy.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_strategy.db")

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
		Path: "./test_migration_multiple.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_multiple.db")

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
		Path: "./test_migration_auto.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_auto.db")

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
		Path: "./test_migration_manual.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_manual.db")

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
		Path: "./test_migration_error.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_error.db")

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
		Path: "./test_migration_noversion.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_noversion.db")

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
		Path: "./test_migration_skip.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_migration_skip.db")

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
