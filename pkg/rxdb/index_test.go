package rxdb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestIndex_CreateIndex(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入一些测试数据
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 创建单字段索引
	index := Index{
		Fields: []string{"name"},
		Name:   "name_idx",
	}
	err = collection.CreateIndex(ctx, index)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 验证索引已创建
	indexes := collection.ListIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
	if indexes[0].Name != "name_idx" {
		t.Errorf("Expected index name 'name_idx', got '%s'", indexes[0].Name)
	}

	// 创建复合索引
	compositeIndex := Index{
		Fields: []string{"name", "age"},
		Name:   "name_age_idx",
	}
	err = collection.CreateIndex(ctx, compositeIndex)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// 验证索引已添加
	indexes = collection.ListIndexes()
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// 验证索引名称唯一性
	err = collection.CreateIndex(ctx, index)
	if err == nil {
		t.Error("Expected error when creating duplicate index, got nil")
	}
}

func TestIndex_CreateIndexDuplicate(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_dup.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_dup.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 创建索引
	index := Index{
		Fields: []string{"name"},
		Name:   "name_idx",
	}
	err = collection.CreateIndex(ctx, index)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 尝试创建同名索引
	err = collection.CreateIndex(ctx, index)
	if err == nil {
		t.Error("Expected error when creating duplicate index name, got nil")
	}

	// 尝试创建相同字段的索引（无名称）
	index2 := Index{
		Fields: []string{"name"},
	}
	err = collection.CreateIndex(ctx, index2)
	if err == nil {
		t.Error("Expected error when creating duplicate index with same fields, got nil")
	}
}

func TestIndex_CreateIndexOnExistingData(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_existing.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_existing.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入大量测试数据
	for i := 0; i < 100; i++ {
		_, err = collection.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("User%d", i%10), // 10个不同的名字
			"age":  20 + (i % 50),
		})
		if err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// 在现有数据上创建索引
	index := Index{
		Fields: []string{"name"},
		Name:   "name_idx",
	}
	err = collection.CreateIndex(ctx, index)
	if err != nil {
		t.Fatalf("Failed to create index on existing data: %v", err)
	}

	// 验证索引已创建
	indexes := collection.ListIndexes()
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indexes))
	}

	// 验证索引数据正确（通过查询验证）
	results, err := collection.Find(map[string]any{"name": "User0"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}
	if len(results) != 10 { // 应该有10个文档名为User0
		t.Errorf("Expected 10 documents, got %d", len(results))
	}
}

func TestIndex_QueryWithIndex(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_query.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_query.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
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
		t.Fatalf("Failed to insert document: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc2",
		"name": "Bob",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc3",
		"name": "Alice",
		"age":  28,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用索引查询
	results, err := collection.Find(map[string]any{"name": "Alice"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(results))
	}

	// 验证结果正确性
	for _, doc := range results {
		if doc.GetString("name") != "Alice" {
			t.Errorf("Expected name 'Alice', got '%s'", doc.GetString("name"))
		}
	}
}

func TestIndex_CompositeIndexQuery(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_composite.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_composite.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name", "age"},
				Name:   "name_age_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	testDocs := []map[string]any{
		{"id": "doc1", "name": "Alice", "age": 30},
		{"id": "doc2", "name": "Alice", "age": 25},
		{"id": "doc3", "name": "Bob", "age": 30},
		{"id": "doc4", "name": "Bob", "age": 25},
	}

	for _, doc := range testDocs {
		_, err = collection.Insert(ctx, doc)
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// 完全匹配查询
	results, err := collection.Find(map[string]any{
		"name": "Alice",
		"age":  30,
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with composite index: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(results))
	}
	if results[0].ID() != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].ID())
	}

	// 前缀匹配（只匹配第一个字段）
	results, err = collection.Find(map[string]any{
		"name": "Alice",
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with prefix match: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(results))
	}
}

func TestIndex_MaintainOnInsert(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_maintain_insert.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_maintain_insert.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证索引已更新（通过查询验证）
	results, err := collection.Find(map[string]any{"name": "Alice"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(results))
	}
	if results[0].ID() != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].ID())
	}
}

func TestIndex_MaintainOnUpdate(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_maintain_update.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_maintain_update.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 更新索引字段
	err = doc.Update(ctx, map[string]any{
		"name": "Bob",
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// 验证旧值不再匹配
	results, err := collection.Find(map[string]any{"name": "Alice"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 documents with name 'Alice', got %d", len(results))
	}

	// 验证新值匹配
	results, err = collection.Find(map[string]any{"name": "Bob"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 document with name 'Bob', got %d", len(results))
	}
	if results[0].ID() != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].ID())
	}
}

func TestIndex_MaintainOnDelete(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_maintain_delete.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_maintain_delete.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 验证索引已清理（通过查询验证）
	results, err := collection.Find(map[string]any{"name": "Alice"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 documents, got %d", len(results))
	}
}

func TestIndex_ListIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_list.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_list.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
			{
				Fields: []string{"age"},
				Name:   "age_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 列出索引
	indexes := collection.ListIndexes()
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// 验证索引信息
	indexMap := make(map[string]Index)
	for _, idx := range indexes {
		indexMap[idx.Name] = idx
	}

	if _, ok := indexMap["name_idx"]; !ok {
		t.Error("Expected name_idx in indexes")
	}
	if _, ok := indexMap["age_idx"]; !ok {
		t.Error("Expected age_idx in indexes")
	}
}

func TestIndex_DropIndex(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_drop.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_drop.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证索引存在
	indexes := collection.ListIndexes()
	if len(indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indexes))
	}

	// 删除索引
	err = collection.DropIndex(ctx, "name_idx")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// 验证索引已删除
	indexes = collection.ListIndexes()
	if len(indexes) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(indexes))
	}

	// 验证查询仍然工作（不使用索引）
	results, err := collection.Find(map[string]any{"name": "Alice"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query after dropping index: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 document, got %d", len(results))
	}

	// 尝试删除不存在的索引
	err = collection.DropIndex(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error when dropping nonexistent index, got nil")
	}
}

func TestIndex_Performance(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_index_perf.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_index_perf.db")
	defer db.Close(ctx)

	// 测试无索引的性能
	schemaNoIndex := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collectionNoIndex, err := db.Collection(ctx, "no_index", schemaNoIndex)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入大量数据
	for i := 0; i < 1000; i++ {
		_, err = collectionNoIndex.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("User%d", i%100),
			"age":  20 + (i % 50),
		})
		if err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// 测试有索引的性能
	schemaWithIndex := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		Indexes: []Index{
			{
				Fields: []string{"name"},
				Name:   "name_idx",
			},
		},
	}

	collectionWithIndex, err := db.Collection(ctx, "with_index", schemaWithIndex)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入相同的数据
	for i := 0; i < 1000; i++ {
		_, err = collectionWithIndex.Insert(ctx, map[string]any{
			"id":   fmt.Sprintf("doc%d", i),
			"name": fmt.Sprintf("User%d", i%100),
			"age":  20 + (i % 50),
		})
		if err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// 验证索引查询结果正确
	results, err := collectionWithIndex.Find(map[string]any{"name": "User0"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 documents, got %d", len(results))
	}

	// 验证无索引查询结果也正确
	results, err = collectionNoIndex.Find(map[string]any{"name": "User0"}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to query without index: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 documents, got %d", len(results))
	}
}
