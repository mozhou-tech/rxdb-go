package rxdb

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestKeyCompression_DefaultEnabled(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_default",
		Path: "../../data/test_key_compression_default.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_default.db")
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		// KeyCompression is nil by default
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"firstName": map[string]any{"type": "string"},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	col := coll.(*collection)
	if col.schema.KeyCompression == nil || !*col.schema.KeyCompression {
		t.Fatal("KeyCompression should be enabled by default")
	}

	// 验证存储是否压缩
	_, _ = coll.Insert(ctx, map[string]any{"id": "doc1", "firstName": "John"})
	data, _ := col.store.Get(ctx, "test", "doc1")
	var storedDoc map[string]any
	json.Unmarshal(data, &storedDoc)
	if _, ok := storedDoc["firstName"]; ok {
		t.Error("Stored document should be compressed by default")
	}
}

func TestKeyCompression_ExplicitDisabled(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_disabled",
		Path: "../../data/test_key_compression_disabled.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_disabled.db")
	defer db.Close(ctx)

	disabled := false
	schema := Schema{
		PrimaryKey:     "id",
		KeyCompression: &disabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"firstName": map[string]any{"type": "string"},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	col := coll.(*collection)
	if col.schema.KeyCompression == nil || *col.schema.KeyCompression {
		t.Fatal("KeyCompression should be disabled explicitly")
	}

	// 验证存储未压缩
	_, _ = coll.Insert(ctx, map[string]any{"id": "doc1", "firstName": "John"})
	data, _ := col.store.Get(ctx, "test", "doc1")
	var storedDoc map[string]any
	json.Unmarshal(data, &storedDoc)
	if _, ok := storedDoc["firstName"]; !ok {
		t.Error("Stored document should NOT be compressed when explicitly disabled")
	}
}

func TestKeyCompression_Complex(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_complex",
		Path: "../../data/test_key_compression_complex.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_complex.db")
	defer db.Close(ctx)

	enabled := true
	schema := Schema{
		PrimaryKey:     "id",
		RevField:       "_rev",
		KeyCompression: &enabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"firstName": map[string]any{"type": "string"},
				"address": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"street": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 验证 FindByID 和 Query
	_, _ = coll.Insert(ctx, map[string]any{
		"id":        "doc1",
		"firstName": "John",
		"address":   map[string]any{"street": "Main St"},
	})

	found, _ := coll.FindByID(ctx, "doc1")
	if found.Data()["firstName"] != "John" {
		t.Errorf("Expected John, got %v", found.Data()["firstName"])
	}

	docs, _ := coll.Find(map[string]any{"address.street": "Main St"}).Exec(ctx)
	if len(docs) != 1 {
		t.Fatalf("Expected 1 doc, got %d", len(docs))
	}
}

func TestGenerateShortKey(t *testing.T) {
	tests := []struct {
		index    int
		expected string
	}{
		{0, "a"},
		{1, "b"},
		{25, "z"},
		{26, "aa"},
		{27, "ab"},
		{51, "az"},
		{52, "ba"},
		{701, "zz"},
		{702, "aaa"},
	}

	for _, tt := range tests {
		got := generateShortKey(tt.index)
		if got != tt.expected {
			t.Errorf("generateShortKey(%d) = %v; want %v", tt.index, got, tt.expected)
		}
	}
}

func TestKeyCompression_Dynamic(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_dynamic",
		Path: "../../data/test_key_compression_dynamic.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_dynamic.db")
	defer db.Close(ctx)

	enabled := true
	schema := Schema{
		PrimaryKey:     "id",
		KeyCompression: &enabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入 schema 中未定义的键
	_, err = coll.Insert(ctx, map[string]any{
		"id":            "doc1",
		"undefinedKey":  "value",
		"anotherNewKey": 123,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	col := coll.(*collection)
	data, _ := col.store.Get(ctx, "test", "doc1")
	var storedDoc map[string]any
	json.Unmarshal(data, &storedDoc)

	// 验证未定义的键是否被压缩
	if _, ok := storedDoc["undefinedKey"]; ok {
		t.Error("Undefined key should be compressed dynamically")
	}
	if _, ok := storedDoc["anotherNewKey"]; ok {
		t.Error("Another new key should be compressed dynamically")
	}

	// 验证查询和取出
	found, _ := coll.FindByID(ctx, "doc1")
	if found.Data()["undefinedKey"] != "value" {
		t.Errorf("Expected value, got %v", found.Data()["undefinedKey"])
	}
	if found.Data()["anotherNewKey"] != float64(123) {
		t.Errorf("Expected 123, got %v", found.Data()["anotherNewKey"])
	}
}

func TestKeyCompression_Persistence(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_key_compression_persistence.db"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	enabled := true
	schema := Schema{
		PrimaryKey:     "id",
		KeyCompression: &enabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":        map[string]any{"type": "string"},
				"longField": map[string]any{"type": "string"},
			},
		},
	}

	var shortKey string
	// 第一次运行：创建数据库并插入数据
	{
		db, err := CreateDatabase(ctx, DatabaseOptions{
			Name: "testdb_p",
			Path: dbPath,
		})
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		coll, err := db.Collection(ctx, "test", schema)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}

		col := coll.(*collection)
		shortKey = col.compressionTable["longField"]

		_, _ = coll.Insert(ctx, map[string]any{
			"id":        "doc1",
			"longField": "value1",
		})
		db.Close(ctx)
	}

	// 第二次运行：重新打开数据库，验证压缩表是否一致
	{
		db, err := CreateDatabase(ctx, DatabaseOptions{
			Name: "testdb_p",
			Path: dbPath,
		})
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer db.Close(ctx)

		coll, err := db.Collection(ctx, "test", schema)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}

		col := coll.(*collection)
		if col.compressionTable["longField"] != shortKey {
			t.Errorf("Compression key changed after reload: want %s, got %s", shortKey, col.compressionTable["longField"])
		}

		// 验证是否能正确读回旧数据
		doc, _ := coll.FindByID(ctx, "doc1")
		if doc == nil || doc.Data()["longField"] != "value1" {
			t.Errorf("Failed to read data after reload: got %v", doc)
		}

		// 验证插入新数据是否使用相同的压缩表
		_, _ = coll.Insert(ctx, map[string]any{
			"id":        "doc2",
			"longField": "value2",
		})
		data, _ := col.store.Get(ctx, "test", "doc2")
		var storedDoc map[string]any
		json.Unmarshal(data, &storedDoc)
		if _, ok := storedDoc[shortKey]; !ok {
			t.Error("New document should use existing compression table")
		}
	}
}

func TestKeyCompression_ArrayOfObjects(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_array",
		Path: "../../data/test_key_compression_array.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_array.db")
	defer db.Close(ctx)

	enabled := true
	schema := Schema{
		PrimaryKey:     "id",
		KeyCompression: &enabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"items": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"itemName":  map[string]any{"type": "string"},
							"itemValue": map[string]any{"type": "number"},
						},
					},
				},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	testData := map[string]any{
		"id": "doc1",
		"items": []any{
			map[string]any{"itemName": "apple", "itemValue": 1.5},
			map[string]any{"itemName": "banana", "itemValue": 2.0},
		},
	}

	_, err = coll.Insert(ctx, testData)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 验证存储内容
	col := coll.(*collection)
	data, _ := col.store.Get(ctx, "test", "doc1")
	var storedDoc map[string]any
	json.Unmarshal(data, &storedDoc)

	// 验证 items 数组内部的键是否被压缩
	itemsKey := col.compressionTable["items"]
	storedItems := storedDoc[itemsKey].([]any)
	for _, item := range storedItems {
		m := item.(map[string]any)
		if _, ok := m["itemName"]; ok {
			t.Error("itemName in array should be compressed")
		}
	}

	// 验证解压缩
	doc, _ := coll.FindByID(ctx, "doc1")
	docData := doc.Data()
	items := docData["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(items))
	}
	if items[0].(map[string]any)["itemName"] != "apple" {
		t.Errorf("Expected apple, got %v", items[0].(map[string]any)["itemName"])
	}
}

func TestKeyCompression_DeeplyNested(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_deep",
		Path: "../../data/test_key_compression_deep.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll("../../data/test_key_compression_deep.db")
	defer db.Close(ctx)

	enabled := true
	schema := Schema{
		PrimaryKey:     "id",
		KeyCompression: &enabled,
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
			},
		},
	}

	coll, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 深度嵌套的数据
	testData := map[string]any{
		"id": "doc1",
		"a": map[string]any{
			"b": map[string]any{
				"c": []any{
					map[string]any{
						"d": "deep-value",
						"e": []any{
							map[string]any{"f": 1},
						},
					},
				},
			},
		},
	}

	_, _ = coll.Insert(ctx, testData)

	// 验证解压缩后的数据完整性
	doc, _ := coll.FindByID(ctx, "doc1")
	data := doc.Data()

	// 验证路径 a.b.c[0].e[0].f
	a := data["a"].(map[string]any)
	b := a["b"].(map[string]any)
	c := b["c"].([]any)
	c0 := c[0].(map[string]any)
	if c0["d"] != "deep-value" {
		t.Errorf("Expected deep-value, got %v", c0["d"])
	}
	e := c0["e"].([]any)
	e0 := e[0].(map[string]any)
	if e0["f"] != float64(1) {
		t.Errorf("Expected 1, got %v", e0["f"])
	}
}
