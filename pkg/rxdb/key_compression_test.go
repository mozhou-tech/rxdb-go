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
