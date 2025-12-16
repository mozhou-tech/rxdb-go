package rxdb

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestAttachment_PutAttachment(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_put",
		Path: "./test_attachment.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment.db")

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

	// 添加附件
	attachment := &Attachment{
		ID:   "att1",
		Name: "test.txt",
		Type: "text/plain",
		Data: []byte("Hello, World!"),
		Size: 13,
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment: %v", err)
	}

	// 验证附件元数据
	if attachment.Created == 0 {
		t.Error("Expected Created timestamp to be set")
	}
	if attachment.Modified == 0 {
		t.Error("Expected Modified timestamp to be set")
	}
	if attachment.Digest == "" {
		t.Error("Expected Digest to be set")
	}

	// 验证附件大小
	if attachment.Size != 13 {
		t.Errorf("Expected size 13, got %d", attachment.Size)
	}
}

func TestAttachment_GetAttachment(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_get",
		Path: "./test_attachment_get.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_get.db")

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

	// 添加附件
	attachmentData := []byte("Hello, World!")
	attachment := &Attachment{
		ID:   "att1",
		Name: "test.txt",
		Type: "text/plain",
		Data: attachmentData,
		Size: int64(len(attachmentData)),
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment: %v", err)
	}

	// 获取附件
	retrieved, err := collection.GetAttachment(ctx, "doc1", "att1")
	if err != nil {
		t.Fatalf("Failed to get attachment: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected attachment, got nil")
	}

	// 验证附件数据
	if retrieved.ID != "att1" {
		t.Errorf("Expected ID 'att1', got '%s'", retrieved.ID)
	}
	if retrieved.Name != "test.txt" {
		t.Errorf("Expected Name 'test.txt', got '%s'", retrieved.Name)
	}
	if retrieved.Type != "text/plain" {
		t.Errorf("Expected Type 'text/plain', got '%s'", retrieved.Type)
	}
	if string(retrieved.Data) != "Hello, World!" {
		t.Errorf("Expected data 'Hello, World!', got '%s'", string(retrieved.Data))
	}
	if retrieved.Size != 13 {
		t.Errorf("Expected size 13, got %d", retrieved.Size)
	}

	// 获取不存在的附件
	_, err = collection.GetAttachment(ctx, "doc1", "nonexistent")
	if err == nil {
		t.Error("Expected error when getting nonexistent attachment, got nil")
	}
}

func TestAttachment_RemoveAttachment(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_remove",
		Path: "./test_attachment_remove.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_remove.db")

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

	// 添加附件
	attachment := &Attachment{
		ID:   "att1",
		Name: "test.txt",
		Type: "text/plain",
		Data: []byte("Hello, World!"),
		Size: 13,
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment: %v", err)
	}

	// 验证附件存在
	_, err = collection.GetAttachment(ctx, "doc1", "att1")
	if err != nil {
		t.Fatalf("Failed to get attachment before removal: %v", err)
	}

	// 删除附件
	err = collection.RemoveAttachment(ctx, "doc1", "att1")
	if err != nil {
		t.Fatalf("Failed to remove attachment: %v", err)
	}

	// 验证附件已删除
	_, err = collection.GetAttachment(ctx, "doc1", "att1")
	if err == nil {
		t.Error("Expected error when getting removed attachment, got nil")
	}
}

func TestAttachment_GetAllAttachments(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_all",
		Path: "./test_attachment_all.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_all.db")

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

	// 添加多个附件
	attachments := []*Attachment{
		{
			ID:   "att1",
			Name: "test1.txt",
			Type: "text/plain",
			Data: []byte("Content 1"),
			Size: 9,
		},
		{
			ID:   "att2",
			Name: "test2.txt",
			Type: "text/plain",
			Data: []byte("Content 2"),
			Size: 9,
		},
		{
			ID:   "att3",
			Name: "image.png",
			Type: "image/png",
			Data: []byte("fake image data"),
			Size: 15,
		},
	}

	for _, att := range attachments {
		err = collection.PutAttachment(ctx, "doc1", att)
		if err != nil {
			t.Fatalf("Failed to put attachment: %v", err)
		}
	}

	// 获取所有附件
	allAttachments, err := collection.GetAllAttachments(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to get all attachments: %v", err)
	}

	if len(allAttachments) != 3 {
		t.Errorf("Expected 3 attachments, got %d", len(allAttachments))
	}

	// 验证附件列表
	attMap := make(map[string]*Attachment)
	for _, att := range allAttachments {
		attMap[att.ID] = att
	}

	if attMap["att1"] == nil {
		t.Error("Expected att1 in attachments")
	}
	if attMap["att2"] == nil {
		t.Error("Expected att2 in attachments")
	}
	if attMap["att3"] == nil {
		t.Error("Expected att3 in attachments")
	}

	// 验证附件数据完整性
	if string(attMap["att1"].Data) != "Content 1" {
		t.Errorf("Expected 'Content 1', got '%s'", string(attMap["att1"].Data))
	}
	if string(attMap["att2"].Data) != "Content 2" {
		t.Errorf("Expected 'Content 2', got '%s'", string(attMap["att2"].Data))
	}
	if string(attMap["att3"].Data) != "fake image data" {
		t.Errorf("Expected 'fake image data', got '%s'", string(attMap["att3"].Data))
	}
}

func TestAttachment_Metadata(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_metadata",
		Path: "./test_attachment_metadata.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_metadata.db")

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

	// 添加附件（手动设置时间戳）
	now := time.Now().Unix()
	attachment := &Attachment{
		ID:       "att1",
		Name:     "test.txt",
		Type:     "text/plain",
		Data:     []byte("Hello, World!"),
		Size:     13,
		Created:  now,
		Modified: now,
		Digest:   fmt.Sprintf("%x", md5.Sum([]byte("Hello, World!"))),
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment: %v", err)
	}

	// 获取附件并验证元数据
	retrieved, err := collection.GetAttachment(ctx, "doc1", "att1")
	if err != nil {
		t.Fatalf("Failed to get attachment: %v", err)
	}

	if retrieved.ID != "att1" {
		t.Errorf("Expected ID 'att1', got '%s'", retrieved.ID)
	}
	if retrieved.Name != "test.txt" {
		t.Errorf("Expected Name 'test.txt', got '%s'", retrieved.Name)
	}
	if retrieved.Type != "text/plain" {
		t.Errorf("Expected Type 'text/plain', got '%s'", retrieved.Type)
	}
	if retrieved.Size != 13 {
		t.Errorf("Expected size 13, got %d", retrieved.Size)
	}
	if retrieved.Created == 0 {
		t.Error("Expected Created timestamp to be set")
	}
	if retrieved.Modified == 0 {
		t.Error("Expected Modified timestamp to be set")
	}
	if retrieved.Digest == "" {
		t.Error("Expected Digest to be set")
	}
}

func TestAttachment_LargeAttachment(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_large",
		Path: "./test_attachment_large.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_large.db")

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

	// 创建大附件（10KB）
	largeData := make([]byte, 10*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	attachment := &Attachment{
		ID:   "large_att",
		Name: "large.bin",
		Type: "application/octet-stream",
		Data: largeData,
		Size: int64(len(largeData)),
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put large attachment: %v", err)
	}

	// 获取并验证大附件
	retrieved, err := collection.GetAttachment(ctx, "doc1", "large_att")
	if err != nil {
		t.Fatalf("Failed to get large attachment: %v", err)
	}

	if len(retrieved.Data) != 10*1024 {
		t.Errorf("Expected size %d, got %d", 10*1024, len(retrieved.Data))
	}

	// 验证数据完整性
	for i := range largeData {
		if retrieved.Data[i] != largeData[i] {
			t.Errorf("Data mismatch at index %d", i)
			break
		}
	}
}

func TestAttachment_DifferentTypes(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_types",
		Path: "./test_attachment_types.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_types.db")

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

	// 测试不同类型的附件
	testCases := []struct {
		name     string
		attID    string
		mimeType string
		data     []byte
	}{
		{"text.txt", "att1", "text/plain", []byte("Plain text content")},
		{"image.png", "att2", "image/png", []byte("fake png data")},
		{"binary.bin", "att3", "application/octet-stream", []byte{0x00, 0x01, 0x02, 0x03}},
		{"json.json", "att4", "application/json", []byte(`{"key":"value"}`)},
	}

	for _, tc := range testCases {
		attachment := &Attachment{
			ID:   tc.attID,
			Name: tc.name,
			Type: tc.mimeType,
			Data: tc.data,
			Size: int64(len(tc.data)),
		}

		err = collection.PutAttachment(ctx, "doc1", attachment)
		if err != nil {
			t.Fatalf("Failed to put attachment %s: %v", tc.name, err)
		}

		// 验证附件
		retrieved, err := collection.GetAttachment(ctx, "doc1", tc.attID)
		if err != nil {
			t.Fatalf("Failed to get attachment %s: %v", tc.name, err)
		}

		if retrieved.Type != tc.mimeType {
			t.Errorf("Expected type %s for %s, got %s", tc.mimeType, tc.name, retrieved.Type)
		}
		if string(retrieved.Data) != string(tc.data) {
			t.Errorf("Data mismatch for %s", tc.name)
		}
	}
}

func TestAttachment_WithDocument(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_doc",
		Path: "./test_attachment_doc.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_doc.db")

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
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 使用文档接口添加附件
	attachment := &Attachment{
		ID:   "att1",
		Name: "test.txt",
		Type: "text/plain",
		Data: []byte("Hello, World!"),
		Size: 13,
	}

	err = doc.PutAttachment(ctx, attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment via document: %v", err)
	}

	// 使用文档接口获取附件
	retrieved, err := doc.GetAttachment(ctx, "att1")
	if err != nil {
		t.Fatalf("Failed to get attachment via document: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected attachment, got nil")
	}

	// 使用文档接口获取所有附件
	allAttachments, err := doc.GetAllAttachments(ctx)
	if err != nil {
		t.Fatalf("Failed to get all attachments via document: %v", err)
	}

	if len(allAttachments) != 1 {
		t.Errorf("Expected 1 attachment, got %d", len(allAttachments))
	}

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 验证附件已清理（尝试获取应该失败）
	_, err = collection.GetAttachment(ctx, "doc1", "att1")
	if err == nil {
		t.Error("Expected error when getting attachment for deleted document, got nil")
	}
}

func TestAttachment_Dump(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_dump",
		Path: "./test_attachment_dump.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_dump.db")

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

	// 添加附件
	attachment := &Attachment{
		ID:   "att1",
		Name: "test.txt",
		Type: "text/plain",
		Data: []byte("Hello, World!"),
		Size: 13,
	}

	err = collection.PutAttachment(ctx, "doc1", attachment)
	if err != nil {
		t.Fatalf("Failed to put attachment: %v", err)
	}

	// 导出 Dump
	dump, err := collection.Dump(ctx)
	if err != nil {
		t.Fatalf("Failed to dump collection: %v", err)
	}

	// 验证 Dump 包含附件
	attachmentsData, ok := dump["attachments"]
	if !ok {
		t.Fatal("Expected attachments in dump")
	}

	attachmentsMap, ok := attachmentsData.(map[string]any)
	if !ok {
		t.Fatal("Expected attachments to be a map")
	}

	docAttachments, ok := attachmentsMap["doc1"]
	if !ok {
		t.Fatal("Expected doc1 attachments in dump")
	}

	docAttachmentsMap, ok := docAttachments.(map[string]any)
	if !ok {
		t.Fatal("Expected doc1 attachments to be a map")
	}

	attData, ok := docAttachmentsMap["att1"]
	if !ok {
		t.Fatal("Expected att1 in dump")
	}

	attMap, ok := attData.(map[string]any)
	if !ok {
		t.Fatal("Expected att1 to be a map")
	}

	if attMap["id"] != "att1" {
		t.Errorf("Expected ID 'att1', got '%v'", attMap["id"])
	}
	if attMap["name"] != "test.txt" {
		t.Errorf("Expected Name 'test.txt', got '%v'", attMap["name"])
	}
}

func TestAttachment_ImportDump(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb_attachment_import",
		Path: "./test_attachment_import.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.Remove("./test_attachment_import.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 创建包含附件的 Dump 数据
	dump := map[string]any{
		"docs": []map[string]any{
			{
				"id":   "doc1",
				"name": "Test Document",
			},
		},
		"attachments": map[string]any{
			"doc1": map[string]any{
				"att1": map[string]any{
					"id":   "att1",
					"name": "test.txt",
					"type": "text/plain",
					"size": float64(13),
					"data": "Hello, World!",
				},
			},
		},
	}

	// 导入 Dump
	err = collection.ImportDump(ctx, dump)
	if err != nil {
		t.Fatalf("Failed to import dump: %v", err)
	}

	// 验证附件已导入
	retrieved, err := collection.GetAttachment(ctx, "doc1", "att1")
	if err != nil {
		t.Fatalf("Failed to get imported attachment: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected attachment, got nil")
	}

	if string(retrieved.Data) != "Hello, World!" {
		t.Errorf("Expected 'Hello, World!', got '%s'", string(retrieved.Data))
	}
}
