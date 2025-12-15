package rxdb

import (
	"context"
	"testing"
)

func TestEncryptDecryptField(t *testing.T) {
	password := "test-password"
	plaintext := "sensitive-data"

	// 加密
	encrypted, err := encryptField(plaintext, password)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if encrypted == plaintext {
		t.Fatal("encrypted value should be different from plaintext")
	}

	// 解密
	decrypted, err := decryptField(encrypted, password)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if decrypted != plaintext {
		t.Fatalf("decrypted value mismatch: expected %s, got %s", plaintext, decrypted)
	}
}

func TestEncryptDecryptDocumentFields(t *testing.T) {
	password := "test-password"
	encryptedFields := []string{"secret", "password"}

	doc := map[string]any{
		"id":       "test-1",
		"name":     "test",
		"secret":   "sensitive-secret",
		"password": "sensitive-password",
		"public":   "public-data",
	}

	// 加密
	err := encryptDocumentFields(doc, encryptedFields, password)
	if err != nil {
		t.Fatalf("failed to encrypt fields: %v", err)
	}

	// 验证加密字段已加密
	if doc["secret"] == "sensitive-secret" {
		t.Fatal("secret field should be encrypted")
	}
	if doc["password"] == "sensitive-password" {
		t.Fatal("password field should be encrypted")
	}
	// 验证非加密字段未改变
	if doc["public"] != "public-data" {
		t.Fatal("public field should not be encrypted")
	}

	// 解密
	err = decryptDocumentFields(doc, encryptedFields, password)
	if err != nil {
		t.Fatalf("failed to decrypt fields: %v", err)
	}

	// 验证解密后的值
	if doc["secret"] != "sensitive-secret" {
		t.Fatalf("secret field decryption failed: expected %s, got %v", "sensitive-secret", doc["secret"])
	}
	if doc["password"] != "sensitive-password" {
		t.Fatalf("password field decryption failed: expected %s, got %v", "sensitive-password", doc["password"])
	}
}

func TestCollectionWithEncryption(t *testing.T) {
	ctx := context.Background()

	// 创建数据库（带密码）
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name:     "test-encryption-db",
		Path:     "./test-encryption.db",
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer db.Destroy(ctx)

	// 创建集合（带加密字段）
	schema := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"secret", "password"},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":       "doc-1",
		"name":     "test",
		"secret":   "sensitive-secret",
		"password": "sensitive-password",
		"public":   "public-data",
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// 验证文档数据（应该已解密）
	if doc.GetString("secret") != "sensitive-secret" {
		t.Fatalf("secret field should be decrypted: got %v", doc.GetString("secret"))
	}
	if doc.GetString("password") != "sensitive-password" {
		t.Fatalf("password field should be decrypted: got %v", doc.GetString("password"))
	}

	// 重新查找文档
	found, err := collection.FindByID(ctx, "doc-1")
	if err != nil {
		t.Fatalf("failed to find document: %v", err)
	}
	if found == nil {
		t.Fatal("document not found")
	}

	// 验证解密后的值
	if found.GetString("secret") != "sensitive-secret" {
		t.Fatalf("secret field should be decrypted: got %v", found.GetString("secret"))
	}
	if found.GetString("password") != "sensitive-password" {
		t.Fatalf("password field should be decrypted: got %v", found.GetString("password"))
	}
	if found.GetString("public") != "public-data" {
		t.Fatalf("public field should be unchanged: got %v", found.GetString("public"))
	}
}

func TestEncryptionWithoutPassword(t *testing.T) {
	ctx := context.Background()

	// 创建数据库（不带密码）
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test-no-encryption-db",
		Path: "./test-no-encryption.db",
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer db.Destroy(ctx)

	// 创建集合（带加密字段配置，但没有密码）
	schema := Schema{
		PrimaryKey:      "id",
		RevField:        "_rev",
		EncryptedFields: []string{"secret"},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入文档（应该不加密，因为没有密码）
	doc, err := collection.Insert(ctx, map[string]any{
		"id":     "doc-1",
		"name":   "test",
		"secret": "sensitive-secret",
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// 验证文档数据（应该未加密，因为没有密码）
	if doc.GetString("secret") != "sensitive-secret" {
		t.Fatalf("secret field should not be encrypted (no password): got %v", doc.GetString("secret"))
	}
}
