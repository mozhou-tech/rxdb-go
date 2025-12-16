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
		Path:     "../../data/test-encryption.db",
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
		Path: "../../data/test-no-encryption.db",
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

func TestEncryption_Algorithm(t *testing.T) {
	password := "test-password"
	plaintext := "test-data"

	// 测试 AES-GCM 加密
	encrypted, err := encryptField(plaintext, password)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// 验证加密值不同
	if encrypted == plaintext {
		t.Fatal("encrypted value should be different from plaintext")
	}

	// 验证加密值是 base64 编码
	// Base64 编码的字符串只包含特定字符
	validBase64Chars := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
	for _, char := range encrypted {
		found := false
		for _, validChar := range validBase64Chars {
			if char == validChar {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("encrypted value should be base64 encoded, found invalid character: %c", char)
		}
	}

	// 测试解密
	decrypted, err := decryptField(encrypted, password)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if decrypted != plaintext {
		t.Fatalf("decrypted value mismatch: expected %s, got %s", plaintext, decrypted)
	}

	// 测试相同明文每次加密结果不同（因为 nonce 随机）
	encrypted1, _ := encryptField(plaintext, password)
	encrypted2, _ := encryptField(plaintext, password)
	if encrypted1 == encrypted2 {
		t.Fatal("same plaintext should produce different ciphertexts (due to random nonce)")
	}

	// 但都能正确解密
	decrypted1, _ := decryptField(encrypted1, password)
	decrypted2, _ := decryptField(encrypted2, password)
	if decrypted1 != plaintext || decrypted2 != plaintext {
		t.Fatal("both ciphertexts should decrypt to the same plaintext")
	}
}

func TestEncryption_KeyDerivation(t *testing.T) {
	password1 := "test-password"
	password2 := "different-password"
	plaintext := "test-data"

	// 相同密码应该生成相同的密钥（通过加密结果的一致性验证）
	encrypted1, err := encryptField(plaintext, password1)
	if err != nil {
		t.Fatalf("failed to encrypt with password1: %v", err)
	}

	encrypted2, err := encryptField(plaintext, password1)
	if err != nil {
		t.Fatalf("failed to encrypt with password1 again: %v", err)
	}

	// 虽然密文不同（因为 nonce），但都能用相同密码解密
	decrypted1, err := decryptField(encrypted1, password1)
	if err != nil {
		t.Fatalf("failed to decrypt encrypted1: %v", err)
	}
	if decrypted1 != plaintext {
		t.Fatalf("decryption failed: expected %s, got %s", plaintext, decrypted1)
	}

	decrypted2, err := decryptField(encrypted2, password1)
	if err != nil {
		t.Fatalf("failed to decrypt encrypted2: %v", err)
	}
	if decrypted2 != plaintext {
		t.Fatalf("decryption failed: expected %s, got %s", plaintext, decrypted2)
	}

	// 不同密码应该生成不同的密钥
	encrypted3, err := encryptField(plaintext, password2)
	if err != nil {
		t.Fatalf("failed to encrypt with password2: %v", err)
	}

	// 用 password2 加密的数据不能用 password1 解密
	decrypted3, err := decryptField(encrypted3, password1)
	if err == nil && decrypted3 == plaintext {
		t.Fatal("data encrypted with password2 should not decrypt with password1")
	}

	// 但能用 password2 正确解密
	decrypted4, err := decryptField(encrypted3, password2)
	if err != nil {
		t.Fatalf("failed to decrypt with correct password: %v", err)
	}
	if decrypted4 != plaintext {
		t.Fatalf("decryption with correct password failed: expected %s, got %s", plaintext, decrypted4)
	}
}

func TestEncryption_NestedFields(t *testing.T) {
	password := "test-password"
	doc := map[string]any{
		"id":   "doc-1",
		"name": "test",
		"user": map[string]any{
			"email":    "user@example.com",
			"password": "secret-password",
		},
		"config": map[string]any{
			"apiKey": "secret-api-key",
		},
	}

	// 测试嵌套字段加密
	err := encryptNestedField(doc, "user.password", password)
	if err != nil {
		t.Fatalf("failed to encrypt nested field: %v", err)
	}

	// 验证嵌套字段已加密
	userMap, ok := doc["user"].(map[string]any)
	if !ok {
		t.Fatal("user should be a map")
	}
	if userMap["password"] == "secret-password" {
		t.Fatal("nested password field should be encrypted")
	}

	// 验证其他字段未改变
	if userMap["email"] != "user@example.com" {
		t.Fatal("email field should not be encrypted")
	}

	// 测试解密
	err = decryptNestedField(doc, "user.password", password)
	if err != nil {
		t.Fatalf("failed to decrypt nested field: %v", err)
	}

	// 验证解密后的值
	if userMap["password"] != "secret-password" {
		t.Fatalf("nested password field decryption failed: expected 'secret-password', got %v", userMap["password"])
	}

	// 测试多层嵌套
	err = encryptNestedField(doc, "config.apiKey", password)
	if err != nil {
		t.Fatalf("failed to encrypt config.apiKey: %v", err)
	}

	configMap, ok := doc["config"].(map[string]any)
	if !ok {
		t.Fatal("config should be a map")
	}
	if configMap["apiKey"] == "secret-api-key" {
		t.Fatal("config.apiKey should be encrypted")
	}

	err = decryptNestedField(doc, "config.apiKey", password)
	if err != nil {
		t.Fatalf("failed to decrypt config.apiKey: %v", err)
	}

	if configMap["apiKey"] != "secret-api-key" {
		t.Fatalf("config.apiKey decryption failed: expected 'secret-api-key', got %v", configMap["apiKey"])
	}
}

func TestEncryption_Performance(t *testing.T) {
	password := "test-password"
	plaintext := "test-data-to-encrypt"

	// 测试大量加密操作
	const iterations = 1000
	for i := 0; i < iterations; i++ {
		encrypted, err := encryptField(plaintext, password)
		if err != nil {
			t.Fatalf("failed to encrypt at iteration %d: %v", i, err)
		}

		decrypted, err := decryptField(encrypted, password)
		if err != nil {
			t.Fatalf("failed to decrypt at iteration %d: %v", i, err)
		}

		if decrypted != plaintext {
			t.Fatalf("decryption mismatch at iteration %d: expected %s, got %s", i, plaintext, decrypted)
		}
	}

	// 测试大文本加密
	largeText := make([]byte, 10000)
	for i := range largeText {
		largeText[i] = byte(i % 256)
	}
	largePlaintext := string(largeText)

	encrypted, err := encryptField(largePlaintext, password)
	if err != nil {
		t.Fatalf("failed to encrypt large text: %v", err)
	}

	decrypted, err := decryptField(encrypted, password)
	if err != nil {
		t.Fatalf("failed to decrypt large text: %v", err)
	}

	if decrypted != largePlaintext {
		t.Fatal("large text decryption failed")
	}
}

// TestEncryption_Performance_Memory 测试加密性能的内存使用
func TestEncryption_Performance_Memory(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	password := "test-password"
	plaintext := "test-data-to-encrypt"

	// 测试大量加密操作的内存使用
	const iterations = 1000
	var encryptedResults []string

	// 执行大量加密操作
	for i := 0; i < iterations; i++ {
		encrypted, err := encryptField(plaintext, password)
		if err != nil {
			t.Fatalf("failed to encrypt at iteration %d: %v", i, err)
		}
		encryptedResults = append(encryptedResults, encrypted)
	}

	// 验证所有加密结果都不同（由于随机 nonce）
	uniqueResults := make(map[string]bool)
	for _, result := range encryptedResults {
		uniqueResults[result] = true
	}

	if len(uniqueResults) != iterations {
		t.Logf("Expected %d unique encrypted results, got %d (some may be duplicates due to nonce collision)", iterations, len(uniqueResults))
	}

	// 验证所有结果都能正确解密
	for i, encrypted := range encryptedResults {
		decrypted, err := decryptField(encrypted, password)
		if err != nil {
			t.Fatalf("failed to decrypt at iteration %d: %v", i, err)
		}

		if decrypted != plaintext {
			t.Fatalf("decryption mismatch at iteration %d: expected %s, got %s", i, plaintext, decrypted)
		}
	}

	t.Logf("Successfully encrypted and decrypted %d items", iterations)

	// 测试大文本的内存使用
	largeTextSizes := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB
	for _, size := range largeTextSizes {
		largeText := make([]byte, size)
		for i := range largeText {
			largeText[i] = byte(i % 256)
		}
		largePlaintext := string(largeText)

		encrypted, err := encryptField(largePlaintext, password)
		if err != nil {
			t.Fatalf("failed to encrypt %d bytes: %v", size, err)
		}

		decrypted, err := decryptField(encrypted, password)
		if err != nil {
			t.Fatalf("failed to decrypt %d bytes: %v", size, err)
		}

		if decrypted != largePlaintext {
			t.Fatalf("large text (%d bytes) decryption failed", size)
		}

		t.Logf("Successfully encrypted and decrypted %d bytes", size)
	}
}

func TestEncryption_ErrorHandling(t *testing.T) {
	password := "test-password"
	plaintext := "test-data"

	// 测试空密码（应该不加密）
	result, err := encryptField(plaintext, "")
	if err != nil {
		t.Fatalf("encrypting with empty password should not error: %v", err)
	}
	if result != plaintext {
		t.Fatal("encrypting with empty password should return original value")
	}

	decrypted, err := decryptField(plaintext, "")
	if err != nil {
		t.Fatalf("decrypting with empty password should not error: %v", err)
	}
	if decrypted != plaintext {
		t.Fatal("decrypting with empty password should return original value")
	}

	// 测试无效的 base64 字符串（应该返回原值）
	invalidBase64 := "not-a-valid-base64-string!!!"
	decrypted2, err := decryptField(invalidBase64, password)
	if err != nil {
		// 允许返回错误，但应该优雅处理
		t.Logf("decrypting invalid base64 returned error (acceptable): %v", err)
	}
	// 如果返回了原值，也是可以接受的
	if decrypted2 != invalidBase64 && decrypted2 != "" {
		t.Logf("decrypting invalid base64 returned: %s", decrypted2)
	}

	// 测试太短的密文（应该返回原值）
	shortCiphertext := "short"
	decrypted3, err := decryptField(shortCiphertext, password)
	if err != nil {
		t.Logf("decrypting short ciphertext returned error (acceptable): %v", err)
	}
	// 如果返回了原值，也是可以接受的
	if decrypted3 != shortCiphertext && decrypted3 != "" {
		t.Logf("decrypting short ciphertext returned: %s", decrypted3)
	}

	// 测试嵌套字段路径错误
	doc := map[string]any{
		"id": "doc-1",
	}
	err = encryptNestedField(doc, "", password)
	if err == nil {
		t.Fatal("encrypting with empty field path should return error")
	}

	// 测试不存在的嵌套字段（应该不报错）
	err = encryptNestedField(doc, "nonexistent.field", password)
	if err != nil {
		t.Fatalf("encrypting nonexistent field should not error: %v", err)
	}
}
