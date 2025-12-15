package rxdb

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// deriveKey 从密码派生加密密钥（使用 SHA-256）
func deriveKey(password string) []byte {
	hash := sha256.Sum256([]byte(password))
	return hash[:]
}

// encryptField 加密单个字段值（字符串）
func encryptField(value string, password string) (string, error) {
	if password == "" {
		return value, nil // 没有密码时不加密
	}

	key := deriveKey(password)

	// 创建 AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	// 使用 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	// 生成随机 nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// 加密数据
	ciphertext := gcm.Seal(nonce, nonce, []byte(value), nil)

	// 返回 base64 编码的密文
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptField 解密单个字段值（字符串）
func decryptField(encryptedValue string, password string) (string, error) {
	if password == "" {
		return encryptedValue, nil // 没有密码时不解密
	}

	key := deriveKey(password)

	// 解码 base64
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedValue)
	if err != nil {
		// 如果不是 base64 编码，可能是未加密的值，直接返回
		return encryptedValue, nil
	}

	// 创建 AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	// 使用 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	// 检查密文长度
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		// 密文太短，可能是未加密的值，直接返回
		return encryptedValue, nil
	}

	// 提取 nonce 和密文
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	// 解密数据
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		// 解密失败，可能是未加密的值，直接返回
		return encryptedValue, nil
	}

	return string(plaintext), nil
}

// encryptDocumentFields 加密文档中需要加密的字段
func encryptDocumentFields(doc map[string]any, encryptedFields []string, password string) error {
	if len(encryptedFields) == 0 || password == "" {
		return nil
	}

	for _, field := range encryptedFields {
		value, exists := doc[field]
		if !exists {
			continue
		}

		// 只加密字符串类型的值
		if strValue, ok := value.(string); ok {
			encrypted, err := encryptField(strValue, password)
			if err != nil {
				return fmt.Errorf("failed to encrypt field %s: %w", field, err)
			}
			doc[field] = encrypted
		}
	}

	return nil
}

// decryptDocumentFields 解密文档中需要解密的字段
func decryptDocumentFields(doc map[string]any, encryptedFields []string, password string) error {
	if len(encryptedFields) == 0 || password == "" {
		return nil
	}

	for _, field := range encryptedFields {
		value, exists := doc[field]
		if !exists {
			continue
		}

		// 只解密字符串类型的值
		if strValue, ok := value.(string); ok {
			decrypted, err := decryptField(strValue, password)
			if err != nil {
				// 解密失败时，保持原值（可能是未加密的值）
				continue
			}
			doc[field] = decrypted
		}
	}

	return nil
}

// isEncryptedField 检查字段是否需要加密
func isEncryptedField(field string, encryptedFields []string) bool {
	for _, ef := range encryptedFields {
		if ef == field {
			return true
		}
	}
	return false
}

// encryptNestedField 加密嵌套字段（支持 "field.subfield" 格式）
func encryptNestedField(doc map[string]any, fieldPath string, password string) error {
	if password == "" {
		return nil
	}

	parts := splitFieldPath(fieldPath)
	if len(parts) == 0 {
		return errors.New("empty field path")
	}

	// 获取嵌套值
	value := getNestedValueForEncryption(doc, parts)
	if value == nil {
		return nil // 字段不存在，跳过
	}

	// 只加密字符串类型
	if strValue, ok := value.(string); ok {
		encrypted, err := encryptField(strValue, password)
		if err != nil {
			return fmt.Errorf("failed to encrypt field %s: %w", fieldPath, err)
		}
		setNestedValue(doc, parts, encrypted)
	}

	return nil
}

// decryptNestedField 解密嵌套字段
func decryptNestedField(doc map[string]any, fieldPath string, password string) error {
	if password == "" {
		return nil
	}

	parts := splitFieldPath(fieldPath)
	if len(parts) == 0 {
		return errors.New("empty field path")
	}

	// 获取嵌套值
	value := getNestedValueForEncryption(doc, parts)
	if value == nil {
		return nil // 字段不存在，跳过
	}

	// 只解密字符串类型
	if strValue, ok := value.(string); ok {
		decrypted, err := decryptField(strValue, password)
		if err != nil {
			// 解密失败时，保持原值
			return nil
		}
		setNestedValue(doc, parts, decrypted)
	}

	return nil
}

// splitFieldPath 分割字段路径（支持 "field.subfield" 格式）
func splitFieldPath(path string) []string {
	parts := make([]string, 0)
	current := ""
	for _, char := range path {
		if char == '.' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// getNestedValueForEncryption 获取嵌套字段值（用于加密）
func getNestedValueForEncryption(doc map[string]any, parts []string) interface{} {
	current := doc
	for i, part := range parts {
		if i == len(parts)-1 {
			// 最后一个部分，返回值
			return current[part]
		}
		// 中间部分，继续深入
		if next, ok := current[part]; ok {
			if nextMap, ok := next.(map[string]any); ok {
				current = nextMap
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	return nil
}

// setNestedValue 设置嵌套字段值
func setNestedValue(doc map[string]any, parts []string, value interface{}) {
	current := doc
	for i, part := range parts {
		if i == len(parts)-1 {
			// 最后一个部分，设置值
			current[part] = value
			return
		}
		// 中间部分，确保路径存在
		if next, ok := current[part]; ok {
			if nextMap, ok := next.(map[string]any); ok {
				current = nextMap
			} else {
				// 路径中断，创建新的 map
				newMap := make(map[string]any)
				current[part] = newMap
				current = newMap
			}
		} else {
			// 路径不存在，创建新的 map
			newMap := make(map[string]any)
			current[part] = newMap
			current = newMap
		}
	}
}
