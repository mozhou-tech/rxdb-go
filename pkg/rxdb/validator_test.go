package rxdb

import (
	"strings"
	"testing"
)

func TestValidator_RequiredFields(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"required": []any{"id", "name"},
			"properties": map[string]any{
				"id":   map[string]any{"type": "string"},
				"name": map[string]any{"type": "string"},
			},
		},
	}

	// 测试缺少必需字段
	doc := map[string]any{
		"id": "doc1",
		// 缺少 "name"
	}

	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when required field is missing")
	}

	// 测试所有必需字段存在
	doc = map[string]any{
		"id":   "doc1",
		"name": "Test",
	}

	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when all required fields are present: %v", err)
	}
}

func TestValidator_TypeValidation(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id":      map[string]any{"type": "string"},
				"age":     map[string]any{"type": "integer"},
				"price":   map[string]any{"type": "number"},
				"active":  map[string]any{"type": "boolean"},
				"tags":    map[string]any{"type": "array"},
				"address": map[string]any{"type": "object"},
			},
		},
	}

	// 测试字符串类型
	doc := map[string]any{
		"id":  "doc1",
		"age": "not a number", // 错误类型
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation for wrong type")
	}

	// 测试正确的类型
	doc = map[string]any{
		"id":     "doc1",
		"age":    30,
		"price":  99.99,
		"active": true,
		"tags":   []any{"tag1", "tag2"},
		"address": map[string]any{
			"street": "123 Main St",
		},
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for correct types: %v", err)
	}
}

func TestValidator_StringMaxLength(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"name": map[string]any{
					"type":      "string",
					"maxLength": float64(10),
				},
			},
		},
	}

	// 测试超过最大长度
	doc := map[string]any{
		"id":   "doc1",
		"name": "This is too long", // 超过 10 个字符
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when string exceeds maxLength")
	}

	// 测试等于最大长度
	doc = map[string]any{
		"id":   "doc1",
		"name": "Exactly10", // 正好 10 个字符
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when string equals maxLength: %v", err)
	}

	// 测试小于最大长度
	doc = map[string]any{
		"id":   "doc1",
		"name": "Short", // 小于 10 个字符
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when string is less than maxLength: %v", err)
	}
}

func TestValidator_StringMinLength(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"name": map[string]any{
					"type":      "string",
					"minLength": float64(5),
				},
			},
		},
	}

	// 测试小于最小长度
	doc := map[string]any{
		"id":   "doc1",
		"name": "Hi", // 小于 5 个字符
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when string is less than minLength")
	}

	// 测试等于最小长度
	doc = map[string]any{
		"id":   "doc1",
		"name": "Hello", // 正好 5 个字符
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when string equals minLength: %v", err)
	}
}

func TestValidator_StringPattern(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"email": map[string]any{
					"type":    "string",
					"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
				},
			},
		},
	}

	// 测试不匹配模式
	doc := map[string]any{
		"id":    "doc1",
		"email": "invalid-email", // 不匹配邮箱模式
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when string does not match pattern")
	}

	// 测试匹配模式
	doc = map[string]any{
		"id":    "doc1",
		"email": "test@example.com", // 匹配邮箱模式
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when string matches pattern: %v", err)
	}
}

func TestValidator_NumberMaximum(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"age": map[string]any{
					"type":    "integer",
					"maximum": float64(100),
				},
			},
		},
	}

	// 测试超过最大值
	doc := map[string]any{
		"id":  "doc1",
		"age": 150, // 超过 100
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when number exceeds maximum")
	}

	// 测试等于最大值
	doc = map[string]any{
		"id":  "doc1",
		"age": 100, // 等于 100
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when number equals maximum: %v", err)
	}
}

func TestValidator_NumberMinimum(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"age": map[string]any{
					"type":    "integer",
					"minimum": float64(18),
				},
			},
		},
	}

	// 测试小于最小值
	doc := map[string]any{
		"id":  "doc1",
		"age": 15, // 小于 18
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when number is less than minimum")
	}

	// 测试等于最小值
	doc = map[string]any{
		"id":  "doc1",
		"age": 18, // 等于 18
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when number equals minimum: %v", err)
	}
}

func TestValidator_IntegerType(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id":  map[string]any{"type": "string"},
				"age": map[string]any{"type": "integer"},
			},
		},
	}

	// 测试浮点数（应该失败）
	doc := map[string]any{
		"id":  "doc1",
		"age": 30.5, // 浮点数
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when float is provided for integer type")
	}

	// 测试整数（应该成功）
	doc = map[string]any{
		"id":  "doc1",
		"age": 30, // 整数
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for integer type: %v", err)
	}
}

func TestValidator_ArrayMinItems(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"tags": map[string]any{
					"type":     "array",
					"minItems": float64(2),
				},
			},
		},
	}

	// 测试少于最小元素数
	doc := map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1"}, // 只有 1 个元素
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when array has fewer items than minItems")
	}

	// 测试等于最小元素数
	doc = map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2"}, // 正好 2 个元素
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when array has minItems: %v", err)
	}
}

func TestValidator_ArrayMaxItems(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"tags": map[string]any{
					"type":     "array",
					"maxItems": float64(3),
				},
			},
		},
	}

	// 测试超过最大元素数
	doc := map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2", "tag3", "tag4"}, // 4 个元素
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when array has more items than maxItems")
	}

	// 测试等于最大元素数
	doc = map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2", "tag3"}, // 正好 3 个元素
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when array has maxItems: %v", err)
	}
}

func TestValidator_ApplyDefaults(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"name": map[string]any{
					"type":    "string",
					"default": "Unknown",
				},
				"age": map[string]any{
					"type":    "integer",
					"default": float64(0),
				},
			},
		},
	}

	doc := map[string]any{
		"id": "doc1",
		// name 和 age 缺失
	}

	ApplyDefaults(schema, doc)

	if doc["name"] != "Unknown" {
		t.Errorf("Expected default name 'Unknown', got '%v'", doc["name"])
	}

	if doc["age"] != float64(0) {
		t.Errorf("Expected default age 0, got '%v'", doc["age"])
	}

	// 验证已有字段不被覆盖
	doc = map[string]any{
		"id":   "doc1",
		"name": "Custom",
	}

	ApplyDefaults(schema, doc)
	if doc["name"] != "Custom" {
		t.Error("Existing field should not be overwritten by default")
	}
}

func TestValidator_ValidateDocumentWithPath(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"required": []any{"id", "name"},
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"name": map[string]any{
					"type":      "string",
					"minLength": float64(5),
				},
				"age": map[string]any{
					"type":    "integer",
					"maximum": float64(100),
				},
			},
		},
	}

	// 测试多个验证错误
	doc := map[string]any{
		"id":   "doc1",
		"name": "Hi", // 太短
		"age":  150,  // 超过最大值
	}

	errors := ValidateDocumentWithPath(schema, doc)
	if len(errors) == 0 {
		t.Error("Should return validation errors")
	}

	// 验证错误路径
	foundNameError := false
	foundAgeError := false
	for _, err := range errors {
		if err.Path == "name" {
			foundNameError = true
		}
		if err.Path == "age" {
			foundAgeError = true
		}
	}

	if !foundNameError {
		t.Error("Should have error for 'name' field")
	}
	if !foundAgeError {
		t.Error("Should have error for 'age' field")
	}
}

func TestValidator_CompositePrimaryKey(t *testing.T) {
	schema := Schema{
		PrimaryKey: []string{"id1", "id2"},
		RevField:   "_rev",
	}

	// 测试缺少主键字段
	doc := map[string]any{
		"id1": "value1",
		// 缺少 id2
	}

	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when composite primary key field is missing")
	}

	// 测试所有主键字段存在
	doc = map[string]any{
		"id1": "value1",
		"id2": "value2",
	}

	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation when all composite primary key fields are present: %v", err)
	}
}

func TestValidator_ArrayItems(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"tags": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "string",
					},
				},
			},
		},
	}

	// 测试数组元素类型验证
	doc := map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2", 123}, // 包含非字符串元素
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when array contains wrong type")
	}

	// 测试正确的数组元素类型
	doc = map[string]any{
		"id":   "doc1",
		"tags": []any{"tag1", "tag2", "tag3"},
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for correct array item types: %v", err)
	}

	// 测试嵌套数组验证
	schema = Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"matrix": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "integer",
						},
					},
				},
			},
		},
	}

	doc = map[string]any{
		"id": "doc1",
		"matrix": []any{
			[]any{1, 2, 3},
			[]any{4, 5, 6},
		},
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for nested arrays: %v", err)
	}
}

func TestValidator_ObjectProperties(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"address": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"street": map[string]any{"type": "string"},
						"city":   map[string]any{"type": "string"},
						"zip":    map[string]any{"type": "integer"},
					},
					"required": []any{"street", "city"},
				},
			},
		},
	}

	// 测试对象属性类型验证
	doc := map[string]any{
		"id": "doc1",
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "New York",
			"zip":    "12345", // 错误类型（应该是整数）
		},
	}
	err := ValidateDocument(schema, doc)
	if err == nil {
		t.Error("Should fail validation when object property has wrong type")
	}

	// 测试正确的对象属性
	doc = map[string]any{
		"id": "doc1",
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "New York",
			"zip":    12345,
		},
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for correct object properties: %v", err)
	}

	// 测试嵌套对象验证
	schema = Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"user": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"name": map[string]any{"type": "string"},
						"contact": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"email": map[string]any{"type": "string"},
								"phone": map[string]any{"type": "string"},
							},
						},
					},
				},
			},
		},
	}

	doc = map[string]any{
		"id": "doc1",
		"user": map[string]any{
			"name": "John",
			"contact": map[string]any{
				"email": "john@example.com",
				"phone": "123-456-7890",
			},
		},
	}
	err = ValidateDocument(schema, doc)
	if err != nil {
		t.Errorf("Should pass validation for nested objects: %v", err)
	}
}

func TestValidator_FinalFields(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "string"},
				"name": map[string]any{
					"type":  "string",
					"final": true,
				},
				"age": map[string]any{
					"type": "integer",
				},
			},
		},
	}

	// 创建初始文档
	oldDoc := map[string]any{
		"id":   "doc1",
		"name": "Original Name",
		"age":  25,
	}

	// 测试修改 final 字段（应该失败）
	newDoc := map[string]any{
		"id":   "doc1",
		"name": "Modified Name", // 尝试修改 final 字段
		"age":  30,
	}

	err := ValidateFinalFields(schema, oldDoc, newDoc)
	if err == nil {
		t.Error("Should fail validation when final field is modified")
	}

	if err != nil && !strings.Contains(err.Error(), "final") {
		t.Errorf("Error message should mention 'final': %v", err)
	}

	// 测试不修改 final 字段（应该成功）
	newDoc = map[string]any{
		"id":   "doc1",
		"name": "Original Name", // 保持不变
		"age":  30,              // 修改非 final 字段
	}

	err = ValidateFinalFields(schema, oldDoc, newDoc)
	if err != nil {
		t.Errorf("Should pass validation when final field is not modified: %v", err)
	}

	// 测试新文档（没有旧文档，应该允许设置 final 字段）
	err = ValidateFinalFields(schema, nil, newDoc)
	if err != nil {
		t.Errorf("Should allow setting final field for new document: %v", err)
	}
}

func TestValidator_ValidationError(t *testing.T) {
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"required": []any{"id", "name"},
			"properties": map[string]any{
				"id": map[string]any{
					"type":      "string",
					"minLength": float64(3),
				},
				"name": map[string]any{
					"type":      "string",
					"maxLength": float64(10),
				},
				"age": map[string]any{
					"type":    "integer",
					"minimum": float64(18),
					"maximum": float64(100),
				},
			},
		},
	}

	// 测试多个验证错误
	doc := map[string]any{
		"id":   "ab",               // 太短
		"name": "This is too long", // 太长
		"age":  150,                // 超过最大值
	}

	errors := ValidateDocumentWithPath(schema, doc)
	if len(errors) == 0 {
		t.Error("Should return validation errors")
	}

	// 验证错误类型和消息
	foundIDError := false
	foundNameError := false
	foundAgeError := false

	for _, err := range errors {
		if err.Path == "id" {
			foundIDError = true
			if err.Message == "" {
				t.Error("Error message should not be empty")
			}
		}
		if err.Path == "name" {
			foundNameError = true
		}
		if err.Path == "age" {
			foundAgeError = true
		}

		// 验证错误实现了 Error() 方法
		errStr := err.Error()
		if errStr == "" {
			t.Error("Error() method should return non-empty string")
		}
	}

	if !foundIDError {
		t.Error("Should have error for 'id' field")
	}
	if !foundNameError {
		t.Error("Should have error for 'name' field")
	}
	if !foundAgeError {
		t.Error("Should have error for 'age' field")
	}

	// 测试单个验证错误
	doc = map[string]any{
		"id":   "doc1",
		"name": "Valid Name",
		"age":  15, // 小于最小值
	}

	errors = ValidateDocumentWithPath(schema, doc)
	if len(errors) == 0 {
		t.Error("Should return validation error for age")
	}

	if len(errors) > 0 && errors[0].Path != "age" {
		t.Errorf("Expected error path 'age', got '%s'", errors[0].Path)
	}
}
