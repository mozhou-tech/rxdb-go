package rxdb

import (
	"fmt"
	"reflect"
	"strings"
)

// getPrimaryKeyFields 获取主键字段列表（支持单个和复合主键）。
func getPrimaryKeyFields(schema Schema) []string {
	switch pk := schema.PrimaryKey.(type) {
	case string:
		if pk != "" {
			return []string{pk}
		}
		return []string{}
	case []string:
		return pk
	case []interface{}:
		// 处理 JSON 解析后的数组
		fields := make([]string, 0, len(pk))
		for _, f := range pk {
			if str, ok := f.(string); ok {
				fields = append(fields, str)
			}
		}
		return fields
	default:
		return []string{}
	}
}

// ValidateDocument 根据 Schema 验证文档。
func ValidateDocument(schema Schema, doc map[string]any) error {
	if schema.JSON == nil {
		// 如果没有 JSON Schema，只验证主键存在
		fields := getPrimaryKeyFields(schema)
		for _, field := range fields {
			if _, ok := doc[field]; !ok {
				return fmt.Errorf("missing required field: %s", field)
			}
		}
		return nil
	}

	// 验证 required 字段
	if required, ok := schema.JSON["required"].([]any); ok {
		for _, req := range required {
			if field, ok := req.(string); ok {
				if _, exists := doc[field]; !exists {
					return fmt.Errorf("missing required field: %s", field)
				}
			}
		}
	}

	// 验证 properties
	if properties, ok := schema.JSON["properties"].(map[string]any); ok {
		for field, propDef := range properties {
			if propMap, ok := propDef.(map[string]any); ok {
				if err := validateField(doc[field], propMap, field); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// validateField 验证单个字段。
func validateField(value any, propDef map[string]any, fieldName string) error {
	// 如果值为 nil，检查是否允许 null
	if value == nil {
		if typeVal, ok := propDef["type"].(string); ok {
			if typeVal == "null" {
				return nil
			}
			// 检查是否允许 null（通过 type 数组）
			if types, ok := propDef["type"].([]any); ok {
				for _, t := range types {
					if tStr, ok := t.(string); ok && tStr == "null" {
						return nil
					}
				}
			}
		}
		// 如果字段不存在且不是 required，则跳过验证
		return nil
	}

	// 获取类型定义
	typeVal, ok := propDef["type"].(string)
	if !ok {
		// 可能是类型数组，检查第一个非 null 类型
		if types, ok := propDef["type"].([]any); ok {
			for _, t := range types {
				if tStr, ok := t.(string); ok && tStr != "null" {
					typeVal = tStr
					break
				}
			}
		}
	}

	if typeVal == "" {
		return nil // 没有类型定义，跳过验证
	}

	// 类型验证
	switch typeVal {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("field %s: expected string, got %T", fieldName, value)
		}
		strVal := value.(string)
		// 验证 maxLength
		if maxLen, ok := propDef["maxLength"].(float64); ok {
			if len(strVal) > int(maxLen) {
				return fmt.Errorf("field %s: string length %d exceeds maxLength %d", fieldName, len(strVal), int(maxLen))
			}
		}
		// 验证 minLength
		if minLen, ok := propDef["minLength"].(float64); ok {
			if len(strVal) < int(minLen) {
				return fmt.Errorf("field %s: string length %d is less than minLength %d", fieldName, len(strVal), int(minLen))
			}
		}
		// 验证 pattern (正则表达式)
		if pattern, ok := propDef["pattern"].(string); ok {
			// 简单的正则验证（可以使用 regexp 包）
			// 这里简化处理，实际应该使用完整的正则引擎
		}

	case "number", "integer":
		var numVal float64
		switch v := value.(type) {
		case int:
			numVal = float64(v)
		case int64:
			numVal = float64(v)
		case float64:
			numVal = v
		case float32:
			numVal = float64(v)
		default:
			return fmt.Errorf("field %s: expected number, got %T", fieldName, value)
		}
		// 验证 maximum
		if max, ok := propDef["maximum"].(float64); ok {
			if numVal > max {
				return fmt.Errorf("field %s: value %f exceeds maximum %f", fieldName, numVal, max)
			}
		}
		// 验证 minimum
		if min, ok := propDef["minimum"].(float64); ok {
			if numVal < min {
				return fmt.Errorf("field %s: value %f is less than minimum %f", fieldName, numVal, min)
			}
		}
		// integer 类型额外验证
		if typeVal == "integer" {
			if numVal != float64(int64(numVal)) {
				return fmt.Errorf("field %s: expected integer, got float", fieldName)
			}
		}

	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("field %s: expected boolean, got %T", fieldName, value)
		}

	case "array":
		if _, ok := value.([]any); !ok {
			// 尝试转换其他数组类型
			rv := reflect.ValueOf(value)
			if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
				return fmt.Errorf("field %s: expected array, got %T", fieldName, value)
			}
		}
		// 验证 items schema
		if items, ok := propDef["items"].(map[string]any); ok {
			arrVal := value.([]any)
			for i, item := range arrVal {
				if err := validateField(item, items, fmt.Sprintf("%s[%d]", fieldName, i)); err != nil {
					return err
				}
			}
		}
		// 验证 minItems
		if minItems, ok := propDef["minItems"].(float64); ok {
			arrVal := value.([]any)
			if len(arrVal) < int(minItems) {
				return fmt.Errorf("field %s: array length %d is less than minItems %d", fieldName, len(arrVal), int(minItems))
			}
		}
		// 验证 maxItems
		if maxItems, ok := propDef["maxItems"].(float64); ok {
			arrVal := value.([]any)
			if len(arrVal) > int(maxItems) {
				return fmt.Errorf("field %s: array length %d exceeds maxItems %d", fieldName, len(arrVal), int(maxItems))
			}
		}

	case "object":
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("field %s: expected object, got %T", fieldName, value)
		}
		// 验证 properties
		if properties, ok := propDef["properties"].(map[string]any); ok {
			objVal := value.(map[string]any)
			for propField, propDef := range properties {
				if propMap, ok := propDef.(map[string]any); ok {
					if err := validateField(objVal[propField], propMap, fmt.Sprintf("%s.%s", fieldName, propField)); err != nil {
						return err
					}
				}
			}
		}

	case "null":
		if value != nil {
			return fmt.Errorf("field %s: expected null, got %T", fieldName, value)
		}
	}

	return nil
}

// ValidateDocumentWithPath 验证文档并返回详细的错误路径。
func ValidateDocumentWithPath(schema Schema, doc map[string]any) []ValidationError {
	var errors []ValidationError

	if schema.JSON == nil {
		fields := getPrimaryKeyFields(schema)
		for _, field := range fields {
			if _, ok := doc[field]; !ok {
				errors = append(errors, ValidationError{
					Path:    field,
					Message: fmt.Sprintf("missing required field: %s", field),
				})
			}
		}
		return errors
	}

	// 验证 required 字段
	if required, ok := schema.JSON["required"].([]any); ok {
		for _, req := range required {
			if field, ok := req.(string); ok {
				if _, exists := doc[field]; !exists {
					errors = append(errors, ValidationError{
						Path:    field,
						Message: fmt.Sprintf("missing required field: %s", field),
					})
				}
			}
		}
	}

	// 验证 properties
	if properties, ok := schema.JSON["properties"].(map[string]any); ok {
		for field, propDef := range properties {
			if propMap, ok := propDef.(map[string]any); ok {
				fieldErrors := validateFieldWithPath(doc[field], propMap, field)
				errors = append(errors, fieldErrors...)
			}
		}
	}

	return errors
}

// ValidationError 表示验证错误。
type ValidationError struct {
	Path    string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Path, e.Message)
}

// validateFieldWithPath 验证字段并返回带路径的错误列表。
func validateFieldWithPath(value any, propDef map[string]any, fieldName string) []ValidationError {
	var errors []ValidationError

	if value == nil {
		if typeVal, ok := propDef["type"].(string); ok {
			if typeVal == "null" {
				return nil
			}
		}
		return nil
	}

	typeVal, ok := propDef["type"].(string)
	if !ok {
		if types, ok := propDef["type"].([]any); ok {
			for _, t := range types {
				if tStr, ok := t.(string); ok && tStr != "null" {
					typeVal = tStr
					break
				}
			}
		}
	}

	if typeVal == "" {
		return nil
	}

	switch typeVal {
	case "string":
		if _, ok := value.(string); !ok {
			errors = append(errors, ValidationError{
				Path:    fieldName,
				Message: fmt.Sprintf("expected string, got %T", value),
			})
			return errors
		}
		strVal := value.(string)
		if maxLen, ok := propDef["maxLength"].(float64); ok {
			if len(strVal) > int(maxLen) {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("string length %d exceeds maxLength %d", len(strVal), int(maxLen)),
				})
			}
		}
		if minLen, ok := propDef["minLength"].(float64); ok {
			if len(strVal) < int(minLen) {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("string length %d is less than minLength %d", len(strVal), int(minLen)),
				})
			}
		}

	case "number", "integer":
		var numVal float64
		switch v := value.(type) {
		case int:
			numVal = float64(v)
		case int64:
			numVal = float64(v)
		case float64:
			numVal = v
		case float32:
			numVal = float64(v)
		default:
			errors = append(errors, ValidationError{
				Path:    fieldName,
				Message: fmt.Sprintf("expected number, got %T", value),
			})
			return errors
		}
		if max, ok := propDef["maximum"].(float64); ok {
			if numVal > max {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("value %f exceeds maximum %f", numVal, max),
				})
			}
		}
		if min, ok := propDef["minimum"].(float64); ok {
			if numVal < min {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("value %f is less than minimum %f", numVal, min),
				})
			}
		}
		if typeVal == "integer" {
			if numVal != float64(int64(numVal)) {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: "expected integer, got float",
				})
			}
		}

	case "boolean":
		if _, ok := value.(bool); !ok {
			errors = append(errors, ValidationError{
				Path:    fieldName,
				Message: fmt.Sprintf("expected boolean, got %T", value),
			})
		}

	case "array":
		arrVal, ok := value.([]any)
		if !ok {
			rv := reflect.ValueOf(value)
			if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("expected array, got %T", value),
				})
				return errors
			}
			// 转换其他数组类型
			arrVal = make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				arrVal[i] = rv.Index(i).Interface()
			}
		}
		if items, ok := propDef["items"].(map[string]any); ok {
			for i, item := range arrVal {
				itemErrors := validateFieldWithPath(item, items, fmt.Sprintf("%s[%d]", fieldName, i))
				errors = append(errors, itemErrors...)
			}
		}
		if minItems, ok := propDef["minItems"].(float64); ok {
			if len(arrVal) < int(minItems) {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("array length %d is less than minItems %d", len(arrVal), int(minItems)),
				})
			}
		}
		if maxItems, ok := propDef["maxItems"].(float64); ok {
			if len(arrVal) > int(maxItems) {
				errors = append(errors, ValidationError{
					Path:    fieldName,
					Message: fmt.Sprintf("array length %d exceeds maxItems %d", len(arrVal), int(maxItems)),
				})
			}
		}

	case "object":
		objVal, ok := value.(map[string]any)
		if !ok {
			errors = append(errors, ValidationError{
				Path:    fieldName,
				Message: fmt.Sprintf("expected object, got %T", value),
			})
			return errors
		}
		if properties, ok := propDef["properties"].(map[string]any); ok {
			for propField, propDef := range properties {
				if propMap, ok := propDef.(map[string]any); ok {
					propPath := strings.Join([]string{fieldName, propField}, ".")
					propErrors := validateFieldWithPath(objVal[propField], propMap, propPath)
					errors = append(errors, propErrors...)
				}
			}
		}
	}

	return errors
}

// ApplyDefaults 根据 Schema 应用字段默认值。
func ApplyDefaults(schema Schema, doc map[string]any) {
	if schema.JSON == nil {
		return
	}

	properties, ok := schema.JSON["properties"].(map[string]any)
	if !ok {
		return
	}

	for field, propDef := range properties {
		propMap, ok := propDef.(map[string]any)
		if !ok {
			continue
		}

		// 如果字段不存在，应用默认值
		if _, exists := doc[field]; !exists {
			if defaultValue, hasDefault := propMap["default"]; hasDefault {
				doc[field] = defaultValue
			}
		}
	}
}

// ValidateFinalFields 验证不可变字段（final fields）是否被修改。
func ValidateFinalFields(schema Schema, oldDoc map[string]any, newDoc map[string]any) error {
	if schema.JSON == nil || oldDoc == nil {
		return nil
	}

	properties, ok := schema.JSON["properties"].(map[string]any)
	if !ok {
		return nil
	}

	for field, propDef := range properties {
		propMap, ok := propDef.(map[string]any)
		if !ok {
			continue
		}

		// 检查字段是否标记为 final
		if final, isFinal := propMap["final"].(bool); isFinal && final {
			oldVal, oldExists := oldDoc[field]
			newVal, newExists := newDoc[field]

			// 如果旧文档中存在该字段，新文档中不能修改
			if oldExists {
				if !newExists || oldVal != newVal {
					return fmt.Errorf("field %s is final and cannot be modified", field)
				}
			}
		}
	}

	return nil
}

