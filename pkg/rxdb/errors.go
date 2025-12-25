package rxdb

import (
	"fmt"
)

// ErrorType 定义错误类型
type ErrorType string

const (
	ErrorTypeValidation    ErrorType = "validation"
	ErrorTypeNotFound      ErrorType = "not_found"
	ErrorTypeAlreadyExists ErrorType = "already_exists"
	ErrorTypeClosed        ErrorType = "closed"
	ErrorTypeIO            ErrorType = "io"
	ErrorTypeEncryption    ErrorType = "encryption"
	ErrorTypeIndex         ErrorType = "index"
	ErrorTypeQuery         ErrorType = "query"
	ErrorTypeSchema        ErrorType = "schema"
	ErrorTypeConflict      ErrorType = "conflict"
	ErrorTypeUnknown       ErrorType = "unknown"
)

// RxDBError 是 rxdb-go 的自定义错误类型
type RxDBError struct {
	Type    ErrorType
	Message string
	Context map[string]interface{}
	Err     error
}

// Error 实现 error 接口
func (e *RxDBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Type, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

// Unwrap 返回底层错误
func (e *RxDBError) Unwrap() error {
	return e.Err
}

// NewError 创建新的 RxDBError
func NewError(errType ErrorType, message string, err error) *RxDBError {
	return &RxDBError{
		Type:    errType,
		Message: message,
		Err:     err,
		Context: make(map[string]interface{}),
	}
}

// WithContext 添加上下文信息
func (e *RxDBError) WithContext(key string, value interface{}) *RxDBError {
	e.Context[key] = value
	return e
}

// IsValidationError 检查是否是验证错误
func IsValidationError(err error) bool {
	if e, ok := err.(*RxDBError); ok {
		return e.Type == ErrorTypeValidation
	}
	return false
}

// IsNotFoundError 检查是否是未找到错误
func IsNotFoundError(err error) bool {
	if e, ok := err.(*RxDBError); ok {
		return e.Type == ErrorTypeNotFound
	}
	return false
}

// IsAlreadyExistsError 检查是否是已存在错误
func IsAlreadyExistsError(err error) bool {
	if e, ok := err.(*RxDBError); ok {
		return e.Type == ErrorTypeAlreadyExists
	}
	return false
}

// IsClosedError 检查是否是已关闭错误
func IsClosedError(err error) bool {
	if e, ok := err.(*RxDBError); ok {
		return e.Type == ErrorTypeClosed
	}
	return false
}

// IsConflictError 检查是否是冲突错误（修订号不匹配）
func IsConflictError(err error) bool {
	if e, ok := err.(*RxDBError); ok {
		return e.Type == ErrorTypeConflict
	}
	return false
}
