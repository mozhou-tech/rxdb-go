package rxdb

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

// LogLevel 定义日志级别
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelNone // 禁用日志
)

// Logger 定义日志接口
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	SetLevel(level LogLevel)
	GetLevel() LogLevel
}

// defaultLogger 默认日志实现
type defaultLogger struct {
	level  LogLevel
	logger *log.Logger
	mu     sync.RWMutex
}

var (
	globalLogger Logger
	loggerOnce   sync.Once
)

// initGlobalLogger 初始化全局日志器
func initGlobalLogger() {
	loggerOnce.Do(func() {
		globalLogger = NewLogger(LogLevelWarn, os.Stderr)
	})
}

// GetLogger 获取全局日志器
func GetLogger() Logger {
	initGlobalLogger()
	return globalLogger
}

// SetLogger 设置全局日志器
func SetLogger(logger Logger) {
	globalLogger = logger
}

// NewLogger 创建新的日志器
func NewLogger(level LogLevel, output io.Writer) Logger {
	return &defaultLogger{
		level:  level,
		logger: log.New(output, "[rxdb] ", log.LstdFlags|log.Lshortfile),
	}
}

// SetLevel 设置日志级别
func (l *defaultLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel 获取日志级别
func (l *defaultLogger) GetLevel() LogLevel {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

// Debug 记录调试日志
func (l *defaultLogger) Debug(format string, args ...interface{}) {
	l.mu.RLock()
	level := l.level
	logger := l.logger
	l.mu.RUnlock()

	if level <= LogLevelDebug {
		logger.Output(2, fmt.Sprintf("[DEBUG] "+format, args...))
	}
}

// Info 记录信息日志
func (l *defaultLogger) Info(format string, args ...interface{}) {
	l.mu.RLock()
	level := l.level
	logger := l.logger
	l.mu.RUnlock()

	if level <= LogLevelInfo {
		logger.Output(2, fmt.Sprintf("[INFO] "+format, args...))
	}
}

// Warn 记录警告日志
func (l *defaultLogger) Warn(format string, args ...interface{}) {
	l.mu.RLock()
	level := l.level
	logger := l.logger
	l.mu.RUnlock()

	if level <= LogLevelWarn {
		logger.Output(2, fmt.Sprintf("[WARN] "+format, args...))
	}
}

// Error 记录错误日志
func (l *defaultLogger) Error(format string, args ...interface{}) {
	l.mu.RLock()
	level := l.level
	logger := l.logger
	l.mu.RUnlock()

	if level <= LogLevelError {
		logger.Output(2, fmt.Sprintf("[ERROR] "+format, args...))
	}
}

// NoOpLogger 空操作日志器（用于禁用日志）
type NoOpLogger struct{}

func (n *NoOpLogger) Debug(format string, args ...interface{}) {}
func (n *NoOpLogger) Info(format string, args ...interface{})  {}
func (n *NoOpLogger) Warn(format string, args ...interface{})  {}
func (n *NoOpLogger) Error(format string, args ...interface{}) {}
func (n *NoOpLogger) SetLevel(level LogLevel)                  {}
func (n *NoOpLogger) GetLevel() LogLevel                       { return LogLevelNone }
