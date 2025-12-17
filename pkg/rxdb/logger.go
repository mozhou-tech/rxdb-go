package rxdb

import (
	"io"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	globalLogger *logrus.Logger
	loggerOnce   sync.Once
)

// initGlobalLogger 初始化全局日志器（使用 logrus）
func initGlobalLogger() {
	loggerOnce.Do(func() {
		globalLogger = logrus.New()
		globalLogger.SetLevel(logrus.WarnLevel)
		globalLogger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
		globalLogger.SetOutput(os.Stderr)
	})
}

// GetLogger 获取全局 logrus 日志器
func GetLogger() *logrus.Logger {
	initGlobalLogger()
	return globalLogger
}

// SetLogger 设置全局日志器
func SetLogger(logger *logrus.Logger) {
	globalLogger = logger
}

// SetLogLevel 设置日志级别
func SetLogLevel(level logrus.Level) {
	initGlobalLogger()
	globalLogger.SetLevel(level)
}

// SetLogOutput 设置日志输出
func SetLogOutput(output io.Writer) {
	initGlobalLogger()
	globalLogger.SetOutput(output)
}

// SetLogFormatter 设置日志格式
func SetLogFormatter(formatter logrus.Formatter) {
	initGlobalLogger()
	globalLogger.SetFormatter(formatter)
}
