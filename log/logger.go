package log

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger interface with context-aware log levels
type Logger interface {
	Debugf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Warnf(message string, args ...interface{})
	Errorf(message string, args ...interface{})
	Fatalf(message string, args ...interface{})
}

// ZapLogger is a zap-based implementation of the Logger interface
type ZapLogger struct {
	logger *zap.SugaredLogger
}

var l Logger = nil

func InitLogger(logDir string) {
	l = NewDefaultLogger(logDir)
}

// SetLogger allows overriding the global logger instance
func SetLogger(customLogger Logger) {
	l = customLogger
}

// NewZapLogger creates a new ZapLogger instance with custom configuration
func NewDefaultLogger(logDir string) *ZapLogger {

	err := os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("couldn't create log directory:[%s], err: %v", logDir, err))
	}

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{
		logDir + "/app.log",
		"stdout",
	}
	config.ErrorOutputPaths = []string{
		logDir + "/app-error.log",
		"stderr",
	}

	// Example of log rollover settings (size-based, time-based not directly supported by zap, use external tools for time-based)
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	sugar := logger.Sugar()

	return &ZapLogger{logger: sugar}
}

func (l *ZapLogger) Infof(message string, args ...interface{}) {
	l.logger.Infof(message, args...)
}

func (l *ZapLogger) Warnf(message string, args ...interface{}) {
	l.logger.Warnf(message, args...)
}

func (l *ZapLogger) Errorf(message string, args ...interface{}) {
	l.logger.Errorf(message, args...)
}

func (l *ZapLogger) Debugf(message string, args ...interface{}) {
	l.logger.Debugf(message, args...)
}

func (l *ZapLogger) Fatalf(message string, args ...interface{}) {
	l.logger.Fatalf(message, args...)
}

func Debugf(message string, args ...interface{}) {
	if l != nil {
		l.Debugf(message, args...)
	}
}

func Infof(message string, args ...interface{}) {
	if l != nil {
		l.Infof(message, args...)
	}
}

func Warnf(message string, args ...interface{}) {
	if l != nil {
		l.Warnf(message, args...)
	}
}

func Errorf(message string, args ...interface{}) {
	if l != nil {
		l.Errorf(message, args...)
	}
}

func Fatalf(message string, args ...interface{}) {
	if l != nil {
		l.Fatalf(message, args...)
	}
}
