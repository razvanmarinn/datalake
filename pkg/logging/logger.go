package logging

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger
	sugar       *zap.SugaredLogger
	serviceName string
	version     string
}

type Config struct {
	Level       string
	Format      string // "json" or "console"
	ServiceName string
	Version     string
	Environment string
}

// LogLevel constants for consistent usage
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
	LevelFatal = "fatal"
	LevelPanic = "panic"
)

// Format constants
const (
	FormatJSON    = "json"
	FormatConsole = "console"
)

// Field name constants for Loki compatibility
const (
	FieldService   = "service"
	FieldVersion   = "version"
	FieldEnv       = "environment"
	FieldTraceID   = "trace_id"
	FieldSpanID    = "span_id"
	FieldUserID    = "user_id"
	FieldRequestID = "request_id"
	FieldMethod    = "method"
	FieldPath      = "path"
	FieldStatus    = "status"
	FieldDuration  = "duration_ms"
	FieldError     = "error"
	FieldDatabase  = "database"
	FieldOperation = "operation"
	FieldKafka     = "kafka_topic"
	FieldProject   = "project"
)

// NewLogger creates a new structured logger instance
func NewLogger(config Config) *Logger {
	// Configure log level
	var level zapcore.Level
	switch strings.ToLower(config.Level) {
	case LevelDebug:
		level = zapcore.DebugLevel
	case LevelInfo:
		level = zapcore.InfoLevel
	case LevelWarn:
		level = zapcore.WarnLevel
	case LevelError:
		level = zapcore.ErrorLevel
	case LevelFatal:
		level = zapcore.FatalLevel
	case LevelPanic:
		level = zapcore.PanicLevel
	default:
		level = zapcore.InfoLevel
	}

	// Configure encoder
	var encoderConfig zapcore.EncoderConfig
	var encoder zapcore.Encoder

	if strings.ToLower(config.Format) == FormatJSON {
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.LevelKey = "level"
		encoderConfig.MessageKey = "message"
		encoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
		encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Create core
	core := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)

	// Create logger with caller info
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Add service metadata to all logs
	logger = logger.With(
		zap.String(FieldService, config.ServiceName),
		zap.String(FieldVersion, config.Version),
		zap.String(FieldEnv, config.Environment),
	)

	return &Logger{
		Logger:      logger,
		sugar:       logger.Sugar(),
		serviceName: config.ServiceName,
		version:     config.Version,
	}
}


func NewDefaultLogger(serviceName string) *Logger {
	config := Config{
		Level:       getEnv("LOG_LEVEL", LevelInfo),
		Format:      getEnv("LOG_FORMAT", FormatJSON),
		ServiceName: serviceName,
		Version:     getEnv("SERVICE_VERSION", "unknown"),
		Environment: getEnv("ENVIRONMENT", "development"),
	}

	return NewLogger(config)
}

func (l *Logger) WithContext(ctx context.Context) *zap.Logger {
	fields := []zap.Field{
		zap.String("timestamp", time.Now().Format(time.RFC3339Nano)),
	}

	if traceID := getTraceIDFromContext(); traceID != "" {
		fields = append(fields, zap.String(FieldTraceID, traceID))
	}
	if spanID := getSpanIDFromContext(); spanID != "" {
		fields = append(fields, zap.String(FieldSpanID, spanID))
	}

	return l.Logger.With(fields...)
}

func (l *Logger) WithRequest(c *gin.Context) *zap.Logger {

	if c != nil {
		logger := l.WithContext(c.Request.Context())
		fields := []zap.Field{
			zap.String(FieldRequestID, getRequestID(c)),
			zap.String(FieldMethod, c.Request.Method),
			zap.String(FieldPath, c.Request.URL.Path),
		}

		if userID := getUserID(c); userID != "" {
			fields = append(fields, zap.String(FieldUserID, userID))
		}

		logger = logger.With(fields...)
		return logger

	}
	return l.Logger

}

// WithDatabase creates a logger for database operations
func (l *Logger) WithDatabase(operation string) *zap.Logger {
	return l.Logger.With(
		zap.String(FieldDatabase, "postgres"),
		zap.String(FieldOperation, operation),
	)
}

// WithKafka creates a logger for Kafka operations
func (l *Logger) WithKafka(topic string) *zap.Logger {
	return l.Logger.With(zap.String(FieldKafka, topic))
}

// WithProject creates a logger with project context
func (l *Logger) WithProject(projectName string) *zap.Logger {
	return l.Logger.With(zap.String(FieldProject, projectName))
}

// WithError creates a logger with error information
func (l *Logger) WithError(err error) *zap.Logger {
	return l.Logger.With(zap.String(FieldError, err.Error()))
}

// HTTP request logging middleware for Gin
func (l *Logger) GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Calculate request duration
		duration := time.Since(start).Milliseconds()

		// Build log entry
		logger := l.WithRequest(c).With(
			zap.Int("status", c.Writer.Status()),
			zap.Int64(FieldDuration, duration),
		)

		if raw != "" {
			path = path + "?" + raw
		}

		// Log based on status code
		if c.Writer.Status() >= 500 {
			logger.Error("HTTP request completed with server error")
		} else if c.Writer.Status() >= 400 {
			logger.Warn("HTTP request completed with client error")
		} else {
			logger.Info("HTTP request completed successfully")
		}
	}
}

// Graceful error logging that doesn't crash the service
func (l *Logger) LogStartupError(component string, err error) {
	l.Logger.Error("Service startup failed",
		zap.String("component", component),
		zap.String(FieldError, err.Error()),
	)
}

// Database connection logging
func (l *Logger) LogDatabaseConnection(host string, port int, database string) {
	l.WithDatabase("connect").Info("Attempting database connection",
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("database", database),
	)
}

func (l *Logger) LogDatabaseSuccess(database string) {
	l.WithDatabase("connect").Info("Database connection established successfully",
		zap.String("database", database),
	)
}

// User operation logging
func (l *Logger) LogUserRegistration(username string, success bool) {
	logger := l.Logger.With(
		zap.String(FieldOperation, "user_registration"),
		zap.String("username", username),
		zap.Bool("success", success),
	)

	if success {
		logger.Info("User registered successfully")
	} else {
		logger.Error("User registration failed")
	}
}

func (l *Logger) LogUserLogin(username string, success bool) {
	logger := l.Logger.With(
		zap.String(FieldOperation, "user_login"),
		zap.String("username", username),
		zap.Bool("success", success),
	)

	if success {
		logger.Info("User login successful")
	} else {
		logger.Warn("User login failed")
	}
}

// Project operation logging
func (l *Logger) LogProjectRegistration(projectName string, ownerID string) {
	l.WithProject(projectName).Info("Project registered successfully",
		zap.String(FieldOperation, "project_registration"),
		zap.String("owner_id", ownerID),
	)
}

// Kafka operation logging
func (l *Logger) LogKafkaMessage(topic string, key string, success bool) {
	logger := l.WithKafka(topic).With(
		zap.String(FieldOperation, "kafka_publish"),
		zap.String("message_key", key),
		zap.Bool("success", success),
	)

	if success {
		logger.Info("Kafka message published successfully")
	} else {
		logger.Error("Failed to publish Kafka message")
	}
}

// Sugar provides access to the sugared logger for printf-style logging
func (l *Logger) Sugar() *zap.SugaredLogger {
	return l.sugar
}

// Sync flushes any buffered log entries
func (l *Logger) Sync() error {
	return l.Logger.Sync()
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getRequestID(c *gin.Context) string {
	if requestID := c.GetHeader("X-Request-ID"); requestID != "" {
		return requestID
	}
	if requestID := c.GetHeader("X-Correlation-ID"); requestID != "" {
		return requestID
	}
	return ""
}

func getUserID(c *gin.Context) string {
	if userID, exists := c.Get("user_id"); exists {
		return fmt.Sprintf("%v", userID)
	}
	return ""
}

func getTraceIDFromContext() string {
	return ""
}

func getSpanIDFromContext() string {
	return ""
}