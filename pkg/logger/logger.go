package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LoggerEnv int8

const (
	LoggerEnvDev LoggerEnv = iota
	LoggerEnvProd
)

// 全局变量
var (
	Logger        *zap.Logger
	logRotator    *lumberjack.Logger // 保存 rotator 实例以便关闭
	IsInitialized bool               = false
	SkipStep      int                = 6
	Env           LoggerEnv          = LoggerEnvDev
)

// InitLogger 初始化日志系统
func InitLogger(logPath, serviceName string, env LoggerEnv) {
	var writerSyncer zapcore.WriteSyncer

	if logPath != "" {
		// 1. 配置 Lumberjack 日志轮转
		logRotator = &lumberjack.Logger{
			Filename:   logPath, // 日志文件路径
			MaxSize:    100,     // 单个日志文件最大 100MB
			MaxBackups: 5,       // 最多保留 5 个旧文件
			MaxAge:     30,      // 旧文件最多保留 30 天
			Compress:   true,    // 是否压缩旧文件 (gzip)
		}
		// 同时输出到控制台和文件
		writerSyncer = zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(logRotator))
	} else {
		// 仅输出到控制台（Docker 原生日志模式推荐）
		writerSyncer = zapcore.AddSync(os.Stdout)
	}

	// 2. 配置日志编码器 - 适配 Loki 格式
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",         // Loki 时间戳字段
		LevelKey:       "level",      // 日志级别
		NameKey:        "logger",     // logger 名称
		CallerKey:      "caller",     // 调用者信息
		FunctionKey:    "func",       // 函数名
		MessageKey:     "msg",        // 日志消息
		StacktraceKey:  "stacktrace", // 堆栈信息
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写级别
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder, // RFC3339 格式时间
		EncodeDuration: zapcore.SecondsDurationEncoder, // 持续时间以秒表示
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 短文件路径
	}

	// 3. (WriterSyncer 已在上方配置)

	// 4. 创建 Core
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writerSyncer,
		zapcore.InfoLevel, // 日志级别
	)

	// 5. 创建 Logger
	// 注意：这里没有直接赋值给全局 Logger，而是先创建基础 logger
	baseLogger := zap.New(
		core,
		zap.AddCaller(),                       // 添加文件名和行号
		zap.AddStacktrace(zapcore.ErrorLevel), // Error 级别自动添加堆栈
	)

	// 添加全局标签
	baseLogger = baseLogger.With(
		zap.String("service", serviceName),
		zap.String("env", os.Getenv("ENV")),
		zap.String("host", getHostname()),
	)

	Logger = baseLogger
	IsInitialized = true
	Env = env
}

// getHostname 获取主机名
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// 如果直接调用 Logger.Info，Zap 获取的 Caller 会指向 logger.go 而不是调用方。
// 为了解决这个问题，我们在下面的封装中使用了 .WithOptions(zap.AddCallerSkip(1))
// 这样行号才会显示 main.go:xx 而不是 logger.go:xx

func Info(msg string, fields ...zap.Field) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Info(msg, fields...)
}

func Infof(f string, args ...any) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Info(fmt.Sprintf(f, args...))
}

func Errorf(f string, args ...any) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Error(fmt.Sprintf(f, args...))
}

func Error(msg string, fields ...zap.Field) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Error(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Warn(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	if Logger == nil {
		return
	}
	Logger.WithOptions(zap.AddCallerSkip(SkipStep)).Debug(msg, fields...)
}

// Sync 程序退出前调用
func Sync() error {
	var err error
	if Logger != nil {
		// 刷新缓存到磁盘
		err = Logger.Sync()
	}
	// 关闭 lumberjack 文件句柄
	if logRotator != nil {
		_ = logRotator.Close()
	}
	return err
}
