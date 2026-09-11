// Package logging 是三个进程（api/agentd/edge-proxy）共用的 slog 初始化。
//
//	FIREPAAS_LOG_LEVEL:  debug|info|warn|error（默认 info）
//	FIREPAAS_LOG_FORMAT: text|json（默认 text，保持既有输出）
//
// 只依赖标准库；shared 包不得反向依赖具体进程。
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup 按环境变量安装进程默认 logger。
func Setup() {
	lvl := new(slog.LevelVar)
	switch strings.ToLower(strings.TrimSpace(os.Getenv("FIREPAAS_LOG_LEVEL"))) {
	case "debug":
		lvl.Set(slog.LevelDebug)
	case "warn":
		lvl.Set(slog.LevelWarn)
	case "error":
		lvl.Set(slog.LevelError)
	default:
		lvl.Set(slog.LevelInfo)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if strings.EqualFold(strings.TrimSpace(os.Getenv("FIREPAAS_LOG_FORMAT")), "json") {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}
