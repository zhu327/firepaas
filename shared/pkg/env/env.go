// Package env 是各进程共用的环境变量读取（agentd/api/edge-proxy）。
//
// 语义：空值回退默认；非法值 slog.Warn + 回退默认。
// 特殊语义（0 禁用、0 合法、(0,1) 区间）由各进程在本地保留处理，
// 本包只做无范围的严格解析（Int 要求 >0，Dur 要求 >0，Float 只要求可解析）。
package env

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Get 返回非空环境变量，否则回退默认。
func Get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Bool 解析布尔环境变量（strconv.ParseBool，前后去空白）。
// 空值回退默认；非法值 Warn + 回退默认。
func Bool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid bool env value; using default", "key", key, "value", raw, "default", def)
		return def
	}
	return b
}

// Int 解析正整数环境变量。空值回退默认；非法/非正值 Warn + 回退默认。
// 需要 0 合法的调用方（如 egress 端口）请保留本地处理。
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		slog.Warn("invalid env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

// Dur 解析时长环境变量。空值回退默认；非法/非正值 Warn + 回退默认。
func Dur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		slog.Warn("invalid duration env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return d
}

// Float 解析浮点环境变量（不做区间限制）。空值回退默认；
// 非法值 Warn + 回退默认。需要 (0,1) 区间的调用方请在本地加范围检查。
func Float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		slog.Warn("invalid env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return f
}
