package logging

import (
	"context"
	"log/slog"
	"testing"
)

// Setup 按环境变量安装全局 logger；非法值回退默认且绝不 panic。
func TestSetupLevels(t *testing.T) {
	cases := []struct {
		level     string
		debugWant bool
	}{
		{"debug", true},
		{"info", false},
		{"warn", false},
		{"error", false},
		{"bogus-level", false},
		{"", false},
		{" DEBUG ", true},
	}
	for _, c := range cases {
		t.Setenv("FIREPAAS_LOG_LEVEL", c.level)
		t.Setenv("FIREPAAS_LOG_FORMAT", "text")
		Setup()
		if got := slog.Default().Enabled(context.Background(), slog.LevelDebug); got != c.debugWant {
			t.Errorf("LEVEL=%q debug enabled = %v, want %v", c.level, got, c.debugWant)
		}
	}
}

func TestSetupJSONFormatNoPanic(t *testing.T) {
	t.Setenv("FIREPAAS_LOG_LEVEL", "info")
	t.Setenv("FIREPAAS_LOG_FORMAT", "json")
	Setup()
	slog.Info("json format smoke test")
	t.Setenv("FIREPAAS_LOG_FORMAT", "bogus-format")
	Setup() // 非法 format 回退 text，不 panic
	slog.Info("fallback format smoke test")
	Setup() // 复位默认，避免污染其它包的全局 logger
}
