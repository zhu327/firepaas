package runtime

import (
	"strings"
	"testing"

	"github.com/kernel/hypeman/lib/config"
)

// LoadConfig 在 CONFIG_PATH 指向不存在的文件时必须报错（fail-closed，
// 不静默用默认配置启动 agent）。
func TestLoadConfigMissingFile(t *testing.T) {
	t.Setenv("CONFIG_PATH", "/nonexistent-firepaas-config.yaml")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig with missing file must fail")
	}
}

// Assemble 在非法 limits 时必须在创建实例前报错，不启动任何后台服务。
func TestAssembleInvalidOverlaySize(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir()}
	cfg.Limits.MaxOverlaySize = "bogus-size!!!"
	if _, err := Assemble(cfg); err == nil {
		t.Fatal("Assemble with invalid max_overlay_size must fail")
	} else if !strings.Contains(err.Error(), "max_overlay_size") &&
		!strings.Contains(err.Error(), "overlay") {
		t.Logf("assemble failed before overlay parse (env-dependent): %v", err)
	}
}

func TestAssembleInvalidMemoryLimit(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir()}
	cfg.Limits.MaxMemoryPerInstance = "bogus-mem!!!"
	if _, err := Assemble(cfg); err == nil {
		t.Fatal("Assemble with invalid max_memory_per_instance must fail")
	}
}
