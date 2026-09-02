package main

import (
	"testing"
	"time"
)

// env 解析卫生：非法/越界值回退默认（告警行为属日志侧，断言可见返回值）。
func TestEnvIntInvalidValuesFallBack(t *testing.T) {
	t.Setenv("FP_TEST_INT", "abc")
	if got := envInt("FP_TEST_INT", 7); got != 7 {
		t.Fatalf("invalid value: got %d, want default 7", got)
	}
	t.Setenv("FP_TEST_INT", "0")
	if got := envInt("FP_TEST_INT", 7); got != 7 {
		t.Fatalf("non-positive: got %d, want default 7", got)
	}
	t.Setenv("FP_TEST_INT", "-3")
	if got := envInt("FP_TEST_INT", 7); got != 7 {
		t.Fatalf("negative: got %d, want default 7", got)
	}
	t.Setenv("FP_TEST_INT", "42")
	if got := envInt("FP_TEST_INT", 7); got != 42 {
		t.Fatalf("valid: got %d, want 42", got)
	}
}

func TestEnvFloatBounds(t *testing.T) {
	for _, v := range []string{"abc", "0", "-0.5", "1", "1.5"} {
		t.Setenv("FP_TEST_FLOAT", v)
		if got := envFloat("FP_TEST_FLOAT", 0.9); got != 0.9 {
			t.Fatalf("value %q: got %v, want default 0.9", v, got)
		}
	}
	t.Setenv("FP_TEST_FLOAT", "0.7")
	if got := envFloat("FP_TEST_FLOAT", 0.9); got != 0.7 {
		t.Fatalf("valid: got %v, want 0.7", got)
	}
}

// FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL：默认 5m；<=0 交给调用方作禁用
// 语义；非法值回退默认。
func TestSlotReconcileInterval(t *testing.T) {
	if got := slotReconcileInterval(); got != 5*time.Minute {
		t.Fatalf("unset: got %v, want 5m", got)
	}
	t.Setenv("FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL", "30s")
	if got := slotReconcileInterval(); got != 30*time.Second {
		t.Fatalf("30s: got %v", got)
	}
	t.Setenv("FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL", "0")
	if got := slotReconcileInterval(); got != 0 {
		t.Fatalf("0 must pass through as disable signal: got %v", got)
	}
	t.Setenv("FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL", "bogus")
	if got := slotReconcileInterval(); got != 5*time.Minute {
		t.Fatalf("invalid: got %v, want default 5m", got)
	}
}
