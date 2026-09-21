package main

import (
	"testing"
	"time"
)

// openAgentState 默认路径 + 默认保留窗口：缺文件即新建，返回非 nil 的三份
// 持久状态与 24h 默认 retention。
func TestOpenAgentStateDefaults(t *testing.T) {
	ledger, fences, fabric, retention, err := openAgentState(t.TempDir())
	if err != nil {
		t.Fatalf("openAgentState: %v", err)
	}
	if ledger == nil || fences == nil || fabric == nil {
		t.Fatalf("nil durable state: ledger=%v fences=%v fabric=%v", ledger, fences, fabric)
	}
	if retention != 24*time.Hour {
		t.Fatalf("default retention = %v, want 24h", retention)
	}
}

// 自定义保留窗口经 env 覆盖并被解析。
func TestOpenAgentStateRetentionOverride(t *testing.T) {
	t.Setenv("FIREPAAS_AGENT_LEDGER_RETENTION", "90m")
	_, _, _, retention, err := openAgentState(t.TempDir())
	if err != nil {
		t.Fatalf("openAgentState: %v", err)
	}
	if retention != 90*time.Minute {
		t.Fatalf("retention = %v, want 90m", retention)
	}
}

// 非法/非正保留窗口必须报错（fail-closed，不静默回退默认）。
func TestOpenAgentStateBadRetentionFails(t *testing.T) {
	for _, v := range []string{"not-a-duration", "0", "-5m"} {
		t.Setenv("FIREPAAS_AGENT_LEDGER_RETENTION", v)
		if _, _, _, _, err := openAgentState(t.TempDir()); err == nil {
			t.Fatalf("retention %q must fail, got nil error", v)
		}
	}
}
