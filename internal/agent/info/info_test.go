package info

import (
	"testing"
)

// cgroup v2 cpu.max 解析边界：quota/period 微秒对、max 无限制、亚核钳 1、
// 非法输入一律视为无限制（与 readCgroupMemMax 同策略）。
func TestParseCPUMax(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"max 100000", 0},
		{"200000 100000", 2},
		{"150000 100000", 1}, // 分数配额向下取整（硬准入宁少勿超）
		{"50000 100000", 1},  // 亚核配额钳到 1
		{"800000 100000", 8},
		{"100000 100000", 1},
		{"0 100000", 0},
		{"-1 100000", 0},
		{"200000 0", 0},
		{"200000", 0},
		{"100000 100000 1", 0},
		{"garbage", 0},
		{"", 0},
		{"quota period", 0},
	}
	for _, c := range cases {
		if got := parseCPUMax(c.in); got != c.want {
			t.Errorf("parseCPUMax(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestCapabilityProjection 覆盖 v1.2-A（ADR-0023）：ServiceInfoResponse 必须
// 上报 protocol_version / feature_ids / snapshot_compatibility_key。
func TestCapabilityProjection(t *testing.T) {
	p := New("node-1", "1.0.0", "abc123", "compute", "v1.14.2", "10.11.0.0/16", "/tmp", nil, nil)
	p.SetCapabilities("firepaas.agent.v1",
		[]string{"guest.exec.v1", "guest.copy.v1", "guest.logs.v1"},
		"firecracker-v1.14.2-amd64")

	resp := p.Response()
	if resp.ProtocolVersion != "firepaas.agent.v1" {
		t.Fatalf("protocol_version: got %q", resp.ProtocolVersion)
	}
	if resp.SnapshotCompatibilityKey != "firecracker-v1.14.2-amd64" {
		t.Fatalf("snapshot_compatibility_key: got %q", resp.SnapshotCompatibilityKey)
	}
	if len(resp.FeatureIds) != 3 {
		t.Fatalf("feature_ids: want 3, got %v", resp.FeatureIds)
	}
	// 未配置能力时不得伪造非空 feature（fail closed 由 scheduler 兑底）。
	p2 := New("node-2", "1.0.0", "abc123", "compute", "v1.14.2", "", "/tmp", nil, nil)
	if got := p2.Response().FeatureIds; len(got) != 0 {
		t.Fatalf("unset capabilities must be empty, got %v", got)
	}
}
