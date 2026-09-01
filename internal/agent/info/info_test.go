package info

import (
	"testing"
)

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
