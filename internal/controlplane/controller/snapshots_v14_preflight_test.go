// snapshots_v14_preflight_test.go：v1.4-D restore preflight 决策回归——
// preflight 与实际 rescue 派发共用 DecideRestore；memory 不兼容明确拒绝，
// auto 只报告可否降级。
package controller

import (
	"reflect"
	"testing"

	"github.com/zhu327/firepaas/internal/capabilities"
)

func TestDecideRestore(t *testing.T) {
	key := "fc-1.9/kernel-6.6/x86_64"
	cases := []struct {
		name                          string
		kind, snapKey, targetKey, mod string
		wantMemory                    bool
		wantResolved                  string
		wantDegradable                bool
		wantReason                    bool
	}{
		{"memory match", "MEMORY", key, key, "memory", true, "memory", false, false},
		{"memory key mismatch", "MEMORY", key, "other", "memory", false, "memory", false, true},
		{"memory kind mismatch", "FILESYSTEM", key, key, "memory", false, "memory", false, true},
		{"memory missing source key", "MEMORY", "", key, "memory", false, "memory", false, true},
		{"memory missing target key", "MEMORY", key, "", "memory", false, "memory", false, true},
		{"auto compatible", "MEMORY", key, key, "auto", true, "memory", false, false},
		{"auto degrades on key mismatch", "MEMORY", key, "other", "auto", false, "filesystem", true, true},
		{"auto degrades on kind", "FILESYSTEM", key, key, "auto", false, "filesystem", true, true},
		{"filesystem always available", "MEMORY", key, "other", "filesystem", false, "filesystem", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideRestore(tc.kind, tc.snapKey, tc.targetKey, tc.mod)
			if got.MemoryCompatible != tc.wantMemory {
				t.Fatalf("memory compatible = %v", got.MemoryCompatible)
			}
			if got.ResolvedMode != tc.wantResolved {
				t.Fatalf("resolved mode = %q, want %q", got.ResolvedMode, tc.wantResolved)
			}
			if got.Degradable != tc.wantDegradable {
				t.Fatalf("degradable = %v", got.Degradable)
			}
			if (got.Reason != "") != tc.wantReason {
				t.Fatalf("reason presence = %q", got.Reason)
			}
		})
	}
}

func TestRestoreCapabilitiesMatchResolvedMode(t *testing.T) {
	key := "fc-compatible"
	memory := DecideRestore("memory", key, key, "memory")
	if required, ok := RestoreCapability(memory, false, true); ok || required != capabilities.SnapshotMemoryV1 {
		t.Fatalf("memory capability decision = %q,%v", required, ok)
	}
	if got := AvailableRestoreModes(memory, false, true); !reflect.DeepEqual(got, []string{"filesystem"}) {
		t.Fatalf("available modes = %v", got)
	}

	auto := DecideRestore("MEMORY", key, "other", "auto")
	if required, ok := RestoreCapability(auto, true, false); ok || required != capabilities.SnapshotFilesystemV1 {
		t.Fatalf("degraded auto capability decision = %q,%v", required, ok)
	}
	if got := AvailableRestoreModes(auto, true, false); len(got) != 0 {
		t.Fatalf("incompatible memory must not be advertised: %v", got)
	}
}
