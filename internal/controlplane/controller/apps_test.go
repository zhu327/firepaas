package controller

import (
	"testing"

	"github.com/example/firepaas/internal/controlplane/store"
)

func TestAllReady(t *testing.T) {
	mk := func(ordinal int, state, readiness string) store.Machine {
		return store.Machine{
			ReplicaOrdinal:    ordinal,
			ObservedState:     state,
			ObservedReadiness: readiness,
		}
	}
	cases := []struct {
		name     string
		machines []store.Machine
		replicas int
		want     bool
	}{
		{"all running ready", []store.Machine{
			mk(0, "RUNNING", "READY"), mk(1, "RUNNING", "READY"), mk(2, "RUNNING", "READY"),
		}, 3, true},
		{"unconfigured counts as ready (ADR-0008)", []store.Machine{
			mk(0, "RUNNING", "UNCONFIGURED"),
		}, 1, true},
		{"not ready blocks cutover", []store.Machine{
			mk(0, "RUNNING", "READY"), mk(1, "RUNNING", "NOT_READY"), mk(2, "RUNNING", "READY"),
		}, 3, false},
		{"unknown blocks cutover", []store.Machine{
			mk(0, "RUNNING", "UNKNOWN"),
		}, 1, false},
		{"initializing blocks cutover", []store.Machine{
			mk(0, "INITIALIZING", "READY"),
		}, 1, false},
		{"missing ordinal blocks cutover", []store.Machine{
			mk(0, "RUNNING", "READY"),
		}, 2, false},
		// v1.1（ADR-0017）：PAUSED（standby）是可服务态——readiness 冻结
		// 在入睡前值，计入切流门控（首请求经 autoresume 唤醒）。
		{"paused with frozen readiness passes (ADR-0017)", []store.Machine{
			mk(0, "PAUSED", "READY"), mk(1, "PAUSED", "UNCONFIGURED"),
		}, 2, true},
		{"paused not_ready blocks cutover", []store.Machine{
			mk(0, "PAUSED", "NOT_READY"),
		}, 1, false},
		{"mixed running/paused passes", []store.Machine{
			mk(0, "RUNNING", "READY"), mk(1, "PAUSED", "READY"),
		}, 2, true},
	}
	for _, c := range cases {
		if got := allReady(c.machines, c.replicas); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestFilterMachines(t *testing.T) {
	ms := []store.Machine{
		{Generation: 1, ReplicaOrdinal: 0},
		{Generation: 2, ReplicaOrdinal: 0},
		{Generation: 2, ReplicaOrdinal: 1},
	}
	got := filterMachines(ms, func(m store.Machine) bool { return m.Generation == 2 })
	if len(got) != 2 {
		t.Fatalf("filtered len = %d", len(got))
	}
}

// TestUserDeleteOpIDContract：controller 的 opID 规则与 store 约定一致
// （P0-2：API 与 controller 不得发散）。
func TestUserDeleteOpIDContract(t *testing.T) {
	if got := userDeleteOpID("m-1", "exec-11111111-2222"); got != store.UserDeleteOpID("m-1", "exec-11111111-2222") {
		t.Fatalf("controller opID rule diverged from store: %q vs %q", got, store.UserDeleteOpID("m-1", "exec-11111111-2222"))
	}
}

// v1.1-F：rolling batch 大小与 machineServing 口径。
func TestRollingBatchSize(t *testing.T) {
	cases := []struct{ replicas, want int }{
		{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {8, 2}, {12, 3}, {100, 25},
	}
	for _, c := range cases {
		if got := rollingBatchSize(c.replicas); got != c.want {
			t.Errorf("rollingBatchSize(%d) = %d, want %d", c.replicas, got, c.want)
		}
	}
}

// Regression for F2: per-ordinal route cut must not delete its old execution.
// Deletion is allowed only after all target ordinals still serve in CUTOVER.
func TestRollingOldGenerationDeleteAllowed(t *testing.T) {
	serving := []store.Machine{
		{ReplicaOrdinal: 0, ObservedState: "RUNNING", ObservedReadiness: "READY"},
		{ReplicaOrdinal: 1, ObservedState: "RUNNING", ObservedReadiness: "READY"},
	}
	cases := []struct {
		name     string
		status   string
		target   []store.Machine
		replicas int
		want     bool
	}{
		{"rolling preparing cut ordinal retains old execution", "PREPARING", serving, 2, false},
		{"rollback retains old execution", "ROLLING_BACK", serving, 2, false},
		{"cutover deletes only after every target ordinal serves", "CUTOVER", serving, 2, true},
		{"cutover holds old execution when target loses readiness", "CUTOVER", []store.Machine{
			serving[0], {ReplicaOrdinal: 1, ObservedState: "RUNNING", ObservedReadiness: "NOT_READY"},
		}, 2, false},
	}
	for _, tc := range cases {
		if got := rollingOldGenerationDeleteAllowed(tc.status, tc.target, tc.replicas); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestMachineServing(t *testing.T) {
	if !machineServing(store.Machine{ObservedState: "RUNNING", ObservedReadiness: "READY"}) {
		t.Error("running+ready must serve")
	}
	if !machineServing(store.Machine{ObservedState: "PAUSED", ObservedReadiness: "READY"}) {
		t.Error("paused with frozen readiness must serve (ADR-0017)")
	}
	if machineServing(store.Machine{ObservedState: "PAUSED", ObservedReadiness: "NOT_READY"}) {
		t.Error("paused not_ready must not serve")
	}
	if machineServing(store.Machine{ObservedState: "", ObservedReadiness: "READY"}) {
		t.Error("unobserved must not serve")
	}
	if machineServing(store.Machine{ObservedState: "RUNNING", ObservedReadiness: ""}) {
		t.Error("empty readiness must not serve or cut over")
	}
}
