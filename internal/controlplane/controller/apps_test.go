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
