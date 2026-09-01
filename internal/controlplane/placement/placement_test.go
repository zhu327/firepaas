package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func TestAssembleSchedulerNodesCombinesOneSnapshot(t *testing.T) {
	live := []liveNode{
		{
			NodeID: "agent-1", NomadID: "nomad-1", Status: scheduler.StatusHealthy,
			Node: nodemanager.Node{
				NomadNodeID: "nomad-1", NodePool: "compute",
				Info: &pb.ServiceInfoResponse{
					NodeId: "agent-1", Labels: map[string]string{"zone": "a"},
					Capacity: &pb.NodeCapacity{VcpuTotal: 8, MemTotalMib: 16384, DiskTotalMib: 50000},
					Usage: &pb.NodeUsage{
						MemAllocatedMib:  2048,
						DiskAllocatedMib: 4096,
						CpuPercent:       25,
						MemUsedMib:       1024,
					},
				},
			},
		},
	}
	stored := []store.Node{{
		ID: "agent-1", NomadNodeID: "nomad-1", Draining: true,
		ImageCache: []string{"sha256:cached"}, FeatureIDs: []string{capabilities.SecretOneShotV1},
	}}
	got := assembleSchedulerNodes(live,
		map[string]store.Allocated{"agent-1": {VCPU: 3, MemMib: 9999, DiskMib: 9999}},
		map[string]store.PendingUsage{"agent-1": {NodeID: "agent-1", VCPU: 2, MemMib: 512, DiskMib: 1024}}, stored)
	if len(got) != 1 {
		t.Fatalf("nodes = %d, want 1", len(got))
	}
	n := got[0]
	if !n.Draining || n.CPUAllocated != 3 || n.MemAllocated != 2048 || n.DiskAllocated != 4096 ||
		n.CPUPending != 2 || n.MemPending != 512 || n.DiskPending != 1024 {
		t.Fatalf("assembled node = %+v", n)
	}
	if !n.CachedImageDigests["sha256:cached"] || !n.FeatureIDs[capabilities.SecretOneShotV1] {
		t.Fatalf("projection sets missing: %+v", n)
	}
}

func TestAssembleSchedulerNodesFallsBackToPGAccountingAndLiveFeatures(t *testing.T) {
	live := []liveNode{
		{
			NodeID: "agent-1", NomadID: "nomad-1", Status: scheduler.StatusHealthy,
			Node: nodemanager.Node{Info: &pb.ServiceInfoResponse{
				FeatureIds: []string{"feature.v1"},
				Capacity:   &pb.NodeCapacity{VcpuTotal: 4, MemTotalMib: 4096},
			}},
		},
	}
	got := assembleSchedulerNodes(live,
		map[string]store.Allocated{"agent-1": {VCPU: 1, MemMib: 700, DiskMib: 800}}, nil, nil)[0]
	if got.CPUAllocated != 1 || got.MemAllocated != 700 || got.DiskAllocated != 800 || !got.FeatureIDs["feature.v1"] {
		t.Fatalf("fallback node = %+v", got)
	}
}

func TestRequiredFeaturesInvalidEgressFailsClosed(t *testing.T) {
	got := RequiredFeatures(nil, false, []byte(`{"mode":`))
	if len(got) != 1 || got[0] != "egress.invalid-policy" {
		t.Fatalf("required features = %v", got)
	}
}

func TestCommitDispatchCompensatesReservationOnPersistenceFailure(t *testing.T) {
	persistErr := errors.New("pg unavailable")
	released := false
	err := commitDispatch(context.Background(), "op-1", "node-1", time.Second,
		func(context.Context, string, string) error { return persistErr },
		func(_ context.Context, opID string) error {
			released = opID == "op-1"
			return nil
		})
	if !errors.Is(err, persistErr) || !released {
		t.Fatalf("err=%v released=%v", err, released)
	}
}

func TestCommitDispatchCompensationIsDetachedAndBounded(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	persistErr := errors.New("pg unavailable")
	var inheritedCancellation, hasDeadline bool
	var remaining time.Duration
	err := commitDispatch(parent, "op-1", "node-1", time.Hour,
		func(context.Context, string, string) error { return persistErr },
		func(ctx context.Context, _ string) error {
			inheritedCancellation = ctx.Err() != nil
			deadline, ok := ctx.Deadline()
			hasDeadline = ok
			remaining = time.Until(deadline)
			return nil
		})
	if !errors.Is(err, persistErr) {
		t.Fatalf("err = %v, want persistence failure", err)
	}
	if inheritedCancellation {
		t.Fatal("compensation inherited canceled parent")
	}
	if !hasDeadline || remaining <= 0 || remaining > time.Hour {
		t.Fatalf("compensation remaining timeout = %v, hasDeadline=%v", remaining, hasDeadline)
	}
}

func TestCommitDispatchDoesNotReleaseCommittedReservation(t *testing.T) {
	released := false
	err := commitDispatch(context.Background(), "op-1", "node-1", time.Second,
		func(context.Context, string, string) error { return nil },
		func(context.Context, string) error { released = true; return nil })
	if err != nil || released {
		t.Fatalf("err=%v released=%v", err, released)
	}
}

func TestPlacementHelpers(t *testing.T) {
	if got := ImageDigest("registry/app@sha256:abc"); got != "sha256:abc" {
		t.Fatalf("digest = %q", got)
	}
	if ImageDigest("registry/app:latest") != "" {
		t.Fatal("tag-only reference must not produce digest")
	}
	if MachineQuotaExceeded(1, 1) || !MachineQuotaExceeded(2, 1) || MachineQuotaExceeded(2, 0) {
		t.Fatal("machine quota boundary changed")
	}
}
