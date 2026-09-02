package placement

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/reservations"
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

// 契约 C-3（P1#14b）：预约层节点 CPU 上限与调度器硬准入使用同一个
// 超售比 R——R=2 时两侧必须在同一边界同时 accept/reject（修复 Lua 硬编码
// *4 与可配置 R 的发散）。
func TestReservationSchedulerOversubscribeConsistency(t *testing.T) {
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS=127.0.0.1:6379 to run consistency test")
	}
	// 使用独立 Redis DB：go test 跨包并发执行，reservations 包测试也在
	// DB0 写 resv:* 键空间，共享会导致跨包污染 flake（AGENTS.md 禁共享
	// 可变全局状态）。
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 7})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	m := reservations.New(rdb, 120*time.Second)
	ctx := context.Background()
	t.Cleanup(func() {
		keys, _ := rdb.Keys(ctx, "resv:*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	})

	const R = 2.0
	const nodeVCPU = 4 // 边界 = 4×2 = 8 vcpu

	cfg := scheduler.DefaultBestOfKConfig()
	cfg.R = R
	placer := scheduler.New(cfg, scheduler.Options{})
	rnd := rand.New(rand.NewSource(1))
	node := func(pending uint64) scheduler.Node {
		n := scheduler.Node{
			ID: "n1", Pool: "compute", Status: scheduler.StatusHealthy,
			CPUTotal: nodeVCPU, MemTotalMib: 1 << 20, CPUPending: pending,
		}
		return n
	}
	fitsScheduler := func(vcpu, pending uint64) bool {
		_, err := placer.Place(scheduler.Request{VCPU: vcpu, MemMib: 1},
			[]scheduler.Node{node(pending)}, rnd)
		return err == nil
	}
	acquire := func(opID string, vcpu uint64) error {
		return m.AcquireR(ctx, opID, "n1", "dev", vcpu, 1, 0,
			nodeVCPU, 1<<20, 0, 1<<20, 1<<30, 0, placer.Config().R)
	}

	// pending=6：再放 2 vcpu 恰好到边界 8——调度与预约必须同时接受。
	if err := acquire("op-c3-a", 6); err != nil {
		t.Fatalf("acquire base pending: %v", err)
	}
	if !fitsScheduler(2, 6) {
		t.Fatal("scheduler must accept at boundary pending 6 + 2 == 4*R")
	}
	if err := acquire("op-c3-b", 2); err != nil {
		t.Fatalf("reservation must accept at boundary pending 6 + 2 == 4*R, got %v", err)
	}
	// pending=8（已满）：再放 1 vcpu 越界——两侧必须同时拒绝。
	if fitsScheduler(1, 8) {
		t.Fatal("scheduler must reject over boundary pending 8 + 1 > 4*R")
	}
	if err := acquire("op-c3-c", 1); !errors.Is(err, reservations.ErrNodeCapacity) {
		t.Fatalf("reservation must reject over boundary, got %v", err)
	}
	// 发散回归守卫：R=2 下 9 vcpu 必须被拒绝（旧硬编码 4 会放行到 16）。
	if err := m.AcquireR(ctx, "op-c3-d", "n2", "dev", 9, 1, 0,
		nodeVCPU, 1<<20, 0, 1<<20, 1<<30, 0, placer.Config().R); !errors.Is(err, reservations.ErrNodeCapacity) {
		t.Fatalf("R=2 must reject 9 > 4*2 (old hardcoded 4 would admit), got %v", err)
	}
}

// per-deployment 放置锁:D1 回归——同一 deployment 的并发放置必须串行,
// 不同 deployment 不互相阻塞。
func TestDeploymentPlacementLockSerializesPerDeployment(t *testing.T) {
	s := New(nil, nil, nil, nil, nil, 0)
	var mu sync.Mutex
	maxInFlight, inFlight := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := s.lockDeployment("dep-1")
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()
	if maxInFlight != 1 {
		t.Fatalf("same-deployment placements must serialize, max in-flight = %d", maxInFlight)
	}

	// 不同 deployment 不互相阻塞:A 持锁时 B 必须能获得。
	releaseA := s.lockDeployment("dep-a")
	done := make(chan struct{})
	go func() {
		releaseB := s.lockDeployment("dep-b")
		releaseB()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("different deployment blocked behind held placement lock")
	}
	releaseA()
}
