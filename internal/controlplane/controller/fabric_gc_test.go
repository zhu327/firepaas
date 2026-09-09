package controller

// W2 P0：sweepFabricULA 泄漏兜底（PG-gated；FIREPAAS_TEST_POSTGRES 未设时跳过）。

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func mustCellPrefix(t *testing.T) netip.Prefix {
	t.Helper()
	return netip.MustParsePrefix("fd7a:9a55::/40")
}

func seedGCNode(t *testing.T, s *store.Store, ctx context.Context, suffix, nodeID string) netip.Prefix {
	t.Helper()
	prefix, _, err := s.EnsureNodeFabric(ctx, mustCellPrefix(t), nodeID, "pk-"+suffix, "10.9.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID)
	})
	return prefix
}

func allocGCULA(
	t *testing.T,
	s *store.Store,
	ctx context.Context,
	suffix, project, app, nodeID string,
	prefix netip.Prefix,
	machineID, executionID string,
) {
	t.Helper()
	if _, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, "api"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AllocateULA(ctx, mustCellPrefix(t), prefix, project, nodeID, machineID, executionID, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().
			Exec(ctx, `DELETE FROM ipam_allocations WHERE machine_id=$1 AND execution_id=$2`, machineID, executionID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM workload_identities WHERE project_id=$1`, project)
	})
}

func gcActive(t *testing.T, s *store.Store, ctx context.Context, machineID, executionID string) bool {
	t.Helper()
	rows, err := s.ListActiveULAs(ctx, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.MachineID == machineID && r.ExecutionID == executionID {
			return true
		}
	}
	return false
}

// TestSweepFabricULA：死亡 execution 行被释放，现役/在途行受保护。
func TestSweepFabricULA(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project, app := "p-gc-"+suffix, "app-gc-"+suffix
	nodeID := "node-gc-" + suffix
	seedR2App(t, s, ctx, project, app)
	prefix := seedGCNode(t, s, ctx, suffix, nodeID)
	c := &Controller{store: s}

	newMachine := func(mid, exec string) {
		insertR2Machine(t, s, ctx, app, mid, "dep-"+mid, exec, 1, nodeID)
		t.Cleanup(func() {
			_, _ = s.Pool().Exec(ctx, `DELETE FROM operations WHERE machine_id=$1`, mid)
			_, _ = s.Pool().Exec(ctx, `DELETE FROM machines WHERE id=$1`, mid)
		})
	}

	// 1) 终删残留：desired=DELETED + 当前 execution 已换代 → 释放旧行。
	m1, e1old, e1new := "m-gc1-"+suffix, "exec-old", "exec-new"
	newMachine(m1, e1new)
	if _, err := s.Pool().Exec(ctx, `UPDATE machines SET desired_state='DELETED' WHERE id=$1`, m1); err != nil {
		t.Fatal(err)
	}
	allocGCULA(t, s, ctx, suffix, project, app, nodeID, prefix, m1, e1old)
	// 2) 现役行：current == row → 永不释放。
	m2, e2 := "m-gc2-"+suffix, "exec-live"
	newMachine(m2, e2)
	allocGCULA(t, s, ctx, suffix, project, app, nodeID, prefix, m2, e2)
	// 3) 在途 op 保护：旧代但有 CLAIMED op → 保留。
	m3, e3 := "m-gc3-"+suffix, "exec-stale"
	newMachine(m3, "exec-other")
	allocGCULA(t, s, ctx, suffix, project, app, nodeID, prefix, m3, e3)
	insertR2OpInFlight(t, s, ctx, project, "op-gc-"+suffix, m3, e3, 1, "machine_create", []byte(`{}`))
	// 4) 机器行已删（部署删除）：无在途 op → 释放。
	m4, e4 := "m-gc4-"+suffix, "exec-gone"
	allocGCULA(t, s, ctx, suffix, project, app, nodeID, prefix, m4, e4)
	// 5) 常规终删残留：desired=DELETED 且 current 仍指被删 execution
	//   （delete 完成后不清 current）→ 释放。
	m5, e5 := "m-gc5-"+suffix, "exec-deleted-current"
	newMachine(m5, e5)
	if _, err := s.Pool().Exec(ctx, `UPDATE machines SET desired_state='DELETED' WHERE id=$1`, m5); err != nil {
		t.Fatal(err)
	}
	allocGCULA(t, s, ctx, suffix, project, app, nodeID, prefix, m5, e5)

	for _, tc := range [][2]string{{m1, e1old}, {m2, e2}, {m3, e3}, {m4, e4}, {m5, e5}} {
		if !gcActive(t, s, ctx, tc[0], tc[1]) {
			t.Fatalf("setup: allocation %v not active", tc)
		}
	}
	if err := c.sweepFabricULA(ctx); err != nil {
		t.Fatal(err)
	}
	if gcActive(t, s, ctx, m1, e1old) {
		t.Fatal("deleted-machine stale row must be released")
	}
	if !gcActive(t, s, ctx, m2, e2) {
		t.Fatal("live execution row must be kept")
	}
	if !gcActive(t, s, ctx, m3, e3) {
		t.Fatal("in-flight op row must be kept")
	}
	if gcActive(t, s, ctx, m4, e4) {
		t.Fatal("machine-absent row must be released")
	}
	if gcActive(t, s, ctx, m5, e5) {
		t.Fatal("deleted-machine row with stale current execution must be released")
	}
	// 幂等：重跑无变化、不报错。
	if err := c.sweepFabricULA(ctx); err != nil {
		t.Fatal(err)
	}
}
