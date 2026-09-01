// snapshots_v14_test.go：v1.4-A 基线加固回归——snapshot 删除对 DELETING 的
// 幂等收敛、终态重派、引用生命周期（重取幂等 + 终态回收）。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

func seedReadySnapshot(t *testing.T, s *Store, project, snapID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateSnapshot(ctx, Snapshot{ID: snapID, ProjectID: project,
		SourceMachineID: "m-v14", SourceExecutionID: "exec-v14", SourceGeneration: 1,
		Kind: "MEMORY", NodeID: "node-v14"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSnapshotArtifact(ctx, snapID, 1024, "sha256:v14", "none", "none", "", nil, true); err != nil {
		t.Fatal(err)
	}
}

func deleteParams(project, snapID, opID string, raw []byte) EnqueueOperationParams {
	return EnqueueOperationParams{OperationID: opID, ProjectID: project, MachineID: "m-v14",
		ExecutionID: "exec-v14", Generation: 1, Kind: "snapshot_delete",
		Request: raw, DispatchNodeID: "node-v14"}
}

// v1.4-A：对已进入 DELETING 的快照重试删除必须幂等收敛，不得因
// DELETING→DELETING 冲突失败。
func TestBeginSnapshotDeleteIdempotentWhileDeleting(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-snapdel-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "snapdel-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	snapID := "snap-del-v14"
	seedReadySnapshot(t, s, project, snapID)
	opID := "op-snap-del-" + snapID
	raw := []byte(fmt.Sprintf(`{"snapshot_id":%q,"operation_id":%q}`, snapID, opID))
	p := deleteParams(project, snapID, opID, raw)

	first, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, p)
	if err != nil {
		t.Fatal(err)
	}
	// 状态已进入 DELETING；同键重试必须返回同一操作，而不是冲突。
	second, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, p)
	if err != nil {
		t.Fatalf("idempotent delete retry on DELETING snapshot: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("retry re-enqueued a new operation: %s vs %s", second.ID, first.ID)
	}
	snap, err := s.GetSnapshot(ctx, snapID)
	if err != nil || snap.Status != "DELETING" {
		t.Fatalf("snapshot status = %q, err=%v", snap.Status, err)
	}
	var ops int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE kind='snapshot_delete' AND request::text LIKE '%' || $1 || '%'`, snapID).Scan(&ops); err != nil || ops != 1 {
		t.Fatalf("duplicate delete operations created: %d (err=%v)", ops, err)
	}
}

// v1.4-A：上次删除尝试已终结（FAILED）时，重试必须以新 attempt 键重派，
// 使 DELETING 状态可收敛，而不是永远返回已死操作。
func TestBeginSnapshotDeleteReEnqueuesAfterTerminalFailure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-snapdel2-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "snapdel2-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	snapID := "snap-del-v14b"
	seedReadySnapshot(t, s, project, snapID)
	opID := "op-snap-del-" + snapID
	raw := []byte(fmt.Sprintf(`{"snapshot_id":%q,"operation_id":%q}`, snapID, opID))
	p := deleteParams(project, snapID, opID, raw)

	if _, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, p); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOperation(ctx, opID, "FAILED", nil, "simulated terminal failure"); err != nil {
		t.Fatal(err)
	}
	retry, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, p)
	if err != nil {
		t.Fatalf("re-enqueue after terminal failure: %v", err)
	}
	if retry.ID == opID {
		t.Fatal("terminal attempt must be re-keyed, not replayed")
	}
	if retry.Status != "PENDING" {
		t.Fatalf("re-enqueued operation status = %q", retry.Status)
	}
	// 二次重试（新键仍在途）依旧幂等。
	again, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, p)
	if err != nil || again.ID != retry.ID {
		t.Fatalf("retry of in-flight re-keyed delete: %v (op=%s want %s)", err, again.ID, retry.ID)
	}
}

// v1.4-A：引用保护与操作生命周期一致——已持有引用的操作可在快照离开
// READY 后幂等重取；操作终结后引用由回收器释放。
func TestForkAndRescueRejectBlockedIntegrityAtomically(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-snap-integrity-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "snapshot-integrity-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedReadySnapshot(t, s, project, "snap-integrity")
	if err := s.SetSnapshotIntegrity(ctx, "snap-integrity", "CORRUPT"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateForkMachineAndEnqueue(ctx, "snap-integrity", ForkMachineParams{
		ProjectID: project, AppID: "missing-app", MachineID: "m-fork-blocked", ExecutionID: "exec-fork", NodeID: "node-v14",
	}, EnqueueOperationParams{OperationID: "op-fork-blocked", ProjectID: project, MachineID: "m-fork-blocked",
		ExecutionID: "exec-fork", Generation: 1, Kind: "fork", Request: []byte(`{}`), DispatchNodeID: "node-v14"}); !errors.Is(err, ErrSnapshotStatusConflict) {
		t.Fatalf("fork corrupt snapshot = %v", err)
	}
	var machines, ops int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM machines WHERE id='m-fork-blocked'`).Scan(&machines)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE id='op-fork-blocked'`).Scan(&ops)
	if machines != 0 || ops != 0 {
		t.Fatalf("blocked fork leaked machine/op: %d/%d", machines, ops)
	}
}

func TestRescuePreflightMismatchPreservesExecutionAndRoute(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-rescue-mismatch-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "rescue-mismatch-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	machineID := "m-rescue-mismatch"
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-rescue-mismatch", "rescue-mismatch.local", "img:1",
		1, 512, 0, 80, machineID, "dep-rescue-mismatch", "exec-old", "op-create-rescue-mismatch",
		1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertNode(ctx, Node{ID: "node-mismatch", NomadNodeID: "nomad-mismatch", Status: "HEALTHY",
		FeatureIDs: []string{"snapshot.memory.v1"}, SnapshotCompatibilityKey: "target-key"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineNodeAndObserved(ctx, machineID, "node-mismatch", "exec-old", "RUNNING", "10.0.0.2", "READY"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSnapshot(ctx, Snapshot{ID: "snap-mismatch", ProjectID: project, SourceMachineID: machineID,
		SourceExecutionID: "exec-old", SourceGeneration: 1, Kind: "MEMORY", NodeID: "node-mismatch", CompatibilityKey: "source-key"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSnapshotArtifact(ctx, "snap-mismatch", 1, "sha256:test", "none", "none", "", nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO routes(id,app_id,hostname,active_generation) VALUES('route-mismatch','app-rescue-mismatch','rescue-mismatch.local',1)
		ON CONFLICT (hostname,port) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO route_backends(route_id,generation,machine_id,execution_id)
		SELECT id,1,$1,'exec-old' FROM routes WHERE app_id='app-rescue-mismatch'`, machineID); err != nil {
		t.Fatal(err)
	}
	_, err := s.EnqueueRescueReplacement(ctx, RescueReplacementParams{ProjectID: project, MachineID: machineID,
		OldExecutionID: "exec-old", OldGeneration: 1, NewExecutionID: "exec-new", OperationID: "op-rescue-mismatch",
		SnapshotID: "snap-mismatch", Request: []byte(`{}`), DispatchNodeID: "node-mismatch",
		RequiredFeature: "snapshot.memory.v1", TargetCompatibilityKey: "target-key", RequireMemoryCompatible: true})
	if !errors.Is(err, ErrRescueConflict) {
		t.Fatalf("rescue mismatch = %v, want ErrRescueConflict", err)
	}
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m.CurrentExecutionID != "exec-old" || m.Generation != 1 {
		t.Fatalf("machine changed on mismatch: %+v err=%v", m, err)
	}
	var backends, ops int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM route_backends WHERE machine_id=$1 AND execution_id='exec-old'`, machineID).Scan(&backends)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE id='op-rescue-mismatch'`).Scan(&ops)
	if backends != 1 || ops != 0 {
		t.Fatalf("mismatch changed route/op: backends=%d ops=%d", backends, ops)
	}
}

func TestSnapshotReferenceLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-snapref-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "snapref-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	snapID := "snap-ref-v14"
	seedReadySnapshot(t, s, project, snapID)
	opID := "op-fork-v14"

	if err := s.AcquireSnapshotReference(ctx, snapID, opID, "fork"); err != nil {
		t.Fatal(err)
	}
	// 快照进入 DELETING（或 UNAVAILABLE）后，同操作重取必须幂等成功。
	if _, err := s.TransitionSnapshot(ctx, snapID, "READY", "UNAVAILABLE"); err != nil {
		t.Fatal(err)
	}
	if err := s.AcquireSnapshotReference(ctx, snapID, opID, "fork"); err != nil {
		t.Fatalf("re-acquire held reference after snapshot left READY: %v", err)
	}
	// 其他操作仍不能在非 READY 快照上新建引用。
	if err := s.AcquireSnapshotReference(ctx, snapID, "op-fork-other", "fork"); !errors.Is(err, ErrSnapshotStatusConflict) {
		t.Fatalf("fresh acquire on non-READY snapshot = %v, want conflict", err)
	}

	// 删除被 active 引用阻塞。
	if _, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, deleteParams(project, snapID,
		"op-snap-del-"+snapID, []byte(fmt.Sprintf(`{"snapshot_id":%q}`, snapID)))); !errors.Is(err, ErrSnapshotStatusConflict) {
		t.Fatalf("delete with active reference = %v, want conflict", err)
	}

	// 操作终结（含崩溃路径未显式释放）后，回收器必须释放引用。
	if _, err := s.pool.Exec(ctx, `INSERT INTO operations(id,project_id,machine_id,execution_id,generation,kind,idempotency_key,status,request,dispatch_node_id)
		VALUES($1,$2,'m-v14','exec-v14',1,'fork',$1,'FAILED','{}'::jsonb,'node-v14')`, opID, project); err != nil {
		t.Fatal(err)
	}
	n, err := s.ReleaseTerminalOperationReferences(ctx)
	if err != nil || n < 1 {
		t.Fatalf("release terminal references: n=%d err=%v", n, err)
	}
	referenced, err := s.SnapshotReferenced(ctx, snapID)
	if err != nil || referenced {
		t.Fatalf("reference after reaper: %v err=%v", referenced, err)
	}
	// 引用释放后删除可继续。
	if _, err := s.BeginSnapshotDeleteAndEnqueue(ctx, snapID, deleteParams(project, snapID,
		"op-snap-del-"+snapID, []byte(fmt.Sprintf(`{"snapshot_id":%q}`, snapID)))); err != nil {
		t.Fatalf("delete after reference release: %v", err)
	}
}
