package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProjectUsageIncludesAllocatedAndPending(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-usage-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "usage-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	machineID := "m-usage-" + fmt.Sprint(os.Getpid())
	op, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-usage", "usage.local", "img:1",
		2, 1024, 0, 80, machineID, "dep-usage", "exec-1", "op-usage-"+fmt.Sprint(os.Getpid()),
		1, 0, []byte(`{}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if vcpu, mem, disk, err := s.ProjectUsage(ctx, project); err != nil || vcpu != 2 || mem != 1024 || disk != 10240 {
		t.Fatalf("pending usage = (%d,%d,%d), err=%v; want (2,1024,10240)", vcpu, mem, disk, err)
	}
	if err := s.UpdateMachineNodeAndObserved(ctx, machineID, "node-1", "exec-1", "RUNNING", "", "READY"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), ""); err != nil {
		t.Fatal(err)
	}
	if vcpu, mem, disk, err := s.ProjectUsage(ctx, project); err != nil || vcpu != 2 || mem != 1024 || disk != 10240 {
		t.Fatalf("allocated usage = (%d,%d,%d), err=%v; want (2,1024,10240)", vcpu, mem, disk, err)
	}
}

func TestProjectMachineUsageCountsCurrentCreateAndRestartOnce(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-machine-usage-" + suffix
	if err := s.EnsureProject(ctx, project, "machine-usage-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	machineID := "m-machine-usage-" + suffix
	op, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-machine-usage", "machine-usage.local", "img:1",
		1, 512, 1024, 80, machineID, "dep-machine-usage", "exec-1", "op-machine-usage-1-"+suffix,
		1, 0, []byte(`{"generation":"1"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.ProjectMachineUsage(ctx, project); err != nil || got != 1 {
		t.Fatalf("pending create usage = %d, err=%v; want 1", got, err)
	}
	if err := s.UpdateMachineNodeAndObserved(ctx, machineID, "node-1", "exec-1", "RUNNING", "", "READY"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), ""); err != nil {
		t.Fatal(err)
	}
	_, err = s.EnsureAppAndEnqueueCreate(ctx, project, "app-machine-usage", "machine-usage.local", "img:1",
		1, 512, 1024, 80, machineID, "dep-machine-usage", "exec-2", "op-machine-usage-2-"+suffix,
		2, 0, []byte(`{"generation":"2"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.ProjectMachineUsage(ctx, project); err != nil || got != 1 {
		t.Fatalf("restart replacement usage = %d, err=%v; want net usage 1", got, err)
	}
}

func TestEnsureCreateRejectsImmutableOwnershipConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	ownerProject := "p-owner-" + sfx
	otherProject := "p-other-" + sfx
	for _, project := range []string{ownerProject, otherProject} {
		if err := s.EnsureProject(ctx, project, project); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cleanupProject(t, s, project) })
	}

	appID := "app-owned-" + sfx
	machineID := "m-owned-" + sfx
	deploymentID := "dep-owned-" + sfx
	firstOp := "op-owned-1-" + sfx
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, ownerProject, appID, appID+".test", "img:1",
		1, 512, 0, 80, machineID, deploymentID, "exec-1", firstOp,
		1, 0, []byte(`{"request":1}`), nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, project, app, deployment, operation string
	}{
		{"app project", otherProject, appID, deploymentID, "op-cross-project-" + sfx},
		{"machine app", ownerProject, "app-other-" + sfx, deploymentID, "op-cross-app-" + sfx},
		{"machine deployment", ownerProject, appID, "dep-other-" + sfx, "op-cross-dep-" + sfx},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.EnsureAppAndEnqueueCreate(ctx, tc.project, tc.app, tc.app+".test", "img:2",
				2, 1024, 0, 8080, machineID, tc.deployment, "exec-attacker", tc.operation,
				9, 4, []byte(`{"request":2}`), nil)
			if !errors.Is(err, ErrOwnershipConflict) || !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("want typed ownership/request conflict, got %v", err)
			}
			var n int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE id=$1`, tc.operation).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("conflict inserted operation %s", tc.operation)
			}
			m, err := s.GetMachine(ctx, machineID)
			if err != nil || m == nil {
				t.Fatal(err)
			}
			if m.AppID != appID || m.DeploymentID != deploymentID || m.CurrentExecutionID != "exec-1" || m.Generation != 1 {
				t.Fatalf("conflict mutated machine: %+v", m)
			}
		})
	}
}

func TestEnsureCreateRejectsExistingDeploymentForOtherApp(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-dep-owner-" + sfx
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if _, err := s.pool.Exec(ctx, `INSERT INTO apps(id, project_id, hostname) VALUES($1,$2,$3),($4,$2,$5)`,
		"app-dep-owner-"+sfx, project, "dep-owner-"+sfx+".test",
		"app-dep-other-"+sfx, "dep-other-"+sfx+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO deployments(id, app_id, generation, image_ref) VALUES($1,$2,1,'img')`,
		"dep-shared-"+sfx, "app-dep-owner-"+sfx); err != nil {
		t.Fatal(err)
	}

	opID := "op-dep-conflict-" + sfx
	_, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-dep-other-"+sfx, "dep-other-"+sfx+".test", "img",
		1, 512, 0, 80, "m-dep-conflict-"+sfx, "dep-shared-"+sfx, "exec-1", opID,
		1, 0, []byte(`{}`), nil)
	if !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("want deployment ownership conflict, got %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE id=$1`, opID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("conflict operation count=%d err=%v", n, err)
	}
}

func TestJSONEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []byte
		want bool
	}{
		{"same object different key order", []byte(`{"a":1,"b":"2"}`), []byte(`{"b":"2","a":1}`), true},
		{"protojson 64-bit ints as strings", []byte(`{"generation":"1","vcpu":"2"}`), []byte(`{"vcpu":"2","generation":"1"}`), true},
		{"different values", []byte(`{"a":1}`), []byte(`{"a":2}`), false},
		{"different types", []byte(`{"a":1}`), []byte(`{"a":"1"}`), false},
		{"missing key", []byte(`{"a":1}`), []byte(`{"a":1,"b":2}`), false},
		{"invalid json a", []byte(`{`), []byte(`{}`), false},
		{"invalid json b", []byte(`{}`), []byte(`}`), false},
	}
	for _, tc := range cases {
		if got := jsonEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: jsonEqual(%s, %s) = %v, want %v", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}

// testStore 返回跑过迁移的 PG store；未设置 FIREPAAS_TEST_POSTGRES 时跳过。
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run store tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return New(pool)
}

func cleanupProject(t *testing.T, s *Store, projectID string) {
	t.Helper()
	// pgx 带参 Exec 走扩展协议，不支持多语句拼接——必须逐条执行，
	// 否则清理静默失败，残留行会让下一个用例的固定 opID 撞主键。
	ctx := context.Background()
	stmts := []string{
		`DELETE FROM volume_attachments WHERE volume_id IN (SELECT id FROM volumes WHERE project_id=$1)`,
		`DELETE FROM volumes WHERE project_id=$1`,
		`DELETE FROM snapshot_references WHERE snapshot_id IN (SELECT id FROM snapshots WHERE project_id=$1)`,
		`DELETE FROM snapshots WHERE project_id=$1`,
		`DELETE FROM operations WHERE project_id=$1`,
		`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
		`DELETE FROM apps WHERE project_id=$1`,
		`DELETE FROM projects WHERE id=$1`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q, projectID); err != nil {
			t.Logf("cleanup %q: %v", q, err)
		}
	}
}

func TestBeginVolumeDeleteAndEnqueueIsAtomicAndIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-volume-delete-" + sfx
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	volumeID, opID := "vol-delete-"+sfx, "op-vol-delete-"+sfx
	if _, err := s.pool.Exec(ctx, `INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state) VALUES($1,$2,$3,'LOCAL_RW','node-1',1048576,'READY')`, volumeID, project, volumeID); err != nil {
		t.Fatal(err)
	}
	p := EnqueueOperationParams{OperationID: opID, ProjectID: project, Kind: "volume_delete", Request: []byte(`{"volume_id":"` + volumeID + `"}`), DispatchNodeID: "node-1"}
	for i := 0; i < 2; i++ {
		if _, err := s.BeginVolumeDeleteAndEnqueue(ctx, volumeID, p); err != nil {
			t.Fatal(err)
		}
	}
	var state string
	var operations int
	if err := s.pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM operations WHERE id=$2) FROM volumes WHERE id=$1`, volumeID, opID).Scan(&state, &operations); err != nil {
		t.Fatal(err)
	}
	if state != "DELETING" || operations != 1 {
		t.Fatalf("state=%s operations=%d", state, operations)
	}
}

// P1-2：终态 FAILED 的 delete 在同请求重入队时必须复活为 PENDING，
// 否则确定性 opID 的清理路径（R2/R5/R6）一次失败即永久卡死。
func TestEnqueueDeleteResurrectFailed(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-resurrect-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "resurrect-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	sfx := fmt.Sprintf("%d", os.Getpid())
	req := []byte(`{"machine_id":"m1","execution_id":"exec-1","generation":1,"operation_id":"opd-` + sfx + `"}`)
	op, err := s.EnqueueDelete(ctx, project, "m1", "exec-1", "opd-1-"+sfx, 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOperation(ctx, op.ID, "FAILED", nil, "agent unreachable"); err != nil {
		t.Fatal(err)
	}

	// 同请求重入队 → 复活为 PENDING（可被重新派发）。
	op2, err := s.EnqueueDelete(ctx, project, "m1", "exec-1", "opd-1-"+sfx, 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if op2.Status != "PENDING" {
		t.Fatalf("FAILED delete must resurrect to PENDING, got %s", op2.Status)
	}

	// 不同请求体仍必须拒绝（幂等冲突语义不变）。
	other := []byte(`{"machine_id":"m1","execution_id":"exec-1","generation":2,"operation_id":"opd-` + sfx + `"}`)
	if _, err := s.EnqueueDelete(ctx, project, "m1", "exec-1", "opd-1-"+sfx, 1, other); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}

	// SUCCEEDED 不复活（已收敛）。
	if err := s.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), ""); err != nil {
		t.Fatal(err)
	}
	op3, err := s.EnqueueDelete(ctx, project, "m1", "exec-1", "opd-1-"+sfx, 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if op3.Status != "SUCCEEDED" {
		t.Fatalf("SUCCEEDED delete must not resurrect, got %s", op3.Status)
	}
}

// P1-3：FailedCreateAttempts 只统计最近一次 SUCCEEDED create 之后的连续失败，
// 作为指数退避的输入。
func TestFailedCreateAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-attempts-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "attempts-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	sfx := fmt.Sprintf("%d", os.Getpid())
	ensure := func(opID, exec string, gen int64) {
		t.Helper()
		op, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-a", "h.local", "img:1",
			1, 512, 0, 80, "m-att-"+sfx, "dep-a", exec, opID+"-"+sfx, gen, 0, []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteOperation(ctx, op.ID, "FAILED", nil, "x"); err != nil {
			t.Fatal(err)
		}
	}
	succeed := func(opID, exec string, gen int64) {
		t.Helper()
		op, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-a", "h.local", "img:1",
			1, 512, 0, 80, "m-att-"+sfx, "dep-a", exec, opID+"-"+sfx, gen, 0, []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}

	ensure("op-a1", "exec-1", 1)
	ensure("op-a2", "exec-1", 1)
	if n, err := s.FailedCreateAttempts(ctx, "m-att-"+sfx); err != nil || n != 2 {
		t.Fatalf("want 2 attempts, got %d err=%v", n, err)
	}
	succeed("op-a3", "exec-1", 1)
	if n, err := s.FailedCreateAttempts(ctx, "m-att-"+sfx); err != nil || n != 0 {
		t.Fatalf("want 0 attempts after success, got %d err=%v", n, err)
	}
	ensure("op-a4", "exec-1", 1)
	if n, err := s.FailedCreateAttempts(ctx, "m-att-"+sfx); err != nil || n != 1 {
		t.Fatalf("want 1 attempt after new failure, got %d err=%v", n, err)
	}
}

// P1-3：machine 的 generation 单调不回退——换代重建后携带旧默认值（gen=1）
// 的重试不得拉低 fence 水位。
func TestEnsureCreateGenerationMonotonic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-monotic-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "monotonic-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	sfx := fmt.Sprintf("%d", os.Getpid())
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-m", "m.local", "img:1",
		1, 512, 0, 80, "m-mono-"+sfx, "dep-m", "exec-1", "op-m1-"+sfx, 1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	// 模拟换代重建已把水位推到 5（recreateMachine bump 路径）。
	if _, err := s.pool.Exec(ctx,
		`UPDATE machines SET generation=5 WHERE id=$1`, "m-mono-"+sfx); err != nil {
		t.Fatal(err)
	}
	// 用户/重试用 gen=1 重试：GREATEST 必须保住 5。
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-m", "m.local", "img:1",
		1, 512, 0, 80, "m-mono-"+sfx, "dep-m", "exec-9", "op-m2-"+sfx, 1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMachine(ctx, "m-mono-"+sfx)
	if err != nil || m == nil {
		t.Fatal(err)
	}
	if m.Generation != 5 {
		t.Fatalf("generation must stay monotonic at 5, got %d", m.Generation)
	}
	// 更高的 generation 正常推进。
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-m", "m.local", "img:1",
		1, 512, 0, 80, "m-mono-"+sfx, "dep-m", "exec-10", "op-m3-"+sfx, 6, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	m, _ = s.GetMachine(ctx, "m-mono-"+sfx)
	if m.Generation != 6 {
		t.Fatalf("generation must advance to 6, got %d", m.Generation)
	}
}

// M2 真机验收修复：换代（execution 变化）时旧 observed 必须作废，否则
// R8 的“已 RUNNING 即补账”会把重建循环里的 create 永远短路成 SUCCEEDED，
// agent 侧实际没有这台 machine，形成无限换代。
func TestMarkMachineObservedMissingPreservesLastObservation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-missing-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "missing-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	machineID := "m-missing-" + fmt.Sprint(os.Getpid())
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-missing", "missing.local", "img:1",
		1, 512, 0, 80, machineID, "dep-missing", "exec-1", "op-missing-"+fmt.Sprint(os.Getpid()),
		1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineObserved(ctx, machineID, "exec-1", "RUNNING", "10.0.0.1", "READY"); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetMachine(ctx, machineID)
	if err != nil || before == nil || before.LastObservedAt == nil {
		t.Fatalf("missing initial observation: %v", err)
	}
	if err := s.MarkMachineObservedMissing(ctx, machineID); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetMachine(ctx, machineID)
	if err != nil || after == nil {
		t.Fatal(err)
	}
	if after.ObservedState != "UNKNOWN" || after.LastObservedAt == nil || !after.LastObservedAt.Equal(*before.LastObservedAt) {
		t.Fatalf("missing mark must retain last successful observation, before=%+v after=%+v", before, after)
	}
}

func TestUpdateObservedRejectsStaleExecution(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-observed-cas-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "observed-cas-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	machineID := "m-observed-cas-" + fmt.Sprint(os.Getpid())
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-observed-cas", "cas.local", "img:1",
		1, 512, 0, 80, machineID, "dep-cas", "exec-current", "op-cas-"+fmt.Sprint(os.Getpid()),
		2, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineObserved(ctx, machineID, "exec-stale", "RUNNING", "10.0.0.1", "READY"); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		t.Fatal(err)
	}
	if m.ObservedState != "" || m.ObservedSlotIP != "" {
		t.Fatalf("stale execution overwrote observed state: %+v", m)
	}
}

func TestEnqueueRescueReplacementAtomic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-rescue-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "rescue-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	machineID := "m-rescue-" + fmt.Sprint(os.Getpid())
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-rescue", "rescue.local", "img:1",
		1, 512, 0, 80, machineID, "dep-rescue", "exec-old", "op-create-rescue-"+fmt.Sprint(os.Getpid()),
		1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineNodeAndObserved(ctx, machineID, "node-1", "exec-old", "RUNNING", "10.0.0.2", "READY"); err != nil {
		t.Fatal(err)
	}
	snapID := "snap-rescue-" + fmt.Sprint(os.Getpid())
	if _, err := s.CreateSnapshot(ctx, Snapshot{ID: snapID, ProjectID: project, SourceMachineID: machineID,
		SourceExecutionID: "exec-old", SourceGeneration: 1, Kind: "MEMORY", NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSnapshotArtifact(ctx, snapID, 1, "sha256:test", "none", "none", "", nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO routes(id,app_id,hostname,active_generation) VALUES($1,$2,$3,1)
		ON CONFLICT (hostname, port) DO NOTHING`, "route-rescue-"+fmt.Sprint(os.Getpid()), "app-rescue", "rescue.local"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO route_backends(route_id,generation,machine_id,execution_id)
		SELECT id,1,$2,$3 FROM routes WHERE app_id=$1`, "app-rescue", machineID, "exec-old"); err != nil {
		t.Fatal(err)
	}
	opID := "op-rescue-" + fmt.Sprint(os.Getpid())
	raw := []byte(fmt.Sprintf(`{"snapshot_id":%q}`, snapID))
	op, err := s.EnqueueRescueReplacement(ctx, RescueReplacementParams{ProjectID: project, MachineID: machineID,
		OldExecutionID: "exec-old", OldGeneration: 1, NewExecutionID: "exec-new", OperationID: opID,
		SnapshotID: snapID, Request: raw, DispatchNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	if op.ExecutionID != "exec-new" || op.Generation != 2 || op.Kind != "rescue" {
		t.Fatalf("unexpected rescue operation: %+v", op)
	}
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		t.Fatal(err)
	}
	if m.CurrentExecutionID != "exec-new" || m.Generation != 2 || m.ObservedState != "" || m.ObservedReadiness != "UNKNOWN" {
		t.Fatalf("unexpected replacement machine: %+v", m)
	}
	var backends int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM route_backends WHERE machine_id=$1`, machineID).Scan(&backends); err != nil || backends != 0 {
		t.Fatalf("route backends after rescue = %d, err=%v", backends, err)
	}
	if _, err := s.EnqueueRescueReplacement(ctx, RescueReplacementParams{ProjectID: project, MachineID: machineID,
		OldExecutionID: "exec-old", OldGeneration: 1, NewExecutionID: "exec-other", OperationID: opID + "-stale",
		SnapshotID: snapID, Request: raw, DispatchNodeID: "node-1"}); !errors.Is(err, ErrRescueConflict) {
		t.Fatalf("stale rescue CAS error = %v, want ErrRescueConflict", err)
	}
}

func TestEnsureCreateExecutionChangeClearsObserved(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-execclr-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "exec-clear-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	sfx := fmt.Sprintf("%d", os.Getpid())
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-c", "c.local", "img:1",
		1, 512, 0, 80, "m-clr-"+sfx, "dep-c", "exec-1", "op-c1-"+sfx, 1, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineNodeAndObserved(ctx, "m-clr-"+sfx, "n1", "exec-1",
		"RUNNING", "10.100.0.2", "UNCONFIGURED"); err != nil {
		t.Fatal(err)
	}

	// 换代重建：exec-1 → exec-2。旧 RUNNING 观测必须清空，节点回空。
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-c", "c.local", "img:1",
		1, 512, 0, 80, "m-clr-"+sfx, "dep-c", "exec-2", "op-c2-"+sfx, 2, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMachine(ctx, "m-clr-"+sfx)
	if err != nil || m == nil {
		t.Fatal(err)
	}
	if m.ObservedState != "" || m.ObservedSlotIP != "" || m.ObservedReadiness != "UNKNOWN" || m.NodeID != "" {
		t.Fatalf("execution change must clear observed: %+v", m)
	}

	// 同 execution 重试（用户幂等重试）不清 observed。
	if err := s.UpdateMachineNodeAndObserved(ctx, "m-clr-"+sfx, "n1", "exec-2",
		"RUNNING", "10.100.0.3", "UNCONFIGURED"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-c", "c.local", "img:1",
		1, 512, 0, 80, "m-clr-"+sfx, "dep-c", "exec-2", "op-c3-"+sfx, 2, 0, []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	m, _ = s.GetMachine(ctx, "m-clr-"+sfx)
	if m.ObservedState != "RUNNING" {
		t.Fatalf("same-execution retry must keep observed, got %+v", m)
	}
}
