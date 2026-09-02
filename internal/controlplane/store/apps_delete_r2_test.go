// apps_delete_r2_test.go：R2 评审——app 删除原子化（墓碑 + 入队单事务）、
// 崩溃后重试收敛（中途崩溃/部分入队）、终态 operations 保留窗清理。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// buildTestDeleteOp 复刻 cmd/api deleteApp 的载荷构造（同幂等键约定）。
func buildTestDeleteOp(m Machine) AppDeleteOp {
	opID := UserDeleteOpID(m.ID, m.CurrentExecutionID)
	raw, _ := protojson.Marshal(&pb.DeleteMachineRequest{
		MachineId: m.ID, ExecutionId: m.CurrentExecutionID,
		Generation: uint64(m.Generation), OperationId: opID,
	})
	return AppDeleteOp{
		MachineID: m.ID, ExecutionID: m.CurrentExecutionID,
		Generation: m.Generation, OperationID: opID, Request: raw,
	}
}

// seedTwoMachines 造 app + 两台不同 replica_ordinal 的 machine（seedMachine
// 只支持单机）。
func seedTwoMachines(t *testing.T, s *Store, project, appID, depID string, pairs [][2]string) {
	t.Helper()
	ctx := context.Background()
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES($1,$2,$3,'img',1,512,1,1) ON CONFLICT (id) DO NOTHING`,
		appID, project, appID+".test"); err != nil {
		t.Fatal(err)
	}
	for i, p := range pairs {
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
				desired_state, generation, current_execution_id, requested_vcpu,
				requested_mem_mib, image_ref, node_id, ingress_port)
			VALUES($1,$2,$3,$4,$5,'CREATED',1,$6,1,512,'img','n-x',8080)`,
			p[0], appID, depID, i, p[0]+".test", p[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func countDeleteOps(t *testing.T, s *Store, project string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM operations WHERE project_id=$1 AND kind='delete'`,
		project).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 正常路径：单事务完成墓碑 + 两台副本的 delete 入队。
func TestSoftDeleteAppAndEnqueueDeletesAtomic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-appdel-" + sfx
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedTwoMachines(t, s, project, "app1-"+sfx, "dep1",
		[][2]string{{"m1-" + sfx, "e1-" + sfx}, {"m2-" + sfx, "e2-" + sfx}})

	res, err := s.SoftDeleteAppAndEnqueueDeletes(ctx, "app1-"+sfx, buildTestDeleteOp)
	if err != nil {
		t.Fatal(err)
	}
	if res.AlreadyDeleted || res.Pending != 2 {
		t.Fatalf("result = %+v, want {false 2}", res)
	}
	app, err := s.GetApp(ctx, "app1-"+sfx)
	if err != nil || app == nil || !app.Deleted {
		t.Fatalf("app must be tombstoned: %+v err=%v", app, err)
	}
	if n := countDeleteOps(t, s, project); n != 2 {
		t.Fatalf("delete ops = %d, want 2", n)
	}

	// crash-after-commit 模型：全部已提交后的重试必须幂等（行数不变，
	// AlreadyDeleted=true；仍报告未收敛机器数）。
	res, err = s.SoftDeleteAppAndEnqueueDeletes(ctx, "app1-"+sfx, buildTestDeleteOp)
	if err != nil {
		t.Fatal(err)
	}
	if !res.AlreadyDeleted || res.Pending != 2 {
		t.Fatalf("retry result = %+v, want {true 2}", res)
	}
	if n := countDeleteOps(t, s, project); n != 2 {
		t.Fatalf("retry must not duplicate ops, count = %d", n)
	}
}

// 入队中途崩溃恢复（crash-mid-enqueue 残余）：墓碑存在、但只下发过部分
// delete 的状态（历史实现可把行留在这种形态）；重试必须补发缺失的那台。
func TestSoftDeleteAppRetryReenqueuesAfterPartialEnqueue(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-appdel2-" + sfx
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedTwoMachines(t, s, project, "app2-"+sfx, "dep2",
		[][2]string{{"m3-" + sfx, "e3-" + sfx}, {"m4-" + sfx, "e4-" + sfx}})

	if _, err := s.SoftDeleteAppAndEnqueueDeletes(ctx, "app2-"+sfx, buildTestDeleteOp); err != nil {
		t.Fatal(err)
	}
	// 模拟「旧实现入队到第二台之前崩溃」的残余：m4 的 delete 不在队里。
	m4, err := s.GetMachine(ctx, "m4-"+sfx)
	if err != nil || m4 == nil {
		t.Fatal("m4 missing")
	}
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM operations WHERE id=$1`,
		UserDeleteOpID(m4.ID, m4.CurrentExecutionID)); err != nil {
		t.Fatal(err)
	}
	if n := countDeleteOps(t, s, project); n != 1 {
		t.Fatalf("residue setup failed, delete ops = %d, want 1", n)
	}

	// 重试 DELETE：墓碑已在（already_deleted），缺失的 m4 delete 重新入队，
	// 幂等键与原驱逐キー一致。
	res, err := s.SoftDeleteAppAndEnqueueDeletes(ctx, "app2-"+sfx, buildTestDeleteOp)
	if err != nil {
		t.Fatal(err)
	}
	if !res.AlreadyDeleted || res.Pending != 2 {
		t.Fatalf("retry result = %+v, want {true 2}", res)
	}
	if n := countDeleteOps(t, s, project); n != 2 {
		t.Fatalf("retry must restore the missing op, count = %d", n)
	}
}

// crash-before-commit 模型：同幂等键不同请求体的历史脏数据（旧版裸 opID）
// 冲突时，整个事务回滚——墓碑与其他副本的入队都不允许半提交。
func TestSoftDeleteAppConflictRollsBackEverything(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-appdel3-" + sfx
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedTwoMachines(t, s, project, "app3-"+sfx, "dep3",
		[][2]string{{"m5-" + sfx, "e5-" + sfx}, {"m6-" + sfx, "e6-" + sfx}})

	// 预置一条同键不同请求体的历史行（m6）。
	m6, err := s.GetMachine(ctx, "m6-"+sfx)
	if err != nil || m6 == nil {
		t.Fatal("m6 missing")
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request)
		VALUES($1,$2,$3,$4,1,'delete',$1,'PENDING','{}'::jsonb)`,
		UserDeleteOpID(m6.ID, m6.CurrentExecutionID), project, m6.ID, m6.CurrentExecutionID); err != nil {
		t.Fatal(err)
	}

	_, err = s.SoftDeleteAppAndEnqueueDeletes(ctx, "app3-"+sfx, buildTestDeleteOp)
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("expected ErrRequestConflict, got %v", err)
	}
	// 整体回滚：app 未墓碑、m5 的 delete 未入库。
	app, err := s.GetApp(ctx, "app3-"+sfx)
	if err != nil || app == nil || app.Deleted {
		t.Fatalf("app must not be tombstoned after rollback: %+v err=%v", app, err)
	}
	if n := countDeleteOps(t, s, project); n != 1 {
		t.Fatalf("rollback must leave only the pre-seeded row, count = %d, want 1", n)
	}
}

// 不存在的 app → ErrNotFound（handler 404）。
func TestSoftDeleteAppNotFound(t *testing.T) {
	s := testStore(t)
	if _, err := s.SoftDeleteAppAndEnqueueDeletes(context.Background(),
		"app-missing", buildTestDeleteOp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// 终态 operations 保留窗：只有「终态 + 早于窗口」的行被删；在途与窗口内
// 终态行保留（幂等键重放窗口）。
func TestDeleteTerminalOperationsRetention(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-opret-" + sfx
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	insert := func(id, status, age string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request, updated_at)
			VALUES($1,$4,'m','e',1,'create',$1,$2,'{}'::jsonb, now() - $3::interval)`,
			id, status, age, project); err != nil {
			t.Fatal(err)
		}
	}
	insert("op-ret-old-done-"+sfx, "SUCCEEDED", "48 hours")
	insert("op-ret-old-fail-"+sfx, "FAILED", "48 hours")
	insert("op-ret-fresh-done-"+sfx, "SUCCEEDED", "1 hour")
	insert("op-ret-pending-"+sfx, "PENDING", "48 hours")
	insert("op-ret-claimed-"+sfx, "CLAIMED", "48 hours")

	n, err := s.DeleteTerminalOperationsOlderThan(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("purged = %d, want 2 (old SUCCEEDED + old FAILED)", n)
	}
	var remain int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM operations WHERE project_id=$1`, project).Scan(&remain); err != nil {
		t.Fatal(err)
	}
	if remain != 3 {
		t.Fatalf("remaining = %d, want 3 (fresh terminal + PENDING + CLAIMED)", remain)
	}
}

// UpdateMachineObservedWithFenceCAS：fence 漂移时行不被触碰（false）；
// fence 一致才落账（true）。
func TestUpdateMachineObservedWithFenceCAS(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := "cas-" + fmt.Sprint(os.Getpid())
	project := "p-" + sfx
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedTwoMachines(t, s, project, "app-"+sfx, "dep", [][2]string{{"m-" + sfx, "e2-" + sfx}})
	if _, err := s.pool.Exec(ctx, `UPDATE machines SET generation=2 WHERE id=$1`, "m-"+sfx); err != nil {
		t.Fatal(err)
	}

	ok, err := s.UpdateMachineObservedWithFenceCAS(ctx, "m-"+sfx, "n-x", "e1-"+sfx, 1,
		"PAUSED", "", "READY")
	if err != nil || ok {
		t.Fatalf("stale fence must not update (ok=%v err=%v)", ok, err)
	}
	ok, err = s.UpdateMachineObservedWithFenceCAS(ctx, "m-"+sfx, "n-x", "e2-"+sfx, 2,
		"PAUSED", "", "READY")
	if err != nil || !ok {
		t.Fatalf("current fence must update (ok=%v err=%v)", ok, err)
	}
	m, err := s.GetMachine(ctx, "m-"+sfx)
	if err != nil || m == nil || m.ObservedState != "PAUSED" {
		t.Fatalf("observed must be PAUSED after CAS: %+v err=%v", m, err)
	}
}
