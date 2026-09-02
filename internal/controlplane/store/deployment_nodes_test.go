// deployment_nodes_test.go：deployment 反亲和可见节点集合（D1 回归）：
// 同 tick 并发 create 的第二放置必须看到第一放置的在途 dispatch node，
// 否则 anti-affinity=DEPLOYMENT 过滤被跳过、双副本落同节点（多节点真机
// 验收复现：两次 placement candidates=2、无 degraded 事件、同分同节点）。
package store

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func seedDeploymentNodeRows(t *testing.T, s *Store, ctx context.Context, project, appID, depID string) {
	t.Helper()
	if err := s.EnsureProject(ctx, project, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas)
		VALUES($1,$2,$3,'img',1,512,2) ON CONFLICT (id) DO NOTHING`, appID, project, appID+".test"); err != nil {
		t.Fatal(err)
	}
}

// 已提交 machine 的 node_id 必须出现在集合中（原语义保持）。
func TestMachineNodesByDeploymentIncludesCommittedMachines(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project, appID, depID := "p-depnodes-"+sfx, "app-depnodes-"+sfx, "dep-depnodes-"+sfx
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedDeploymentNodeRows(t, s, ctx, project, appID, depID)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, image_ref, node_id, ingress_port)
		VALUES($1,$2,$3,0,$4,'CREATED',1,'exec-a',1,512,'img','node-a',80)`,
		"m-depnodes-a-"+sfx, appID, depID, "m-depnodes-a-"+sfx); err != nil {
		t.Fatal(err)
	}
	set, err := s.MachineNodesByDeployment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !set[depID]["node-a"] {
		t.Fatalf("committed machine node missing from deployment set: %v", set[depID])
	}
}

// 在途 create（PENDING/CLAIMED 且已定 dispatch node）必须计入：并发第二
// 放置依赖它把第一放置的节点排除出反亲和候选。
func TestMachineNodesByDeploymentIncludesInflightCreateDispatch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "x"
	project, appID, depID := "p-depnodes-"+sfx, "app-depnodes-"+sfx, "dep-depnodes-"+sfx
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedDeploymentNodeRows(t, s, ctx, project, appID, depID)
	machineID := "m-depnodes-b-" + sfx
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, image_ref, node_id, ingress_port)
		VALUES($1,$2,$3,1,$4,'CREATED',1,'exec-b',1,512,'img','',80)`,
		machineID, appID, depID, machineID); err != nil {
		t.Fatal(err)
	}
	// create op 已被调度到 node-b（dispatch 已提交、machine 行尚未写入 node_id）。
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request, dispatch_node_id, claimed_at)
		VALUES($1,$2,$3,'exec-b',1,'create',$1,'CLAIMED','{}','node-b',now())`,
		"op-depnodes-b-"+sfx, project, machineID); err != nil {
		t.Fatal(err)
	}
	set, err := s.MachineNodesByDeployment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !set[depID]["node-b"] {
		t.Fatalf("in-flight create dispatch node must be visible to anti-affinity: %v", set[depID])
	}
	// 终态 delete op 的 dispatch 不得计入。
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request, dispatch_node_id, claimed_at)
		VALUES($1,$2,$3,'exec-c',1,'delete',$1,'SUCCEEDED','{}','node-c',now())`,
		"op-depnodes-c-"+sfx, project, machineID); err != nil {
		t.Fatal(err)
	}
	set, err = s.MachineNodesByDeployment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if set[depID]["node-c"] {
		t.Fatalf("terminal delete dispatch node must not leak into deployment set: %v", set[depID])
	}
}

// D2 回归（scale down→up 墓碑复活）：app 对账对墓碑行必须能换新 execution
// 复活并入队 create；严格变体（用户直建/恢复守卫）必须继续拒绝复活。
func TestEnsureAppAndEnqueueCreateResurrectsTombstone(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "r"
	project, appID, depID := "p-resurrect-"+sfx, "app-resurrect-"+sfx, "dep-resurrect-"+sfx
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedDeploymentNodeRows(t, s, ctx, project, appID, depID)
	machineID := "m-resurrect-" + sfx
	// 墓碑行：scale down 后的形态（desired=DELETED, 换代执行已死）。
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, image_ref, node_id, ingress_port)
		VALUES($1,$2,$3,1,$4,'DELETED',3,'exec-dead',1,512,'img','node-x',80)`,
		machineID, appID, depID, machineID); err != nil {
		t.Fatal(err)
	}
	// 严格变体：拒绝复活（ADR-0026 守卫语义不回归）。
	if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, appID, appID+".test", "img", 1, 512, 1024, 80,
		machineID, depID, "exec-new-strict", "op-resurrect-strict-"+sfx, 1, 1, []byte(`{}`), nil); err == nil {
		t.Fatal("strict variant must reject tombstone resurrection")
	}
	// 复活变体：允许对账路径显式复活（换 execution、清 observed/node、generation 不回退）。
	op, err := s.EnsureAppAndEnqueueCreateResurrect(ctx, project, appID, appID+".test", "img", 1, 512, 1024, 80,
		machineID, depID, "exec-new", "op-resurrect-ok-"+sfx, 1, 1, []byte(`{}`), nil)
	if err != nil {
		t.Fatalf("resurrect variant must accept tombstone: %v", err)
	}
	if op.Status != "PENDING" {
		t.Fatalf("resurrected create op status = %s, want PENDING", op.Status)
	}
	var desired, node, exec string
	var gen int64
	if err := s.pool.QueryRow(ctx,
		`SELECT desired_state, node_id, current_execution_id, generation FROM machines WHERE id=$1`, machineID).
		Scan(&desired, &node, &exec, &gen); err != nil {
		t.Fatal(err)
	}
	if desired != "CREATED" || node != "" || exec != "exec-new" || gen < 3 {
		t.Fatalf("tombstone not reset: desired=%s node=%q exec=%s gen=%d", desired, node, exec, gen)
	}
}
