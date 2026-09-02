// reconcile_copies_test.go：(machine, 节点) 多副本对账的行为回归（多节点
// 真机验收 finding D3/D5/D6）：
//   - agent 持旧代/外来代副本 → reap 必须 pin 在“观察到副本的节点”，
//     而不是 PG 登记的 node_id（否则围栏拒绝+无限重试+阻塞 evacuate）；
//   - desired=DELETED 且多节点并存分歧 execution → 分歧副本必须收到
//     按其自身 fencing generation 的 pinned delete（否则删除幂等成功、
//     真实在跑的 execution 泄漏）。
package controller

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func mustGetMachine(t *testing.T, s *store.Store, ctx context.Context, id string) store.Machine {
	t.Helper()
	m, err := s.GetMachine(ctx, id)
	if err != nil || m == nil {
		t.Fatalf("machine %s: %v", id, err)
	}
	return *m
}

// cleanupCopiesProject 清理本文件用例的 project 域（共享测试库可重复运行，
// 与 store.cleanupProject 同模式）。
func cleanupCopiesProject(t *testing.T, s *store.Store, project string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`DELETE FROM operations WHERE project_id=$1`,
		`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
		`DELETE FROM apps WHERE project_id=$1`,
		`DELETE FROM projects WHERE id=$1`,
	} {
		if _, err := s.Pool().Exec(ctx, q, project); err != nil {
			t.Logf("cleanup %q: %v", q, err)
		}
	}
}

// TestProcessAgentMachineStaleCopyPinnedToReportingNode：failover+旧节点恢复
// 场景——PG 当前 execution 在 node-1，旧节点 node-2 上报旧代副本；reap op
// 必须携带 execution=旧代、dispatch=node-2。
func TestProcessAgentMachineStaleCopyPinnedToReportingNode(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "p"
	project, appID := "t-copies-p"+sfx, "t-copies-app"+sfx
	machineID, depID := "t-copies-m"+sfx, "dep-copies"+sfx
	cleanupCopiesProject(t, s, project)
	t.Cleanup(func() { cleanupCopiesProject(t, s, project) })
	seedR2App(t, s, ctx, project, appID)
	insertR2Machine(t, s, ctx, appID, machineID, depID, "exec-new-"+sfx, 2, "node-1")

	nm, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm.Close()
	c := newR2Controller(t, nm, s)

	stale := &pb.Machine{
		MachineId: machineID, ExecutionId: "exec-old-" + sfx, Generation: 1,
		State: pb.MachineState_RUNNING,
	}
	c.processAgentMachine(ctx, stale, nodeView{agentID: "node-2"})

	var opID, exec, dispatch, kind string
	err = s.Pool().QueryRow(ctx, `
		SELECT id, execution_id, coalesce(dispatch_node_id,''), kind FROM operations
		WHERE machine_id=$1 ORDER BY created_at DESC LIMIT 1`, machineID).
		Scan(&opID, &exec, &dispatch, &kind)
	if err != nil {
		t.Fatalf("stale copy must enqueue a pinned reap op: %v", err)
	}
	if exec != "exec-old-"+sfx {
		t.Fatalf("reap must target the stale execution, got %q", exec)
	}
	if dispatch != "node-2" {
		t.Fatalf("reap must be pinned to the reporting node, got %q (want node-2)", dispatch)
	}
	if kind != "reap" && kind != "delete" {
		t.Fatalf("unexpected op kind %q", kind)
	}
}

// TestProcessPGMachineDesiredDeletedReapsForeignCopy：desired=DELETED 且 PG 记录
// 的 execution 已不在任何节点（分歧换代后），仅 node-2 上有外来副本——delete
// 必须 pin 到 node-2 并按副本观测 generation 派发，否则泄漏 VM。
func TestProcessPGMachineDesiredDeletedReapsForeignCopy(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "f"
	project, appID := "t-copies-p"+sfx, "t-copies-app"+sfx
	machineID, depID := "t-copies-m"+sfx, "dep-copies"+sfx
	cleanupCopiesProject(t, s, project)
	t.Cleanup(func() { cleanupCopiesProject(t, s, project) })
	seedR2App(t, s, ctx, project, appID)
	insertR2Machine(t, s, ctx, appID, machineID, depID, "exec-pg-"+sfx, 2, "node-1")
	if _, err := s.Pool().Exec(ctx, `UPDATE machines SET desired_state='DELETED' WHERE id=$1`, machineID); err != nil {
		t.Fatal(err)
	}

	nm, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm.Close()
	c := newR2Controller(t, nm, s)

	copies := map[string]*pb.Machine{
		"node-2": {
			MachineId: machineID, ExecutionId: "exec-foreign-" + sfx, Generation: 5,
			State: pb.MachineState_RUNNING,
		},
	}
	c.processPGMachine(ctx, mustGetMachine(t, s, ctx, machineID), copies)

	var exec, dispatch string
	var gen int64
	err = s.Pool().QueryRow(ctx, `
		SELECT execution_id, coalesce(dispatch_node_id,''), generation FROM operations
		WHERE machine_id=$1 ORDER BY created_at DESC LIMIT 1`, machineID).
		Scan(&exec, &dispatch, &gen)
	if err != nil {
		t.Fatalf("foreign copy must be reaped: %v", err)
	}
	if exec != "exec-foreign-"+sfx || dispatch != "node-2" || gen != 5 {
		t.Fatalf("foreign reap = (exec=%q,dispatch=%q,gen=%d), want (exec-foreign,node-2,5)", exec, dispatch, gen)
	}
}

// 崩溃恢复账本里的死条目仍占用实例名（复验实证：不清则同节点同名重建
// 永久撞名），必须下 pinned cleanup（re-arm 到真删除）；但它不是存活外来
// 副本，不得阻塞 R3 换代重建。
func TestDeadLedgerCopyReapedButDoesNotBlockRebuild(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "d"
	project, appID := "t-copies-p"+sfx, "t-copies-app"+sfx
	machineID, depID := "t-copies-m"+sfx, "dep-copies"+sfx
	cleanupCopiesProject(t, s, project)
	t.Cleanup(func() { cleanupCopiesProject(t, s, project) })
	seedR2App(t, s, ctx, project, appID)
	// node_id=''：测试 controller 无 node 视图，避免 R4（节点失联分支）
	// 抢先截获；这里验的是死条目不再走 foreignCount 早退。
	insertR2Machine(t, s, ctx, appID, machineID, depID, "exec-new-"+sfx, 2, "")

	nm, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm.Close()
	c := newR2Controller(t, nm, s)

	// 死账本条目：非当前 execution + UNSPECIFIED（agent 重启后状态未知）。
	dead := &pb.Machine{
		MachineId: machineID, ExecutionId: "exec-dead-" + sfx, Generation: 1,
		State: pb.MachineState_MACHINE_STATE_UNSPECIFIED,
	}
	c.processAgentMachine(ctx, dead, nodeView{agentID: "node-2"})
	// 必须下 pin 到 node-2 的清理 op：死条目的实例名仍占网络/slot 分配，
	// 不清则同节点同名重建永久撞名（复验实证）；re-arm 保证真删到位。
	var dispatch string
	if err := s.Pool().QueryRow(ctx,
		`SELECT coalesce(dispatch_node_id,'') FROM operations WHERE machine_id=$1 ORDER BY created_at DESC LIMIT 1`,
		machineID).Scan(&dispatch); err != nil {
		t.Fatalf("dead ledger copy must enqueue a cleanup op: %v", err)
	}
	if dispatch != "node-2" {
		t.Fatalf("cleanup op must pin to reporting node, got %q", dispatch)
	}

	// 模拟 cleanup 真删成功且下一轮 List 不再返回死条目：终态 reap 是 R3
	// 重建的充分前置；若仍把已消失的 foreign copy 缓存在 seen 中会早退。
	if _, err := s.Pool().Exec(ctx, `
		UPDATE operations SET status='SUCCEEDED', updated_at=now()-interval '1 hour'
		WHERE machine_id=$1`, machineID); err != nil {
		t.Fatal(err)
	}
	m := mustGetMachine(t, s, ctx, machineID)
	c.processPGMachine(ctx, m, nil)
	// R3 换代重建：recreateMachine 会 bump generation 并下单新 create。
	var gen int64
	var exec string
	if err := s.Pool().QueryRow(ctx,
		`SELECT generation,current_execution_id FROM machines WHERE id=$1`, machineID).
		Scan(&gen, &exec); err != nil {
		t.Fatal(err)
	}
	if gen <= 2 && exec == "exec-new-"+sfx {
		t.Fatalf("dead ledger copy blocked R3 rebuild: gen=%d exec=%s", gen, exec)
	}
}

// TestOrphanReapReArmedAfterIneffectiveTerminal：agent 恢复窗口里 delete 无
// 效果返回成功（实例后才恢复列出）——同幂等键命中终态 op 时，必须派生新
// op 重执，否则副本及其实例名占用（网络分配）永久泄漏（复验实证）。
func TestOrphanReapReArmedAfterIneffectiveTerminal(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid()) + "e"
	project, appID := "t-copies-p"+sfx, "t-copies-app"+sfx
	machineID, depID := "t-copies-m"+sfx, "dep-copies"+sfx
	cleanupCopiesProject(t, s, project)
	t.Cleanup(func() { cleanupCopiesProject(t, s, project) })
	seedR2App(t, s, ctx, project, appID)
	insertR2Machine(t, s, ctx, appID, machineID, depID, "exec-new-"+sfx, 2, "node-1")

	nm, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm.Close()
	c := newR2Controller(t, nm, s)

	stale := &pb.Machine{
		MachineId: machineID, ExecutionId: "exec-old-" + sfx, Generation: 1,
		State: pb.MachineState_RUNNING,
	}
	// 第一次：按下未终态 op。
	c.processAgentMachine(ctx, stale, nodeView{agentID: "node-2"})
	var firstOp string
	if err := s.Pool().QueryRow(ctx,
		`SELECT id FROM operations WHERE machine_id=$1 AND status IN ('PENDING','CLAIMED')`, machineID).
		Scan(&firstOp); err != nil {
		t.Fatalf("first reap op: %v", err)
	}
	// 终态化（模拟效果落空的 SUCCEEDED）。
	if _, err := s.Pool().Exec(ctx,
		`UPDATE operations SET status='SUCCEEDED', updated_at=now() WHERE id=$1`, firstOp); err != nil {
		t.Fatal(err)
	}
	// 处理完 op 后同一副本仍被观测 → 必须派生新 op。
	c.processAgentMachine(ctx, stale, nodeView{agentID: "node-2"})
	var secondOp string
	if err := s.Pool().QueryRow(ctx,
		`SELECT id FROM operations WHERE machine_id=$1 AND status IN ('PENDING','CLAIMED')`, machineID).
		Scan(&secondOp); err != nil {
		t.Fatalf("re-armed reap op: %v", err)
	}
	if secondOp == firstOp {
		t.Fatalf("re-arm must derive a new op id, got same %s", secondOp)
	}
}
