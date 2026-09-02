package server

import (
	"context"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// R2-2：Pause/Resume 走 claimed mutation——claim 在途（崩溃窗口）时从实例
// 实际状态收敛，同 op 重放幂等。测试直接把 fake 实例置于目标态并向与
// server 共享的 ledger 写入未完成 claim，等价“效果已生效、Complete 未
// 落盘”的崩溃点。
func TestLifecycleClaimInFlightConverges(t *testing.T) {
	ctx := context.Background()

	t.Run("pause claim in flight + instance already paused converges", func(t *testing.T) {
		h := newCreateRecoveryHarness(t)
		srv, _ := h.open(t)
		if _, err := srv.CreateMachine(ctx, createReq("m-conv-p", 1, "op-conv-p-create")); err != nil {
			t.Fatal(err)
		}
		req := &pb.PauseMachineRequest{Operation: &pb.MachineOperationRequest{
			MachineId: "m-conv-p", ExecutionId: "exec-op-conv-p-create", Generation: 1,
			OperationId: "op-conv-p-pause",
		}}
		// 崩溃窗口：claim 落盘 + standby 已生效，Complete 未落盘。
		if _, _, err := srv.ledger.Begin(state.Record{
			OperationID: "op-conv-p-pause", MachineID: "m-conv-p", ExecutionID: "exec-op-conv-p-create",
			Generation: 1, Kind: "machine.lifecycle", RequestHash: hashRequest(req),
		}); err != nil {
			t.Fatal(err)
		}
		inst, err := h.fake.GetInstance(ctx, "m-conv-p")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.fake.StandbyInstance(ctx, inst.Id, instances.StandbyInstanceRequest{}); err != nil {
			t.Fatal(err)
		}
		before := h.fake.standbyCalls
		m, err := srv.PauseMachine(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if m.GetState() != pb.MachineState_PAUSED {
			t.Fatalf("converged pause state = %v, want PAUSED", m.GetState())
		}
		// 收敛路径不重跑 effect。
		if h.fake.standbyCalls != before {
			t.Fatalf("converged claim reran standby effect: %d -> %d", before, h.fake.standbyCalls)
		}
		// 同 op 重放按记录结果返回（幂等）。
		m2, err := srv.PauseMachine(ctx, req)
		if err != nil || m2.GetState() != pb.MachineState_PAUSED {
			t.Fatalf("pause replay: %v %v", m2.GetState(), err)
		}
	})

	t.Run("resume claim in flight + instance already running converges", func(t *testing.T) {
		h := newCreateRecoveryHarness(t)
		srv, _ := h.open(t)
		if _, err := srv.CreateMachine(ctx, createReq("m-conv-r", 1, "op-conv-r-create")); err != nil {
			t.Fatal(err)
		}
		req := &pb.ResumeMachineRequest{Operation: &pb.MachineOperationRequest{
			MachineId: "m-conv-r", ExecutionId: "exec-op-conv-r-create", Generation: 1,
			OperationId: "op-conv-r-resume",
		}}
		if _, _, err := srv.ledger.Begin(state.Record{
			OperationID: "op-conv-r-resume", MachineID: "m-conv-r", ExecutionID: "exec-op-conv-r-create",
			Generation: 1, Kind: "machine.lifecycle", RequestHash: hashRequest(req),
		}); err != nil {
			t.Fatal(err)
		}
		inst, err := h.fake.GetInstance(ctx, "m-conv-r")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.fake.RestoreInstance(ctx, inst.Id); err != nil {
			t.Fatal(err)
		}
		before := h.fake.restoreCalls
		m, err := srv.ResumeMachine(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if m.GetState() != pb.MachineState_RUNNING {
			t.Fatalf("converged resume state = %v, want RUNNING", m.GetState())
		}
		if h.fake.restoreCalls != before {
			t.Fatalf("converged claim reran restore effect: %d -> %d", before, h.fake.restoreCalls)
		}
	})

	t.Run("claim in flight without convergence re-runs effect", func(t *testing.T) {
		h := newCreateRecoveryHarness(t)
		srv, _ := h.open(t)
		if _, err := srv.CreateMachine(ctx, createReq("m-conv-n", 1, "op-conv-n-create")); err != nil {
			t.Fatal(err)
		}
		req := &pb.PauseMachineRequest{Operation: &pb.MachineOperationRequest{
			MachineId: "m-conv-n", ExecutionId: "exec-op-conv-n-create", Generation: 1,
			OperationId: "op-conv-n-pause",
		}}
		// claim 在途但实例仍 Running（崩溃发生在 effect 生效之前）→ 重跑 effect。
		if _, _, err := srv.ledger.Begin(state.Record{
			OperationID: "op-conv-n-pause", MachineID: "m-conv-n", ExecutionID: "exec-op-conv-n-create",
			Generation: 1, Kind: "machine.lifecycle", RequestHash: hashRequest(req),
		}); err != nil {
			t.Fatal(err)
		}
		before := h.fake.standbyCalls
		m, err := srv.PauseMachine(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if m.GetState() != pb.MachineState_PAUSED {
			t.Fatalf("re-run pause state = %v, want PAUSED", m.GetState())
		}
		if h.fake.standbyCalls != before+1 {
			t.Fatalf("non-converged claim must re-run effect exactly once: %d -> %d",
				before, h.fake.standbyCalls)
		}
		// 同 op 重放不再重跑。
		if _, err := srv.PauseMachine(ctx, req); err != nil {
			t.Fatal(err)
		}
		if h.fake.standbyCalls != before+1 {
			t.Fatalf("replay reran effect: %d", h.fake.standbyCalls)
		}
	})
}
