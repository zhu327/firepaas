package machine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kernel/hypeman/lib/instances"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// fakeSlots 是 slotManager 的可失败替身（staged delete 测试用）。
type fakeSlots struct {
	releaseErr   error
	releaseCalls int
	released     []string
	attachCalls  int
}

func (f *fakeSlots) Attach(context.Context, string, string, string) (slot.Slot, error) {
	f.attachCalls++
	return slot.Slot{}, nil
}

func (f *fakeSlots) Release(_ context.Context, machineID string) error {
	f.releaseCalls++
	if f.releaseErr != nil {
		return f.releaseErr
	}
	f.released = append(f.released, machineID)
	return nil
}

func (f *fakeSlots) SlotFor(machineID string) (slot.Slot, bool) { return slot.Slot{}, false }

// TestDeletePhasesContinuePastRuntimeNotFound：R2-3——VM NotFound 只代表
// runtime 阶段完成，slot release 等后续阶段照常执行；阶段失败聚合为可重试
// 错误，重试在同一 machine 上补齐失败阶段后收敛成功。
func TestDeletePhasesContinuePastRuntimeNotFound(t *testing.T) {
	ctx := context.Background()
	slots := &fakeSlots{releaseErr: errors.New("kernel busy")}
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, slots, nil)

	// 实例不存在（GetInstance → NotFound）→ runtime 阶段视为完成；
	// slot release 失败：错误聚合返回，不静默吞掉。
	err := a.Delete(ctx, "m-missing", "", 1)
	if err == nil || !strings.Contains(err.Error(), "slot release") {
		t.Fatalf("slot release failure must be surfaced as retryable error: %v", err)
	}
	if slots.releaseCalls != 1 {
		t.Fatalf("slot release must run despite runtime NotFound: calls=%d", slots.releaseCalls)
	}
	// 重试（failure 修复后）：同一组阶段重放，全部幂等收敛。
	slots.releaseErr = nil
	if err := a.Delete(ctx, "m-missing", "", 1); err != nil {
		t.Fatalf("retry must complete cleanup: %v", err)
	}
	if slots.releaseCalls != 2 || len(slots.released) != 1 || slots.released[0] != "m-missing" {
		t.Fatalf("retry did not complete slot cleanup: calls=%d released=%v",
			slots.releaseCalls, slots.released)
	}
}

// TestDeletePhaseFailureDoesNotBlockRuntimeRemoval：runtime 删除成功而后续
// 阶段失败时，实例依然被删（错误照旧上报，重试补做剩余阶段）。
func TestDeletePhaseFailureDoesNotBlockRuntimeRemoval(t *testing.T) {
	ctx := context.Background()
	slots := &fakeSlots{releaseErr: errors.New("kernel busy")}
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, slots, nil)
	if _, err := im.CreateInstance(ctx, instances.CreateInstanceRequest{Name: "m1"}); err != nil {
		t.Fatal(err)
	}
	err := a.Delete(ctx, "m1", "", 1)
	if err == nil || !strings.Contains(err.Error(), "slot release") {
		t.Fatalf("phase failure must surface: %v", err)
	}
	if im.deleted != "internal-1" {
		t.Fatalf("runtime phase must complete before later phases fail: deleted=%q", im.deleted)
	}
}

// TestDeleteExecutionMismatchAbortsAllPhases：execution 不匹配整体中止——
// 当前实例属于更新的 execution，任何清理都会误伤 live VM。
func TestDeleteExecutionMismatchAbortsAllPhases(t *testing.T) {
	ctx := context.Background()
	slots := &fakeSlots{}
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, slots, nil)
	if _, err := im.CreateInstance(ctx, instances.CreateInstanceRequest{
		Name: "m1",
		Tags: map[string]string{tagExecution: "exec-new"},
	}); err != nil {
		t.Fatal(err)
	}
	err := a.Delete(ctx, "m1", "exec-old", 2)
	if err == nil || !strings.Contains(err.Error(), "execution mismatch") {
		t.Fatalf("execution mismatch must abort: %v", err)
	}
	if im.deleted != "" || slots.releaseCalls != 0 {
		t.Fatalf("no phase may run on execution mismatch: deleted=%q releaseCalls=%d",
			im.deleted, slots.releaseCalls)
	}
}

// TestConvergePauseResume：claimed lifecycle 的收敛观测——已处目标态即认领，
// 未收敛返回 Found=false 由协议层重跑 effect；execution 不匹配是错误。
func TestConvergePauseResume(t *testing.T) {
	ctx := context.Background()
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	inst, err := im.CreateInstance(ctx, instances.CreateInstanceRequest{
		Name: "m1",
		Tags: map[string]string{tagExecution: "exec-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 替身初始为 Initializing；restore 到 Running 以模拟运行中的实例。
	if _, err := im.RestoreInstance(ctx, inst.Id); err != nil {
		t.Fatal(err)
	}

	// Running → pause 未收敛；resume 已收敛。
	if _, found, err := a.ConvergePause(ctx, "m1", "exec-1"); err != nil || found {
		t.Fatalf("running instance must not converge pause: found=%v err=%v", found, err)
	}
	m, found, err := a.ConvergeResume(ctx, "m1", "exec-1")
	if err != nil || !found || m.GetState() != pb.MachineState_RUNNING {
		t.Fatalf("running instance must converge resume: found=%v state=%v err=%v",
			found, m.GetState(), err)
	}

	// Standby → pause 收敛、resume 未收敛。
	if _, err := im.StandbyInstance(ctx, inst.Id, instances.StandbyInstanceRequest{}); err != nil {
		t.Fatal(err)
	}
	m, found, err = a.ConvergePause(ctx, "m1", "exec-1")
	if err != nil || !found || m.GetState() != pb.MachineState_PAUSED {
		t.Fatalf("standby instance must converge pause: found=%v state=%v err=%v",
			found, m.GetState(), err)
	}
	if _, found, err := a.ConvergeResume(ctx, "m1", "exec-1"); err != nil || found {
		t.Fatalf("standby instance must not converge resume: found=%v err=%v", found, err)
	}

	// 不存在 → Found=false（协议层重跑 effect，由 effect 给出 NotFound 终态错误）。
	if _, found, err := a.ConvergePause(ctx, "m-missing", ""); err != nil || found {
		t.Fatalf("missing machine convergence: found=%v err=%v", found, err)
	}
	// execution 不匹配 → 错误（旧代操作不得触碰新代实例）。
	if _, _, err := a.ConvergeResume(ctx, "m1", "exec-other"); err == nil ||
		!strings.Contains(err.Error(), "execution mismatch") {
		t.Fatalf("execution mismatch must be an error: %v", err)
	}
}
