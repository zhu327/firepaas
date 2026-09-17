// wave0_test.go：Wave0（P0 正确性修正）的行为测试——
//   - W0.1：节点 gauge 按 HEALTHY|DRAINING|UNHEALTHY|UNKNOWN 计数（HEALTHY
//     唯一健康；DRAINING 单独计数）；machines_observed 按轮 ResetFamily 后
//     旧 state 不残留；
//   - W0.3：ctx 取消时 dispatchBounded 投喂不再阻塞，快速返回。
package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
)

// W0.1：4 种 status 的 gauge 计数——HEALTHY 唯一健康；DRAINING 只计入
// draining，不计入 unhealthy（排水是计划内状态，不触发 critical 告警；
// 旧代码用 != "READY" 恒为真，unhealthy 恒等于 total）。
func TestNodeGaugeCountsFourStatuses(t *testing.T) {
	views := []nodeView{
		{agentID: "n-healthy", status: "HEALTHY"},
		{agentID: "n-draining", status: "DRAINING"},
		{agentID: "n-unhealthy", status: "UNHEALTHY"},
		{agentID: "n-unknown", status: "UNKNOWN"},
	}
	total, unhealthy, draining := nodeGaugeCounts(views)
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if unhealthy != 2 {
		t.Fatalf("unhealthy = %d, want 2 (UNHEALTHY+UNKNOWN; DRAINING excluded)", unhealthy)
	}
	if draining != 1 {
		t.Fatalf("draining = %d, want 1", draining)
	}
}

// W0.1：machines_observed 走 publishGauges 相同的 ResetFamily→Set 序列时，
// 消失的 state 不得残留；DELETED 机器不计入。
func TestObservedStateCountsResetFamilyNoStaleState(t *testing.T) {
	reg := metrics.New()
	publish := func(machines []store.Machine) {
		reg.ResetFamily("firepaas_machines_observed")
		for state, n := range observedStateCounts(machines) {
			reg.Set("firepaas_machines_observed", map[string]string{"state": state}, n)
		}
	}
	publish([]store.Machine{
		{DesiredState: "CREATED", ObservedState: "RUNNING"},
		{DesiredState: "CREATED", ObservedState: "PAUSED"},
		{DesiredState: "DELETED", ObservedState: "RUNNING"}, // 不计入
	})
	publish([]store.Machine{
		{DesiredState: "CREATED", ObservedState: "RUNNING"},
	})
	snap := reg.Snapshot()
	for k, v := range snap {
		if strings.Contains(k, "state=PAUSED") {
			t.Fatalf("stale PAUSED series survived ResetFamily: %s=%d", k, v)
		}
	}
	running := uint64(0)
	for k, v := range snap {
		if strings.Contains(k, "firepaas_machines_observed") && strings.Contains(k, "state=RUNNING") {
			running = v
		}
	}
	if running != 1 {
		t.Fatalf("RUNNING = %d, want 1 (snapshot: %v)", running, snap)
	}
}

// W0.3：ctx 取消时投喂不再卡在 ch <- op——两个 worker 各占一个在途的
// ctx 感知 RPC（取消即中止），第 3 个 op 的投喂必须随取消快速返回，
// 而不是等满一个 AgentRPCTimeout。同步点全走 channel，无 sleep。
func TestDispatchBoundedCtxCancelReturnsFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newLockController()
	started := make(chan struct{}, 4)
	process := func(ctx context.Context, op store.Operation) {
		started <- struct{}{}
		<-ctx.Done() // 模拟 ctx 感知的 agent RPC：取消即中止
	}
	ops := []store.Operation{
		{ID: "op-1", MachineID: "m-1"},
		{ID: "op-2", MachineID: "m-2"},
		{ID: "op-3", MachineID: "m-3"},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatchBounded(ctx, ops, 2, c.lockMachine, process)
	}()
	// 两个 worker 都进入在途 RPC 后投喂必然阻塞在第 3 个 op，再取消。
	<-started
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dispatchBounded did not return after ctx cancel (feed blocked without ctx awareness)")
	}
}
