// scaleup_signal.go：placement 无候选 → 节点扩容信号（P1）。
//
// 此前 ErrNoCandidates 只记 scheduler_events + 冻结本 app 扩容（notePlacementFailure），
// op 重入列后静默重试——集群缺容量时没有任何向节点池的扩容表达。本文件把该事件
// 接成显式的、可消费的扩容信号：
//   - 默认：durable 的 scheduler event（kind=scale_up_signal，可经
//     GET /v1/system/scheduler-events 查询）+ 指标
//     firepaas_scale_up_signals_total{pool} + 结构化日志；外部 autoscaler
//     （Nomad Autoscaler / 节点池 API）按此消费扩容；
//   - 可选：NodeScaleUpNotifier 钩子（cfg 注入，nil-safe），后续直连 Nomad
//     scaling API 时实现本接口即可，派发路径不动。
//
// 注意与 app 级 autoscale 冻结的关系：缺容量时冻结本 app 的副本数上调仍保留
// （避免不可放置的副本热循环），节点扩容信号并行发出——两者正交，不互相替代。
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// ScaleUpSignal 是一次节点扩容请求（无候选 placement 的结构化表达）。
type ScaleUpSignal struct {
	ProjectID   string
	AppID       string
	MachineID   string
	OperationID string
	Pool        string // 为空 = compute（与调度默认值一致）
	VCPU        int64
	MemMib      int64
	DiskMib     int64
	Reason      string // 主导拒绝原因（调度错误原文；逐节点明细见同 op 的 filter_rejection 事件）
}

// NodeScaleUpNotifier 是节点池扩容的外部实现（Nomad scaling API 等）。
// 返回错误只记日志，不阻塞派发（信号的 durable 形态是 scheduler event）。
type NodeScaleUpNotifier interface {
	NotifyScaleUp(ctx context.Context, sig ScaleUpSignal) error
}

// scaleUpSignalFor 从 placement 请求构造扩容信号（纯函数，可单测）。
func scaleUpSignalFor(op store.Operation, req *pb.CreateMachineRequest, err error) ScaleUpSignal {
	pool := req.GetSpec().GetPlacement().GetNodePool()
	if pool == "" {
		pool = "compute"
	}
	reason := "no placement candidates"
	if err != nil {
		reason = err.Error()
	}
	return ScaleUpSignal{
		ProjectID: op.ProjectID, AppID: req.GetSpec().GetAppId(),
		MachineID: op.MachineID, OperationID: op.ID, Pool: pool,
		VCPU: int64(req.GetSpec().GetVcpu()), MemMib: int64(req.GetSpec().GetMemMib()),
		DiskMib: int64(req.GetSpec().GetDiskMib()), Reason: reason,
	}
}

// scaleUpCooldown 是同 pool 扩容信号的最小间隔（评审 blocker：无候选持续时
// op 每轮重入列，不能每 tick 都记 durable 事件/打指标/调外部通知；外部
// autoscaler 按事件轮询，5 分钟提醒一次足够，容量恢复后的新信号不受影响）。
const scaleUpCooldown = 5 * time.Minute

// scaleUpThrottle 按 pool 节流扩容信号（进程内 + leader 任期；切换 leader 后
// 最多提前一次，无害）。零值可用（now 由调用方传入，可单测）。
type scaleUpThrottle struct {
	mu         sync.Mutex
	lastByPool map[string]time.Time
}

// allow 同 pool 距上次信号超过 cooldown 才放行（放行即记录本次时间）。
func (t *scaleUpThrottle) allow(pool string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastByPool == nil {
		t.lastByPool = map[string]time.Time{}
	}
	if last, ok := t.lastByPool[pool]; ok && now.Sub(last) < scaleUpCooldown {
		return false
	}
	t.lastByPool[pool] = now
	return true
}

// isNoCandidates 判定 placement 错误是否为无候选（值类型 ErrNoCandidates 用 errors.As）。
func isNoCandidates(err error) bool {
	var nce scheduler.ErrNoCandidates
	return errors.As(err, &nce)
}

// signalNodeScaleUp 发出节点扩容信号：durable 事件 + 指标 + 日志 + 可选外部通知。
// 调用方保证 err 是 ErrNoCandidates（其它错误不发扩容信号）。同 pool 5 分钟内
// 只发一次（op 重入列不重复打扰；见 scaleUpCooldown）。
func (c *Controller) signalNodeScaleUp(
	ctx context.Context,
	op store.Operation,
	req *pb.CreateMachineRequest,
	err error,
) {
	sig := scaleUpSignalFor(op, req, err)
	if !c.scaleUpThrottle.allow(sig.Pool, time.Now()) {
		return
	}
	c.recordEvent(ctx, "scale_up_signal", op.MachineID, op.ID, "",
		fmt.Sprintf("pool=%s vcpu=%d mem=%dMiB disk=%dMiB: %s",
			sig.Pool, sig.VCPU, sig.MemMib, sig.DiskMib, sig.Reason), nil)
	c.metrics.Inc("firepaas_scale_up_signals_total", map[string]string{"pool": sig.Pool}, 1)
	slog.Warn("placement has no candidates; node scale-up signaled",
		"pool", sig.Pool, "app_id", sig.AppID, "machine_id", sig.MachineID,
		"operation_id", sig.OperationID, "reason", sig.Reason)
	// 外部通知异步投递：webhook 最长 10s 超时，不能占派发 worker
	// （durable 事件已落库，通知丢了也可由事件重放）。
	if n := c.cfg.ScaleUpNotifier; n != nil {
		go func() {
			if nerr := n.NotifyScaleUp(context.WithoutCancel(ctx), sig); nerr != nil {
				slog.Warn("node scale-up notify failed (signal already journaled as event)",
					"pool", sig.Pool, "error", nerr)
			}
		}()
	}
}
