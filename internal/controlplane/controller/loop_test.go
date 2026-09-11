package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/observability/metrics"
)

// 单环 panic 必须被隔离：已 panic 的拍不阻止后续拍，也不终止其他环。
func TestRunLoopIsolatesPanicAndKeepsTicking(t *testing.T) {
	c := &Controller{metrics: metrics.New()}
	var calls atomic.Int64
	loop := reconcileLoop{
		name:     "panic-test",
		interval: 5 * time.Millisecond,
		runOnce: func(context.Context) error {
			calls.Add(1)
			panic("boom")
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.runLoop(ctx, loop)
	if got := calls.Load(); got < 2 {
		t.Fatalf("loop stopped after panic: calls=%d, want >=2", got)
	}
}

// 环错误只记日志，不停止后续拍。
func TestRunLoopContinuesAfterError(t *testing.T) {
	c := &Controller{metrics: metrics.New()}
	var calls atomic.Int64
	loop := reconcileLoop{
		name:     "error-test",
		interval: 5 * time.Millisecond,
		runOnce: func(context.Context) error {
			calls.Add(1)
			return context.DeadlineExceeded
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.runLoop(ctx, loop)
	if got := calls.Load(); got < 2 {
		t.Fatalf("loop stopped after error: calls=%d, want >=2", got)
	}
}

// ctx 取消后 runLoop 必须在下一个调度点前返回，不泄漏 goroutine。
func TestRunLoopStopsOnContextCancel(t *testing.T) {
	c := &Controller{metrics: metrics.New()}
	loop := reconcileLoop{
		name:     "stop-test",
		interval: time.Hour,
		runOnce:  func(context.Context) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.runLoop(ctx, loop)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runLoop did not stop on context cancel")
	}
}

// reconcileLoops 必须覆盖原 Run select 的全部节拍，且周期与配置一致。
func TestReconcileLoopsCoverAllRings(t *testing.T) {
	c := &Controller{}
	c.cfg.OpPollInterval = time.Second
	c.cfg.SyncInterval = 5 * time.Second
	c.cfg.RebuildInterval = 30 * time.Second
	c.cfg.FabricGCInterval = 5 * time.Minute
	c.cfg.AutoscaleInterval = 10 * time.Second
	c.gc.Interval = 5 * time.Minute
	c.scrub.Interval = time.Hour

	loops := c.reconcileLoops()
	want := map[string]time.Duration{
		"operations":     time.Second,
		"observed":       5 * time.Second,
		"rollout":        5 * time.Second,
		"evacuation":     5 * time.Second,
		"node-resources": 5 * time.Second,
		"autoscale":      10 * time.Second,
		"route-rebuild":  30 * time.Second,
		"stale-claims":   30 * time.Second,
		"retention":      time.Minute,
		"image-gc":       5 * time.Minute,
		"snapshot-scrub": time.Hour,
		"fabric-gc":      5 * time.Minute,
	}
	if len(loops) != len(want) {
		t.Fatalf("loops=%d, want %d", len(loops), len(want))
	}
	for _, loop := range loops {
		if loop.runOnce == nil {
			t.Fatalf("loop %q has nil runOnce", loop.name)
		}
		interval, ok := want[loop.name]
		if !ok {
			t.Fatalf("unexpected loop %q", loop.name)
		}
		if loop.interval != interval {
			t.Errorf("loop %q interval=%v, want %v", loop.name, loop.interval, interval)
		}
	}
}
