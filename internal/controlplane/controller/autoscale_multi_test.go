package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func multiPolicy() store.AutoscalePolicy {
	return store.AutoscalePolicy{
		Enabled: true, MinReplicas: 1, MaxReplicas: 10,
		TargetConcurrency: 20, ScaleDownDelaySec: 120, PanicThreshold: 2.0,
	}
}

func TestComputeAutoscaleWantMultiMax(t *testing.T) {
	p := multiPolicy()
	p.TargetRPS = 100
	p.TargetCPURatio = 0.7
	sig := AutoscaleSignal{Served: 10, TotalRPS: 350, WindowSec: 10, HasFresh: true}
	// 并发 want=1，RPS want=ceil(35/100)=1，CPU want=ceil(2*0.9/0.7)=3 → max=3。
	extra := extraSignals{rpsRate: 35, rpsOK: true, cpuUtil: 0.9, cpuOK: true}
	want, panic, byDim := computeAutoscaleWantMulti(p, sig, 2, 0, extra)
	if want != 3 || panic {
		t.Fatalf("want=%d panic=%v byDim=%v, want 3/false", want, panic, byDim)
	}
	// 关闭维度不参与：无 extra 时退化为并发 want=1。
	want, _, _ = computeAutoscaleWantMulti(p, sig, 2, 0, extraSignals{})
	if want != 1 {
		t.Fatalf("disabled dims want=%d, want 1", want)
	}
	// 自定义 absolute：value=9/target=2 → 5，主导。
	p.CustomPromQuery = "queue{app={app_id}}"
	p.CustomTarget = 2
	p.CustomMode = "absolute"
	want, _, byDim = computeAutoscaleWantMulti(p, sig, 2, 0,
		extraSignals{customValue: 9, customOK: true})
	if want != 5 || byDim["custom"] != 5 {
		t.Fatalf("custom absolute want=%d byDim=%v, want 5", want, byDim)
	}
	// 自定义 per_replica：ceil(2*0.8/0.5)=4。
	p.CustomMode = "per_replica"
	p.CustomTarget = 0.5
	want, _, _ = computeAutoscaleWantMulti(p, sig, 2, 0,
		extraSignals{customValue: 0.8, customOK: true})
	if want != 4 {
		t.Fatalf("custom per_replica want=%d, want 4", want)
	}
	// 上限 clamp。
	p.MaxReplicas = 3
	want, _, _ = computeAutoscaleWantMulti(p, sig, 2, 0,
		extraSignals{customValue: 99, customOK: true})
	if want != 3 {
		t.Fatalf("clamped want=%d, want 3", want)
	}
}

func TestComputeAutoscaleWantMultiPanicConcurrencyOnly(t *testing.T) {
	p := multiPolicy()
	p.TargetRPS = 1000 // RPS 维度开启但负载低，不触发 panic。
	sig := AutoscaleSignal{Served: 100, PanicReject: true, HasFresh: true}
	extra := extraSignals{rpsRate: 1, rpsOK: true}
	want, panic, _ := computeAutoscaleWantMulti(p, sig, 2, 0, extra)
	if !panic {
		t.Fatal("hard reject must still panic")
	}
	// panic 放大只看并发：want=ceil(100/20)=5 vs boosted=max(5, ceil(2*2), 4)=5。
	if want != 5 {
		t.Fatalf("panic want=%d, want 5", want)
	}
}

func TestFetchExtraSignalsHold(t *testing.T) {
	c := &Controller{}
	app := &store.App{ID: "app", Hostname: "app.test"}
	p := multiPolicy()
	p.TargetCPURatio = 0.7
	sig := AutoscaleSignal{HasFresh: true, WindowSec: 10}
	// 未配 PrometheusAddr → hold（fail-closed）。
	if _, reason, hold := c.fetchExtraSignals(context.Background(), app, p, sig, 2); !hold || reason == "" {
		t.Fatalf("hold=%v reason=%q, want hold with reason", hold, reason)
	}
	// 冷起（eff==0）→ 跳过外部维度，不 hold。
	if _, _, hold := c.fetchExtraSignals(context.Background(), app, p, sig, 0); hold {
		t.Fatal("cold start must not hold on external dims")
	}
	// 无外部维度 → 不 hold。
	if _, _, hold := c.fetchExtraSignals(context.Background(), app, multiPolicy(), sig, 2); hold {
		t.Fatal("no external dims must not hold")
	}
	// RPS 维度纯本地，不 hold。
	p2 := multiPolicy()
	p2.TargetRPS = 50
	extra, _, hold := c.fetchExtraSignals(context.Background(), app, p2,
		AutoscaleSignal{TotalRPS: 200, WindowSec: 10, HasFresh: true}, 2)
	if hold || !extra.rpsOK || extra.rpsRate != 20 {
		t.Fatalf("rps extra=%+v hold=%v", extra, hold)
	}
}

func TestCPUQueryBuilder(t *testing.T) {
	q := cpuUtilQuery("my-app", 120000000000)
	if !strings.Contains(q, `instance_name=~"my-app-r[0-9]+-g[0-9]+"`) {
		t.Fatalf("query = %s", q)
	}
	if !strings.Contains(q, "hypeman_vm_cpu_seconds_total") ||
		!strings.Contains(q, "hypeman_vm_allocated_vcpus") {
		t.Fatalf("query = %s", q)
	}
}

func TestRenderCustomQuery(t *testing.T) {
	got := renderCustomQuery(`queue{host="{hostname}",app="{app_id}"}`, "a.test", "app")
	if got != `queue{host="a.test",app="app"}` {
		t.Fatalf("rendered = %s", got)
	}
}

func TestFetchExtraSignalsQueryError(t *testing.T) {
	c := &Controller{cfg: Config{PrometheusAddr: "http://prom:9090"}}
	// 地址不可达 → 查询失败 → hold（fail-closed）。
	app := &store.App{ID: "app", Hostname: "app.test"}
	p := multiPolicy()
	p.TargetCPURatio = 0.7
	_, reason, hold := c.fetchExtraSignals(context.Background(), app, p,
		AutoscaleSignal{HasFresh: true, WindowSec: 10}, 2)
	if !hold || !strings.Contains(reason, "cpu signal unavailable") {
		t.Fatalf("hold=%v reason=%q", hold, reason)
	}
	_ = errors.New
}
