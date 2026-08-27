package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/healthcheck"
	"github.com/kernel/hypeman/lib/instances"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
)

func TestEncodeParsePolicy(t *testing.T) {
	// HTTP 探针。
	tag, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:               pb.HealthCheckSpec_HTTP,
		Target:             "http://127.0.0.1:8080/health",
		IntervalSeconds:    2,
		TimeoutSeconds:     1,
		UnhealthyThreshold: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	p := ParsePolicy(tag)
	if p == nil || p.Type != healthcheck.TypeHTTP {
		t.Fatalf("policy = %+v", p)
	}
	if p.HTTP.Port != 8080 || p.HTTP.Path != "/health" || p.HTTP.Scheme != "http" {
		t.Fatalf("http check = %+v", p.HTTP)
	}
	if p.FailureThreshold != 3 || p.Interval != "2s" || p.Timeout != "1s" {
		t.Fatalf("thresholds = %+v", p)
	}

	// TCP 探针。
	tag, err = EncodePolicy(&pb.HealthCheckSpec{
		Type:   pb.HealthCheckSpec_TCP,
		Target: "tcp://127.0.0.1:9090",
	})
	if err != nil {
		t.Fatal(err)
	}
	p = ParsePolicy(tag)
	if p == nil || p.Type != healthcheck.TypeTCP || p.TCP.Port != 9090 {
		t.Fatalf("tcp policy = %+v", p)
	}

	// 未声明/EXEC（M3 不支持）。
	if tag, err := EncodePolicy(nil); err != nil || tag != "" {
		t.Fatalf("nil policy: %q %v", tag, err)
	}
	if _, err := EncodePolicy(&pb.HealthCheckSpec{Type: pb.HealthCheckSpec_EXEC}); err == nil {
		t.Fatal("exec should be rejected in M3")
	}
	if p := ParsePolicy("0"); p != nil {
		t.Fatal("'0' must parse to nil")
	}
	if p := ParsePolicy("not-json"); p != nil {
		t.Fatal("garbage must parse to nil")
	}
}

func TestReadinessFromRuntime(t *testing.T) {
	now := time.Now()
	inst := healthcheck.Instance{State: healthcheck.StateRunning, StartedAt: &now}
	policy := ParsePolicy(mustPolicy(t, &pb.HealthCheckSpec{
		Type: pb.HealthCheckSpec_HTTP, Target: "http://127.0.0.1:80/",
	}))

	cases := []struct {
		runtime *healthcheck.Runtime
		want    pb.MachineReadiness
	}{
		{&healthcheck.Runtime{Status: healthcheck.StatusHealthy}, pb.MachineReadiness_READY},
		{&healthcheck.Runtime{Status: healthcheck.StatusUnhealthy}, pb.MachineReadiness_NOT_READY},
		{&healthcheck.Runtime{Status: healthcheck.StatusStarting}, pb.MachineReadiness_UNKNOWN},
		{nil, pb.MachineReadiness_UNKNOWN},
	}
	for _, c := range cases {
		if got := readinessFromRuntime(policy, inst, c.runtime, now); got != c.want {
			t.Errorf("runtime %+v: got %v want %v", c.runtime, got, c.want)
		}
	}
	if got := readinessFromRuntime(nil, inst, nil, now); got != pb.MachineReadiness_UNCONFIGURED {
		t.Errorf("no policy: got %v", got)
	}
}

// newProbeServer 返回 200 OK 的 httptest server。
func newProbeServer(t *testing.T) (string, int) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	return "127.0.0.1", port
}

// workerHarness 把 Worker 的 probeDue 单轮化（不跑 ticker 循环）。
func workerHarness(t *testing.T, tr *Tracker, listed []instances.Instance) *Worker {
	t.Helper()
	w := &Worker{
		tracker:     tr,
		list:        func(ctx context.Context) ([]instances.Instance, error) { return listed, nil },
		probeBudget: 5 * time.Second,
		tick:        time.Hour, // 不自动循环
	}
	return w
}

// TestWorkerProbeFlow：Observe 注册 → probeDue 执行 → READY；
// ListMachines 路径（Observe+Readiness）本身零网络 IO。
func TestWorkerProbeFlow(t *testing.T) {
	ip, port := newProbeServer(t)
	tag, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:            pb.HealthCheckSpec_HTTP,
		Target:          fmt.Sprintf("http://%s:%d/", ip, port),
		IntervalSeconds: 1,
		TimeoutSeconds:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	tr := New()
	inst := instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id: "inst-1", Name: "m-1", IP: ip, NetworkEnabled: true,
			Tags:      map[string]string{tagHealth: tag},
			CreatedAt: time.Now().Add(-time.Minute),
		},
		State: instances.StateRunning,
	}
	listed := []instances.Instance{inst}
	w := workerHarness(t, tr, listed)

	// 第一轮：Observe + probeDue → 探针成功 → READY。
	tr.Observe(context.Background(), &inst)
	w.probeDue(context.Background())
	if r, _ := tr.Readiness("m-1"); r != pb.MachineReadiness_READY {
		t.Fatalf("readiness after first probe = %v", r)
	}

	// UNCONFIGURED：无策略实例。
	inst2 := instances.Instance{
		StoredMetadata: instances.StoredMetadata{Id: "i2", Name: "m-2", IP: ip},
		State:          instances.StateRunning,
	}
	tr.Observe(context.Background(), &inst2)
	if r, _ := tr.Readiness("m-2"); r != pb.MachineReadiness_UNCONFIGURED {
		t.Fatalf("unconfigured = %v", r)
	}

	tr.Remove("m-1")
	if tr.Count() != 1 {
		t.Fatalf("count after remove = %d", tr.Count())
	}
}

// TestWorkerNotReady：探针失败（连接拒绝端口）→ 未达阈值时 UNKNOWN，
// 达到 FailureThreshold 后 NOT_READY。
func TestWorkerNotReady(t *testing.T) {
	// 找一个必然拒绝的端口：监听后立刻关闭。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	tag2, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:               pb.HealthCheckSpec_HTTP,
		Target:             fmt.Sprintf("http://127.0.0.1:%d/", port),
		IntervalSeconds:    1,
		TimeoutSeconds:     1,
		UnhealthyThreshold: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = tag2

	tr := New()
	inst := instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id: "inst-1", Name: "m-1", IP: "127.0.0.1", NetworkEnabled: true,
			Tags:      map[string]string{tagHealth: tag2},
			CreatedAt: time.Now().Add(-time.Minute),
		},
		State: instances.StateRunning,
	}
	w := workerHarness(t, tr, []instances.Instance{inst})

	tr.Observe(context.Background(), &inst)
	w.probeDue(context.Background())
	if r, _ := tr.Readiness("m-1"); r != pb.MachineReadiness_UNKNOWN {
		t.Fatalf("after 1 failure (threshold=2, start period passed) = %v, want UNKNOWN", r)
	}
	time.Sleep(1100 * time.Millisecond) // 推过 interval，重新到期
	tr.Observe(context.Background(), &inst)
	w.probeDue(context.Background())
	if r, _ := tr.Readiness("m-1"); r != pb.MachineReadiness_NOT_READY {
		t.Fatalf("after 2 failures = %v, want NOT_READY", r)
	}
}

// TestObserveResetsOnNewExecution（P2-2）：换代（CreatedAt 变晚）必须重置
// readiness/runtime，不能把上一代的 READY 带给新 execution。
func TestObserveResetsOnNewExecution(t *testing.T) {
	ip, port := newProbeServer(t)
	tag, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:               pb.HealthCheckSpec_HTTP,
		Target:             fmt.Sprintf("http://%s:%d/", ip, port),
		IntervalSeconds:    10,
		TimeoutSeconds:     1,
		UnhealthyThreshold: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	tr := New()
	oldInst := instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id: "inst-1", Name: "m-1", IP: ip, NetworkEnabled: true,
			Tags:      map[string]string{tagHealth: tag},
			CreatedAt: time.Now().Add(-time.Hour),
		},
		State: instances.StateRunning,
	}
	w := workerHarness(t, tr, []instances.Instance{oldInst})
	tr.Observe(context.Background(), &oldInst)
	w.probeDue(context.Background())
	if r, _ := tr.Readiness("m-1"); r != pb.MachineReadiness_READY {
		t.Fatalf("old execution should be READY, got %v", r)
	}

	// 换代重建：同名实例、新 CreatedAt、不可达 IP（探针必然失败，但
	// interval=10s 未到期不会立即执行）。Observe 必须立即把 readiness
	// 打回 UNKNOWN（新 VM 尚未证明自己）。
	newInst := oldInst
	newInst.StoredMetadata.CreatedAt = time.Now()
	newInst.StoredMetadata.IP = "127.0.0.1" // 与探针 target 无关；重点在重置
	tr.Observe(context.Background(), &newInst)
	if r, _ := tr.Readiness("m-1"); r != pb.MachineReadiness_UNKNOWN {
		t.Fatalf("new execution must reset readiness to UNKNOWN, got %v", r)
	}
}

// TestWorkerBudget（P1-1）：预算耗尽时剩余机器本轮跳过，readiness 沿用
// 上次值；不执行网络 IO。
func TestWorkerBudget(t *testing.T) {
	ip, port := newProbeServer(t)
	tag, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:            pb.HealthCheckSpec_HTTP,
		Target:          fmt.Sprintf("http://%s:%d/", ip, port),
		IntervalSeconds: 10,
		TimeoutSeconds:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	tr := New()
	var listed []instances.Instance
	for i := 0; i < 3; i++ {
		listed = append(listed, instances.Instance{
			StoredMetadata: instances.StoredMetadata{
				Id: fmt.Sprintf("inst-%d", i), Name: fmt.Sprintf("m-%d", i), IP: ip,
				NetworkEnabled: true,
				Tags:           map[string]string{tagHealth: tag},
				CreatedAt:      time.Now().Add(-time.Minute),
			},
			State: instances.StateRunning,
		})
	}
	w := workerHarness(t, tr, listed)
	// 预算 = 0：probeDue 应在任何探针前返回，三个机器全部沿用 UNKNOWN。
	w.probeBudget = time.Nanosecond

	for i := range listed {
		tr.Observe(context.Background(), &listed[i])
	}
	w.probeDue(context.Background())
	for i := 0; i < 3; i++ {
		if r, _ := tr.Readiness(fmt.Sprintf("m-%d", i)); r != pb.MachineReadiness_UNKNOWN {
			t.Fatalf("budget-exhausted machine %d should stay UNKNOWN, got %v", i, r)
		}
	}
}

// TestProbeHTTPClientTimeoutNotCappingPolicy：policyTimeout 读取用户配置
// （P3：不再被硬编码 2s 客户端截断）。
func TestProbeHTTPClientTimeoutNotCappingPolicy(t *testing.T) {
	p := toHypemanPolicy(&PolicyJSON{Type: "http", Port: 8080, TimeoutSeconds: 5})
	if got := policyTimeout(p); got != 5*time.Second {
		t.Fatalf("policyTimeout = %v, want 5s", got)
	}
	if got := probeHTTPClient().Timeout; got < 5*time.Second {
		t.Fatalf("shared client timeout %v must not cap policy timeout", got)
	}
}

func mustPolicy(t *testing.T, h *pb.HealthCheckSpec) string {
	t.Helper()
	tag, err := EncodePolicy(h)
	if err != nil {
		t.Fatal(err)
	}
	return tag
}
