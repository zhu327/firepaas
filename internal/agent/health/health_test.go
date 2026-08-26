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

func TestTrackerProbeFlow(t *testing.T) {
	// 真实 HTTP 探针：前两次 500，第三次 200，success_threshold=1 → READY。
	reqs := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs++
		if reqs < 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	port := srv.Listener.Addr().(*net.TCPAddr).Port
	tag, err := EncodePolicy(&pb.HealthCheckSpec{
		Type:               pb.HealthCheckSpec_HTTP,
		Target:             fmt.Sprintf("http://127.0.0.1:%d/", port),
		IntervalSeconds:    1,
		TimeoutSeconds:     1,
		UnhealthyThreshold: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	tr := New()
	// 用 httptest 地址替换 guest IP：直接构造 hypeman Instance（IP=127.0.0.1），
	// 走真实 DefaultProbeRunner 路径。
	inst := &instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id: "inst-1", Name: "m-1", IP: "127.0.0.1", NetworkEnabled: true,
			Tags:      map[string]string{tagHealth: tag},
			CreatedAt: time.Now().Add(-time.Minute),
		},
		State: instances.StateRunning,
	}
	ctx := context.Background()

	// 第一轮：INITIALIZING 前（模拟）→ 先观测一次 RUNNING。
	tr.Observe(ctx, inst)
	r, _ := tr.Readiness("m-1")
	if r != pb.MachineReadiness_NOT_READY && r != pb.MachineReadiness_UNKNOWN {
		t.Fatalf("initial readiness = %v (reqs=%d)", r, reqs)
	}
	// 推过 interval 再探。
	time.Sleep(1100 * time.Millisecond)
	tr.Observe(ctx, inst)
	time.Sleep(1100 * time.Millisecond)
	tr.Observe(ctx, inst) // 第 3 次成功 → READY
	r, _ = tr.Readiness("m-1")
	if r != pb.MachineReadiness_READY {
		t.Fatalf("readiness after success = %v (reqs=%d)", r, reqs)
	}

	// UNCONFIGURED：无策略实例。
	tr.Observe(ctx, &instances.Instance{
		StoredMetadata: instances.StoredMetadata{Id: "i2", Name: "m-2", IP: "127.0.0.1"},
		State:          instances.StateRunning,
	})
	r, _ = tr.Readiness("m-2")
	if r != pb.MachineReadiness_UNCONFIGURED {
		t.Fatalf("unconfigured = %v", r)
	}

	tr.Remove("m-1")
	if _, _ = tr.Readiness("m-1"); tr.Count() != 1 {
		t.Fatalf("count after remove = %d", tr.Count())
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
