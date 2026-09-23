// trace 端到端透传测试（Wave6）：edge → agent proxy → guest。
//
// 断言：客户端 traceparent 被 edge 续接并下行（同 trace-id）；agent 转
// guest 前剥离 traceparent/tracestate（与 request-id 同纪律，guest 不可信）。
package edge_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentproxy "github.com/zhu327/firepaas/internal/agent/proxy"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/edge"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestTraceparentEndToEnd(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var workloadTraceparent, workloadReqID string
	var agentTraceparent string
	workload := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			workloadTraceparent = r.Header.Get("traceparent")
			workloadReqID = r.Header.Get("X-Firepaas-Request-ID")
			w.WriteHeader(http.StatusOK)
		},
	))
	defer workload.Close()

	agentProxy := agentproxy.NewForTest(nil,
		func(machineID, executionID string, wantPort int) (string, int, error) {
			host := strings.TrimPrefix(workload.URL, "http://")
			hostname, portStr, _ := strings.Cut(host, ":")
			port := 0
			for _, c := range portStr {
				port = port*10 + int(c-'0')
			}
			return hostname, port, nil
		})
	agentSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			agentTraceparent = r.Header.Get("traceparent")
			agentProxy.ServeHTTP(w, r)
		},
	))
	defer agentSrv.Close()

	agentHost := strings.TrimPrefix(agentSrv.URL, "http://")
	edgeHandler := edge.NewHandler(edge.Config{
		Catalog: &grpcRouteStub{route: &catalog.Route{
			RouteGeneration: 7,
			Backends: []catalog.Backend{{
				MachineID:         "m-trace",
				ExecutionID:       "e-trace",
				NodeProxyEndpoint: agentHost,
				Readiness:         "READY",
				Weight:            100,
				Generation:        3,
			}},
		}},
		Routes:          edge.NewRouteCache(time.Minute, time.Minute),
		Tokens:          edge.NewTokenClient("", "", time.Minute),
		Limiter:         edge.NewRateLimiter(0, 0),
		HardConcurrency: 64,
	})
	edgeSrv := httptest.NewServer(http.HandlerFunc(edgeHandler.ServeHTTP))
	defer edgeSrv.Close()

	req, err := http.NewRequest(http.MethodGet, edgeSrv.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "trace.test"
	// 上游带合法 traceparent 入站（末位 01 = sampled）。
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	// agent 跳看到同 trace-id 的 traceparent（edge server span 续接）。
	if !strings.HasPrefix(agentTraceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("agent traceparent = %q", agentTraceparent)
	}
	// guest 跳：trace 与内部 request-id 双双剥离。
	if workloadTraceparent != "" {
		t.Fatalf("workload saw traceparent %q, must be stripped", workloadTraceparent)
	}
	if workloadReqID != "" {
		t.Fatalf("workload saw request-id %q, must be stripped", workloadReqID)
	}
}
