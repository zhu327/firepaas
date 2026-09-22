// gRPC workload 端到端 H2 测试（P1：对标主流 PaaS 的协议能力）。
//
// 链路：h2c 客户端 → edge（h2c 服务端 + gRPC 分流 Transport）→
// agent proxy（h2c 服务端 + gRPC 分流 Transport）→ workload（h2c）。
// 断言每一跳都协商到 HTTP/2（prior-knowledge h2c）且负载原样回显；
// 非 gRPC 请求仍走 HTTP/1.1（行为不变）。
package edge_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentproxy "github.com/zhu327/firepaas/internal/agent/proxy"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/edge"
	"github.com/zhu327/firepaas/shared/pkg/h2transport"
)

type grpcRouteStub struct{ route *catalog.Route }

func (s *grpcRouteStub) GetRouteForHostname(context.Context, string) (*catalog.Route, error) {
	return s.route, nil
}

func (s *grpcRouteStub) GetRouteForPort(
	ctx context.Context,
	host string,
	port int,
) (*catalog.Route, bool, error) {
	return s.route, true, nil
}

// grpcFrame 构造最小 gRPC 数据帧（压缩标志 0 + 长度 + 负载，不做真正的
// protobuf 编码——本测试验证的是 H2 端到端传输能力，不是 gRPC 语义）。
func grpcFrame(payload []byte) []byte {
	frame := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)
	return frame
}

func TestGRPCWorkloadEndToEndOverH2(t *testing.T) {
	var workloadProto, agentProto, edgeProto int

	// workload：h2c 服务端（标准库原生协议），原样回显。
	workload := httptest.NewUnstartedServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			workloadProto = r.ProtoMajor
			body, _ := io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/grpc")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		},
	))
	workload.Config.Protocols = h2transport.ServerProtocols(nil)
	workload.Start()
	defer workload.Close()

	// agent proxy：h2c 服务端，记录入站协议后转给 workload。
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
	agentSrv := httptest.NewUnstartedServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			agentProto = r.ProtoMajor
			agentProxy.ServeHTTP(w, r)
		},
	))
	agentSrv.Config.Protocols = h2transport.ServerProtocols(nil)
	agentSrv.Start()
	defer agentSrv.Close()

	// edge：h2c 服务端，单 backend 指向 agent。
	agentHost := strings.TrimPrefix(agentSrv.URL, "http://")
	edgeHandler := edge.NewHandler(edge.Config{
		Catalog: &grpcRouteStub{route: &catalog.Route{
			RouteGeneration: 7,
			Backends: []catalog.Backend{{
				MachineID:         "m-grpc",
				ExecutionID:       "e-grpc",
				NodeProxyEndpoint: agentHost,
				Readiness:         "READY",
				Weight:            100,
			}},
		}},
		Routes:          edge.NewRouteCache(time.Minute, time.Minute),
		Tokens:          edge.NewTokenClient("", "", time.Minute), // 禁用：Get 恒为空凭证
		Limiter:         edge.NewRateLimiter(0, 0),
		HardConcurrency: 64,
	})
	edgeSrv := httptest.NewUnstartedServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			edgeProto = r.ProtoMajor
			edgeHandler.ServeHTTP(w, r)
		},
	))
	edgeSrv.Config.Protocols = h2transport.ServerProtocols(nil)
	edgeSrv.Start()
	defer edgeSrv.Close()

	// h2c 客户端（gRPC 请求经 h2transport 分流到 prior-knowledge H2）。
	client := &http.Client{Transport: h2transport.New(nil), Timeout: 10 * time.Second}
	payload := grpcFrame([]byte("hello-grpc-workload"))
	req, err := http.NewRequest(
		http.MethodPost,
		edgeSrv.URL+"/grpc.Test/Echo",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "grpc.test"
	req.Header.Set("Content-Type", "application/grpc")
	req.ContentLength = int64(len(payload))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("grpc e2e request: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("grpc e2e status=%d want 200", resp.StatusCode)
	}
	if !bytes.Equal(respBody, payload) {
		t.Fatalf("grpc e2e body=%q want %q", respBody, payload)
	}
	if edgeProto != 2 {
		t.Fatalf("edge inbound proto=%d want 2 (h2c)", edgeProto)
	}
	if agentProto != 2 {
		t.Fatalf("agent inbound proto=%d want 2 (h2c)", agentProto)
	}
	if workloadProto != 2 {
		t.Fatalf("workload inbound proto=%d want 2 (h2c)", workloadProto)
	}

	// 非 gRPC 请求仍走 HTTP/1.1（回归：分流不得影响既有流量）。
	plainReq, _ := http.NewRequest(http.MethodGet, edgeSrv.URL+"/health", nil)
	plainReq.Host = "grpc.test"
	plainResp, err := client.Do(plainReq)
	if err != nil {
		t.Fatalf("plain request: %v", err)
	}
	_, _ = io.Copy(io.Discard, plainResp.Body)
	_ = plainResp.Body.Close()
	if plainResp.StatusCode != http.StatusOK {
		t.Fatalf("plain status=%d want 200", plainResp.StatusCode)
	}
	if workloadProto != 1 {
		t.Fatalf("plain workload inbound proto=%d want 1 (HTTP/1.1)", workloadProto)
	}
}
