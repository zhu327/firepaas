package fabricingress

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/proxy"
	"github.com/zhu327/firepaas/internal/agent/state"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

// fakeMachines：GetEndpointForPort 的最小替身（proxy 包的机器接口已具体
// 到 *machine.Adapter——终结器测试用真实 Proxy 构造，Adapter 由本包
// 无法伪造；改为在 proxy 层之上用 httptest 上游验证 ServeCredential 的
// 路由语义：见 proxy 包的 TestServeCredential。本文件验证终结器的
// 监听绑定/头剥离/端口参数转发。）
//
// 由于 proxy.NewWithVerifier 需要 *machine.Adapter（具体类型），这里用
// 一个仅验证请求头语义的 httptest 上游 + 自建 handler 链不可行——
// 改为构造真实 proxy.Proxy 依赖的 Adapter 不可行（hypeman）。因此本测试
// 直接构造 Server 并注入 proxy 包导出的测试构造器（见 proxy_test 同层）。

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, portStr, _ := strings.Cut(l.Addr().String(), ":")
	port, _ := strconv.Atoi(portStr)
	return port
}

// TestEnsureBindingLifecycle：随快照前缀幂等绑定/重绑/关闭；空前缀不启动。
// 不依赖 proxy（构造后立刻 Ensure/Close；HTTP 路径由 proxy 包测试覆盖）。
func TestEnsureBindingLifecycle(t *testing.T) {
	port := freePort(t)
	srv := New(nil, port) // proxy nil：本测试不发起 HTTP 请求
	defer srv.Close()

	if err := srv.Ensure(context.Background(), ""); err != nil {
		t.Fatalf("empty prefix must be no-op: %v", err)
	}
	if err := srv.Ensure(context.Background(), "127.0.0.1/32"); err != nil { // 测试用 loopback（真实形态 /64 → 基址由 root 套件覆盖）
		t.Fatal(err)
	}
	// 同前缀幂等。
	if err := srv.Ensure(context.Background(), "127.0.0.1/32"); err != nil { // 测试用 loopback（真实形态 /64 → 基址由 root 套件覆盖）
		t.Fatal(err)
	}
	// 端口已监听：第二个 server 同前缀不同端口也应成功（端口互不冲突）。
	srv2 := New(nil, freePort(t))
	defer srv2.Close()
	if err := srv2.Ensure(context.Background(), "127.0.0.1/32"); err != nil { // 同前缀不同端口
		t.Fatal(err)
	}
	// 重绑（前缀变化 = 节点 /64 重规划）。
	if err := srv.Ensure(context.Background(), "127.0.0.2/32"); err != nil { // 重绑（前缀变化）
		t.Fatal(err)
	}
	// 非法前缀。
	if err := srv.Ensure(context.Background(), "not-a-prefix"); err == nil {
		t.Fatal("bad prefix must fail")
	}
}

// TestServerStripsRoutingHeaders：终结器剥离 X-Firepaas 路由头（伪造不可
// 注入），凭证与端口参数透传给 proxy.ServeCredential。
func TestServerStripsRoutingHeaders(t *testing.T) {
	var (
		gotCred string
		gotPort int
		gotMach string
		gotExec string
	)
	// ServeCredential 的替身：包内无法替换 Proxy——用真实 Creds + 无 Adapter
	// 会在转发时报错，但凭证反查发生在转发前，足以断言剥离/反查语义。
	creds, err := state.OpenCreds(t.TempDir() + "/creds.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := creds.Set("m1", "e1", state.Digest("tok-1")); err != nil {
		t.Fatal(err)
	}
	// proxy.NewWithVerifier(nil adapter) 会在 endpoint 解析路径 panic——
	// 构造带 nil Adapter 的 Proxy 只用于凭证反查断言（转发前返回）。
	p := proxy.NewForTest(creds, func(machineID, executionID string, wantPort int) (string, int, error) {
		gotMach, gotExec, gotPort = machineID, executionID, wantPort
		gotCred = "resolved"
		return "127.0.0.1", 1, nil
	})
	srv := New(p, freePort(t))
	defer srv.Close()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://x.internal/", nil)
	req.Header.Set(traffic.HeaderCredential, "tok-1")
	req.Header.Set("X-Firepaas-Machine-ID", "forged")
	req.Header.Set("X-Firepaas-Execution-ID", "forged")
	req.Header.Set("X-Firepaas-App-Port", "8080")
	srv.ServeHTTP(rr, req)
	if gotMach != "m1" || gotExec != "e1" {
		t.Fatalf("credential lookup = %s/%s, want m1/e1", gotMach, gotExec)
	}
	if gotPort != 8080 {
		t.Fatalf("port = %d, want 8080", gotPort)
	}
	if gotCred != "resolved" {
		t.Fatal("endpoint resolver not reached")
	}

	// 未知凭证 → 403（不触达 endpoint）。
	gotCred = ""
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "http://x.internal/", nil)
	req2.Header.Set(traffic.HeaderCredential, "wrong")
	srv.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusForbidden {
		t.Fatalf("unknown credential: code=%d", rr2.Code)
	}
	if gotCred != "" {
		t.Fatal("unknown credential must not reach endpoint resolution")
	}
}
