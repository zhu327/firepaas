package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/machine"
)

type testInstances struct {
	instance *instances.Instance
	err      error
}

func (m *testInstances) CreateInstance(context.Context, instances.CreateInstanceRequest) (*instances.Instance, error) {
	return nil, errors.New("unused")
}

func (m *testInstances) ListInstances(context.Context, *instances.ListInstancesFilter) ([]instances.Instance, error) {
	return nil, nil
}

func (m *testInstances) GetInstance(context.Context, string) (*instances.Instance, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.instance, nil
}
func (m *testInstances) DeleteInstance(context.Context, string) error { return nil }

func (m *testInstances) StandbyInstance(
	context.Context,
	string,
	instances.StandbyInstanceRequest,
) (*instances.Instance, error) {
	return nil, errors.New("unused")
}

func (m *testInstances) RestoreInstance(context.Context, string) (*instances.Instance, error) {
	return nil, errors.New("unused")
}

// Keep images imported only through the adapter's interface surface explicit;
// proxy endpoint tests do not exercise image operations.
var _ machine.ImageManager = (*testImages)(nil)

type testImages struct{}

func (*testImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, nil
}
func (*testImages) WaitForReady(context.Context, string) error         { return nil }
func (*testImages) ListImages(context.Context) ([]images.Image, error) { return nil, nil }
func (*testImages) DeleteImage(context.Context, string) error          { return nil }

func proxyRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://agent/", nil)
	r.Header.Set(HeaderMachineID, "m1")
	r.Header.Set(HeaderExecutionID, "e1")
	return r
}

func TestProxyMarksEndpointResolution502Retryable(t *testing.T) {
	p := New(machine.New(&testInstances{err: errors.New("instance gone")}, &testImages{}, nil, nil))
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, proxyRequest())

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if got := rr.Header().Get(HeaderRetryable); got != retryableValue {
		t.Fatalf("retry marker = %q, want %q", got, retryableValue)
	}
}

func TestProxyMarksGuestTransport502Retryable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	_ = listener.Close() // preserve a known-unused endpoint for the proxy transport.
	if err != nil {
		t.Fatal(err)
	}
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		IP: "127.0.0.1", Tags: map[string]string{
			"firepaas/execution_id": "e1", "firepaas/port": port,
		},
	}}
	p := New(machine.New(&testInstances{instance: inst}, &testImages{}, nil, nil))
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, proxyRequest())
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if got := rr.Header().Get(HeaderRetryable); got != retryableValue {
		t.Fatalf("retry marker = %q, want %q", got, retryableValue)
	}
	// P0#4：transport 502 正文不得携带 guest IP:port 等内部拓扑。
	body := rr.Body.String()
	if strings.Contains(body, "127.0.0.1") || strings.Contains(body, port) || strings.Contains(body, "dial") {
		t.Fatalf("502 body leaks internal address: %q", body)
	}
}

func TestProxyDoesNotForwardOrTrustRetryHeaderFromWorkload(t *testing.T) {
	guestSawInternalHeader := false
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		guestSawInternalHeader = r.Header.Get(HeaderRetryable) != ""
		w.Header().Set(HeaderRetryable, retryableValue)
		http.Error(w, "workload failure", http.StatusBadGateway)
	}))
	defer guest.Close()

	// Use the server's host as the endpoint. The proxy's URL construction adds
	// http itself, so this is intentionally an HTTP guest server.
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		IP: "127.0.0.1", Tags: map[string]string{"firepaas/execution_id": "e1"},
	}}
	// httptest exposes a dynamic port; split it through the instance port tag.
	_, port, err := net.SplitHostPort(guest.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	inst.Tags["firepaas/port"] = port
	p := New(machine.New(&testInstances{instance: inst}, &testImages{}, nil, nil))

	r := proxyRequest()
	r.Header.Set(HeaderRetryable, retryableValue)
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, r)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if guestSawInternalHeader {
		t.Fatal("internal retry marker was forwarded to workload")
	}
	if got := rr.Header().Get(HeaderRetryable); got != "" {
		t.Fatalf("workload-forged retry marker leaked to edge: %q", got)
	}
}

// edge→agent 的请求关联 ID 只用于 agent 日志：绝不转发给 guest。
func TestProxyStripsRequestIDBeforeWorkload(t *testing.T) {
	guestSaw := make(chan string, 1)
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		guestSaw <- r.Header.Get(HeaderRequestID)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer guest.Close()
	_, port, err := net.SplitHostPort(guest.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		IP: "127.0.0.1", Tags: map[string]string{
			"firepaas/execution_id": "e1", "firepaas/port": port,
		},
	}}
	p := New(machine.New(&testInstances{instance: inst}, &testImages{}, nil, nil))
	r := proxyRequest()
	r.Header.Set(HeaderRequestID, "req-1")
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, r)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
	if got := <-guestSaw; got != "" {
		t.Fatalf("internal request id forwarded to workload: %q", got)
	}
}

// timestub 是 guard 时间的确定性替身（并发测试不用任意 sleep）。
type timestub struct {
	cur time.Time
}

func (s *timestub) fn() time.Time { return s.cur }

func timestubNow(now time.Time) *timestub { return &timestub{cur: now} }

// stubCreds 是 ServeCredential 测试替身：hit 时返回固定归属。
type stubCreds struct {
	machineID, executionID string
	lookups                int
}

func (s *stubCreds) Verify(machineID, executionID, raw string) bool {
	return raw != "" && machineID == s.machineID && executionID == s.executionID
}

func (s *stubCreds) LookupByDigest(raw string) (string, string, bool) {
	s.lookups++
	if raw == "good-cred" {
		return s.machineID, s.executionID, true
	}
	return "", "", false
}

func credentialTestProxy(creds credentialVerifier) *Proxy {
	p := NewForTest(creds, func(machineID, executionID string, wantPort int) (string, int, error) {
		return "127.0.0.1", 1, nil // 端口 1 拒绝连接 → transport 502，只断言 guard 语义
	})
	return p
}

// W3.3：ServeCredential 命中记 ok；miss 记 denied 且 403 不泄细节。
func TestServeCredentialCountsOKAndDenied(t *testing.T) {
	creds := &stubCreds{machineID: "m1", executionID: "e1"}
	p := credentialTestProxy(creds)

	rr := httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "good-cred", 0)
	if rr.Code != http.StatusBadGateway { // guard 通过，终点不可达是 transport 语义
		t.Fatalf("hit status = %d, want 502", rr.Code)
	}
	rr = httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "bad-cred", 0)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("miss status = %d, want 403", rr.Code)
	}
	stats := p.CredentialLookupStats()
	if stats["ok"] != 1 || stats["denied"] != 1 {
		t.Fatalf("stats = %v, want ok=1 denied=1", stats)
	}
}

// W3.3：全局 lookup 天花板耗尽后回 429 并记 limited（回查 CPU 被限）。
func TestServeCredentialLookupCeiling(t *testing.T) {
	creds := &stubCreds{machineID: "m1", executionID: "e1"}
	p := credentialTestProxy(creds)
	now := timestubNow(time.Now())
	p.nowFn = now.fn
	p.credMu.Lock()
	p.credTokens = 1
	p.credLast = now.cur
	p.credMu.Unlock()

	rr := httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "good-cred", 0)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("first status = %d, want 502", rr.Code)
	}
	rr = httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "good-cred", 0)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("limited status = %d, want 429", rr.Code)
	}
	if got := p.CredentialLookupStats()["limited"]; got != 1 {
		t.Fatalf("limited counter = %d, want 1", got)
	}
}

// W3.3（P2 修正）：miss 风暴只耗尽 miss 桶（回 429），绝不阻塞合法凭证——
// 命中只受 lookup 天花板约束，每次请求仍查表。
func TestServeCredentialMissNeverBlocksValid(t *testing.T) {
	creds := &stubCreds{machineID: "m1", executionID: "e1"}
	p := credentialTestProxy(creds)
	p.SetCredentialLimits(1000, 1) // miss 桶：1 rps，突发 2

	// 突发内两个 miss 正常 403，之后 miss 回 429（扫描者速率被封顶）。
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "bad-cred", 0)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("miss %d status = %d, want 403", i, rr.Code)
		}
	}
	rr := httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "bad-cred", 0)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("exhausted miss bucket status = %d, want 429", rr.Code)
	}

	// miss 桶空不影响合法凭证：仍查表并放行到 transport。
	before := creds.lookups
	rr = httptest.NewRecorder()
	p.ServeCredential(rr, httptest.NewRequest(http.MethodGet, "http://agent/", nil), "good-cred", 0)
	if rr.Code != http.StatusBadGateway { // guard 通过，终点不可达是 transport 语义
		t.Fatalf("valid status = %d, want 502", rr.Code)
	}
	if creds.lookups != before+1 {
		t.Fatal("valid credential must always consult the table, even with an empty miss bucket")
	}
	stats := p.CredentialLookupStats()
	if stats["denied"] != 2 || stats["limited"] != 1 || stats["ok"] != 1 {
		t.Fatalf("stats = %v, want denied=2 limited=1 ok=1", stats)
	}
}
