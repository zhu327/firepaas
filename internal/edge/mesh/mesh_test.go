package mesh

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

func newEndpointsForTest(fresh, stale time.Duration,
	fetch func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error),
) *Endpoints {
	return &Endpoints{fresh: fresh, stale: stale, cache: map[string]endpointEntry{}, fetchFn: fetch}
}

func TestEndpointsFreshAndStale(t *testing.T) {
	rec := &catalog.MeshEndpointRecord{
		MachineID: "m1", ExecutionID: "e1",
		NodeULA: "fd7a:9a55:0:1::", IngressPort: 5109,
	}
	var fail bool
	ep := newEndpointsForTest(50*time.Millisecond, 300*time.Millisecond,
		func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error) {
			if fail {
				return nil, context.DeadlineExceeded
			}
			return rec, nil
		})
	got, ok := ep.Get(context.Background(), "m1", "e1")
	if !ok || got.NodeULA != "fd7a:9a55:0:1::" || got.IngressPort != 5109 {
		t.Fatalf("get = (%+v, %v)", got, ok)
	}
	// fresh 窗口内不再回源。
	fail = true
	if _, ok = ep.Get(context.Background(), "m1", "e1"); !ok {
		t.Fatal("fresh cache must serve")
	}
	// 超出 fresh、回源失败、stale 窗口内 → last-known-good。
	time.Sleep(80 * time.Millisecond)
	if got, ok = ep.Get(context.Background(), "m1", "e1"); !ok || got.NodeULA != "fd7a:9a55:0:1::" {
		t.Fatalf("stale window must serve last-known-good: (%+v, %v)", got, ok)
	}
	// 超出 stale → fail-open 到回落。
	time.Sleep(300 * time.Millisecond)
	if _, ok = ep.Get(context.Background(), "m1", "e1"); ok {
		t.Fatal("beyond stale must not serve")
	}
	// 无入口（nil 记录）= miss + 短 TTL 负缓存（W4：fresh 窗口内不再回源）。
	fetches := 0
	ep2 := newEndpointsForTest(time.Minute, time.Minute,
		func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error) {
			fetches++
			return nil, nil
		})
	if _, ok = ep2.Get(context.Background(), "m2", "e2"); ok {
		t.Fatal("missing endpoint must be a miss")
	}
	if _, ok = ep2.Get(context.Background(), "m2", "e2"); ok {
		t.Fatal("missing endpoint must be a miss")
	}
	if fetches != 1 {
		t.Fatalf("negative cache must suppress refetch in fresh window: fetches=%d", fetches)
	}
}

// W2-6：Endpoints 缓存有界——execution 换代会产生新 key，只增不减会持续
// 泄漏；插入前先淘汰超 stale 窗口条目，仍满则按最旧 fetched 淘汰。
func TestEndpointsCacheBounded(t *testing.T) {
	const maxEntries = 4
	newEP := func(fetch func(context.Context, string, string) (*catalog.MeshEndpointRecord, error)) *Endpoints {
		return &Endpoints{
			fresh: time.Minute, stale: time.Minute,
			MaxEntries: maxEntries, cache: map[string]endpointEntry{}, fetchFn: fetch,
		}
	}
	okFetch := func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error) {
		return &catalog.MeshEndpointRecord{
			MachineID: machine, ExecutionID: exec,
			NodeULA: "fd7a:9a55:0:1::", IngressPort: 5109,
		}, nil
	}

	ep := newEP(okFetch)
	for i := 0; i < maxEntries+1; i++ {
		m := fmt.Sprintf("m-%d", i)
		if _, ok := ep.Get(context.Background(), m, "e"); !ok {
			t.Fatalf("get %s miss", m)
		}
	}
	if len(ep.cache) > maxEntries {
		t.Fatalf("endpoint cache unbounded: %d entries", len(ep.cache))
	}
	latest := fmt.Sprintf("m-%d", maxEntries) + "\x00e"
	if _, ok := ep.cache[latest]; !ok {
		t.Fatal("latest endpoint entry evicted")
	}
	if _, ok := ep.cache["m-0\x00e"]; ok {
		t.Fatal("oldest endpoint entry must be evicted first")
	}

	// 负缓存条目同样受容量约束。
	ep2 := newEP(func(context.Context, string, string) (*catalog.MeshEndpointRecord, error) {
		return nil, nil
	})
	for i := 0; i < maxEntries+1; i++ {
		if _, ok := ep2.Get(context.Background(), fmt.Sprintf("miss-%d", i), "e"); ok {
			t.Fatal("nil endpoint must be a miss")
		}
	}
	if len(ep2.cache) > maxEntries {
		t.Fatalf("negative endpoint cache unbounded: %d entries", len(ep2.cache))
	}

	// 超 stale 窗口的条目优先于最旧 fresh 条目被淘汰。
	ep3 := newEP(okFetch)
	now := time.Now()
	ep3.cache["stale\x00e"] = endpointEntry{
		rec:     catalog.MeshEndpointRecord{NodeULA: "fd7a:9a55:0:1::"},
		fetched: now.Add(-2 * time.Minute),
	}
	for i := 0; i < maxEntries-1; i++ {
		ep3.cache[fmt.Sprintf("fresh-%d\x00e", i)] = endpointEntry{
			rec:     catalog.MeshEndpointRecord{NodeULA: "fd7a:9a55:0:1::"},
			fetched: now,
		}
	}
	if _, ok := ep3.Get(context.Background(), "new", "e"); !ok {
		t.Fatal("get new miss")
	}
	if _, ok := ep3.cache["stale\x00e"]; ok {
		t.Fatal("beyond-stale entry must be evicted before fresh entries")
	}
	if _, ok := ep3.cache["fresh-0\x00e"]; !ok {
		t.Fatal("fresh entry must survive stale pruning")
	}
	if len(ep3.cache) > maxEntries {
		t.Fatalf("cache unbounded after stale pruning: %d", len(ep3.cache))
	}
}

// TestTransportRoutingAndHeaders：Transport 按凭证/端口改写目标、剥离
// X-Firepaas 路由头、只留凭证与 AppPort；无 DirectInfo → ErrNoDirect。
func TestTransportRoutingAndHeaders(t *testing.T) {
	var served *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = r
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	_, portStr, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	port, _ := strconv.Atoi(portStr)

	tr := NewTransport(newEndpointsForTest(time.Minute, time.Minute,
		func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error) {
			return &catalog.MeshEndpointRecord{
				MachineID: machine, ExecutionID: exec,
				NodeULA: "127.0.0.1", IngressPort: port,
			}, nil
		}))

	req, _ := http.NewRequestWithContext(
		WithDirectInfo(context.Background(), DirectInfo{
			MachineID: "m1", ExecutionID: "e1", Credential: "tok", AppPort: 8080,
		}), "GET", "http://mesh-direct.invalid/path", nil)
	req.Header.Set("X-Firepaas-Machine-ID", "forged")
	req.Header.Set("X-Firepaas-Execution-ID", "forged")
	req.Header.Set(traffic.HeaderCredential, "client-supplied")

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if served == nil {
		t.Fatal("no request served")
	}
	if served.Host != srv.Listener.Addr().String() {
		t.Fatalf("host = %q, want %q", served.Host, srv.Listener.Addr().String())
	}
	if served.Header.Get("X-Firepaas-Machine-ID") != "" || served.Header.Get("X-Firepaas-Execution-ID") != "" {
		t.Fatal("routing headers must be stripped on the direct path")
	}
	if served.Header.Get(traffic.HeaderCredential) != "tok" {
		t.Fatalf("credential = %q, want edge-computed value", served.Header.Get(traffic.HeaderCredential))
	}
	if served.Header.Get("X-Firepaas-App-Port") != "8080" {
		t.Fatalf("app port = %q", served.Header.Get("X-Firepaas-App-Port"))
	}

	// 无 DirectInfo → ErrNoDirect（回落信号）。
	if _, err := tr.RoundTrip(httptest.NewRequest("GET", "http://mesh-direct.invalid/", nil)); err != ErrNoDirect {
		t.Fatalf("missing direct info err = %v, want ErrNoDirect", err)
	}
	// 无入口（endpoint miss）→ ErrNoDirect。
	tr2 := NewTransport(newEndpointsForTest(time.Minute, time.Minute,
		func(ctx context.Context, machine, exec string) (*catalog.MeshEndpointRecord, error) {
			return nil, nil
		}))
	req2, _ := http.NewRequestWithContext(
		WithDirectInfo(context.Background(), DirectInfo{MachineID: "m", ExecutionID: "e"}),
		"GET", "http://mesh-direct.invalid/", nil)
	if _, err := tr2.RoundTrip(req2); err != ErrNoDirect {
		t.Fatalf("endpoint miss err = %v, want ErrNoDirect", err)
	}
}

// TestWGPeerConfigShape：SyncPeers 生成的 wg 配置形态（接口/peer/AllowedIPs）。
func TestWGPeerConfigShape(t *testing.T) {
	w := NewWG("fp-edge-test", t.TempDir(), 51821)
	w.priv = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	cfg := w.buildPeerConfigLocked([]catalog.MeshPeerRecord{
		{NodeID: "n1", Pubkey: "PUB1", Endpoint: "10.0.0.1:51820", NodePrefix: "fd7a:9a55:0:1::/64"},
		{NodeID: "n2", Pubkey: "PUB2", Endpoint: "10.0.0.2:51820", NodePrefix: "fd7a:9a55:0:2::/64"},
	})
	for _, want := range []string{
		"PrivateKey = AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"ListenPort = 51821",
		"PublicKey = PUB1", "Endpoint = 10.0.0.1:51820", "AllowedIPs = fd7a:9a55:0:1::/64",
		"PublicKey = PUB2", "AllowedIPs = fd7a:9a55:0:2::/64",
	} {
		if !strings.Contains(cfg, want) {
			t.Fatalf("peer config missing %q:\n%s", want, cfg)
		}
	}
}

// TestSplitSelfPeer（P2 独立评审）：自身记录分离 + 自地址构造 + ID 不一致
// fail-closed（自身留 peer 集、不配地址）。
func TestSplitSelfPeer(t *testing.T) {
	peers := []catalog.MeshPeerRecord{
		{NodeID: "edge-hub", NodePrefix: "fd7a:9a55:0:3::/64"},
		{NodeID: "node-a", NodePrefix: "fd7a:9a55:0:1::/64"},
		{NodeID: "node-b", NodePrefix: "fd7a:9a55:0:2::/64"},
	}
	self, filtered := splitSelfPeer(peers, "edge-hub")
	if self == nil || self.NodeID != "edge-hub" {
		t.Fatalf("self = %+v", self)
	}
	if len(filtered) != 2 || filtered[0].NodeID != "node-a" || filtered[1].NodeID != "node-b" {
		t.Fatalf("filtered = %+v", filtered)
	}
	if got := selfULAAddr(self.NodePrefix); got != "fd7a:9a55:0:3::/64" {
		t.Fatalf("self addr = %q", got)
	}
	// ID 不一致：无分离（自身留 peer 集，不配地址）。
	self, filtered = splitSelfPeer(peers, "edge-hub-2")
	if self != nil {
		t.Fatalf("mismatched edge ID must not split self: %+v", self)
	}
	if len(filtered) != 3 {
		t.Fatalf("mismatched edge ID must keep all peers: %+v", filtered)
	}
	if got := selfULAAddr("not-a-prefix"); got != "" {
		t.Fatalf("bad prefix must yield empty addr: %q", got)
	}
}
