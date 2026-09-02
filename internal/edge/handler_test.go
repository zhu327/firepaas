package edge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

type fakeCatalog struct {
	route    *catalog.Route
	err      error
	declared bool
}

func (f *fakeCatalog) GetRouteForHostname(context.Context, string) (*catalog.Route, error) {
	return f.route, f.err
}

func (f *fakeCatalog) GetRouteForPort(context.Context, string, int) (*catalog.Route, bool, error) {
	return f.route, f.declared, f.err
}

// acquire/load 仅测试使用：生产路径是 selectAndAcquire + release 的原子
// 组合，单独的 acquire 无并发上限检查，会绕过 hard limit。
func (t *inflightTracker) acquire(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[id]
	if e == nil {
		e = &inflightEntry{}
		t.entries[id] = e
	}
	e.count.Add(1)
}

func (t *inflightTracker) load(id string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[id]; e != nil {
		return e.count.Load()
	}
	return 0
}

func testBackend(id, endpoint string) catalog.Backend {
	return catalog.Backend{
		MachineID:         id,
		ExecutionID:       "e-" + id,
		NodeProxyEndpoint: endpoint,
		AppPort:           80,
		Readiness:         "READY",
	}
}

func testHandler(cat RouteCatalog, hard int) *Handler {
	return NewHandler(
		Config{
			Catalog:         cat,
			Routes:          NewRouteCache(time.Minute, time.Minute),
			Tokens:          NewTokenClient("", "", time.Minute),
			Limiter:         NewRateLimiter(0, 0),
			HardConcurrency: int64(hard),
			EdgePorts:       map[int]bool{8081: true, 8447: true},
		},
	)
}

func TestSelectAndAcquireHonorsHardLimitAtomically(t *testing.T) {
	h := testHandler(nil, 8)
	eligible := []catalog.Backend{testBackend("m1", "")}
	results := make(chan bool, 64)
	for i := 0; i < 64; i++ {
		go func() { _, over := h.inflight.selectAndAcquire(eligible, 8); results <- !over }()
	}
	n := 0
	for i := 0; i < 64; i++ {
		if <-results {
			n++
		}
	}
	if n != 8 {
		t.Fatalf("reserved %d slots, want 8", n)
	}
	if got := h.inflight.load("m1"); got != 8 {
		t.Fatalf("inflight=%d", got)
	}
}

func TestInflightLifecycle(t *testing.T) {
	h := testHandler(nil, 8)
	h.inflight.acquire("m1")
	h.inflight.release("m1")
	if len(h.inflight.snapshot()) != 0 {
		t.Fatal("released entry leaked")
	}
}

func TestRequestRoutePort(t *testing.T) {
	h := testHandler(nil, 8)
	cases := map[string]int{"app.test": 0, "app.test:8081": 0, "app.test:8447": 0, "app.test:80": 80}
	for host, want := range cases {
		r := httptest.NewRequest("GET", "http://"+host+"/", nil)
		if got := h.requestRoutePort(r); got != want {
			t.Fatalf("%s=%d want %d", host, got, want)
		}
	}
	r := WithListenPort(httptest.NewRequest("GET", "http://app.test/", nil), 9000)
	if got := h.requestRoutePort(r); got != 9000 {
		t.Fatalf("listen port=%d", got)
	}
}

func TestHandlerAuthoritativeMissNeverServesStale(t *testing.T) {
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", "127.0.0.1:1")}},
		declared: true,
	}
	h := testHandler(cat, 8)
	// Seed last-known-good without forwarding by filling the cache directly.
	_, _, err := h.routes.Get(
		context.Background(),
		routeCacheKey("app.test", 0),
		func(context.Context, string) (any, error) { return cat.route, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	h.routes.nowFn = func() time.Time { return time.Now().Add(2 * time.Minute) }
	cat.route = nil
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-Firepaas-Stale") != "" {
		t.Fatal("authoritative miss served stale")
	}
}

func TestHandlerRetriesBodylessMarked502OnceWithoutLeakingHeader(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			w.Header().Set(headerProxyRetryable, retryableProxyValue)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	endpoint := strings.TrimPrefix(up.URL, "http://")
	cat := &fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{testBackend("m1", endpoint)}}, declared: true}
	h := testHandler(cat, 8)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	if hits != 2 {
		t.Fatalf("hits=%d", hits)
	}
	if rr.Header().Get(headerProxyRetryable) != "" {
		t.Fatal("internal retry header leaked")
	}
	if rr.Header().Get(HeaderMachineID) != "m1" {
		t.Fatalf("machine=%q", rr.Header().Get(HeaderMachineID))
	}
}

func TestHandlerSanitizesAllClientRoutingHeaders(t *testing.T) {
	seen := make(chan http.Header, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Set(traffic.HeaderCredential, "reflected-secret")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"token":"trusted-credential","execution_id":"e-m1"}`)
	}))
	defer tokens.Close()
	backend := testBackend("m1", strings.TrimPrefix(up.URL, "http://"))
	backend.AppPort = 0
	cat := &fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{backend}}, declared: true}
	h := testHandler(cat, 8)
	h.tokens = NewTokenClient(tokens.URL, "bearer", time.Minute)
	r := httptest.NewRequest("GET", "http://app.test/", nil)
	for name, value := range map[string]string{
		HeaderMachineID:          "forged-machine",
		HeaderExecutionID:        "forged-execution",
		HeaderAppPort:            "65535",
		HeaderPinMachine:         "m1",
		headerProxyRetryable:     retryableProxyValue,
		traffic.HeaderCredential: "forged-credential",
	} {
		r.Header.Set(name, value)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	got := <-seen
	if got.Get(HeaderMachineID) != "m1" || got.Get(HeaderExecutionID) != "e-m1" {
		t.Fatalf(
			"untrusted routing identity forwarded: machine=%q execution=%q",
			got.Get(HeaderMachineID),
			got.Get(HeaderExecutionID),
		)
	}
	if got.Get(traffic.HeaderCredential) != "trusted-credential" {
		t.Fatalf("credential=%q", got.Get(traffic.HeaderCredential))
	}
	for _, name := range []string{HeaderAppPort, HeaderPinMachine, headerProxyRetryable} {
		if value := got.Get(name); value != "" {
			t.Errorf("client-controlled %s forwarded: %q", name, value)
		}
	}
	if got := rr.Header().Get(traffic.HeaderCredential); got != "" {
		t.Fatalf("upstream credential leaked: %q", got)
	}
}

func TestHandlerDoesNotReplayConsumedBody(t *testing.T) {
	var hits int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set(headerProxyRetryable, retryableProxyValue)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	h := testHandler(cat, 8)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "http://app.test/", bytes.NewBufferString("payload")))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("code=%d", rr.Code)
	}
	if hits != 1 {
		t.Fatalf("body request replayed: hits=%d", hits)
	}
}

func TestAttemptStateIsRequestLocal(t *testing.T) {
	h := testHandler(nil, 8)
	state := &attemptState{}
	r := httptest.NewRequest("GET", "http://app.test/", nil)
	r = r.WithContext(context.WithValue(r.Context(), attemptStateKey{}, state))
	r = r.WithContext(context.WithValue(r.Context(), transportRetryKey{}, true))
	rr := httptest.NewRecorder()
	h.handleProxyError(rr, r, errors.New("dial"))
	if state.reason != retryTransport {
		t.Fatalf("reason=%v", state.reason)
	}
	other := &attemptState{}
	if other.reason != retryNone {
		t.Fatal("attempt state leaked")
	}
}

// P0#4：兜底代理错误的响应体必须是固定文案，不得回传 transport 拨号
// 错误（会携带 RFC1918 内部地址）。
func TestHandlerProxyErrorBodyIsGeneric(t *testing.T) {
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", "127.0.0.1:1")}},
		declared: true,
	}
	h := testHandler(cat, 8)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	if body := rr.Body.String(); body != "agent proxy unavailable\n" {
		t.Fatalf("body must be fixed generic text, got %q", body)
	}
	if strings.Contains(rr.Body.String(), "127.0.0.1") {
		t.Fatalf("internal dial error leaked to client: %q", rr.Body.String())
	}
}

// P0#4：token 获取失败的 503 不得携带 X-Firepaas-Machine-ID（选中 backend
// 的内部标识不能泄露给外部客户端的失败响应）。
func TestHandlerTokenFailureDoesNotLeakBackendID(t *testing.T) {
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer tokens.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", "127.0.0.1:1")}},
		declared: true,
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient(tokens.URL, "bearer", 0), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get(HeaderMachineID); got != "" {
		t.Fatalf("token failure leaked backend id: %q", got)
	}
	if counters.tokenErrors.Load() != 1 {
		t.Fatalf("tokenErrors=%d", counters.tokenErrors.Load())
	}
}

// P3：pinHits 每个请求只计一次（含 403 后的单次重试路径）；proxiedReqs
// 同样每客户端请求只计一次（R2 审查 P3：重试不重复计数），且与是否持有
// 凭证无关。
func TestHandlerPinCountedOnceAndProxiedCountsForwards(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n%2 == 1 { // 每个请求的首跳都 403，触发 invalidate+retry
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	// 首次请求不含 pin：route cache 预热；第二次才是 pin 断言对象。
	for _, pin := range []string{"", "m1"} {
		r := httptest.NewRequest("GET", "http://app.test/", nil)
		if pin != "" {
			r.Header.Set(HeaderPinMachine, pin)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("pin=%q code=%d body=%q", pin, rr.Code, rr.Body.String())
		}
	}
	// 主 pin 请求经历了 403 重试，但 pinHits 只能计一次。
	if got := counters.pinHits.Load(); got != 1 {
		t.Fatalf("pinHits=%d, want 1 (no double count across forbidden retry)", got)
	}
	if got := counters.forbiddenRetry.Load(); got != 2 {
		t.Fatalf("forbiddenRetry=%d", got)
	}
	// 凭证为空（tokens 禁用）的转发也计入 proxiedReqs；但每个客户端
	// 请求只计一次（两次请求各经历首跳+重试，重试不计）。
	if got := counters.proxiedReqs.Load(); got != 2 {
		t.Fatalf("proxiedReqs=%d, want 2 (once per client request, retries excluded)", got)
	}
}

// F：readiness 白名单与 publisher machineServing() 契约严格一致——
// READY/UNCONFIGURED 可服务；空串/NOT_READY/拼写漂移一律拒绝并计数。
func TestSelectBackendReadinessStrictness(t *testing.T) {
	mk := func(id, readiness string, draining bool) catalog.Backend {
		b := testBackend(id, "127.0.0.1:1")
		b.Readiness = readiness
		b.Draining = draining
		return b
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: nil, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	route := &catalog.Route{Backends: []catalog.Backend{
		mk("m-ready", "READY", false),
		mk("m-unconfigured", "UNCONFIGURED", false),
		mk("m-empty", "", false),
		mk("m-not-ready", "NOT_READY", false),
		mk("m-typo", "ready", false),
		mk("m-draining-ready", "READY", true),
	}}
	for i := 0; i < 10; i++ {
		chosen, err := h.selectBackend(route, "")
		if err != nil {
			t.Fatal(err)
		}
		if chosen.MachineID != "m-ready" && chosen.MachineID != "m-unconfigured" {
			t.Fatalf("ineligible backend selected: %s readiness=%q", chosen.MachineID, chosen.Readiness)
		}
		h.inflight.release(chosen.MachineID)
	}
	if got := counters.backendIneligibleEmpty.Load(); got == 0 {
		t.Fatal("empty readiness must count as ineligible")
	}
	if got := counters.backendIneligibleNotReady.Load(); got == 0 {
		t.Fatal("NOT_READY must count as ineligible")
	}
	if got := counters.backendIneligibleUnknown.Load(); got == 0 {
		t.Fatal("typo readiness must count as unknown ineligible")
	}

	// pin 到非白名单 readiness 的 machine 必须 pin miss（404 语义）。
	if _, err := h.selectBackend(route, "m-empty"); !errors.Is(err, errPinMiss) {
		t.Fatalf("pin to empty-readiness backend: err=%v", err)
	}

	// 全部非白名单 → errNoEligible。
	onlyBad := &catalog.Route{Backends: []catalog.Backend{mk("m1", "", false), mk("m2", "NOT_READY", false)}}
	if _, err := h.selectBackend(onlyBad, ""); !errors.Is(err, errNoEligible) {
		t.Fatalf("all-ineligible: err=%v", err)
	}
}

// F：handler 层面空 readiness backend 不再被放行；draining 依旧被拒绝。
func TestHandlerRejectsEmptyReadinessBackend(t *testing.T) {
	backend := testBackend("m1", "127.0.0.1:1")
	backend.Readiness = ""
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{backend}},
		declared: true,
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	if got := counters.backendIneligibleEmpty.Load(); got != 1 {
		t.Fatalf("backendIneligibleEmpty=%d, want 1", got)
	}
	out := httptest.NewRecorder()
	counters.WritePrometheus(out)
	if !strings.Contains(out.Body.String(), `firepaas_edge_backend_ineligible_total{reason="empty"} 1`) {
		t.Fatalf("metrics output missing ineligible counter:\n%s", out.Body.String())
	}
}

// F：firepaas_edge_requests_total{code_class=} 每客户端请求恰好计一次，
// 与内部重试（403 retry）无关；/healthz 不计入。
func TestRequestsTotalCodeClassesOncePerClientRequest(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 { // 首次转发 403 触发 invalidate+retry，第二次成功
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	hMiss := NewHandler(Config{ // 权威 miss 口径的独立 handler，共享同一组 counters
		Catalog: &fakeCatalog{route: nil, declared: true}, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	do := func(handler *Handler, host string) int {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest("GET", "http://"+host+"/", nil))
		return rr.Code
	}
	// /healthz 不打点。
	if code := doHealth(h); code != http.StatusOK {
		t.Fatalf("healthz=%d", code)
	}
	if code := do(h, "app.test"); code != http.StatusNoContent { // 403+retry → 204
		t.Fatalf("proxied=%d", code)
	}
	if code := do(hMiss, "unknown.test"); code != http.StatusNotFound { // 无路由 → 404
		t.Fatalf("miss=%d", code)
	}
	if got := counters.req2xx.Load(); got != 1 {
		t.Fatalf("req2xx=%d, want 1 (retry must not double count)", got)
	}
	if got := counters.req4xx.Load(); got != 1 {
		t.Fatalf("req4xx=%d, want 1", got)
	}
	if got := counters.req5xx.Load(); got != 0 {
		t.Fatalf("req5xx=%d, want 0", got)
	}
	out := httptest.NewRecorder()
	counters.WritePrometheus(out)
	body := out.Body.String()
	for _, want := range []string{
		`firepaas_edge_requests_total{code_class="2xx"} 1`,
		`firepaas_edge_requests_total{code_class="4xx"} 1`,
		`firepaas_edge_requests_total{code_class="5xx"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
}

func doHealth(h *Handler) int {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/healthz", nil))
	return rr.Code
}

// P3：回源失败降级复用 stale token 时（死指标）必须打点。
func TestHandlerCountsStaleTokenServes(t *testing.T) {
	var fail atomic.Bool
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, `{"token":"tok-1","execution_id":"e-m1"}`)
	}))
	defer tokens.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	counters := &Counters{}
	tc := NewTokenClient(tokens.URL, "bearer", 30*time.Second)
	now := time.Now()
	tc.nowFn = func() time.Time { return now }
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: tc, Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	do := func() {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
		}
	}
	do() // 正常回源，缓存 token
	if counters.tokenStaleServes.Load() != 0 {
		t.Fatal("fresh token fetch must not count as stale serve")
	}
	// token API 失联、过 fresh TTL 但仍在 stale 窗口 → last-known-good。
	fail.Store(true)
	now = now.Add(31 * time.Second)
	do()
	if got := counters.tokenStaleServes.Load(); got != 1 {
		t.Fatalf("tokenStaleServes=%d, want 1", got)
	}
}

// P3：三个延迟直方图在请求生命周期内打点并导出。
func TestHandlerExportsLatencyHistograms(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	counters := &Counters{}
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	out := httptest.NewRecorder()
	counters.WritePrometheus(out)
	body := out.Body.String()
	for _, want := range []string{
		"# TYPE firepaas_edge_route_lookup_seconds histogram",
		"firepaas_edge_route_lookup_seconds_count 1",
		"firepaas_edge_token_fetch_seconds_count 1",
		"firepaas_edge_upstream_rtt_seconds_count 1",
		"firepaas_edge_upstream_rtt_seconds_bucket{le=\"+Inf\"} 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
}
