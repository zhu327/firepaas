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
func testBackend(id, endpoint string) catalog.Backend {
	return catalog.Backend{MachineID: id, ExecutionID: "e-" + id, NodeProxyEndpoint: endpoint, AppPort: 80, Readiness: "READY"}
}
func testHandler(cat RouteCatalog, hard int) *Handler {
	return NewHandler(Config{Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute), Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0), HardConcurrency: int64(hard), EdgePorts: map[int]bool{8081: true, 8447: true}})
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
	cat := &fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{testBackend("m1", "127.0.0.1:1")}}, declared: true}
	h := testHandler(cat, 8)
	// Seed last-known-good without forwarding by filling the cache directly.
	_, _, err := h.routes.Get(context.Background(), routeCacheKey("app.test", 0), func(context.Context, string) (any, error) { return cat.route, nil })
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
		fmt.Fprint(w, `{"token":"trusted-credential","execution_id":"e-m1"}`)
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
		t.Fatalf("untrusted routing identity forwarded: machine=%q execution=%q", got.Get(HeaderMachineID), got.Get(HeaderExecutionID))
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
	cat := &fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}}, declared: true}
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
