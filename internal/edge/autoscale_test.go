package edge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
)

// EWMA 数学：5s 采样、alpha=0.4；恒定 inflight 收敛，归零衰减。
func TestAutoscaleEWMAMath(t *testing.T) {
	tr := NewAutoscaleTracker(100)
	now := time.Now()
	for i := 0; i < 4; i++ {
		tr.Acquire("h")
	}
	s := tr.snapshotForReport(now)
	if got := s["h"].EWMA; got != 0.4*4 {
		t.Fatalf("ewma=%v, want %v", got, 0.4*4)
	}
	if got := s["h"].RPS; got != 4 {
		t.Fatalf("rps=%v, want 4", got)
	}
	if got := s["h"].WinMs; got != 10000 {
		t.Fatalf("win_ms=%d, want 10000", got)
	}
	if s["h"].TsMs != now.UnixMilli() {
		t.Fatalf("ts_ms=%d, want %d", s["h"].TsMs, now.UnixMilli())
	}
	for i := 0; i < 4; i++ {
		tr.Release("h")
	}
	s = tr.snapshotForReport(now)
	// 衰减：1.6+0.4*(0-1.6)=0.96；滚动和：本窗 0 + 上窗 4。
	if got := s["h"].EWMA; got < 0.959 || got > 0.961 {
		t.Fatalf("ewma=%v, want ~0.96", got)
	}
	if got := s["h"].RPS; got != 4 {
		t.Fatalf("rolling rps=%v, want 4", got)
	}
	s = tr.snapshotForReport(now)
	if got := s["h"].RPS; got != 0 {
		t.Fatalf("idle rolling rps=%v, want 0", got)
	}
}

// 有界表：满则逐出最旧，不无界增长。
func TestAutoscaleTrackerBounded(t *testing.T) {
	tr := NewAutoscaleTracker(2)
	tr.Acquire("a")
	tr.Acquire("b")
	tr.Acquire("c")
	tr.mu.Lock()
	n := tr.hosts.len()
	tr.mu.Unlock()
	if n > 2 {
		t.Fatalf("hosts=%d, want <=2", n)
	}
}

// fakeAutoscaleRedis 捕获 Eval（Lua HSET+EXPIRE）调用。
type fakeAutoscaleRedis struct {
	keys []string
	args []any
	err  error
}

func (f *fakeAutoscaleRedis) Eval(_ context.Context, _ string, keys []string, args ...any) *redis.Cmd {
	f.keys = keys
	f.args = args
	cmd := redis.NewCmd(context.Background())
	if f.err != nil {
		cmd.SetErr(f.err)
	} else {
		cmd.SetVal(int64(1))
	}
	return cmd
}

func TestAutoscaleReporterWritesTTLKey(t *testing.T) {
	tr := NewAutoscaleTracker(100)
	counters := &Counters{}
	fr := &fakeAutoscaleRedis{}
	now := time.UnixMilli(1700000000000)
	r := NewAutoscaleReporter(fr, "edge-1", tr, counters, time.Second)
	r.nowFn = func() time.Time { return now }
	tr.Acquire("app.local")
	tr.AddUnserved("app.local")
	r.Report(context.Background())
	if len(fr.keys) != 1 || fr.keys[0] != "autoscale:app.local" {
		t.Fatalf("keys=%v, want [autoscale:app.local]", fr.keys)
	}
	if len(fr.args) != 3 || fr.args[0] != "edge-1" || fr.args[2] != int64(20) {
		t.Fatalf("args=%v, want [edge-1 json 20]", fr.args)
	}
	var s HostSample
	if err := json.Unmarshal([]byte(fr.args[1].(string)), &s); err != nil {
		t.Fatal(err)
	}
	if s.Unserved != 1 || s.RPS != 1 || s.TsMs != now.UnixMilli() || s.WinMs != 10000 {
		t.Fatalf("sample=%+v", s)
	}
	if got := counters.autoscaleReports.Load(); got != 1 {
		t.Fatalf("reports=%d, want 1", got)
	}
	// 失败只记指标。
	frErr := &fakeAutoscaleRedis{err: errors.New("redis down")}
	rErr := NewAutoscaleReporter(frErr, "edge-1", tr, counters, time.Second)
	rErr.nowFn = func() time.Time { return now }
	rErr.Report(context.Background())
	if got := counters.autoscaleReportErrors.Load(); got != 1 {
		t.Fatalf("reportErrors=%d, want 1", got)
	}
	if got := counters.autoscaleReports.Load(); got != 1 {
		t.Fatalf("reports=%d, want still 1", got)
	}
}

func TestResolveEdgeIDFallback(t *testing.T) {
	if got := ResolveEdgeID("e1", "m1", nil); got != "e1" {
		t.Fatalf("got %q", got)
	}
	if got := ResolveEdgeID("", "m1", nil); got != "m1" {
		t.Fatalf("got %q", got)
	}
	if got := ResolveEdgeID("", "", func() (string, error) { return "h", nil }); got != "h" {
		t.Fatalf("got %q", got)
	}
}

// TestHandlerAutoscaleServedAccounting：成功转发恰好一次 served（含 inflight
// 配对释放），与 proxiedReqs 同口径。
func TestHandlerAutoscaleServedAccounting(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer up.Close()
	cat := &fakeCatalog{
		route:    &catalog.Route{Backends: []catalog.Backend{testBackend("m1", strings.TrimPrefix(up.URL, "http://"))}},
		declared: true,
	}
	counters := &Counters{}
	tracker := NewAutoscaleTracker(100)
	h := NewHandler(Config{
		Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
		Tokens: NewTokenClient("", "", time.Minute), Limiter: NewRateLimiter(0, 0),
		Counters: counters, HardConcurrency: 8, Tracker: tracker,
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://app.test/", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("code=%d body=%q", rr.Code, rr.Body.String())
	}
	s := tracker.snapshotForReport(time.Now())
	if got := s["app.test"].RPS; got != 1 {
		t.Fatalf("served rps=%v, want 1", got)
	}
	if got := s["app.test"].Unserved; got != 0 {
		t.Fatalf("unserved=%d, want 0", got)
	}
	// 请求结束 inflight 已配对释放（EWMA 采瞬时值，短请求在拍边界外
	// 不留痕是正确语义；EWMA 收敛由单测覆盖）。
	tracker.mu.Lock()
	e, ok := tracker.hosts.get("app.test")
	tracker.mu.Unlock()
	if !ok || e.inflight != 0 {
		t.Fatalf("inflight not released: ok=%v e=%+v", ok, e)
	}
}

// TestHandlerAutoscaleUnservedClassification：零容量三分路记 unserved；
// pin-miss / hard / 429 明确排除。
func TestHandlerAutoscaleUnservedClassification(t *testing.T) {
	serve := func(h *Handler, host string, setup func(*http.Request)) int {
		r := httptest.NewRequest("GET", "http://"+host+"/", nil)
		if setup != nil {
			setup(r)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr.Code
	}
	mkHandler := func(cat RouteCatalog, hard int64, limiter *RateLimiter) (*Handler, *Counters, *AutoscaleTracker) {
		counters := &Counters{}
		tracker := NewAutoscaleTracker(100)
		return NewHandler(Config{
			Catalog: cat, Routes: NewRouteCache(time.Minute, time.Minute),
			Tokens: NewTokenClient("", "", time.Minute), Limiter: limiter,
			Counters: counters, HardConcurrency: hard, Tracker: tracker,
		}), counters, tracker
	}

	// 1) route 查无 → unserved。
	h, counters, _ := mkHandler(&fakeCatalog{err: ErrNotFound}, 8, NewRateLimiter(0, 0))
	if code := serve(h, "missing.test", nil); code != http.StatusNotFound {
		t.Fatalf("code=%d", code)
	}
	if got := counters.unservedRequests.Load(); got != 1 {
		t.Fatalf("unserved=%d, want 1", got)
	}

	// 2) Backends 为空 → unserved。
	h, counters, _ = mkHandler(&fakeCatalog{route: &catalog.Route{}, declared: true}, 8, NewRateLimiter(0, 0))
	if code := serve(h, "empty.test", nil); code != http.StatusNotFound {
		t.Fatalf("code=%d", code)
	}
	if got := counters.unservedRequests.Load(); got != 1 {
		t.Fatalf("unserved=%d, want 1", got)
	}

	// 3) 无 eligible backend（NOT_READY）→ 503 + unserved。
	bad := testBackend("m1", "127.0.0.1:1")
	bad.Readiness = "NOT_READY"
	h, counters, _ = mkHandler(
		&fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{bad}}, declared: true},
		8, NewRateLimiter(0, 0))
	if code := serve(h, "notready.test", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", code)
	}
	if got := counters.unservedRequests.Load(); got != 1 {
		t.Fatalf("unserved=%d, want 1", got)
	}

	// 4) pin miss → 404 但不记 unserved。
	ok := testBackend("m1", "127.0.0.1:1")
	h, counters, _ = mkHandler(
		&fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{ok}}, declared: true},
		8, NewRateLimiter(0, 0))
	code := serve(h, "pin.test", func(r *http.Request) { r.Header.Set(HeaderPinMachine, "ghost") })
	if code != http.StatusNotFound {
		t.Fatalf("code=%d", code)
	}
	if got := counters.unservedRequests.Load(); got != 0 {
		t.Fatalf("pin-miss unserved=%d, want 0", got)
	}

	// 5) hard limit → 503 + hardRejected，但不记 unserved。
	h, counters, tracker := mkHandler(
		&fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{ok}}, declared: true},
		1, NewRateLimiter(0, 0))
	h.inflight.acquire("m1") // 占满 per-machine 硬限
	code = serve(h, "hard.test", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", code)
	}
	if got := counters.hardRejected.Load(); got != 1 {
		t.Fatalf("hardRejected=%d, want 1", got)
	}
	if got := counters.unservedRequests.Load(); got != 0 {
		t.Fatalf("hard-limit unserved=%d, want 0", got)
	}
	if s := tracker.snapshotForReport(time.Now()); s["hard.test"].HardRejected != 1 {
		t.Fatalf("per-host hard=%+v", s["hard.test"])
	}

	// 6) 429 → 不记 unserved。
	h, counters, _ = mkHandler(
		&fakeCatalog{route: &catalog.Route{Backends: []catalog.Backend{ok}}, declared: true},
		8, NewRateLimiter(1, 1))
	serve(h, "limited.test", nil) // 首请求耗尽桶
	if code := serve(h, "limited.test", nil); code != http.StatusTooManyRequests {
		t.Fatalf("code=%d", code)
	}
	if got := counters.unservedRequests.Load(); got != 0 {
		t.Fatalf("rate-limited unserved=%d, want 0", got)
	}
}

// P2-8：上报间隔防呆（>TTL/2 钳制）与 alpha 随节拍推导。
func TestAutoscaleReporterIntervalGuard(t *testing.T) {
	tr := NewAutoscaleTracker(10)
	r := NewAutoscaleReporter(&fakeAutoscaleRedis{}, "e", tr, nil, 60*time.Second)
	if r.interval != autoscaleKeyTTL/2 {
		t.Fatalf("interval=%v, want clamp to %v", r.interval, autoscaleKeyTTL/2)
	}
	if a := ewmaAlphaForInterval(5 * time.Second); a < 0.39 || a > 0.40 {
		t.Fatalf("alpha(5s)=%v, want ~0.393", a)
	}
	if a := ewmaAlphaForInterval(0); a != ewmaAlpha {
		t.Fatalf("alpha(0)=%v, want default", a)
	}
	if a := ewmaAlphaForInterval(time.Hour); a != 0.95 {
		t.Fatalf("alpha(1h)=%v, want clamp 0.95", a)
	}
}
