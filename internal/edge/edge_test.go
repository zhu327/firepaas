package edge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// M4.4 核心：Redis 回源失败时，stale 窗口内 serve last-known-good；
// 超窗后 ErrBeyondStale（受控降级 503）。
func TestRouteCacheServeStale(t *testing.T) {
	rc := NewRouteCache(5*time.Second, 60*time.Second)
	now := time.Now()
	rc.nowFn = func() time.Time { return now }

	calls := 0
	loader := func(ctx context.Context, key string) (any, error) {
		calls++
		return "route-v1", nil
	}

	v, _, err := rc.Get(context.Background(), "h", loader)
	if err != nil || v != "route-v1" || calls != 1 {
		t.Fatalf("initial load: %v %v calls=%d", v, err, calls)
	}
	// fresh 窗口内不再回源，且 fresh 命中不是 stale（servedStale 必须为
	// false——仅 last-known-good 降级才算 stale，否则 X-Firepaas-Stale 头
	// 会在正常缓存命中时误报）。
	if _, stale, _ := rc.Get(context.Background(), "h", loader); stale != false || calls != 1 {
		t.Fatalf("fresh window must hit cache without stale flag: stale=%v calls=%d", stale, calls)
	}

	// 过 fresh TTL、回源失败 → 命中 last-known-good（serve-stale，此路径
	// servedStale 才为 true）。
	now = now.Add(6 * time.Second)
	failLoader := func(ctx context.Context, key string) (any, error) { return nil, errors.New("redis down") }
	v, stale, err := rc.Get(context.Background(), "h", failLoader)
	if err != nil || v != "route-v1" || !stale {
		t.Fatalf("within stale window must serve lkg with stale=true: %v %v stale=%v", v, err, stale)
	}

	// 超过 stale 窗口 → 受控降级。
	now = now.Add(61 * time.Second)
	if _, _, err := rc.Get(context.Background(), "h", failLoader); !errors.Is(err, ErrBeyondStale) {
		t.Fatalf("beyond window must fail with ErrBeyondStale, got %v", err)
	}

	// 恢复后正常回源并刷新。
	v, _, err = rc.Get(context.Background(), "h", loader)
	if err != nil || v != "route-v1" {
		t.Fatalf("recovered: %v %v", v, err)
	}
}

// P2-8：权威 miss（load 返回 ErrNotFound）绝不 serve-stale——否则已删除
// 路由会在 stale 窗口内继续被服务；且短暂负缓存内不重复回源。
func TestRouteCacheAuthoritativeMiss(t *testing.T) {
	rc := NewRouteCache(5*time.Second, 60*time.Second)
	now := time.Now()
	rc.nowFn = func() time.Time { return now }

	calls := 0
	missLoader := func(ctx context.Context, key string) (any, error) {
		calls++
		return nil, ErrNotFound
	}
	_, _, err := rc.Get(context.Background(), "gone", missLoader)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("authoritative miss must surface ErrNotFound, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	// 负缓存窗口内重复 miss 不回源。
	if _, _, err := rc.Get(context.Background(), "gone", missLoader); !errors.Is(err, ErrNotFound) {
		t.Fatalf("negative cache must hold, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("negative cache must prevent origin hit, calls=%d", calls)
	}

	// 关键回归：曾有 last-known-good 的 key 被权威删除 → 也必须 404，
	// 不允许 stale 复活（stale 只用于回源失败，不用于权威删除）。
	okLoader := func(ctx context.Context, key string) (any, error) { return "route", nil }
	if _, _, err := rc.Get(context.Background(), "h", okLoader); err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Second) // 过 fresh TTL
	if _, _, err := rc.Get(context.Background(), "h", missLoader); !errors.Is(err, ErrNotFound) {
		t.Fatalf("authoritative delete must not serve stale, got err=%v", err)
	}
}

// P2-8 防御：load 返回 (nil, nil) 规范化为权威 miss。
func TestRouteCacheNilNilIsMiss(t *testing.T) {
	rc := NewRouteCache(5*time.Second, 60*time.Second)
	_, _, err := rc.Get(context.Background(), "h",
		func(ctx context.Context, key string) (any, error) { return nil, nil })
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRateLimiterBurst(t *testing.T) {
	l := NewRateLimiter(10, 3) // burst 3：连打第 4 个立刻拒绝
	for i := 0; i < 3; i++ {
		if !l.Allow("host") {
			t.Fatalf("burst request %d must pass", i+1)
		}
	}
	if l.Allow("host") {
		t.Fatal("request beyond burst must be rejected")
	}
	if !l.Allow("other-host") {
		t.Fatal("independent host must not be affected")
	}
}

// ---- TokenClient（P1-2/P1-3/P2-5） ----

func tokenServer(t *testing.T, token string, fail *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail != nil && fail.Load() {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"token":%q,"execution_id":"exec-1"}`, token)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTokenClientRejectsExecutionMismatchAndRetriesFetch(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := hits.Add(1)
		execution := "wrong-execution"
		if n > 1 {
			execution = "exec-1"
		}
		_, _ = fmt.Fprintf(w, `{"token":"tok-%d","execution_id":%q}`, n, execution)
	}))
	defer srv.Close()
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	ctx := context.Background()

	if tok, err := tc.Get(ctx, "m1", "exec-1"); !errors.Is(err, errTokenExecutionMismatch) || tok != "" {
		t.Fatalf("mismatch: token=%q err=%v", tok, err)
	}
	if tok, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-2" {
		t.Fatalf("retry: token=%q err=%v", tok, err)
	}
	if tok, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-2" {
		t.Fatalf("cache: token=%q err=%v", tok, err)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("mismatch was cached or valid token was not cached: hits=%d", got)
	}
}

func TestTokenClientMismatchDoesNotServeStale(t *testing.T) {
	var mismatch atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		execution := "exec-1"
		if mismatch.Load() {
			execution = "exec-2"
		}
		_, _ = fmt.Fprintf(w, `{"token":"tok","execution_id":%q}`, execution)
	}))
	defer srv.Close()
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	now := time.Now()
	tc.nowFn = func() time.Time { return now }
	if _, err := tc.Get(context.Background(), "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	mismatch.Store(true)
	if tok, err := tc.Get(context.Background(), "m1", "exec-1"); !errors.Is(err, errTokenExecutionMismatch) ||
		tok != "" {
		t.Fatalf("mismatch served stale token: token=%q err=%v", tok, err)
	}
}

// P1-3：回源失败时，execution 匹配的 last-known-good 在 stale 窗口内复用。
func TestTokenClientServeStale(t *testing.T) {
	var fail atomic.Bool
	srv := tokenServer(t, "tok-1", &fail)
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	now := time.Now()
	tc.nowFn = func() time.Time { return now }
	ctx := context.Background()

	if tok, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" {
		t.Fatalf("initial: %v %q", err, tok)
	}
	fail.Store(true)
	// fresh TTL 内命中缓存，不回源。
	if tok, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" {
		t.Fatalf("fresh hit: %v %q", err, tok)
	}
	// 过 fresh TTL：回源失败，但 execution 匹配 + stale 窗口内 → lkg。
	now = now.Add(31 * time.Second)
	if tok, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" {
		t.Fatalf("stale reuse: %v %q", err, tok)
	}
	// 超过 stale 窗口 → 错误（fail-closed，不盲发）。
	now = now.Add(121 * time.Second)
	if _, err := tc.Get(ctx, "m1", "exec-1"); err == nil {
		t.Fatal("beyond stale window must fail")
	}
	// execution 不匹配（换代）→ 不允许用旧 execution 的 lkg。
	if _, err := tc.Get(ctx, "m1", "exec-2"); err == nil {
		t.Fatal("mismatched execution must not reuse stale token")
	}
}

// P1-2：Invalidate 后缓存清空，下次回源。
func TestTokenClientInvalidate(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"token":"tok","execution_id":"exec-1"}`)
	}))
	defer srv.Close()
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	ctx := context.Background()

	if _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	tc.Invalidate("m1") // 丢弃该 machine 全部 execution 的条目
	if _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 {
		t.Fatalf("invalidate must force re-fetch, hits=%d", hits.Load())
	}
}

// P2-5：并发取同一 machine 的 token 只回源一次（flight 合并）；
// 不同 machine 并发不互相阻塞。
func TestTokenClientConcurrentSingleFlight(t *testing.T) {
	var inflight, maxInflight atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		for {
			old := maxInflight.Load()
			if cur <= old || maxInflight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond) // 拉长回源窗口制造并发
		inflight.Add(-1)
		select {
		case <-release:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"token":"tok","execution_id":"exec-1"}`)
	}))
	defer srv.Close()
	close(release)

	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	ctx := context.Background()

	// 8 个并发请求同一 (machine, execution)：应只回源一次。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
				t.Errorf("concurrent get: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxInflight.Load() != 1 {
		t.Fatalf("same-key concurrent gets must single-flight, max inflight=%d", maxInflight.Load())
	}
}
