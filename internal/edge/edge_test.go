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

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
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

	if tok, _, err := tc.Get(ctx, "m1", "exec-1"); !errors.Is(err, errTokenExecutionMismatch) || tok != "" {
		t.Fatalf("mismatch: token=%q err=%v", tok, err)
	}
	if tok, stale, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-2" || stale {
		t.Fatalf("retry: token=%q stale=%v err=%v", tok, stale, err)
	}
	if tok, stale, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-2" || stale {
		t.Fatalf("cache: token=%q stale=%v err=%v", tok, stale, err)
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
	if _, _, err := tc.Get(context.Background(), "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	mismatch.Store(true)
	if tok, _, err := tc.Get(context.Background(), "m1", "exec-1"); !errors.Is(err, errTokenExecutionMismatch) ||
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

	if tok, stale, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" || stale {
		t.Fatalf("initial: %v %q stale=%v", err, tok, stale)
	}
	fail.Store(true)
	// fresh TTL 内命中缓存，不回源，不算 stale 降级。
	if tok, stale, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" || stale {
		t.Fatalf("fresh hit: %v %q stale=%v", err, tok, stale)
	}
	// 过 fresh TTL：回源失败，但 execution 匹配 + stale 窗口内 → lkg。
	// 此路径 stale 标记必须为 true（死指标 token_stale_serves 的信号源）。
	now = now.Add(31 * time.Second)
	if tok, stale, err := tc.Get(ctx, "m1", "exec-1"); err != nil || tok != "tok-1" || !stale {
		t.Fatalf("stale reuse: %v %q stale=%v", err, tok, stale)
	}
	// 超过 stale 窗口 → 错误（fail-closed，不盲发）。
	now = now.Add(121 * time.Second)
	if _, _, err := tc.Get(ctx, "m1", "exec-1"); err == nil {
		t.Fatal("beyond stale window must fail")
	}
	// execution 不匹配（换代）→ 不允许用旧 execution 的 lkg。
	if _, _, err := tc.Get(ctx, "m1", "exec-2"); err == nil {
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

	if _, _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	tc.Invalidate("m1") // 丢弃该 machine 全部 execution 的条目
	if _, _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
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
			if _, _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
				t.Errorf("concurrent get: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxInflight.Load() != 1 {
		t.Fatalf("same-key concurrent gets must single-flight, max inflight=%d", maxInflight.Load())
	}
}

// P1-16：RouteCache 容量上限——随机 hostname 扫描不得使内存无界增长；
// 淘汰按 LRU（最久未用先出）。
func TestRouteCacheBounded(t *testing.T) {
	rc := NewRouteCache(time.Minute, time.Minute)
	rc.MaxEntries = 4
	now := time.Now()
	rc.nowFn = func() time.Time { return now }
	loader := func(ctx context.Context, key string) (any, error) { return "route-" + key, nil }

	for i := 0; i < 100; i++ {
		if _, _, err := rc.Get(context.Background(), fmt.Sprintf("scan-%d.example", i), loader); err != nil {
			t.Fatal(err)
		}
	}
	if rc.cache.len() > 4 {
		t.Fatalf("route cache unbounded: %d entries", rc.cache.len())
	}
	// 最近插入的 key 必须仍在缓存（LRU 淘汰最久未用，不是随机/新条目）。
	if _, ok := rc.cache.get("scan-99.example"); !ok {
		t.Fatal("latest key evicted; LRU order broken")
	}
	if _, ok := rc.cache.get("scan-0.example"); ok {
		t.Fatal("oldest key should have been evicted")
	}
	// 负缓存条目同样受容量约束。
	missLoader := func(ctx context.Context, key string) (any, error) { return nil, ErrNotFound }
	for i := 0; i < 100; i++ {
		if _, _, err := rc.Get(context.Background(), fmt.Sprintf("gone-%d.example", i), missLoader); !errors.Is(
			err,
			ErrNotFound,
		) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	}
	if rc.cache.len() > 4 {
		t.Fatalf("negative cache unbounded: %d entries", rc.cache.len())
	}
	// fresh 命中会把条目提到 LRU 前端，避免热点 key 被淘汰。
	rc2 := NewRouteCache(time.Minute, time.Minute)
	rc2.MaxEntries = 2
	if _, _, err := rc2.Get(context.Background(), "hot", loader); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rc2.Get(context.Background(), "cold", loader); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rc2.Get(context.Background(), "hot", loader); err != nil { // 触碰 hot
		t.Fatal(err)
	}
	if _, _, err := rc2.Get(context.Background(), "new", loader); err != nil { // 淘汰 cold
		t.Fatal(err)
	}
	if _, ok := rc2.cache.get("cold"); ok {
		t.Fatal("LRU must evict least-recently-used (cold), not the touched hot key")
	}
	if _, ok := rc2.cache.get("hot"); !ok {
		t.Fatal("hot key must survive")
	}
}

// F：TokenClient 缓存容量上限——key 是 machine\x00execution，死 machine
// 与历史 execution 的条目不得无界驻留；淘汰按 LRU。
func TestTokenClientCacheBounded(t *testing.T) {
	// 可变 execution 的 token 服务：同一 machine 需要两个 execution 条目来
	// 验证 Invalidate 的前缀删除。
	var execID atomic.Value
	execID.Store("exec-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"token":"tok","execution_id":%q}`, execID.Load().(string))
	}))
	defer srv.Close()
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	tc.MaxEntries = 4
	now := time.Now()
	tc.nowFn = func() time.Time { return now }
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if _, _, err := tc.Get(ctx, fmt.Sprintf("m-%d", i), "exec-1"); err != nil {
			t.Fatal(err)
		}
	}
	if tc.cache.len() > 4 {
		t.Fatalf("token cache unbounded: %d entries", tc.cache.len())
	}
	if _, ok := tc.cache.get("m-19\x00exec-1"); !ok {
		t.Fatal("latest key evicted; LRU order broken")
	}
	if _, ok := tc.cache.get("m-0\x00exec-1"); ok {
		t.Fatal("oldest key should have been evicted")
	}

	// fresh 命中提升 LRU 位置：触碰 m-16 后再插入新 key，被淘汰的必须是
	// 更久未用的 m-17 而非 m-16。
	if _, _, err := tc.Get(ctx, "m-16", "exec-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tc.Get(ctx, "m-new", "exec-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := tc.cache.get("m-16\x00exec-1"); !ok {
		t.Fatal("touched hot key must survive LRU eviction")
	}
	if _, ok := tc.cache.get("m-17\x00exec-1"); ok {
		t.Fatal("LRU must evict least-recently-used (m-17), not the touched m-16")
	}

	// Invalidate 按 machine 前缀删除全部 execution 条目。
	execID.Store("exec-2")
	if _, _, err := tc.Get(ctx, "m-16", "exec-2"); err != nil {
		t.Fatal(err)
	}
	tc.Invalidate("m-16")
	if _, ok := tc.cache.get("m-16\x00exec-1"); ok {
		t.Fatal("invalidate must drop old-execution entry")
	}
	if _, ok := tc.cache.get("m-16\x00exec-2"); ok {
		t.Fatal("invalidate must drop new-execution entry")
	}
}

// F：既过 fresh 又过 stale 窗口的条目已无任何服务价值，访问即淘汰——
// 容量淘汰之前，死值不长期占位。
func TestTokenClientEvictsBeyondStaleEntryOnAccess(t *testing.T) {
	var fail atomic.Bool
	srv := tokenServer(t, "tok-1", &fail)
	tc := NewTokenClient(srv.URL, "bearer", 30*time.Second)
	tc.SetStaleWindow(60 * time.Second)
	now := time.Now()
	tc.nowFn = func() time.Time { return now }
	ctx := context.Background()

	if _, _, err := tc.Get(ctx, "m1", "exec-1"); err != nil {
		t.Fatal(err)
	}
	if tc.cache.len() != 1 {
		t.Fatalf("entries=%d, want 1", tc.cache.len())
	}
	// 超过 stale 窗口且回源失败：条目既不能用，访问即被淘汰。
	now = now.Add(61 * time.Second)
	fail.Store(true)
	if _, _, err := tc.Get(ctx, "m1", "exec-1"); err == nil {
		t.Fatal("beyond stale window must fail")
	}
	if tc.cache.len() != 0 {
		t.Fatalf("beyond-stale entry must be evicted on access, entries=%d", tc.cache.len())
	}
}

// P1-16：RateLimiter buckets 容量上限——随机 hostname 扫描不得使内存无界增长。
func TestRateLimiterBounded(t *testing.T) {
	l := NewRateLimiter(10, 5)
	l.MaxBuckets = 4
	for i := 0; i < 100; i++ {
		if !l.Allow(fmt.Sprintf("scan-%d.example", i)) {
			t.Fatalf("fresh host %d must pass burst", i)
		}
	}
	if l.cache.len() > 4 {
		t.Fatalf("rate limiter buckets unbounded: %d", l.cache.len())
	}
	if _, ok := l.cache.get("scan-99.example"); !ok {
		t.Fatal("latest host bucket evicted; LRU order broken")
	}
	if _, ok := l.cache.get("scan-0.example"); ok {
		t.Fatal("oldest host bucket should have been evicted")
	}
}

// D-2：回源投影 revision 低于缓存高水位时拒绝回写（lkg 保留，计 reject），
// 且守卫不破坏 fresh / serve-stale / 负缓存既有语义。
func TestRouteCacheRevisionGuard(t *testing.T) {
	rc := NewRouteCache(5*time.Second, 60*time.Second)
	now := time.Now()
	rc.nowFn = func() time.Time { return now }

	mkRoute := func(rev int64, machine string) *catalog.Route {
		return &catalog.Route{Revision: rev, Backends: []catalog.Backend{{MachineID: machine}}}
	}
	loaderRev := func(rev int64, machine string) Load {
		return func(ctx context.Context, key string) (any, error) { return mkRoute(rev, machine), nil }
	}

	// 初始：rev 5 入缓存。
	v, _, err := rc.Get(context.Background(), "h", loaderRev(5, "m-new"))
	if err != nil {
		t.Fatal(err)
	}
	if v.(*catalog.Route).Backends[0].MachineID != "m-new" {
		t.Fatalf("initial: %+v", v)
	}
	// 过 fresh TTL，回源给出乱序旧快照 rev 3：拒绝回写，保留 rev 5，
	// 以 lkg 语义服务（servedStale=true），reject 计数 +1。
	now = now.Add(6 * time.Second)
	v, stale, err := rc.Get(context.Background(), "h", loaderRev(3, "m-stale"))
	if err != nil || !stale {
		t.Fatalf("lower-rev redraw: v=%v stale=%v err=%v", v, stale, err)
	}
	if v.(*catalog.Route).Backends[0].MachineID != "m-new" || v.(*catalog.Route).Revision != 5 {
		t.Fatalf("cache must keep newer projection: %+v", v)
	}
	if rc.RevisionRejects() != 1 {
		t.Fatalf("reject counter = %d, want 1", rc.RevisionRejects())
	}
	// 再次回源仍返回 rev 3 → 仍拒绝（不是一次性巧合）。
	now = now.Add(6 * time.Second)
	v, _, _ = rc.Get(context.Background(), "h", loaderRev(3, "m-stale"))
	if v.(*catalog.Route).Revision != 5 || rc.RevisionRejects() != 2 {
		t.Fatalf("second lower-rev redraw must also be rejected: %+v rejects=%d", v, rc.RevisionRejects())
	}
	// 更高 revision（乱序恢复后的新发布）：正常回写。
	now = now.Add(6 * time.Second)
	v, stale, err = rc.Get(context.Background(), "h", loaderRev(6, "m-next"))
	if err != nil || stale {
		t.Fatalf("higher revision must apply: stale=%v err=%v", stale, err)
	}
	if v.(*catalog.Route).Revision != 6 {
		t.Fatalf("higher revision must update cache: %+v", v)
	}
	if rc.RevisionRejects() != 2 {
		t.Fatalf("counter = %d, want 2", rc.RevisionRejects())
	}
	// 同 revision 重绘允许（守卫只拒绝“更低”）——与 catalog Lua 的 > 语义
	// 不同是刻意的：edge 端同 rev 重绘是幂等同值写入，无害。
	now = now.Add(6 * time.Second)
	if _, _, err := rc.Get(context.Background(), "h", loaderRev(6, "m-next2")); err != nil {
		t.Fatal(err)
	}
	if rc.RevisionRejects() != 2 {
		t.Fatalf("equal revision must not be rejected: rejects=%d", rc.RevisionRejects())
	}
}

// D-2 与 serve-stale 正交：回源失败（非低 revision）时 stale 窗口内
// last-known-good 照常服务，不受 revision 守卫影响；旧投影升级路径
// （缓存 rev=0 的旧条目 vs 新 revision 发布）必须能前进。
func TestRouteCacheRevisionGuardKeepsServeStale(t *testing.T) {
	rc := NewRouteCache(5*time.Second, 60*time.Second)
	now := time.Now()
	rc.nowFn = func() time.Time { return now }

	revisioned := func(ctx context.Context, key string) (any, error) {
		return &catalog.Route{Revision: 7, Backends: []catalog.Backend{{MachineID: "m"}}}, nil
	}
	if _, _, err := rc.Get(context.Background(), "h", revisioned); err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Second)
	failLoader := func(ctx context.Context, key string) (any, error) { return nil, errors.New("redis down") }
	v, stale, err := rc.Get(context.Background(), "h", failLoader)
	if err != nil || !stale {
		t.Fatalf("origin failure must serve stale regardless of guard: stale=%v err=%v", stale, err)
	}
	if v.(*catalog.Route).Revision != 7 {
		t.Fatalf("stale entry must be the rev-7 one: %+v", v)
	}

	// 升级路径：手工种 rev=0 的旧式 entry（模拟旧 edge 进程写出的缓存），
	// 新发布的 revision 投影必须覆盖（新 > 旧）。
	rc2 := NewRouteCache(0, time.Minute)
	rc2.nowFn = func() time.Time { return now }
	if _, _, err := rc2.Get(context.Background(), "h", func(ctx context.Context, key string) (any, error) {
		return &catalog.Route{Revision: 0, Backends: []catalog.Backend{{MachineID: "legacy"}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	v, _, err = rc2.Get(context.Background(), "h", loaderRevisioned(9, "modern"))
	if err != nil {
		t.Fatal(err)
	}
	if v.(*catalog.Route).Revision != 9 || v.(*catalog.Route).Backends[0].MachineID != "modern" {
		t.Fatalf("legacy rev-0 entry must be upgraded by revisioned projection: %+v", v)
	}
	if rc2.RevisionRejects() != 0 {
		t.Fatalf("upgrade path must not reject: %d", rc2.RevisionRejects())
	}
}

func loaderRevisioned(rev int64, machine string) Load {
	return func(ctx context.Context, key string) (any, error) {
		return &catalog.Route{Revision: rev, Backends: []catalog.Backend{{MachineID: machine}}}, nil
	}
}
