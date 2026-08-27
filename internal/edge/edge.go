// Package edge 承载 edge-proxy 的 M4 增强组件（mvp-plan §8.2/8.3）：
//
//   - TokenClient：从控制面按需拉取 execution-bound proxy credential
//     （ADR-0006；仅内存缓存，绝不落盘/进 Redis）；
//   - RouteCache：Redis catalog 的本地缓存 + serve-stale 窗口——Redis 失联时
//     在声明窗口内用 last-known-good 路由继续服务，超窗受控降级 503；
//   - rateLimiter：每 hostname 令牌桶限流。
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---- TokenClient ----

// tokenEntry 是一个 machine 的缓存凭证（原值 + execution 绑定 + 时间戳）。
type tokenEntry struct {
	token     string
	execution string
	fetchedAt time.Time
}

// TokenClient 缓存 machine → credential，过期回源 API。
//
// P1-2/P1-3/P2-5（M4 评审修复）：
//   - 缓存键是 (machineID, executionID)：执行换代后旧 token 不再命中，
//     消除换代窗口内 edge 携旧凭证被 agent 403 直透客户端的问题；
//   - 回源失败（API 不可达/5xx）时，若缓存条目仍在 stale 窗口内且
//     execution 匹配，降级复用 last-known-good（token 与 route 同源的
//     serve-stale 语义；凭证是 execution-bound 确定性派生，execution
//     一致则 token 必然有效）；execution 不匹配或超窗 → 错误（fail-closed）;
//   - HTTP 回源在锁外执行，同 key 并发请求通过 in-flight 合并（等待
//     先行者的结果），不同 key 互不阻塞。
type TokenClient struct {
	addr    string // 控制面地址（http://host:port）
	token   string // bearer（与 API 同一 token）
	ttl     time.Duration
	stale   time.Duration // 回源失败时的 last-known-good 复用窗口
	hc      *http.Client
	mu      sync.Mutex
	entries map[string]tokenEntry
	flights map[string]*tokenFlight
	nowFn   func() time.Time
}

// tokenFlight 合并同 key 的并发回源。
type tokenFlight struct {
	done chan struct{}
	tok  string
	err  error
}

// NewTokenClient 构造；addr/token 为空表示禁用（返回的 Get 恒为空串）。
// stale <= 0 时禁用 token serve-stale（回源失败即失败）。
func NewTokenClient(addr, token string, ttl time.Duration) *TokenClient {
	return &TokenClient{
		addr: addr, token: token, ttl: ttl,
		stale:   defaultTokenStale,
		hc:      &http.Client{Timeout: 5 * time.Second},
		entries: map[string]tokenEntry{},
		flights: map[string]*tokenFlight{},
		nowFn:   time.Now,
	}
}

// defaultTokenStale 与 edge route 缓存的 stale 窗口同源（120s，可配）。
const defaultTokenStale = 120 * time.Second

// SetStaleWindow 覆盖 token serve-stale 窗口（edge main 启动时对齐
// FIREPAAS_EDGE_STALE_WINDOW；窗口语义与路由缓存一致）。
func (t *TokenClient) SetStaleWindow(d time.Duration) {
	if d > 0 {
		t.stale = d
	}
}

func (t *TokenClient) disabled() bool { return t == nil || t.addr == "" || t.token == "" }

// Get 返回 machine 当前 execution 的 credential。
// executionID 为空时退化为旧语义（仅按 machine 缓存，供兼容路径）。
// 命中条件：缓存 execution 与请求 execution 完全一致且未过 TTL；
// 回源失败时若 execution 一致且在 stale 窗口内 → last-known-good。
func (t *TokenClient) Get(ctx context.Context, machineID, executionID string) (string, error) {
	if t.disabled() {
		return "", nil
	}
	now := t.nowFn()
	key := machineID + "\x00" + executionID

	t.mu.Lock()
	if e, ok := t.entries[key]; ok {
		if now.Sub(e.fetchedAt) < t.ttl {
			t.mu.Unlock()
			return e.token, nil
		}
	}
	// fresh 未命中：看是否有同 key 回源在途，有则等它。
	if fl, ok := t.flights[key]; ok {
		t.mu.Unlock()
		select {
		case <-fl.done:
			return fl.tok, fl.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	fl := &tokenFlight{done: make(chan struct{})}
	t.flights[key] = fl
	t.mu.Unlock()

	// 回源在锁外执行（P2-5：不阻塞其它 machine 的取 token）。
	tok, err := t.fetch(ctx, machineID)

	t.mu.Lock()
	delete(t.flights, key)
	if err == nil && tok != "" {
		t.entries[key] = tokenEntry{token: tok, execution: executionID, fetchedAt: t.nowFn()}
		t.mu.Unlock()
		fl.tok, fl.err = tok, nil
		close(fl.done)
		return tok, nil
	}
	// 回源失败：execution 匹配且在 stale 窗口内 → last-known-good。
	if e, ok := t.entries[key]; ok && e.execution == executionID {
		if now.Sub(e.fetchedAt) < t.stale {
			t.mu.Unlock()
			fl.tok, fl.err = e.token, nil
			close(fl.done)
			return e.token, nil
		}
	}
	t.mu.Unlock()
	fl.tok, fl.err = "", err
	close(fl.done)
	return "", err
}

// fetch 回源控制面 traffic-token 端点（锁外调用）。
func (t *TokenClient) fetch(ctx context.Context, machineID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/machines/%s/traffic-token", t.addr, machineID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+t.token)
	resp, err := t.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("traffic-token %s: HTTP %d", machineID, resp.StatusCode)
	}
	var body struct{ Token, ExecutionID string }
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.Token == "" {
		return "", fmt.Errorf("traffic-token %s decode: %v", machineID, err)
	}
	return body.Token, nil
}

// Invalidate 主动丢弃某 machine 的全部缓存条目（收到 agent proxy 403 后
// 调用，强制下次回源拿新 execution 的凭证）。
func (t *TokenClient) Invalidate(machineID string) {
	if t.disabled() {
		return
	}
	t.mu.Lock()
	for k := range t.entries {
		if strings.HasPrefix(k, machineID+"\x00") {
			delete(t.entries, k)
		}
	}
	t.mu.Unlock()
}

// ---- RouteCache：fresh TTL + serve-stale ----

// CachedRoute 是一条带时间戳的路由。
type CachedRoute struct {
	Value     any
	FetchedAt time.Time
}

// RouteCache 泛型化的本地路由缓存：load 回源；fresh 窗口内直接命中；
// 超过 fresh 先试回源，失败且仍在 stale 窗口内则降级 serve-stale。
type RouteCache struct {
	FreshTTL    time.Duration
	StaleWindow time.Duration

	mu      sync.Mutex
	entries map[string]CachedRoute
	nowFn   func() time.Time
}

func NewRouteCache(freshTTL, staleWindow time.Duration) *RouteCache {
	return &RouteCache{
		FreshTTL: freshTTL, StaleWindow: staleWindow,
		entries: map[string]CachedRoute{}, nowFn: time.Now,
	}
}

// Stale 表示本次命中的是 last-known-good（数据面在 Redis 故障窗口内继续服务）。
var ErrBeyondStale = fmt.Errorf("beyond stale window")

// ErrNotFound 表示权威源明确说 key 不存在（P2-8）。这与"回源失败"
// 是两种语义：权威不存在 → 立即 404（可短暂负缓存，不 serve-stale，
// 否则已删除的路由在 stale 窗口内继续被服务）；回源失败（连接错误/
// 超时）才在 stale 窗口内 serve last-known-good。
var ErrNotFound = fmt.Errorf("route not found at origin")

// Load 尝试回源；返回 (值, 是否来自缓存)。
//
// 错误约定（P2-8）：load 返回 (nil, ErrNotFound) = 权威不存在；
// 其它非 nil error = 回源失败（可 serve-stale）；返回 (nil, nil) 也
// 按权威不存在处理（防御：调用方不应返回无错误的无值）。
type Load func(ctx context.Context, key string) (any, error)

// negativeTTL 是权威 miss 的负缓存时长：期间内同 key 直接 404，不回源。
// （防止恶意 hostname 扫描打穿到 Redis。）
const negativeTTL = 5 * time.Second

func (c *RouteCache) Get(ctx context.Context, key string, load Load) (any, bool, error) {
	now := c.nowFn()
	c.mu.Lock()
	e, ok := c.entries[key]
	// 负缓存条目（Value==nil，权威 miss）优先于 fresh 判断：fresh 分支
	// 会把 nil 当有效值返回，负缓存必须先行。
	negFresh := ok && e.Value == nil && now.Sub(e.FetchedAt) < negativeTTL
	fresh := ok && e.Value != nil && now.Sub(e.FetchedAt) < c.FreshTTL
	c.mu.Unlock()
	if negFresh {
		return nil, false, ErrNotFound
	}
	if fresh {
		// fresh 命中不是 stale：第二个返回值仅表示"本次是否在降级服务
		//（last-known-good）"，fresh 命中是正常缓存行为，不打 stale 标记。
		return e.Value, false, nil
	}
	v, err := load(ctx, key)
	if err == nil && v == nil {
		err = ErrNotFound // 规范化：无错误无值 = 权威不存在
	}
	if err == nil {
		c.mu.Lock()
		c.entries[key] = CachedRoute{Value: v, FetchedAt: now}
		c.mu.Unlock()
		return v, false, nil
	}
	if errors.Is(err, ErrNotFound) {
		// 权威不存在：删除缓存条目并负缓存，绝不 serve last-known-good。
		c.mu.Lock()
		c.entries[key] = CachedRoute{Value: nil, FetchedAt: now}
		c.mu.Unlock()
		return nil, false, ErrNotFound
	}
	// 回源失败：stale 窗口内允许 last-known-good（仅此路径 servedStale=true）。
	if ok && e.Value != nil {
		if now.Sub(e.FetchedAt) < c.StaleWindow {
			return e.Value, true, nil
		}
		return nil, false, ErrBeyondStale
	}
	return nil, false, err
}

// Invalidate 清空单个 host（发布切换后加速收敛；TTL 兜底）。
func (c *RouteCache) Invalidate(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// ---- 每 hostname 令牌桶限流 ----

type bucket struct {
	tokens float64
	last   time.Time
}

// RateLimiter 每 key（hostname）令牌桶。
type RateLimiter struct {
	rate    float64 // tokens/sec
	burst   float64
	mu      sync.Mutex
	buckets map[string]*bucket
	nowFn   func() time.Time
}

func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{rate: rate, burst: burst,
		buckets: map[string]*bucket{}, nowFn: time.Now}
}

// allow 返回该 host 本次请求是否放行。
func (l *RateLimiter) Allow(host string) bool {
	if l.rate <= 0 {
		return true
	}
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[host]
	if !ok {
		l.buckets[host] = &bucket{tokens: l.burst - 1, last: now}
		return true
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
