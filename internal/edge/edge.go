// Package edge 承载 edge-proxy 的 M4 增强组件（mvp-plan §8.2/8.3）：
//
//   - TokenClient：从控制面按需拉取 execution-bound proxy credential
//     （ADR-0006；仅内存缓存，绝不落盘/进 Redis）；
//   - RouteCache：Redis catalog 的本地缓存 + serve-stale 窗口——Redis 失联时
//     在声明窗口内用 last-known-good 路由继续服务，超窗受控降级 503；
//   - rateLimiter：每 hostname 令牌桶限流。
//
// P1-16/F：RouteCache、RateLimiter 的 key 来自客户端可控的 Host 头，
// TokenClient 的 key 来自路由投影中的 machine/execution——三者共用
// lru.go 的容量有界 LRU 原语（超时访问即淘汰 + 容量 LRU 淘汰），否则
// 随机 hostname 扫描与死 machine 凭证驻留都会使内存无界增长。
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
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
	addr  string // 控制面地址（http://host:port）
	token string // bearer（与 API 同一 token）
	ttl   time.Duration
	stale time.Duration // 回源失败时的 last-known-good 复用窗口
	hc    *http.Client
	// MaxEntries 是缓存容量上限；<=0 时使用 defaultTokenCacheMaxEntries。
	MaxEntries int
	mu         sync.Mutex
	cache      *lruCache[tokenEntry]
	flights    map[string]*tokenFlight
	nowFn      func() time.Time
}

// tokenFlight 合并同 key 的并发回源。
type tokenFlight struct {
	done  chan struct{}
	tok   string
	stale bool // 本次结果是否复用了 stale 窗口内的 last-known-good
	err   error
}

// defaultTokenStale 与 edge route 缓存的 stale 窗口同源（120s，可配）。
const defaultTokenStale = 120 * time.Second

// defaultTokenCacheMaxEntries 是 TokenClient 的默认容量上限
// （FIREPAAS_EDGE_TOKEN_CACHE_MAX 可覆盖；F 与 RouteCache/RateLimiter 同一
// 有界口径）。key = machine\x00execution：死 machine 与其历史 execution 的
// 条目不能租期无限驻留。
const defaultTokenCacheMaxEntries = 10000

// NewTokenClient 构造；addr/token 为空表示禁用（返回的 Get 恒为空串）。
// stale <= 0 时禁用 token serve-stale（回源失败即失败）。
func NewTokenClient(addr, token string, ttl time.Duration) *TokenClient {
	return &TokenClient{
		addr: addr, token: token, ttl: ttl,
		stale:      defaultTokenStale,
		hc:         &http.Client{Timeout: 5 * time.Second},
		MaxEntries: defaultTokenCacheMaxEntries,
		cache:      newLRUCache[tokenEntry](),
		flights:    map[string]*tokenFlight{},
		nowFn:      time.Now,
	}
}

func (t *TokenClient) maxEntries() int {
	if t.MaxEntries > 0 {
		return t.MaxEntries
	}
	return defaultTokenCacheMaxEntries
}

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
// 第二个返回值表示本次是否复用了 stale 窗口内的 last-known-good
// （用于 firepaas_edge_token_stale_serves_total 降级打点）。
func (t *TokenClient) Get(ctx context.Context, machineID, executionID string) (string, bool, error) {
	if t.disabled() {
		return "", false, nil
	}
	now := t.nowFn()
	key := machineID + "\x00" + executionID

	t.mu.Lock()
	if e, ok := t.cache.get(key); ok {
		if now.Sub(e.fetchedAt) < t.ttl {
			t.cache.touch(key)
			t.mu.Unlock()
			return e.token, false, nil
		}
		// 既过 fresh 又过 stale 窗口的条目再无任何服务价值：访问即淘汰，
		// 保证驻留条目只含有价值的 last-known-good（也避免在容量淘汰前
		// 长期占位）。
		if now.Sub(e.fetchedAt) >= t.stale {
			t.cache.delete(key)
		}
	}
	// fresh 未命中：看是否有同 key 回源在途，有则等它。
	if fl, ok := t.flights[key]; ok {
		t.mu.Unlock()
		select {
		case <-fl.done:
			return fl.tok, fl.stale, fl.err
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}
	fl := &tokenFlight{done: make(chan struct{})}
	t.flights[key] = fl
	t.mu.Unlock()

	// 回源在锁外执行（P2-5：不阻塞其它 machine 的取 token）。
	tok, err := t.fetch(ctx, machineID, executionID)
	if err == nil && tok == "" {
		// 防御：fetch 契约不允许 (空, nil)。若显式放行，上层会拿着空凭证
		// 转发 → agent 403 → Invalidate 后原地重试空转；此处收敛为显式错误。
		err = errTokenEmpty
	}

	t.mu.Lock()
	delete(t.flights, key)
	if err == nil {
		t.cache.set(key, tokenEntry{token: tok, execution: executionID, fetchedAt: t.nowFn()})
		t.cache.evict(t.maxEntries())
		t.mu.Unlock()
		fl.tok, fl.err = tok, nil
		close(fl.done)
		return tok, false, nil
	}
	// 回源失败：execution 匹配且在 stale 窗口内 → last-known-good。
	//（超过 stale 窗口的条目已在上面的访问检查中淘汰；此处重新 get 拿到
	// 的一定是仍可用窗口内的条目，或 Invalidate/容量淘汰后不存在。）
	if e, ok := t.cache.get(key); ok && e.execution == executionID && !errors.Is(err, errTokenExecutionMismatch) {
		if now.Sub(e.fetchedAt) < t.stale {
			t.cache.touch(key)
			t.mu.Unlock()
			fl.tok, fl.stale, fl.err = e.token, true, nil
			close(fl.done)
			return e.token, true, nil
		}
	}
	t.mu.Unlock()
	fl.tok, fl.err = "", err
	close(fl.done)
	return "", false, err
}

var errTokenExecutionMismatch = errors.New("traffic-token execution mismatch")

// errTokenEmpty 防御回源返回 (空, nil)；见 Get 中的说明。
var errTokenEmpty = errors.New("traffic-token fetch returned empty token")

// fetch 回源控制面 traffic-token 端点（锁外调用）。
func (t *TokenClient) fetch(ctx context.Context, machineID, executionID string) (string, error) {
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
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("traffic-token %s: HTTP %d", machineID, resp.StatusCode)
	}
	var body struct {
		Token       string `json:"token"`
		ExecutionID string `json:"execution_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.Token == "" {
		return "", fmt.Errorf("traffic-token %s decode: %v", machineID, err)
	}
	if executionID != "" && body.ExecutionID != executionID {
		return "", fmt.Errorf("%w for machine %s", errTokenExecutionMismatch, machineID)
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
	t.cache.deletePrefix(machineID + "\x00")
	t.mu.Unlock()
}

// ---- RouteCache：fresh TTL + serve-stale ----

// CachedRoute 是一条带时间戳的路由。
type CachedRoute struct {
	Value     any
	FetchedAt time.Time
	// Revision 是控制面分配的 route 发布 revision（D-2 单调高水位；来自
	// catalog.Route.Revision，非 route 值/旧投影为 0）。用于拒绝回源拿到
	// 的比当前条目更旧的投影（乱序发布/重放），不回写低 revision 快照。
	Revision int64
}

// routeRevisioner 是 RouteCache revision 守卫的取值接口，由
// catalog.Route 实现（指针接收者、nil 安全）。不实现该接口的值一律
// 视为 revision 0（与旧投影同等待遇）。
type routeRevisioner interface {
	RouteRevision() int64
}

func snapshotRevision(v any) int64 {
	if r, ok := v.(routeRevisioner); ok {
		return r.RouteRevision()
	}
	return 0
}

// defaultRouteCacheMaxEntries 是 RouteCache 的默认容量上限
// （FIREPAAS_EDGE_ROUTE_CACHE_MAX 可覆盖）。
const defaultRouteCacheMaxEntries = 10000

// RouteCache 泛型化的本地路由缓存：load 回源；fresh 窗口内直接命中；
// 超过 fresh 先试回源，失败且仍在 stale 窗口内则降级 serve-stale。
// 容量受 MaxEntries 约束，按 LRU 淘汰（P1-16：key 是客户端 Host 头，
// 无上限会被随机 hostname 扫描打爆内存）。
type RouteCache struct {
	FreshTTL    time.Duration
	StaleWindow time.Duration
	MaxEntries  int // <=0 时使用 defaultRouteCacheMaxEntries

	mu    sync.Mutex
	cache *lruCache[CachedRoute]
	nowFn func() time.Time
	// revisionRejects 累计“回源投影 revision 低于缓存高水位而被拒”的次数
	// （D-2；/metrics 出口为 firepaas_edge_route_revision_rejects_total，
	// 由持有 RouteCache 的 handler/metrics 层读取本计数导出）。
	revisionRejects atomic.Uint64
}

// RevisionRejects 返回 D-2 revision 守卫的拒绝计数（单调计数器）。
func (c *RouteCache) RevisionRejects() uint64 { return c.revisionRejects.Load() }

func NewRouteCache(freshTTL, staleWindow time.Duration) *RouteCache {
	return &RouteCache{
		FreshTTL: freshTTL, StaleWindow: staleWindow, MaxEntries: defaultRouteCacheMaxEntries,
		cache: newLRUCache[CachedRoute](), nowFn: time.Now,
	}
}

func (c *RouteCache) maxEntries() int {
	if c.MaxEntries > 0 {
		return c.MaxEntries
	}
	return defaultRouteCacheMaxEntries
}

// ErrBeyondStale 表示缓存条目已超出 serve-stale 窗口：Redis 失联期间
// last-known-good 也只能服务 StaleWindow 时长，超窗必须受控降级（503），
// 不无限期服务过期路由。
var ErrBeyondStale = errors.New("beyond stale window")

// ErrNotFound 表示权威源明确说 key 不存在（P2-8）。这与"回源失败"
// 是两种语义：权威不存在 → 立即 404（可短暂负缓存，不 serve-stale，
// 否则已删除的路由在 stale 窗口内继续被服务）；回源失败（连接错误/
// 超时）才在 stale 窗口内 serve last-known-good。
var ErrNotFound = errors.New("route not found at origin")

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
	e, ok := c.cache.get(key)
	// 负缓存条目（Value==nil，权威 miss）优先于 fresh 判断：fresh 分支
	// 会把 nil 当有效值返回，负缓存必须先行。
	negFresh := ok && e.Value == nil && now.Sub(e.FetchedAt) < negativeTTL
	fresh := ok && e.Value != nil && now.Sub(e.FetchedAt) < c.FreshTTL
	if negFresh || fresh {
		c.cache.touch(key)
	}
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
		rev := snapshotRevision(v)
		c.mu.Lock()
		if cur, has := c.cache.get(key); has && cur.Value != nil && rev < cur.Revision {
			// D-2 revision 回退守卫：回源拿到的是比缓存更旧的投影（乱序
			// 发布/重放），绝不用旧快照回写高水位 entry——继续服务缓存的
			// last-known-good（它比权威此刻给出的还新；与 serve-stale 同源
			// 的降级语义，计 servedStale）。守卫只针对“同 key 的正向 redraw”：
			// 不影响 fresh 命中、不影响回源失败时的 serve-stale、也不影响
			// 负缓存条目（权威 miss 的墓碑不是可服务投影，不适用新旧比较）。
			c.revisionRejects.Add(1)
			c.cache.touch(key)
			c.mu.Unlock()
			return cur.Value, true, nil
		}
		c.cache.set(key, CachedRoute{Value: v, FetchedAt: now, Revision: rev})
		c.cache.evict(c.maxEntries())
		c.mu.Unlock()
		return v, false, nil
	}
	if errors.Is(err, ErrNotFound) {
		// 权威不存在：删除缓存条目并负缓存，绝不 serve last-known-good。
		// 墓碑保留原 entry 的 revision 高水位：随后到到的低 revision 重放
		// 不能靠"删后重建"绕过高水位（redis 侧高水位键同理）。
		c.mu.Lock()
		rev := int64(0)
		if cur, has := c.cache.get(key); has {
			rev = cur.Revision
		}
		c.cache.set(key, CachedRoute{Value: nil, FetchedAt: now, Revision: rev})
		c.cache.evict(c.maxEntries())
		c.mu.Unlock()
		return nil, false, ErrNotFound
	}
	// 回源失败：stale 窗口内允许 last-known-good（仅此路径 servedStale=true）。
	if ok && e.Value != nil {
		if now.Sub(e.FetchedAt) < c.StaleWindow {
			c.mu.Lock()
			c.cache.touch(key)
			c.mu.Unlock()
			return e.Value, true, nil
		}
		return nil, false, ErrBeyondStale
	}
	return nil, false, err
}

// Invalidate 清空单个 host（发布切换后加速收敛；TTL 兜底）。
func (c *RouteCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.delete(key)
}

// ---- 每 hostname 令牌桶限流 ----

// defaultRateLimiterMaxBuckets 是 RateLimiter 的默认容量上限
// （FIREPAAS_EDGE_RATELIMIT_BUCKETS_MAX 可覆盖）。
const defaultRateLimiterMaxBuckets = 10000

// bucket 是每 hostname 的令牌桶状态（指针存入 LRU，命中时原地更新）。
type bucket struct {
	tokens float64
	last   time.Time
}

// RateLimiter 每 key（hostname）令牌桶。容量受 MaxBuckets 约束，按 LRU
// 淘汰（P1-16：key 是客户端 Host 头，无上限会被随机 hostname 扫描打爆）。
type RateLimiter struct {
	rate       float64 // tokens/sec
	burst      float64
	MaxBuckets int // <=0 时使用 defaultRateLimiterMaxBuckets

	mu    sync.Mutex
	cache *lruCache[*bucket]
	nowFn func() time.Time
}

func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{
		rate: rate, burst: burst, MaxBuckets: defaultRateLimiterMaxBuckets,
		cache: newLRUCache[*bucket](), nowFn: time.Now,
	}
}

func (l *RateLimiter) maxBuckets() int {
	if l.MaxBuckets > 0 {
		return l.MaxBuckets
	}
	return defaultRateLimiterMaxBuckets
}

// allow 返回该 host 本次请求是否放行。
func (l *RateLimiter) Allow(host string) bool {
	if l.rate <= 0 {
		return true
	}
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.cache.get(host)
	if !ok {
		l.cache.set(host, &bucket{tokens: l.burst - 1, last: now})
		l.cache.evict(l.maxBuckets())
		return true
	}
	l.cache.touch(host)
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
