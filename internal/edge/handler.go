package edge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

const (
	HeaderMachineID      = "X-Firepaas-Machine-ID"
	HeaderExecutionID    = "X-Firepaas-Execution-ID"
	HeaderPinMachine     = "X-Firepaas-Pin-Machine"
	HeaderAppPort        = "X-Firepaas-App-Port"
	headerProxyRetryable = "X-Firepaas-Proxy-Retryable"
	retryableProxyValue  = "true"
)

// RouteCatalog is the read side of the Redis route projection used by edge.
// catalog.Catalog implements it directly; this boundary does not translate or
// duplicate the persisted representation.
type RouteCatalog interface {
	GetRouteForHostname(context.Context, string) (*catalog.Route, error)
	GetRouteForPort(context.Context, string, int) (*catalog.Route, bool, error)
}

// Counters contains the edge request-lifecycle metrics.
type Counters struct {
	staleServes, beyondStale, redisErrors, tokenErrors atomic.Uint64
	tokenStaleServes, rateLimited, proxiedReqs         atomic.Uint64
	forbiddenRetry, hardRejected, pinHits, pinMisses   atomic.Uint64
	// 非服务态 readiness 被过滤的 backend 观测计数（审视 publisher 契约
	// 偏差；reason 见 selectBackend）。
	backendIneligibleEmpty, backendIneligibleNotReady, backendIneligibleUnknown atomic.Uint64
	// 每客户端请求一次的状态码分母（2xx/4xx/5xx；1xx 升级不计）。
	req2xx, req4xx, req5xx atomic.Uint64

	histOnce                             sync.Once
	routeLookup, tokenFetch, upstreamRTT *latencyHistogram
}

// observeRequest 在 handler 出口记录最终响应码分类。每客户端请求恰好
// 计一次（与内部 forbidden/transport 重试次数无关）；WS 101 升级不计入
// 三个分类（非 2/4/5xx）。/healthz 在打点前提前返回，不计入。
func (c *Counters) observeRequest(status int) {
	switch status / 100 {
	case 2:
		c.req2xx.Add(1)
	case 4:
		c.req4xx.Add(1)
	case 5:
		c.req5xx.Add(1)
	}
}

// defaultLatencyBuckets 是延迟直方图的固定秒桶（Prometheus 默认桶的裁剪版）。
var defaultLatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// latencyHistogram 是最小固定桶直方图（秒），与既有 hand-rolled 指标风格一致。
type latencyHistogram struct {
	bounds []float64
	counts []atomic.Uint64 // len(bounds)+1；最后一格即 +Inf
	sum    atomic.Uint64   // math.Float64bits 位存累计值
	count  atomic.Uint64
}

func newLatencyHistogram(bounds []float64) *latencyHistogram {
	return &latencyHistogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *latencyHistogram) observe(seconds float64) {
	i := 0
	for i < len(h.bounds) && seconds > h.bounds[i] {
		i++
	}
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sum.Load()
		if h.sum.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+seconds)) {
			return
		}
	}
}

func (h *latencyHistogram) writePrometheus(w http.ResponseWriter, name, help string) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	var cumulative uint64
	for i, b := range h.bounds {
		cumulative += h.counts[i].Load()
		_, _ = fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, strconv.FormatFloat(b, 'g', -1, 64), cumulative)
	}
	cumulative += h.counts[len(h.bounds)].Load()
	_, _ = fmt.Fprintf(
		w,
		"%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n",
		name, cumulative, name, math.Float64frombits(h.sum.Load()), name, h.count.Load(),
	)
}

// ensureHistograms 惰性初始化直方图（允许调用方零值构造 &Counters{}）。
func (c *Counters) ensureHistograms() {
	c.histOnce.Do(func() {
		c.routeLookup = newLatencyHistogram(defaultLatencyBuckets)
		c.tokenFetch = newLatencyHistogram(defaultLatencyBuckets)
		c.upstreamRTT = newLatencyHistogram(defaultLatencyBuckets)
	})
}

// observeRouteLookup 记录一次路由查询（含本地缓存命中）耗时，秒。
func (c *Counters) observeRouteLookup(seconds float64) {
	c.ensureHistograms()
	c.routeLookup.observe(seconds)
}

// observeTokenFetch 记录一次 traffic token 获取（含本地缓存命中）耗时，秒。
func (c *Counters) observeTokenFetch(seconds float64) {
	c.ensureHistograms()
	c.tokenFetch.observe(seconds)
}

// observeUpstreamRTT 记录一次转发给 agent proxy 的耗时，秒。
// 对 WS/SSE 长连接，这一观测是整个会话时长而非单次 RTT（已知口径）。
func (c *Counters) observeUpstreamRTT(seconds float64) {
	c.ensureHistograms()
	c.upstreamRTT.observe(seconds)
}

func (c *Counters) WritePrometheus(w http.ResponseWriter) {
	write := func(name, help string, v uint64) {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	write("firepaas_edge_stale_serves_total", "requests served from last-known-good route cache", c.staleServes.Load())
	write("firepaas_edge_beyond_stale_total", "requests rejected 503 beyond serve-stale window", c.beyondStale.Load())
	write("firepaas_edge_redis_errors_total", "route catalog origin fetch failures", c.redisErrors.Load())
	write("firepaas_edge_token_errors_total", "traffic token fetch failures", c.tokenErrors.Load())
	write("firepaas_edge_token_stale_serves_total", "requests using last-known-good token", c.tokenStaleServes.Load())
	write("firepaas_edge_rate_limited_total", "requests rejected 429", c.rateLimited.Load())
	write("firepaas_edge_proxied_requests_total", "requests forwarded to agent proxy", c.proxiedReqs.Load())
	write(
		"firepaas_edge_forbidden_retries_total",
		"agent 403 responses triggering token invalidate+retry",
		c.forbiddenRetry.Load(),
	)
	write(
		"firepaas_edge_hard_rejected_total",
		"requests rejected 503 at per-machine hard concurrency limit",
		c.hardRejected.Load(),
	)
	write(
		"firepaas_edge_pin_hits_total",
		"requests pinned to an eligible backend via X-Firepaas-Pin-Machine",
		c.pinHits.Load(),
	)
	write("firepaas_edge_pin_misses_total", "pin requests whose machine was not eligible (404)", c.pinMisses.Load())
	_, _ = fmt.Fprint(
		w,
		"# HELP firepaas_edge_backend_ineligible_total backend observations filtered out by non-serving readiness during selection\n"+
			"# TYPE firepaas_edge_backend_ineligible_total counter\n",
	)
	for _, kv := range []struct {
		reason string
		v      uint64
	}{
		{"empty", c.backendIneligibleEmpty.Load()},
		{"not_ready", c.backendIneligibleNotReady.Load()},
		{"unknown", c.backendIneligibleUnknown.Load()},
	} {
		_, _ = fmt.Fprintf(w, "firepaas_edge_backend_ineligible_total{reason=%q} %d\n", kv.reason, kv.v)
	}
	_, _ = fmt.Fprint(
		w,
		"# HELP firepaas_edge_requests_total client requests by final status code class (one per request, retries not double-counted)\n"+
			"# TYPE firepaas_edge_requests_total counter\n",
	)
	for _, kv := range []struct {
		class string
		v     uint64
	}{
		{"2xx", c.req2xx.Load()},
		{"4xx", c.req4xx.Load()},
		{"5xx", c.req5xx.Load()},
	} {
		_, _ = fmt.Fprintf(w, "firepaas_edge_requests_total{code_class=%q} %d\n", kv.class, kv.v)
	}
	c.ensureHistograms()
	c.routeLookup.writePrometheus(
		w,
		"firepaas_edge_route_lookup_seconds",
		"route catalog lookup latency (cache hits included)",
	)
	c.tokenFetch.writePrometheus(
		w,
		"firepaas_edge_token_fetch_seconds",
		"traffic token fetch latency (cache hits included)",
	)
	c.upstreamRTT.writePrometheus(
		w,
		"firepaas_edge_upstream_rtt_seconds",
		"upstream agent proxy round-trip (WS/SSE sessions report session duration)",
	)
}

// statusRecorder 记录最终响应码用于结构化访问日志。实现 Unwrap 供
// http.ResponseController 穿透到原始 writer（net/http 及
// httputil.ReverseProxy 的 Flush/Hijack 均走 ResponseController），
// 因此不破坏 WebSocket 升级与 SSE flush 语义。
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wrote {
		s.wrote = true // 未显式 WriteHeader 时默认 200
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

type (
	inflightEntry   struct{ count atomic.Int64 }
	inflightTracker struct {
		mu      sync.Mutex
		entries map[string]*inflightEntry
	}
)

func newInflightTracker() *inflightTracker {
	return &inflightTracker{entries: map[string]*inflightEntry{}}
}

func (t *inflightTracker) release(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[id]; e != nil && e.count.Add(-1) == 0 {
		delete(t.entries, id)
	}
}

func (t *inflightTracker) snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int64{}
	for id, e := range t.entries {
		if n := e.count.Load(); n > 0 {
			out[id] = n
		}
	}
	return out
}

func (t *inflightTracker) selectAndAcquire(bs []catalog.Backend, hard int64) (catalog.Backend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	min := int64(-1)
	candidates := make([]catalog.Backend, 0, len(bs))
	for _, b := range bs {
		n := int64(0)
		if e := t.entries[b.MachineID]; e != nil {
			n = e.count.Load()
		}
		if min < 0 || n < min {
			min = n
			candidates = append(candidates[:0], b)
		} else if n == min {
			candidates = append(candidates, b)
		}
	}
	if min >= hard {
		return catalog.Backend{}, true
	}
	chosen := candidates[rand.Intn(len(candidates))]
	e := t.entries[chosen.MachineID]
	if e == nil {
		e = &inflightEntry{}
		t.entries[chosen.MachineID] = e
	}
	e.count.Add(1)
	return chosen, false
}

// Config provides the complete edge HTTP lifecycle dependencies.
type Config struct {
	Catalog         RouteCatalog
	Routes          *RouteCache
	Tokens          *TokenClient
	Limiter         *RateLimiter
	Counters        *Counters
	AgentTLS        *tls.Config
	HardConcurrency int64
	EdgePorts       map[int]bool
}

type Handler struct {
	catalog         RouteCatalog
	routes          *RouteCache
	tokens          *TokenClient
	limiter         *RateLimiter
	cnt             *Counters
	agentTLS        *tls.Config
	proxy           *httputil.ReverseProxy
	inflight        *inflightTracker
	hardConcurrency int64
	edgePorts       map[int]bool
}

type (
	backendKey        struct{}
	credKey           struct{}
	transportRetryKey struct{}
	listenPortKey     struct{}
	attemptStateKey   struct{}
	retryReason       uint8
)

const (
	retryNone retryReason = iota
	retryForbidden
	retryTransport
)

type attemptState struct{ reason retryReason }

var (
	errRetryForbidden  = errors.New("agent proxy 403: retry with fresh token")
	errRetryProxyRoute = errors.New("agent proxy retryable route failure")
	errNoEligible      = errors.New("no ready backend")
	errPinMiss         = errors.New("pinned machine is not eligible")
	errHardLimit       = errors.New("backend hard concurrency limit")
)

func NewHandler(cfg Config) *Handler {
	if cfg.Counters == nil {
		cfg.Counters = &Counters{}
	}
	if cfg.Limiter == nil {
		cfg.Limiter = NewRateLimiter(0, 0)
	}
	if cfg.HardConcurrency <= 0 {
		cfg.HardConcurrency = 256
	}
	h := &Handler{
		catalog:         cfg.Catalog,
		routes:          cfg.Routes,
		tokens:          cfg.Tokens,
		limiter:         cfg.Limiter,
		cnt:             cfg.Counters,
		agentTLS:        cfg.AgentTLS,
		inflight:        newInflightTracker(),
		hardConcurrency: cfg.HardConcurrency,
		edgePorts:       cfg.EdgePorts,
	}
	h.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			b, _ := req.Context().Value(backendKey{}).(catalog.Backend)
			// Strip every edge-owned header before adding authoritative values.
			// In particular, a zero AppPort means the compatibility path where
			// the header must be absent, not that a client value may survive.
			for _, name := range []string{
				HeaderMachineID,
				HeaderExecutionID,
				HeaderAppPort,
				HeaderPinMachine,
				headerProxyRetryable,
				traffic.HeaderCredential,
			} {
				req.Header.Del(name)
			}
			if h.agentTLS != nil {
				req.URL.Scheme = "https"
			} else {
				req.URL.Scheme = "http"
			}
			req.URL.Host = b.NodeProxyEndpoint
			req.Header.Set(HeaderMachineID, b.MachineID)
			req.Header.Set(HeaderExecutionID, b.ExecutionID)
			if b.AppPort > 0 {
				req.Header.Set(HeaderAppPort, strconv.Itoa(b.AppPort))
			}
			if cred, _ := req.Context().Value(credKey{}).(string); cred != "" {
				req.Header.Set(traffic.HeaderCredential, cred)
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			retryable := resp.StatusCode == http.StatusBadGateway &&
				resp.Header.Get(headerProxyRetryable) == retryableProxyValue
			resp.Header.Del(headerProxyRetryable)
			// The execution credential is edge→agent-only, even if an upstream
			// implementation accidentally reflects or authors this header.
			resp.Header.Del(traffic.HeaderCredential)
			if b, ok := resp.Request.Context().Value(backendKey{}).(catalog.Backend); ok {
				resp.Header.Set(HeaderMachineID, b.MachineID)
			}
			if retryable {
				_ = resp.Body.Close()
				return errRetryProxyRoute
			}
			if resp.StatusCode == http.StatusForbidden {
				_ = resp.Body.Close()
				return errRetryForbidden
			}
			return nil
		},
		ErrorHandler: h.handleProxyError,
		Transport: &http.Transport{
			TLSClientConfig:     cfg.AgentTLS,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	return h
}

// WithListenPort marks the concrete edge listener used for a request.
func WithListenPort(r *http.Request, port int) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), listenPortKey{}, port))
}

func requestHasNoBody(r *http.Request) bool { return r.Body == nil || r.Body == http.NoBody }
func (h *Handler) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	state, _ := r.Context().Value(attemptStateKey{}).(*attemptState)
	mark := func(reason retryReason) bool {
		if state == nil {
			return false
		}
		state.reason = reason
		return true
	}
	if errors.Is(err, errRetryForbidden) {
		if mark(retryForbidden) {
			return
		}
	}
	if errors.Is(err, errRetryProxyRoute) {
		if r.Context().Value(transportRetryKey{}) == true && r.Context().Err() == nil && requestHasNoBody(r) &&
			mark(retryTransport) {
			return
		}
		http.Error(w, "agent proxy unavailable", http.StatusBadGateway)
		return
	}
	if r.Context().Value(transportRetryKey{}) == true && r.Context().Err() == nil && requestHasNoBody(r) &&
		mark(retryTransport) {
		return
	}
	// P0#4：兜底分支不回传 err.Error()——transport 拨号错误通常携带
	// RFC1918 内部地址，对客户端统一固定文案，细节只进内部日志。
	slog.Warn("edge upstream proxy error", "error", err)
	http.Error(w, "agent proxy unavailable", http.StatusBadGateway)
}

func routeCacheKey(host string, port int) string {
	if port == 0 {
		return host + "|primary"
	}
	return host + "|" + strconv.Itoa(port)
}

func (h *Handler) loadRoute(ctx context.Context, host string, port int) (any, error) {
	var r *catalog.Route
	var err error
	if port == 0 {
		r, err = h.catalog.GetRouteForHostname(ctx, host)
	} else {
		var declared bool
		r, declared, err = h.catalog.GetRouteForPort(ctx, host, port)
		if err == nil && !declared {
			r = nil
		}
	}
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ErrNotFound
	}
	return r, nil
}

func (h *Handler) requestRoutePort(r *http.Request) int {
	if p, ok := r.Context().Value(listenPortKey{}).(int); ok && p > 0 {
		return p
	}
	if _, raw, err := net.SplitHostPort(r.Host); err == nil {
		if p, e := strconv.Atoi(raw); e == nil && p > 0 && !h.edgePorts[p] {
			return p
		}
	}
	return 0
}

func stripPort(hostport string) string {
	h, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport
	}
	return h
}

func writePlain(w http.ResponseWriter, code int, s string) {
	w.WriteHeader(code)
	_, _ = w.Write([]byte(s))
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writePlain(w, http.StatusOK, "ok\n")
		return
	}
	start := time.Now()
	host := stripPort(r.Host)
	port := h.requestRoutePort(r)
	// 结构化访问日志：host/port/backend/outcome/duration。凭证、token、
	// Authorization 等敏感材料绝不进日志（AGENTS.md 数据边界）。
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	w = rec
	backendID := ""
	defer func() {
		h.cnt.observeRequest(rec.status)
		slog.Info("edge request",
			"host", host,
			"port", port,
			"backend", backendID,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}()
	if !h.limiter.Allow(host) {
		h.cnt.rateLimited.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	key := routeCacheKey(host, port)
	lookupStart := time.Now()
	v, stale, err := h.routes.Get(
		r.Context(),
		key,
		func(ctx context.Context, _ string) (any, error) { return h.loadRoute(ctx, host, port) },
	)
	h.cnt.observeRouteLookup(time.Since(lookupStart).Seconds())
	if errors.Is(err, ErrNotFound) {
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}
	if errors.Is(err, ErrBeyondStale) {
		h.cnt.beyondStale.Add(1)
		w.Header().Set("Retry-After", "30")
		http.Error(w, "route catalog unavailable beyond serve-stale window", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		h.cnt.redisErrors.Add(1)
		http.Error(w, "catalog error", http.StatusServiceUnavailable)
		return
	}
	route, ok := v.(*catalog.Route)
	if !ok || route == nil || len(route.Backends) == 0 {
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}
	backend, err := h.selectBackend(route, r.Header.Get(HeaderPinMachine))
	if err != nil {
		h.writeSelectionError(w, err)
		return
	}
	backendID = backend.MachineID
	if r.Header.Get(HeaderPinMachine) != "" {
		h.cnt.pinHits.Add(1) // 每个请求只计一次；重试路径不重复计数
	}
	if stale {
		h.cnt.staleServes.Add(1)
	}
	cred, tokenStale, err := h.getToken(r.Context(), backend)
	if err != nil {
		h.inflight.release(backend.MachineID)
		h.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if tokenStale {
		h.cnt.tokenStaleServes.Add(1)
	}
	// P0#4：仅在 token 获取成功（转发决策确定）后回写 backend 标识，
	// 避免 503 错误响应把选中的内部 backend id 泄露给外部客户端。
	w.Header().Set(HeaderMachineID, backend.MachineID)
	// proxiedReqs 统计到达转发决策的客户端请求（每请求只计一次，重试
	// 不重复——评审 R2 P3：否则与 requests_total 分母在重试时语义无分叉）。
	h.cnt.proxiedReqs.Add(1)
	retry := h.tryServe(w, r, backend, cred, stale, true)
	if retry == retryNone {
		return
	}
	if !requestHasNoBody(r) {
		if retry == retryForbidden {
			http.Error(w, "agent proxy rejected request", http.StatusForbidden)
		} else {
			http.Error(w, "agent proxy unavailable", http.StatusBadGateway)
		}
		return
	}
	if retry == retryForbidden {
		h.cnt.forbiddenRetry.Add(1)
	}
	h.tokens.Invalidate(backend.MachineID)
	h.routes.Invalidate(key)
	retryRoute := route
	lookupStart = time.Now()
	if nv, _, e := h.routes.Get(r.Context(), key, func(ctx context.Context, _ string) (any, error) { return h.loadRoute(ctx, host, port) }); e == nil {
		if nr, ok := nv.(*catalog.Route); ok && nr != nil {
			retryRoute = nr
		}
	}
	h.cnt.observeRouteLookup(time.Since(lookupStart).Seconds())
	retryBackend, err := h.selectBackend(retryRoute, r.Header.Get(HeaderPinMachine))
	if err != nil {
		h.writeSelectionError(w, err)
		return
	}
	backendID = retryBackend.MachineID
	retryCred, retryTokenStale, err := h.getToken(r.Context(), retryBackend)
	if err != nil {
		h.inflight.release(retryBackend.MachineID)
		h.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if retryTokenStale {
		h.cnt.tokenStaleServes.Add(1)
	}
	w.Header().Set(HeaderMachineID, retryBackend.MachineID)
	if terminal := h.tryServe(w, r, retryBackend, retryCred, false, false); terminal == retryForbidden {
		http.Error(w, "agent proxy rejected request", http.StatusForbidden)
	}
}

func (h *Handler) selectBackend(route *catalog.Route, pin string) (catalog.Backend, error) {
	eligible := make([]catalog.Backend, 0, len(route.Backends))
	for _, b := range route.Backends {
		if b.Draining {
			continue
		}
		// readiness 白名单与 publisher 契约严格一致：machineServing()
		// (internal/controlplane/routepublisher/publisher.go) 只发布
		// ObservedReadiness ∈ {READY, UNCONFIGURED} 的 backend，任何其它值
		//（空串、NOT_READY、未知/拼写漂移）表示投影异常，拒绝并计数，
		// 不把"空"当作"未完成探针"放行。
		switch b.Readiness {
		case "READY", "UNCONFIGURED":
			eligible = append(eligible, b)
		case "":
			h.cnt.backendIneligibleEmpty.Add(1)
		case "NOT_READY":
			h.cnt.backendIneligibleNotReady.Add(1)
		default:
			h.cnt.backendIneligibleUnknown.Add(1)
		}
	}
	if len(eligible) == 0 {
		return catalog.Backend{}, errNoEligible
	}
	if pin != "" {
		for _, b := range eligible {
			if b.MachineID == pin {
				if chosen, over := h.inflight.selectAndAcquire([]catalog.Backend{b}, h.hardConcurrency); over {
					return catalog.Backend{}, errHardLimit
				} else {
					return chosen, nil
				}
			}
		}
		return catalog.Backend{}, errPinMiss
	}
	if chosen, over := h.inflight.selectAndAcquire(eligible, h.hardConcurrency); over {
		return catalog.Backend{}, errHardLimit
	} else {
		return chosen, nil
	}
}

func (h *Handler) writeSelectionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errPinMiss):
		h.cnt.pinMisses.Add(1)
		w.Header().Set("X-Firepaas-Pin-Error", "machine not eligible (missing, replaced, not ready or draining)")
		http.Error(w, "pinned machine is not an eligible backend", http.StatusNotFound)
	case errors.Is(err, errHardLimit):
		h.cnt.hardRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "backend at hard concurrency limit", http.StatusServiceUnavailable)
	default:
		http.Error(w, "no ready backend for hostname", http.StatusServiceUnavailable)
	}
}

func (h *Handler) tryServe(
	w http.ResponseWriter,
	r *http.Request,
	b catalog.Backend,
	cred string,
	stale, allowTransportRetry bool,
) retryReason {
	defer h.inflight.release(b.MachineID)
	state := &attemptState{}
	ctx := context.WithValue(r.Context(), backendKey{}, b)
	ctx = context.WithValue(ctx, attemptStateKey{}, state)
	if cred != "" {
		ctx = context.WithValue(ctx, credKey{}, cred)
	}
	if allowTransportRetry {
		ctx = context.WithValue(ctx, transportRetryKey{}, true)
	}
	if stale {
		w.Header().Set("X-Firepaas-Stale", "stale")
	}
	upstreamStart := time.Now()
	h.proxy.ServeHTTP(w, r.WithContext(ctx))
	h.cnt.observeUpstreamRTT(time.Since(upstreamStart).Seconds())
	return state.reason
}

// getToken 拉取 backend 凭证并打 token 获取延迟点；返回值同
// TokenClient.Get（凭证, 是否 stale 降级, 错误）。
func (h *Handler) getToken(ctx context.Context, b catalog.Backend) (string, bool, error) {
	start := time.Now()
	cred, stale, err := h.tokens.Get(ctx, b.MachineID, b.ExecutionID)
	h.cnt.observeTokenFetch(time.Since(start).Seconds())
	return cred, stale, err
}

// WriteRouteRevisionRejectsPrometheus 导出 D-2 revision 守卫的拒绝计数
// （回源路由投影 revision 低于缓存高水位，旧快照覆盖被拦）。
func (h *Handler) WriteRouteRevisionRejectsPrometheus(w http.ResponseWriter) {
	_, _ = fmt.Fprintf(w,
		"# HELP firepaas_edge_route_revision_rejects_total route projection redraws rejected for lower revision\n"+
			"# TYPE firepaas_edge_route_revision_rejects_total counter\n"+
			"firepaas_edge_route_revision_rejects_total %d\n",
		h.routes.RevisionRejects())
}

// WriteInflightPrometheus writes the per-machine inflight gauge.
func (h *Handler) WriteInflightPrometheus(w http.ResponseWriter) {
	snap := h.inflight.snapshot()
	if len(snap) == 0 {
		return
	}
	_, _ = fmt.Fprint(
		w,
		"# HELP firepaas_edge_backend_inflight in-flight requests per backend machine (per-edge local view)\n# TYPE firepaas_edge_backend_inflight gauge\n",
	)
	for id, v := range snap {
		_, _ = fmt.Fprintf(w, "firepaas_edge_backend_inflight{machine_id=%q} %d\n", id, v)
	}
}
