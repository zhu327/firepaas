package edge

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/hex"
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
	"github.com/zhu327/firepaas/internal/edge/mesh"
)

const (
	HeaderMachineID      = "X-Firepaas-Machine-ID"
	HeaderExecutionID    = "X-Firepaas-Execution-ID"
	HeaderPinMachine     = "X-Firepaas-Pin-Machine"
	HeaderAppPort        = "X-Firepaas-App-Port"
	headerProxyRetryable = "X-Firepaas-Proxy-Retryable"
	retryableProxyValue  = "true"
	// HeaderRequestID 是 edge→agent 的内部关联 ID（每请求生成，供 edge/agent
	// 日志关联；agent 在转发 guest 前剥离）。
	HeaderRequestID = "X-Firepaas-Request-ID"
	// HeaderClientRequestID 是客户端可见的关联 ID：响应回显 + 转发给
	// workload 供应用日志关联。入站同名头一律被覆盖，不信任客户端值。
	HeaderClientRequestID = "X-Request-Id"
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
	// autoscale（ADR-0041 §5）：reporter 上报成功/失败次数（不带 hostname
	// label，守 ADR-0027 基数约束）与零容量请求总数（冷起链路排障）。
	autoscaleReports, autoscaleReportErrors, unservedRequests atomic.Uint64
	// G2d（ADR-0040 §14）：mesh 直达与回落观测——direct 请求、回落
	// legacy 次数、直达终态失败。回落计数与 direct 计数分母一致：
	// direct_requests = direct 成功 + fallback + errors（直达尝试总数）。
	meshDirect, meshFallback, meshErrors atomic.Uint64
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
	write(
		"firepaas_edge_mesh_direct_requests_total",
		"requests attempted over the mesh direct path (G2d)",
		c.meshDirect.Load(),
	)
	write(
		"firepaas_edge_mesh_direct_fallback_total",
		"mesh direct attempts that fell back to the legacy agent-proxy path",
		c.meshFallback.Load(),
	)
	write(
		"firepaas_edge_mesh_direct_errors_total",
		"mesh direct attempts that failed terminally (no legacy fallback possible)",
		c.meshErrors.Load(),
	)
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
		"firepaas_edge_autoscale_reports_total",
		"autoscale signal samples successfully reported to redis",
		c.autoscaleReports.Load(),
	)
	write(
		"firepaas_edge_autoscale_report_errors_total",
		"autoscale signal report failures (forwarding unaffected)",
		c.autoscaleReportErrors.Load(),
	)
	write(
		"firepaas_edge_unserved_requests_total",
		"requests hitting zero capacity for their hostname (cold-start signal)",
		c.unservedRequests.Load(),
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
	// Tracker（ADR-0041）：per-hostname 并发信号记账。nil = 不记账
	// （reporter 未装配时的零开销形态；上报由 AutoscaleReporter 驱动）。
	Tracker *AutoscaleTracker
	// Direct（G2d，ADR-0040 §14）：mesh 直达 Transport（FIREPAAS_EDGE_MESH_DIRECT
	// 开启时注入）。nil = 功能关闭（全部流量走 legacy :5107，回滚即关）。
	Direct *mesh.Transport
}

type Handler struct {
	catalog         RouteCatalog
	routes          *RouteCache
	tokens          *TokenClient
	limiter         *RateLimiter
	cnt             *Counters
	agentTLS        *tls.Config
	proxy           *httputil.ReverseProxy
	direct          *httputil.ReverseProxy // G2d：mesh 直达（nil = 关）
	inflight        *inflightTracker
	hardConcurrency int64
	edgePorts       map[int]bool
	tracker         *AutoscaleTracker
}

type (
	backendKey        struct{}
	credKey           struct{}
	transportRetryKey struct{}
	listenPortKey     struct{}
	attemptStateKey   struct{}
	// edgeRequestIDKey 承载 handler 入口生成的请求关联 ID。
	edgeRequestIDKey struct{}
	retryReason      uint8
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
		tracker:         cfg.Tracker,
	}
	// G2d（§14）：mesh 直达代理。URL 占位由 Transport 按凭证/endpoint 改写；
	// 响应纪律与 legacy 对齐（P0#4 固定文案 + ModifyResponse 剥内部头/
	// retry 信号），否则 agent 产生的 X-Firepaas-Proxy-Retryable 会直达
	// 外部客户端，且 retryable 502/403 不会触发回落/失效重试。
	if cfg.Direct != nil {
		h.direct = &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// 占位：mesh.Transport.RoundTrip 按 DirectInfo 改写目标。
				req.URL.Scheme = "http"
				req.URL.Host = "mesh-direct.invalid"
				req.Host = "mesh-direct.invalid"
				// 关联 ID 与 legacy 同口径：覆盖客户端入站值后下行。
				req.Header.Del(HeaderRequestID)
				req.Header.Del(HeaderClientRequestID)
				if id := requestIDFrom(req.Context()); id != "" {
					req.Header.Set(HeaderRequestID, id)
					req.Header.Set(HeaderClientRequestID, id)
				}
			},
			ModifyResponse: func(resp *http.Response) error {
				retryable := resp.StatusCode == http.StatusBadGateway &&
					resp.Header.Get(headerProxyRetryable) == retryableProxyValue
				resp.Header.Del(headerProxyRetryable)
				// 凭证与内部头即便被上游误回显也不能出网。
				resp.Header.Del(traffic.HeaderCredential)
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
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				if d, ok := r.Context().Value(directAttemptKey{}).(*directAttempt); ok {
					d.failed = true
					// P1（独立评审）：ErrNoDirect 是纯 pre-dial 应用层 miss
					//（未发一字节，body 未消耗）→ 回落重放安全，与拨
					// 号期失败同等 retriable；否则投影抖动窗口的 POST
					// 会被误判终态 502。
					d.retriable = isDialError(err) || errors.Is(err, mesh.ErrNoDirect)
					if errors.Is(err, errRetryForbidden) {
						d.reason = retryForbidden
					}
				}
				switch {
				case errors.Is(err, mesh.ErrNoDirect):
					// 无入口：tryServe 探测后回落，不写响应。
					return
				case errors.Is(err, errRetryForbidden), errors.Is(err, errRetryProxyRoute):
					// ModifyResponse 产生的应用层重试信号：交 tryServe/
					// ServeHTTP 的重试与回落逻辑处理。
					return
				}
				slog.Warn("edge mesh direct transport error",
					"method", r.Method, "path", r.URL.Path, "error", err)
				// 响应未开始时同样交回落路径；已开始的流错误由
				// ReverseProxy 以 ErrAbortHandler 终结（不进入本 handler）。
			},
			Transport: cfg.Direct,
		}
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
				HeaderRequestID,
				HeaderClientRequestID,
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
			if id := requestIDFrom(req.Context()); id != "" {
				req.Header.Set(HeaderRequestID, id)
				req.Header.Set(HeaderClientRequestID, id)
			}
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
			// W2-6：与 mesh.Transport 同口径——拨号 3s（节点失联/WG 断链时
			// SYN 无 RST，OS 默认重试会吞掉请求预算）+ 首字节 30s（半开连接
			// 兜底）。不改 retry/header 语义；ForceAttemptHTTP2 保持默认。
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
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

// requestIDFrom 返回本请求的关联 ID（handler 入口生成，永远非空）。
func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(edgeRequestIDKey{}).(string)
	return id
}

// newRequestID 生成 16 hex 字符的请求关联 ID。失败时回退时间戳（仍保证
// 非空、可关联，不阻塞请求）。
func newRequestID() string {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writePlain(w, http.StatusOK, "ok\n")
		return
	}
	start := time.Now()
	host := stripPort(r.Host)
	port := h.requestRoutePort(r)
	// 关联 ID：handler 入口生成（不采信客户端入站值），响应回显给客户端并
	// 注入 context；转发时作为内部头/标准 X-Request-Id 下行，供 edge 日志、
	// agent 日志与 workload 应用日志关联。
	reqID := newRequestID()
	r = r.WithContext(context.WithValue(r.Context(), edgeRequestIDKey{}, reqID))
	w.Header().Set(HeaderClientRequestID, reqID)
	// 结构化访问日志：request_id/host/port/backend/execution/generation/
	// outcome/duration。凭证、token、Authorization 等敏感材料绝不进日志
	// （AGENTS.md 数据边界）。
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	w = rec
	backendID := ""
	executionID := ""
	var routeGeneration int64
	defer func() {
		h.cnt.observeRequest(rec.status)
		slog.Info("edge request",
			"request_id", reqID,
			"host", host,
			"port", port,
			"backend", backendID,
			"execution_id", executionID,
			"route_generation", routeGeneration,
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
		h.noteUnserved(host)
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
		h.noteUnserved(host)
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}
	routeGeneration = route.RouteGeneration
	backend, err := h.selectBackend(route, r.Header.Get(HeaderPinMachine))
	if err != nil {
		h.writeSelectionError(w, err, host)
		return
	}
	backendID = backend.MachineID
	executionID = backend.ExecutionID
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
	// ADR-0041 served 记账：与 proxiedReqs 同一判定点 +1，handler 出口
	// defer -1；内部 forbidden/transport 重试不重复 +1（retry 时机器级
	// inflight 先释放再获取，host 计数保持到请求结束）。
	if h.tracker != nil {
		h.tracker.Acquire(host)
		defer h.tracker.Release(host)
	}
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
		h.writeSelectionError(w, err, host)
		return
	}
	backendID = retryBackend.MachineID
	executionID = retryBackend.ExecutionID
	routeGeneration = retryRoute.RouteGeneration
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

func (h *Handler) writeSelectionError(w http.ResponseWriter, err error, host string) {
	switch {
	case errors.Is(err, errPinMiss):
		h.cnt.pinMisses.Add(1)
		w.Header().Set("X-Firepaas-Pin-Error", "machine not eligible (missing, replaced, not ready or draining)")
		http.Error(w, "pinned machine is not an eligible backend", http.StatusNotFound)
	case errors.Is(err, errHardLimit):
		h.cnt.hardRejected.Add(1)
		if h.tracker != nil {
			h.tracker.AddHardRejected(host)
		}
		w.Header().Set("Retry-After", "1")
		http.Error(w, "backend at hard concurrency limit", http.StatusServiceUnavailable)
	default:
		// errNoEligible：零容量（unserved 冷起信号）。pin-miss/hard 已在
		// 上分支排除；429/超窗/catalog 错误不经过本函数。
		h.noteUnserved(host)
		http.Error(w, "no ready backend for hostname", http.StatusServiceUnavailable)
	}
}

// noteUnserved 记录一次零容量请求（per-host 短窗 + edge 全局计数）。
func (h *Handler) noteUnserved(host string) {
	if h.tracker != nil {
		h.tracker.AddUnserved(host)
	}
	h.cnt.unservedRequests.Add(1)
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
	// G2d（§14）：mesh 直达优先（仅 mesh_direct 服务——backend ULA 提示
	// 存在；凭证路由 + WG peer 准入）。凭证缺失不尝试直达（终结器必 403，
	// 不消耗直达计数；过渡期 token 未配置时全量走 legacy）。失败且响应未
	// 开始 → 按 body/错误类型回落 legacy（拨号期失败 body 未消耗，可无损
	// 重试；已消耗 body 的中段失败不再重放，回终态避免静默丢数据）。
	if h.direct != nil && b.ULA != "" && cred != "" {
		h.cnt.meshDirect.Add(1)
		dstate := &directAttempt{}
		dctx := context.WithValue(ctx, directAttemptKey{}, dstate)
		dctx = mesh.WithDirectInfo(dctx, mesh.DirectInfo{
			MachineID: b.MachineID, ExecutionID: b.ExecutionID,
			Credential: cred, AppPort: b.AppPort,
		})
		tracker := &firstWriteTracker{ResponseWriter: w}
		// RTT 口径：per forwarding attempt（与 legacy 重试路径一致：一次
		// 请求的每次尝试各记一条，含 pre-dial miss 与回落前的失败尝试）。
		directStart := time.Now()
		h.direct.ServeHTTP(tracker, r.WithContext(dctx))
		h.cnt.observeUpstreamRTT(time.Since(directStart).Seconds())
		// 直达 403（凭证/执行失效）与 legacy 同语义：交外层失效重试。
		if dstate.reason == retryForbidden {
			return retryForbidden
		}
		// 客户端已取消时不回落（legacy handleProxyError 同守卫：无意义的上游
		// 重试只会多打一枪）。
		if dstate.failed && !tracker.started() && r.Context().Err() == nil &&
			(requestHasNoBody(r) || dstate.retriable) {
			h.cnt.meshFallback.Add(1)
			// 回落 legacy：tracker 未写任何字节，legacy 代理接管。
		} else {
			if dstate.failed {
				h.cnt.meshErrors.Add(1)
				if !tracker.started() {
					// body 已消耗的中段失败：不可回落重放，显式 502
					//（不可静默 200 空回；拨号期失败已在上分支回落）。
					w.WriteHeader(http.StatusBadGateway)
				}
			}
			return retryNone // 直达成功（含终结器业务响应）或已开始的流失败
		}
	}
	upstreamStart := time.Now()
	h.proxy.ServeHTTP(w, r.WithContext(ctx))
	h.cnt.observeUpstreamRTT(time.Since(upstreamStart).Seconds())
	return state.reason
}

// firstWriteTracker 探测直达响应是否已开始（首字节已写给客户端）——
// 已开始则不可回落（body 重放不可行），未开始则 legacy 代理接管。
// Unwrap/Flush/Hijack 透传底层实现（SSE 分块增量 flush 与 WebSocket
// 劫持走直达时不断流；B 评审：缺失会导致流式语义异常）。
type firstWriteTracker struct {
	http.ResponseWriter
	wrote bool
}

func (t *firstWriteTracker) WriteHeader(code int) {
	t.wrote = true
	t.ResponseWriter.WriteHeader(code)
}

func (t *firstWriteTracker) Write(b []byte) (int, error) {
	t.wrote = true
	return t.ResponseWriter.Write(b)
}

func (t *firstWriteTracker) started() bool { return t.wrote }

// Unwrap 暴露底层 ResponseWriter（ResponseController/类型断言兼容）。
func (t *firstWriteTracker) Unwrap() http.ResponseWriter { return t.ResponseWriter }

// Flush 透传（未开始写时先记 started：flush 本身即响应开始）。
func (t *firstWriteTracker) Flush() {
	t.wrote = true
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack 透传（WebSocket 等劫持型升级；劫持即响应开始，不可回落）。
func (t *firstWriteTracker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	t.wrote = true
	if h, ok := t.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("mesh direct: underlying writer does not support hijack")
}

// isDialError 判定 transport 错误发生在拨号期（body 未消耗，回落重放安全）。
func isDialError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

// directAttempt 是直达尝试的失败信号（ErrorHandler → tryServe；
// ReverseProxy 的 ErrorHandler 无法直接返回值，经 context 传递）。
// retriable = 拨号期失败（body 未消耗，回落重放安全）。
type directAttempt struct {
	failed    bool
	retriable bool
	reason    retryReason
}

type directAttemptKey struct{}

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
