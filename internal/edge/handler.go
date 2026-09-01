package edge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
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
}

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

func (t *inflightTracker) release(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[id]; e != nil && e.count.Add(-1) == 0 {
		delete(t.entries, id)
	}
}

func (t *inflightTracker) load(id string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[id]; e != nil {
		return e.count.Load()
	}
	return 0
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
	http.Error(w, err.Error(), http.StatusBadGateway)
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
	host := stripPort(r.Host)
	port := h.requestRoutePort(r)
	if !h.limiter.Allow(host) {
		h.cnt.rateLimited.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	key := routeCacheKey(host, port)
	v, stale, err := h.routes.Get(
		r.Context(),
		key,
		func(ctx context.Context, _ string) (any, error) { return h.loadRoute(ctx, host, port) },
	)
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
	if r.Header.Get(HeaderPinMachine) != "" {
		h.cnt.pinHits.Add(1)
	}
	if stale {
		h.cnt.staleServes.Add(1)
	}
	w.Header().Set(HeaderMachineID, backend.MachineID)
	cred, err := h.tokens.Get(r.Context(), backend.MachineID, backend.ExecutionID)
	if err != nil {
		h.inflight.release(backend.MachineID)
		h.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if cred != "" {
		h.cnt.proxiedReqs.Add(1)
	}
	retry := h.tryServe(w, r, backend, cred, stale, true)
	if retry == retryNone {
		return
	}
	if !requestHasNoBody(r) {
		w.Header().Set(HeaderMachineID, backend.MachineID)
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
	if nv, _, e := h.routes.Get(r.Context(), key, func(ctx context.Context, _ string) (any, error) { return h.loadRoute(ctx, host, port) }); e == nil {
		if nr, ok := nv.(*catalog.Route); ok && nr != nil {
			retryRoute = nr
		}
	}
	retryBackend, err := h.selectBackend(retryRoute, r.Header.Get(HeaderPinMachine))
	if err != nil {
		h.writeSelectionError(w, err)
		return
	}
	if r.Header.Get(HeaderPinMachine) != "" {
		h.cnt.pinHits.Add(1)
	}
	w.Header().Set(HeaderMachineID, retryBackend.MachineID)
	retryCred, err := h.tokens.Get(r.Context(), retryBackend.MachineID, retryBackend.ExecutionID)
	if err != nil {
		h.inflight.release(retryBackend.MachineID)
		h.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if terminal := h.tryServe(w, r, retryBackend, retryCred, false, false); terminal == retryForbidden {
		w.Header().Set(HeaderMachineID, retryBackend.MachineID)
		http.Error(w, "agent proxy rejected request", http.StatusForbidden)
	}
}

func (h *Handler) selectBackend(route *catalog.Route, pin string) (catalog.Backend, error) {
	eligible := make([]catalog.Backend, 0, len(route.Backends))
	for _, b := range route.Backends {
		if !b.Draining && (b.Readiness == "READY" || b.Readiness == "UNCONFIGURED" || b.Readiness == "") {
			eligible = append(eligible, b)
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
	h.proxy.ServeHTTP(w, r.WithContext(ctx))
	return state.reason
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
