// Command edge-proxy 是 firepaas 的边缘路由。
//
// 流量：client → edge(hostname, TLS/限流/serve-stale) → Redis route catalog
// → agent proxy :5107（execution-bound credential）→ VM
// edge 永不读取 slot_ip；只使用 catalog 中的 node_proxy_endpoint。
//
// M4（mvp-plan §8.2/8.3，ADR-0011 实验室形态）：
//   - 客户端 TLS 终止（*.firepaas.local 泛域名证书，内部 CA 签发）；
//   - :80 → 308 https 重定向；
//   - 每 hostname 令牌桶限流（429）；
//   - 本地路由缓存 fresh TTL 5s；Redis 失联时 serve-stale 窗口内继续服务，
//     超窗 503 受控降级；恢复后随 TTL 自然回源；
//   - execution-bound proxy credential 单向下发给 agent proxy（ADR-0006）。
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/example/firepaas/internal/controlplane/traffic"
	edgesvc "github.com/example/firepaas/internal/edge"
	"github.com/example/firepaas/internal/security/mtls"
	"github.com/redis/go-redis/v9"
)

const (
	headerMachineID   = "X-Firepaas-Machine-ID"
	headerExecutionID = "X-Firepaas-Execution-ID"
	headerCredential  = traffic.HeaderCredential

	freshTTLDefault = 5 * time.Second
	staleDefault    = 120 * time.Second
)

// counters 是 edge 的运行计数器（P2-7：stale serves / 回源错误 / token 错误 /
// 限流拒绝），Prometheus 文本格式暴露在 /metrics。
type counters struct {
	staleServes      atomic.Uint64 // 命中 last-known-good（路由或 token）
	beyondStale      atomic.Uint64 // 超窗受控降级 503
	redisErrors      atomic.Uint64 // 路由回源失败（含被 stale 兼蔽的）
	tokenErrors      atomic.Uint64 // token 回源失败（含被 stale 兼蔽的）
	tokenStaleServes atomic.Uint64 // token last-known-good 复用
	rateLimited      atomic.Uint64 // 429 拒绝
	proxiedReqs      atomic.Uint64 // 成功转发到 agent proxy 的请求
	forbiddenRetry   atomic.Uint64 // 403 后 invalidate+重试次数
}

func (c *counters) writeProm(w http.ResponseWriter) {
	write := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	write("firepaas_edge_stale_serves_total", "requests served from last-known-good route cache", c.staleServes.Load())
	write("firepaas_edge_beyond_stale_total", "requests rejected 503 beyond serve-stale window", c.beyondStale.Load())
	write("firepaas_edge_redis_errors_total", "route catalog origin fetch failures", c.redisErrors.Load())
	write("firepaas_edge_token_errors_total", "traffic token fetch failures", c.tokenErrors.Load())
	write("firepaas_edge_token_stale_serves_total", "requests using last-known-good token", c.tokenStaleServes.Load())
	write("firepaas_edge_rate_limited_total", "requests rejected 429", c.rateLimited.Load())
	write("firepaas_edge_proxied_requests_total", "requests forwarded to agent proxy", c.proxiedReqs.Load())
	write("firepaas_edge_forbidden_retries_total", "agent 403 responses triggering token invalidate+retry", c.forbiddenRetry.Load())
}

type backendKey struct{}

func main() {
	if err := run(); err != nil {
		slog.Error("edge-proxy terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	port := envOr("FIREPAAS_EDGE_PORT", "80")
	tlsPort := os.Getenv("FIREPAAS_EDGE_TLS_LISTEN") // 如 ":443"；空 = 不启用客户端 TLS
	redisAddr := envOr("FIREPAAS_REDIS_ADDR", "127.0.0.1:6379")
	apiAddr := envOr("FIREPAAS_API_ADDR", "http://127.0.0.1:8080")
	apiToken := os.Getenv("FIREPAAS_API_TOKEN")

	staleWindow := envDurOr("FIREPAAS_EDGE_STALE_WINDOW", staleDefault)
	freshTTL := envDurOr("FIREPAAS_EDGE_FRESH_TTL", freshTTLDefault)
	rateLimit := envFloatOr("FIREPAAS_EDGE_RATE_LIMIT", 100) // req/s per hostname; <=0 关闭
	rateBurst := envFloatOr("FIREPAAS_EDGE_RATE_BURST", 200)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	cat := catalog.New(rdb)

	var agentTLS *tls.Config
	var err error
	certFile, keyFile, caFile := os.Getenv("FIREPAAS_EDGE_TLS_CERT"), os.Getenv("FIREPAAS_EDGE_TLS_KEY"), os.Getenv("FIREPAAS_EDGE_TLS_CA")
	if certFile != "" && keyFile != "" && caFile != "" {
		agentTLS, err = mtls.ClientConfig(certFile, keyFile, caFile, "agentd")
		if err != nil {
			return fmt.Errorf("edge mTLS config: %w", err)
		}
	}
	var clientCerts []tls.Certificate
	if sc, sk := os.Getenv("FIREPAAS_EDGE_SERVER_CERT"), os.Getenv("FIREPAAS_EDGE_SERVER_KEY"); sc != "" && sk != "" {
		pair, cerr := tls.LoadX509KeyPair(sc, sk)
		if cerr != nil {
			return fmt.Errorf("edge server cert: %w", cerr)
		}
		clientCerts = []tls.Certificate{pair}
	}

	tokens := edgesvc.NewTokenClient(apiAddr, apiToken, 30*time.Second)
	tokens.SetStaleWindow(staleWindow) // token serve-stale 与路由同源（P1-3）
	routes := edgesvc.NewRouteCache(freshTTL, staleWindow)
	limiter := newHostLimiter(rateLimit, rateBurst)
	cnt := &counters{}
	ed := newEdge(cat, routes, tokens, limiter, cnt, agentTLS)

	// P2-7：运维可观测（Prometheus 文本格式；不依赖控制面 registry）。
	metricsPort := envOr("FIREPAAS_EDGE_METRICS_PORT", "")
	if metricsPort != "" {
		mh := http.NewServeMux()
		mh.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			cnt.writeProm(w)
		})
		ml, merr := net.Listen("tcp", ":"+metricsPort)
		if merr != nil {
			return fmt.Errorf("listen metrics :%s: %w", metricsPort, merr)
		}
		go func() {
			slog.Info("edge-proxy metrics listening", "port", metricsPort)
			if err := http.Serve(ml, mh); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("edge metrics serve", "error", err)
			}
		}()
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	// M4：客户端 TLS 启用时，明文监听器专职 308 跳转 https（/healthz 保留
	// 明文探针）。Handler 在任何 listener 启动前定型，避免运行期替换。
	// P3-16：TLS 监听在非标准端口（如 :8443）时，重定向目标保留该端口，
	// 否则跳到 443 会打不通（实验宣 curl --resolve 用法已验证）。
	tlsEnabled := tlsPort != "" && len(clientCerts) > 0
	var plainHandler http.Handler = ed
	if tlsEnabled {
		redirectHost := ""
		if p, perr := strconv.Atoi(strings.TrimPrefix(tlsPort, ":")); perr == nil && p != 443 {
			redirectHost = ":" + strconv.Itoa(p)
		}
		plainHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				writePlain(w, http.StatusOK, "ok\n")
				return
			}
			http.Redirect(w, r, "https://"+stripPort(r.Host)+redirectHost+r.URL.RequestURI(),
				http.StatusPermanentRedirect)
		})
	}
	srv := &http.Server{Handler: plainHandler}
	go func() {
		slog.Info("edge-proxy listening", "port", port,
			"serve_stale_window", staleWindow.String(), "rate_limit_per_host", rateLimit)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("edge serve", "error", err)
		}
	}()

	var tlsSrv *http.Server
	if tlsEnabled {
		tlsCfg := &tls.Config{
			Certificates: clientCerts,
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2", "http/1.1"},
		}
		tlsLis, lerr := net.Listen("tcp", tlsPort)
		if lerr != nil {
			return fmt.Errorf("listen %s: %w", tlsPort, lerr)
		}
		tlsLis = tls.NewListener(tlsLis, tlsCfg)
		tlsSrv = &http.Server{Handler: ed, TLSConfig: tlsCfg}
		go func() {
			slog.Info("edge-proxy TLS listening", "addr", tlsPort)
			if err := tlsSrv.Serve(tlsLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("edge tls serve", "error", err)
			}
		}()
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if tlsSrv != nil {
		_ = tlsSrv.Shutdown(shutdownCtx)
	}
	return srv.Shutdown(shutdownCtx)
}

func writePlain(w http.ResponseWriter, code int, s string) {
	w.WriteHeader(code)
	_, _ = w.Write([]byte(s))
}

// edge 复用单个 ReverseProxy 与 Transport（评审 P3：连接池不得每请求新建）。
// backend 选择结果与凭证经 request context 传给 Director。
type edge struct {
	catalog  *catalog.Catalog
	routes   *edgesvc.RouteCache
	tokens   *edgesvc.TokenClient
	limiter  *hostLimiter
	cnt      *counters
	counter  atomic.Uint64
	agentTLS *tls.Config
	proxy    *httputil.ReverseProxy
}

func newEdge(cat *catalog.Catalog, routes *edgesvc.RouteCache, tokens *edgesvc.TokenClient,
	limiter *hostLimiter, cnt *counters, agentTLS *tls.Config) *edge {
	e := &edge{catalog: cat, routes: routes, tokens: tokens, limiter: limiter,
		cnt: cnt, agentTLS: agentTLS}
	e.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			backend, _ := req.Context().Value(backendKey{}).(catalog.Backend)
			if e.agentTLS != nil {
				req.URL.Scheme = "https"
			} else {
				req.URL.Scheme = "http"
			}
			req.URL.Host = backend.NodeProxyEndpoint
			req.Header.Set(headerMachineID, backend.MachineID)
			req.Header.Set(headerExecutionID, backend.ExecutionID)
			if cred, _ := req.Context().Value(credKey{}).(string); cred != "" {
				req.Header.Set(headerCredential, cred)
			}
		},
		// P1-2：agent 403（执行换代后旧凭证失效）→ 哨兵错误 → ErrorHandler
		// 拦截不写响应 → tryServe 返回 false → 上层 Invalidate+重取 token 重试一次。
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusForbidden {
				resp.Body.Close()
				return errRetryForbidden
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if errors.Is(err, errRetryForbidden) {
				// 403：不写任何响应；把标记写回 request context（request
				// context 是值语义，这里通过独立存储交接给 tryServe）。
				retryFlags.Store(r, true)
				return
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
		Transport: &http.Transport{
			TLSClientConfig:     agentTLS,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	return e
}

// retryFlags 是 ErrorHandler → tryServe 的交接存储（keyed by request 指针）。
// ReverseProxy 在 ErrorHandler 后不重用同一 request context，只能经此交接；
// 条目由 tryServe 出口满扫清理，防泄漏。
var retryFlags sync.Map

type credKey struct{}

type staleKey struct{}

func (e *edge) loadRoute(ctx context.Context, host string) (any, error) {
	route, err := e.catalog.GetRouteForHostname(ctx, host)
	if err != nil {
		return nil, err
	}
	if route == nil {
		// P2-8：权威 miss（hostidx/route 无键）与回源失败区分开：
		// 前者立即 404 + 负缓存，后者才 serve-stale。
		return nil, edgesvc.ErrNotFound
	}
	return route, nil
}

func (e *edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writePlain(w, http.StatusOK, "ok\n")
		return
	}
	host := stripPort(r.Host)

	// M4：每 hostname 限流。
	if !e.limiter.allow(host) {
		e.cnt.rateLimited.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	v, servedStale, err := e.routes.Get(r.Context(), host, e.loadRoute)
	if errors.Is(err, edgesvc.ErrNotFound) {
		// 权威不存在（hostname 无路由）：立即 404，不 serve-stale（P2-8）。
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}
	if errors.Is(err, edgesvc.ErrBeyondStale) {
		// 超过声明 stale 窗口仍无法回源：受控降级，不允许盲转发。
		e.cnt.beyondStale.Add(1)
		w.Header().Set("Retry-After", "30")
		http.Error(w, "route catalog unavailable beyond serve-stale window",
			http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		// 回源失败但仍在 stale 窗口内（serve-stale 已在上面的 Get 内处理）。
		e.cnt.redisErrors.Add(1)
		http.Error(w, "catalog error", http.StatusServiceUnavailable)
		return
	}
	if v == nil {
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}
	route, ok := v.(*catalog.Route)
	if !ok || route == nil || len(route.Backends) == 0 {
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}

	// 只转发可服务流量的 backend：非 draining 且 readiness 为
	// READY/UNCONFIGURED（M1 降级语义；ADR-0008）。
	eligible := make([]catalog.Backend, 0, len(route.Backends))
	for _, b := range route.Backends {
		if b.Draining {
			continue
		}
		switch b.Readiness {
		case "READY", "UNCONFIGURED", "":
			eligible = append(eligible, b)
		}
	}
	if len(eligible) == 0 {
		http.Error(w, "no ready backend for hostname", http.StatusServiceUnavailable)
		return
	}

	backend := eligible[e.counter.Add(1)%uint64(len(eligible))]
	if servedStale {
		e.cnt.staleServes.Add(1)
	}

	// M4（ADR-0006）：execution-bound credential 随请求下发。Get 按
	// (machine, execution) 缓存；换代后旧 token 不命中，回源即得新 token
	//（P1-2）；API 不可达时在 stale 窗口内复用 execution 匹配的
	// last-known-good（P1-3）。缺 token 且无降级可用时 503 fail-closed。
	credential, terr := e.tokens.Get(r.Context(), backend.MachineID, backend.ExecutionID)
	if terr != nil {
		e.cnt.tokenErrors.Add(1)
		slog.Warn("fetch traffic token failed", "machine_id", backend.MachineID, "error", terr)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if credential != "" {
		e.cnt.proxiedReqs.Add(1)
	}

	// 403 重试（P1-2）：agent 拒绝通常是执行换代后 token 过期（或缓存偶发
	// 不一致）——丢弃该 machine 全部缓存 token 与该 hostname 的路由缓存
	//（backend 可能已换代）后重取一次。仍 403 则把错误透传（fail-closed，
	// 不无限重试）。
	if e.tryServe(w, r, backend, credential, servedStale) {
		return
	}
	e.cnt.forbiddenRetry.Add(1)
	e.tokens.Invalidate(backend.MachineID)
	e.routes.Invalidate(host)
	// 路由缓存失效后重读：backend 集合可能已换代（旧 execution 的 backend
	// 已从投影摘除）。重读失败/无 backend 时回退原 backend 重试一次 token。
	var retryBackend = backend
	if nv, _, nerr := e.routes.Get(r.Context(), host, e.loadRoute); nerr == nil {
		if nr, ok := nv.(*catalog.Route); ok && nr != nil && len(nr.Backends) > 0 {
			for _, b := range nr.Backends {
				if !b.Draining {
				retryBackend = b
				break
			}
			}
		}
	}
	retryCred, rerr := e.tokens.Get(r.Context(), retryBackend.MachineID, retryBackend.ExecutionID)
	if rerr != nil {
		e.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	e.tryServe(w, r, retryBackend, retryCred, false)
}

// errRetryForbidden 由 ModifyResponse 返回，驱动 tryServe 的 403 重试：
// ModifyResponse 返回非 nil error 时 ReverseProxy 会调 ErrorHandler，而
// 我们在 ErrorHandler 里识别这个哨兵错误并不写任何响应（返回 false 交给
// 上层重试）。这样 403 响应永远不会半写到客户端。
var errRetryForbidden = errors.New("agent proxy 403: retry with fresh token")

// tryServe 用给定凭证转发一次；返回 true 表示响应已写完（成功或非 403
// 错误），false 表示收到 403 且响应未写给客户端，交由上层重试。
// 403 拦截凭 ModifyResponse → ErrorHandler 哨兵错误链完成：
//   - agent 返回 403 → ModifyResponse 返回 errRetryForbidden；
//   - ErrorHandler 识别哨兵不写响应（其它错误照常 502）；
//   - reverseproxy 内部在 ServeHTTP 返回前把哨兵错误写回 request context
//     （retryFlagKey），tryServe 据此判断。
func (e *edge) tryServe(w http.ResponseWriter, r *http.Request, backend catalog.Backend,
	credential string, servedStale bool) bool {

	ctx := context.WithValue(r.Context(), backendKey{}, backend)
	if credential != "" {
		ctx = context.WithValue(ctx, credKey{}, credential)
	}
	if servedStale {
		ctx = context.WithValue(ctx, staleKey{}, true)
	}
	req := r.WithContext(ctx)
	if servedStale {
		w.Header().Set("X-Firepaas-Stale", "stale")
	}
	defer retryFlags.Delete(req)
	e.proxy.ServeHTTP(w, req)
	// ServeHTTP 返回后查看 ErrorHandler 是否标记了 403 拦截。
	if v, _ := retryFlags.Load(req); v == true {
		retryFlags.Delete(req)
		return false // 403 已拦截未写响应，上层重试
	}
	return true
}

// ---- hosts limiter（薄封装，便于单测） ----

type hostLimiter struct {
	inner *edgesvc.RateLimiter
}

func newHostLimiter(rate, burst float64) *hostLimiter {
	return &hostLimiter{inner: edgesvc.NewRateLimiter(rate, burst)}
}

func (l *hostLimiter) allow(host string) bool { return l.inner.Allow(host) }

func stripPort(hostport string) string {
	h, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport
	}
	return h
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func envFloatOr(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err == nil {
			return f
		}
	}
	return def
}
