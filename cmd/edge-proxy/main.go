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
//
// v1.1-C（ADR-0019/0020）：
//   - 响应头 X-Firepaas-Machine-ID（命中 backend 的 machine id；与
//     edge→proxy 方向的同名请求头不同向，不冲突）；
//   - 请求钉扎 X-Firepaas-Pin-Machine：命中 eligible 集合直选（跳过负载
//     均衡，但不豁免 hard 上限）；不在集合 → 404 显式失败（调试契约）；
//   - per-machine inflight 计数（edge 本地视角）+ least-inflight 选择
//     （随机打散同分）替代 round-robin；soft 语义由 least-inflight 天然承担；
//   - hard 并发上限 FIREPAAS_EDGE_HARD_CONCURRENCY（默认 256/machine）：
//     最闲者仍超限 → 503 + Retry-After（受控降级）。
//
// v1.1-E（ADR-0022）：
//   - FIREPAAS_EDGE_EXTRA_PORTS 附加端口监听（默认关闭）；按
//     (hostname, 请求端口) 查路由；未声明端口 → 404；
//   - 转发带 X-Firepaas-App-Port 请求头（命中 route 的 service
//     internal_port）；头缺失 = 旧行为（proxy 主端口）。
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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
	// headerPinMachine（v1.1，ADR-0019）：客户端→edge 的调试钉扎请求头。
	headerPinMachine = "X-Firepaas-Pin-Machine"
	// headerAppPort（v1.1，ADR-0022）：edge→proxy 的目标 service 端口头。
	headerAppPort = "X-Firepaas-App-Port"
	// headerProxyRetryable is an agent→edge-only marker. It is consumed before
	// a client response is committed and is never exposed to clients.
	headerProxyRetryable = "X-Firepaas-Proxy-Retryable"
	retryableProxyValue  = "true"

	freshTTLDefault = 5 * time.Second
	staleDefault    = 120 * time.Second

	// hardConcurrencyDefault（v1.1，ADR-0020）：per-machine hard 并发上限。
	hardConcurrencyDefault = 256
)

// counters 是 edge 的运行计数器（P2-7：stale serves / 回源错误 / token 错误 /
// 限流拒绝 + v1.1：hard 拒绝/钉扎命中/钉扎 miss），Prometheus 文本格式暴露。
type counters struct {
	staleServes      atomic.Uint64 // 命中 last-known-good（路由或 token）
	beyondStale      atomic.Uint64 // 超窗受控降级 503
	redisErrors      atomic.Uint64 // 路由回源失败（含被 stale 掩蔽的）
	tokenErrors      atomic.Uint64 // token 回源失败（含被 stale 掩蔽的）
	tokenStaleServes atomic.Uint64 // token last-known-good 复用
	rateLimited      atomic.Uint64 // 429 拒绝
	proxiedReqs      atomic.Uint64 // 成功转发到 agent proxy 的请求
	forbiddenRetry   atomic.Uint64 // 403 后 invalidate+重试次数
	hardRejected     atomic.Uint64 // v1.1：hard 并发上限拒绝（503）
	pinHits          atomic.Uint64 // v1.1：钉扎命中（含唤醒 standby）
	pinMisses        atomic.Uint64 // v1.1：钉扎不在 eligible 集合（404）
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
	write("firepaas_edge_hard_rejected_total", "requests rejected 503 at per-machine hard concurrency limit", c.hardRejected.Load())
	write("firepaas_edge_pin_hits_total", "requests pinned to an eligible backend via X-Firepaas-Pin-Machine", c.pinHits.Load())
	write("firepaas_edge_pin_misses_total", "pin requests whose machine was not eligible (404)", c.pinMisses.Load())
}

// ---------------------------------------------------------------------------
// v1.1（ADR-0020）：per-machine inflight 计数（edge 本地视角）
// ---------------------------------------------------------------------------

type inflightEntry struct {
	count    atomic.Int64
	lastSeen atomic.Int64 // unix nano
}

// inflightTracker 维护每 backend machine 的在途请求计数。请求进入 eligible
// 选择后 +1，响应完成（含流式/WS 连接结束）后 -1。条目惰性清理：inflight
// 为 0 且超过 staleAfter 未更新时从 metrics 快照中剔除（防泄漏）。
type inflightTracker struct {
	mu         sync.Mutex
	entries    map[string]*inflightEntry
	staleAfter time.Duration
}

func newInflightTracker() *inflightTracker {
	return &inflightTracker{entries: map[string]*inflightEntry{}, staleAfter: 2 * time.Minute}
}

func (t *inflightTracker) entry(machineID string) *inflightEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[machineID]; ok {
		return e
	}
	e := &inflightEntry{}
	t.entries[machineID] = e
	return e
}

func (t *inflightTracker) acquire(machineID string) {
	e := t.entry(machineID)
	e.count.Add(1)
	e.lastSeen.Store(time.Now().UnixNano())
}

func (t *inflightTracker) release(machineID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[machineID]
	if !ok {
		return
	}
	if e.count.Add(-1) == 0 {
		// Cleanup runs on request completion; it must not depend on metrics being scraped.
		delete(t.entries, machineID)
		return
	}
	e.lastSeen.Store(time.Now().UnixNano())
}

func (t *inflightTracker) load(machineID string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[machineID]; ok {
		return e.count.Load()
	}
	return 0
}

// selectAndAcquire atomically chooses a least-loaded backend and reserves one
// hard-concurrency slot. Selection and reservation must share this lock: a
// separate load/select/acquire sequence can oversubscribe under concurrency.
func (t *inflightTracker) selectAndAcquire(eligible []catalog.Backend, hard int64) (catalog.Backend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	min := int64(-1)
	var candidates []catalog.Backend
	for _, b := range eligible {
		n := int64(0)
		if e := t.entries[b.MachineID]; e != nil {
			n = e.count.Load()
		}
		switch {
		case min < 0 || n < min:
			min, candidates = n, append(candidates[:0], b)
		case n == min:
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
	e.lastSeen.Store(time.Now().UnixNano())
	return chosen, false
}

// snapshot 返回非零计数条目。零计数条目在 release 时清理。
func (t *inflightTracker) snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int64{}
	for id, e := range t.entries {
		if count := e.count.Load(); count > 0 {
			out[id] = count
		}
	}
	return out
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
	hardConcurrency := int64(envFloatOr("FIREPAAS_EDGE_HARD_CONCURRENCY", hardConcurrencyDefault))
	if hardConcurrency <= 0 {
		hardConcurrency = hardConcurrencyDefault
	}

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
	serverCert, serverKey := os.Getenv("FIREPAAS_EDGE_SERVER_CERT"), os.Getenv("FIREPAAS_EDGE_SERVER_KEY")
	if (serverCert == "") != (serverKey == "") {
		return errors.New("FIREPAAS_EDGE_SERVER_CERT and FIREPAAS_EDGE_SERVER_KEY must be set together")
	}
	if tlsPort != "" && (serverCert == "" || serverKey == "") && !isTruthy(os.Getenv("FIREPAAS_ALLOW_INSECURE_DEV")) {
		return errors.New("public edge TLS requires FIREPAAS_EDGE_SERVER_CERT/KEY (set FIREPAAS_ALLOW_INSECURE_DEV=true only for local development)")
	}
	if serverCert != "" {
		pair, cerr := tls.LoadX509KeyPair(serverCert, serverKey)
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
	// v1.1：edge 自身监听端口集合（主明文/TLS/附加）——用于区分“客户端
	// 寻址 edge 的端口”与“客户端显式请求的 app service 端口”。
	edgePorts := map[int]bool{}
	if p, perr := strconv.Atoi(strings.TrimPrefix(port, ":")); perr == nil {
		edgePorts[p] = true
	}
	if tlsPort != "" {
		if p, perr := strconv.Atoi(strings.TrimPrefix(tlsPort, ":")); perr == nil {
			edgePorts[p] = true
		}
	}
	extraPorts, err := parseExtraPorts(os.Getenv("FIREPAAS_EDGE_EXTRA_PORTS"))
	if err != nil {
		return fmt.Errorf("FIREPAAS_EDGE_EXTRA_PORTS: %w", err)
	}
	for _, p := range extraPorts {
		edgePorts[p] = true
	}
	ed := newEdge(cat, routes, tokens, limiter, cnt, agentTLS, hardConcurrency, edgePorts)

	// P2-7：运维可观测（Prometheus 文本格式；不依赖控制面 registry）。
	metricsPort := envOr("FIREPAAS_EDGE_METRICS_PORT", "")
	if metricsPort != "" {
		mh := http.NewServeMux()
		mh.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			cnt.writeProm(w)
			writeInflightProm(w, ed.inflight)
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
			"serve_stale_window", staleWindow.String(), "rate_limit_per_host", rateLimit,
			"hard_concurrency", hardConcurrency)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("edge serve", "error", err)
		}
	}()

	// v1.1（ADR-0022）：附加端口监听（extraPorts 已在上方解析）。请求按
	// (hostname, 监听端口) 查路由；未声明端口 404。
	var extraSrvs []*http.Server
	mainPortNum := 80
	if p, perr := strconv.Atoi(strings.TrimPrefix(port, ":")); perr == nil {
		mainPortNum = p
	}
	for _, p := range extraPorts {
		if p == mainPortNum {
			continue // 主端口由主监听器负责
		}
		p := p
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 注入监听端口：Host 不带端口的客户端也能按正确端口查路由。
			r = r.WithContext(context.WithValue(r.Context(), listenPortKey{}, p))
			ed.ServeHTTP(w, r)
		})
		el, lerr := net.Listen("tcp", ":"+strconv.Itoa(p))
		if lerr != nil {
			return fmt.Errorf("listen extra :%d: %w", p, lerr)
		}
		esrv := &http.Server{Handler: handler}
		extraSrvs = append(extraSrvs, esrv)
		go func() {
			slog.Info("edge-proxy extra port listening", "port", p)
			if err := esrv.Serve(el); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("edge extra serve", "port", p, "error", err)
			}
		}()
	}

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
	for _, esrv := range extraSrvs {
		_ = esrv.Shutdown(shutdownCtx)
	}
	if tlsSrv != nil {
		_ = tlsSrv.Shutdown(shutdownCtx)
	}
	return srv.Shutdown(shutdownCtx)
}

// writeInflightProm 输出 per-machine inflight gauge（v1.1，ADR-0020）。
func writeInflightProm(w http.ResponseWriter, t *inflightTracker) {
	snap := t.snapshot()
	if len(snap) == 0 {
		return
	}
	fmt.Fprintf(w, "# HELP firepaas_edge_backend_inflight in-flight requests per backend machine (per-edge local view)\n"+
		"# TYPE firepaas_edge_backend_inflight gauge\n")
	for id, v := range snap {
		fmt.Fprintf(w, "firepaas_edge_backend_inflight{machine_id=%q} %d\n", id, v)
	}
}

// parseExtraPorts 解析 "8081,9000-9005" 形式的附加端口声明。
func parseExtraPorts(raw string) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if i := strings.Index(part, "-"); i > 0 {
			lo, err1 := strconv.Atoi(part[:i])
			hi, err2 := strconv.Atoi(part[i+1:])
			if err1 != nil || err2 != nil || lo < 1 || hi < lo || hi > 65535 {
				return nil, fmt.Errorf("bad port range %q", part)
			}
			for p := lo; p <= hi; p++ {
				out = append(out, p)
			}
			continue
		}
		p, err := strconv.Atoi(part)
		if err != nil || p < 1 || p > 65535 {
			return nil, fmt.Errorf("bad port %q", part)
		}
		out = append(out, p)
	}
	return out, nil
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
	agentTLS *tls.Config
	proxy    *httputil.ReverseProxy

	// v1.1（ADR-0020/0019/0022）
	inflight        *inflightTracker // per-machine 在途计数（least-inflight 输入）
	hardConcurrency int64            // per-machine hard 上限（最闲者超限 → 503）
	// edgePorts 是 edge 自身的全部监听端口（主明文/TLS/附加）。Host 里的显式
	// 端口若属于 edge 自身 → 客户端只是在寻址 edge（主入口语义）；否则视为
	// app service 端口（如 Host: app:80 打主 service 之外的声明端口）。
	edgePorts map[int]bool
}

func newEdge(cat *catalog.Catalog, routes *edgesvc.RouteCache, tokens *edgesvc.TokenClient,
	limiter *hostLimiter, cnt *counters, agentTLS *tls.Config, hardConcurrency int64, edgePorts map[int]bool) *edge {
	e := &edge{catalog: cat, routes: routes, tokens: tokens, limiter: limiter,
		cnt: cnt, agentTLS: agentTLS, inflight: newInflightTracker(),
		hardConcurrency: hardConcurrency, edgePorts: edgePorts}
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
			// v1.1（ADR-0022）：目标 service 端口（proxy 侧 services 白名单校验）。
			if backend.AppPort > 0 {
				req.Header.Set(headerAppPort, strconv.Itoa(backend.AppPort))
			}
			if cred, _ := req.Context().Value(credKey{}).(string); cred != "" {
				req.Header.Set(headerCredential, cred)
			}
		},
		// P1-2：agent 403（执行换代后旧凭证失效）→ 哨兵错误 → ErrorHandler
		// 拦截不写响应 → tryServe 返回 false → 上层 Invalidate+重取 token 重试一次。
		ModifyResponse: func(resp *http.Response) error {
			// Consume this agent→edge control signal on every response so neither
			// an agent implementation detail nor a forged workload header leaks.
			retryable := resp.StatusCode == http.StatusBadGateway &&
				resp.Header.Get(headerProxyRetryable) == retryableProxyValue
			resp.Header.Del(headerProxyRetryable)
			// Workloads cannot authoritatively set this edge response header.
			if backend, ok := resp.Request.Context().Value(backendKey{}).(catalog.Backend); ok {
				resp.Header.Set(headerMachineID, backend.MachineID)
			}
			if retryable {
				resp.Body.Close()
				return errRetryProxyRoute
			}
			if resp.StatusCode == http.StatusForbidden {
				resp.Body.Close()
				return errRetryForbidden
			}
			return nil
		},
		ErrorHandler: e.handleProxyError,

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

type retryReason uint8

const (
	retryNone retryReason = iota
	retryForbidden
	retryTransport
)

type credKey struct{}

type staleKey struct{}

type transportRetryKey struct{}

type listenPortKey struct{}

func requestHasNoBody(r *http.Request) bool {
	return r.Body == nil || r.Body == http.NoBody
}

// handleProxyError withholds only retryable, pre-response failures. Its caller
// sets transportRetryKey for the initial attempt alone, so a failed retry is
// always emitted as the original final error.
func (e *edge) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errRetryForbidden) {
		// 403：不写任何响应；把标记写回 request context（request context
		// 是值语义，这里通过独立存储交接给 tryServe）。
		retryFlags.Store(r, retryForbidden)
		return
	}
	if errors.Is(err, errRetryProxyRoute) {
		// Agent explicitly identified its 502 as endpoint resolution or guest
		// transport failure. It is safe to retry exactly once under the same
		// bodyless/initial-attempt rules as an edge→agent transport failure.
		if r.Context().Value(transportRetryKey{}) == true && r.Context().Err() == nil && requestHasNoBody(r) {
			retryFlags.Store(r, retryTransport)
			return
		}
		http.Error(w, "agent proxy unavailable", http.StatusBadGateway)
		return
	}
	// A pre-response transport error can expose a route-cache entry whose agent
	// endpoint disappeared during rolling cutover. A request body might already
	// be consumed, so it must fail closed. Never retry after client cancellation.
	if r.Context().Value(transportRetryKey{}) == true && r.Context().Err() == nil && requestHasNoBody(r) {
		retryFlags.Store(r, retryTransport)
		return
	}
	http.Error(w, err.Error(), http.StatusBadGateway)
}

// routeCacheKey 是 (hostname, 请求端口) 的缓存键；port=0 表示主 service。
func routeCacheKey(host string, port int) string {
	if port == 0 {
		return host + "|primary"
	}
	return host + "|" + strconv.Itoa(port)
}

// loadRoute 按请求端口加载路由投影（v1.1：port=0 → hostidx 主端口；
// port>0 → 该端口必须已声明，否则权威 miss）。
func (e *edge) loadRoute(ctx context.Context, host string, port int) (any, error) {
	var route *catalog.Route
	var err error
	if port == 0 {
		route, err = e.catalog.GetRouteForHostname(ctx, host)
	} else {
		var declared bool
		route, declared, err = e.catalog.GetRouteForPort(ctx, host, port)
		if err == nil && !declared {
			route = nil
		}
	}
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

// requestRoutePort 解析请求的目标路由端口：
//   - 附加监听器注入的端口优先（客户端打到 :9080 → 按 9080 查路由）；
//   - Host 显式端口不属于 edge 自身监听集合 → 客户端显式请求该 app service
//     端口（如 Host: app:80 打非主 service；TLS/主端口的 Host 寻址不在此列）；
//   - 其余（无端口 / 端口是 edge 自身）→ 0（主 service）。
func (e *edge) requestRoutePort(r *http.Request) int {
	if lp, ok := r.Context().Value(listenPortKey{}).(int); ok && lp > 0 {
		return lp
	}
	if _, rawPort, err := net.SplitHostPort(r.Host); err == nil {
		if p, perr := strconv.Atoi(rawPort); perr == nil && p > 0 && !e.edgePorts[p] {
			return p
		}
	}
	return 0
}

func (e *edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writePlain(w, http.StatusOK, "ok\n")
		return
	}
	host := stripPort(r.Host)
	reqPort := e.requestRoutePort(r)

	// M4：每 hostname 限流。
	if !e.limiter.allow(host) {
		e.cnt.rateLimited.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	cacheKey := routeCacheKey(host, reqPort)
	v, servedStale, err := e.routes.Get(r.Context(), cacheKey, func(ctx context.Context, _ string) (any, error) {
		return e.loadRoute(ctx, host, reqPort)
	})
	if errors.Is(err, edgesvc.ErrNotFound) {
		// 权威不存在（hostname 无路由 / 端口未声明）：立即 404，不 serve-stale（P2-8）。
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

	backend, selectErr := e.selectBackend(route, r.Header.Get(headerPinMachine))
	if selectErr != nil {
		e.writeSelectionError(w, selectErr)
		return
	}
	if r.Header.Get(headerPinMachine) != "" {
		e.cnt.pinHits.Add(1)
	}

	if servedStale {
		e.cnt.staleServes.Add(1)
	}

	// v1.1（ADR-0019）：命中 backend 标识响应头（覆盖正常转发/serve-stale/
	// 403 重试全部路径——重试前重设）。
	w.Header().Set(headerMachineID, backend.MachineID)

	// M4（ADR-0006）：execution-bound credential 随请求下发。Get 按
	// (machine, execution) 缓存；换代后旧 token 不命中，回源即得新 token
	//（P1-2）；API 不可达时在 stale 窗口内复用 execution 匹配的
	// last-known-good（P1-3）。缺 token 且无降级可用时 503 fail-closed。
	credential, terr := e.tokens.Get(r.Context(), backend.MachineID, backend.ExecutionID)
	if terr != nil {
		e.inflight.release(backend.MachineID)
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
	retry := e.tryServe(w, r, backend, credential, servedStale, true)
	if retry == retryNone {
		return
	}
	// Only bodyless requests are retry-safe. A reverse proxy may already have
	// consumed a body before it observes the upstream 403. Transport retries
	// are marked only for bodyless requests by ErrorHandler, but keep this
	// guard fail-closed if that contract changes.
	if !requestHasNoBody(r) {
		w.Header().Set(headerMachineID, backend.MachineID)
		if retry == retryForbidden {
			http.Error(w, "agent proxy rejected request", http.StatusForbidden)
		} else {
			http.Error(w, "agent proxy unavailable", http.StatusBadGateway)
		}
		return
	}
	if retry == retryForbidden {
		e.cnt.forbiddenRetry.Add(1)
	}
	// Both a rejected credential and a dead cached endpoint can coincide with
	// a rollout; invalidate both projections before the one permitted retry.
	e.tokens.Invalidate(backend.MachineID)
	e.routes.Invalidate(cacheKey)
	// Apply exactly the same eligibility, pin, least-inflight, and atomic hard
	// reservation rules after a 403. If refresh fails, the already validated
	// route is the safe fallback, but it is still reselected/reserved.
	retryRoute := route
	if nv, _, nerr := e.routes.Get(r.Context(), cacheKey, func(ctx context.Context, _ string) (any, error) {
		return e.loadRoute(ctx, host, reqPort)
	}); nerr == nil {
		if nr, ok := nv.(*catalog.Route); ok && nr != nil {
			retryRoute = nr
		}
	}
	retryBackend, selectErr := e.selectBackend(retryRoute, r.Header.Get(headerPinMachine))
	if selectErr != nil {
		e.writeSelectionError(w, selectErr)
		return
	}
	if r.Header.Get(headerPinMachine) != "" {
		e.cnt.pinHits.Add(1)
	}
	w.Header().Set(headerMachineID, retryBackend.MachineID)
	retryCred, rerr := e.tokens.Get(r.Context(), retryBackend.MachineID, retryBackend.ExecutionID)
	if rerr != nil {
		e.inflight.release(retryBackend.MachineID)
		e.cnt.tokenErrors.Add(1)
		http.Error(w, "traffic token unavailable", http.StatusServiceUnavailable)
		return
	}
	if terminal := e.tryServe(w, r, retryBackend, retryCred, false, false); terminal == retryForbidden {
		// The second 403 is terminal. ErrorHandler suppressed it so it could not
		// partially commit; explicitly write it instead of leaving net/http to
		// infer an empty 200 response. A second transport error is not suppressed
		// and is emitted by ErrorHandler as its original 502.
		w.Header().Set(headerMachineID, retryBackend.MachineID)
		http.Error(w, "agent proxy rejected request", http.StatusForbidden)
	}
}

// selectLeastInflight 在 eligible 集合内按 inflight 升序选择；相同 inflight
// 随机打散（v1.1，ADR-0020）。最闲者仍 ≥ hard 上限时返回 over=true。
var (
	errNoEligible = errors.New("no ready backend")
	errPinMiss    = errors.New("pinned machine is not eligible")
	errHardLimit  = errors.New("backend hard concurrency limit")
)

// selectBackend is shared by initial and post-403 attempts. It also reserves
// the selected backend's slot; tryServe releases that reservation.
func (e *edge) selectBackend(route *catalog.Route, pin string) (catalog.Backend, error) {
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
				if chosen, over := e.inflight.selectAndAcquire([]catalog.Backend{b}, e.hardConcurrency); over {
					return catalog.Backend{}, errHardLimit
				} else {
					return chosen, nil
				}
			}
		}
		return catalog.Backend{}, errPinMiss
	}
	if chosen, over := e.inflight.selectAndAcquire(eligible, e.hardConcurrency); over {
		return catalog.Backend{}, errHardLimit
	} else {
		return chosen, nil
	}
}

func (e *edge) writeSelectionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errPinMiss):
		e.cnt.pinMisses.Add(1)
		w.Header().Set("X-Firepaas-Pin-Error", "machine not eligible (missing, replaced, not ready or draining)")
		http.Error(w, "pinned machine is not an eligible backend", http.StatusNotFound)
	case errors.Is(err, errHardLimit):
		e.cnt.hardRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "backend at hard concurrency limit", http.StatusServiceUnavailable)
	default:
		http.Error(w, "no ready backend for hostname", http.StatusServiceUnavailable)
	}
}

// selectLeastInflight remains a test-friendly selector. It reserves a slot;
// callers that use it directly must release the returned backend.
func (e *edge) selectLeastInflight(eligible []catalog.Backend) (catalog.Backend, bool) {
	return e.inflight.selectAndAcquire(eligible, e.hardConcurrency)
}

// errRetryForbidden 由 ModifyResponse 返回，驱动 tryServe 的 403 重试：
// ModifyResponse 返回非 nil error 时 ReverseProxy 会调 ErrorHandler，而
// 我们在 ErrorHandler 里识别这个哨兵错误并不写任何响应（返回 false 交给
// 上层重试）。这样 403 响应永远不会半写到客户端。
var (
	errRetryForbidden  = errors.New("agent proxy 403: retry with fresh token")
	errRetryProxyRoute = errors.New("agent proxy retryable route failure")
)

// tryServe forwards one attempt. It returns a non-zero reason only when the
// ErrorHandler deliberately withheld a pre-response response for the caller's
// single retry. All other errors, including a transport failure after refresh,
// remain ReverseProxy's normal response.
// v1.1（ADR-0020）：inflight 计数覆盖整个请求生命周期（含流式/WS——
// ServeHTTP 阻塞到连接结束才返回）。
func (e *edge) tryServe(w http.ResponseWriter, r *http.Request, backend catalog.Backend,
	credential string, servedStale, allowTransportRetry bool) retryReason {

	// selectBackend already atomically reserved this slot.
	defer e.inflight.release(backend.MachineID)

	ctx := context.WithValue(r.Context(), backendKey{}, backend)
	if credential != "" {
		ctx = context.WithValue(ctx, credKey{}, credential)
	}
	if servedStale {
		ctx = context.WithValue(ctx, staleKey{}, true)
	}
	if allowTransportRetry {
		ctx = context.WithValue(ctx, transportRetryKey{}, true)
	}
	req := r.WithContext(ctx)
	if servedStale {
		w.Header().Set("X-Firepaas-Stale", "stale")
	}
	defer retryFlags.Delete(req)
	e.proxy.ServeHTTP(w, req)
	// ServeHTTP 返回后查看 ErrorHandler 是否标记了 403 拦截。
	if v, ok := retryFlags.Load(req); ok {
		retryFlags.Delete(req)
		if reason, ok := v.(retryReason); ok {
			return reason
		}
	}
	return retryNone
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

func isTruthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
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
