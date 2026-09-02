// Command edge-proxy composes the edge data-plane HTTP service.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	edgesvc "github.com/zhu327/firepaas/internal/edge"
	"github.com/zhu327/firepaas/internal/security/mtls"
)

const (
	freshTTLDefault        = 5 * time.Second
	staleDefault           = 120 * time.Second
	hardConcurrencyDefault = 256
	certReloadDefault      = time.Minute
	// WS/SSE 语义防护：只限制 header 读取与空闲连接；绝不设置
	// WriteTimeout/ReadTimeout——它们对整条响应流/请求计时，会直接切断
	// WebSocket 升级与 SSE 长连接（edge 是长连接的透传代理）。
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 90 * time.Second
)

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
	tlsPort := os.Getenv("FIREPAAS_EDGE_TLS_LISTEN")
	staleWindow := envDurOr("FIREPAAS_EDGE_STALE_WINDOW", staleDefault)
	freshTTL := envDurOr("FIREPAAS_EDGE_FRESH_TTL", freshTTLDefault)
	hardConcurrency := int64(envFloatOr("FIREPAAS_EDGE_HARD_CONCURRENCY", hardConcurrencyDefault))
	if hardConcurrency <= 0 {
		hardConcurrency = hardConcurrencyDefault
	}

	rdb := redis.NewClient(&redis.Options{Addr: envOr("FIREPAAS_REDIS_ADDR", "127.0.0.1:6379")})
	defer func() { _ = rdb.Close() }()

	certReload := envDurOr("FIREPAAS_EDGE_CERT_RELOAD_INTERVAL", certReloadDefault)
	gauges := newCertExpiryGauges()
	agentTLS, agentCertMgr, err := loadAgentTLS(certReload, gauges)
	if err != nil {
		return err
	}
	if agentCertMgr != nil {
		defer agentCertMgr.Close()
	}
	serverCertMgr, err := loadServerCertificates(tlsPort, certReload, gauges)
	if err != nil {
		return err
	}
	if serverCertMgr != nil {
		defer serverCertMgr.Close()
	}

	extraPorts, err := parseExtraPorts(os.Getenv("FIREPAAS_EDGE_EXTRA_PORTS"))
	if err != nil {
		return fmt.Errorf("FIREPAAS_EDGE_EXTRA_PORTS: %w", err)
	}
	edgePorts := listenerPorts(port, tlsPort, extraPorts)
	tokens := edgesvc.NewTokenClient(
		envOr("FIREPAAS_API_ADDR", "http://127.0.0.1:8080"),
		os.Getenv("FIREPAAS_API_TOKEN"),
		30*time.Second,
	)
	tokens.SetStaleWindow(staleWindow)
	if n := envIntOr("FIREPAAS_EDGE_TOKEN_CACHE_MAX", 0); n > 0 { // F：token 缓存容量上限
		tokens.MaxEntries = n
	}
	counters := &edgesvc.Counters{}
	routes := edgesvc.NewRouteCache(freshTTL, staleWindow)
	if n := envIntOr("FIREPAAS_EDGE_ROUTE_CACHE_MAX", 0); n > 0 { // P1-16 容量上限
		routes.MaxEntries = n
	}
	limiter := edgesvc.NewRateLimiter(
		envFloatOr("FIREPAAS_EDGE_RATE_LIMIT", 100),
		envFloatOr("FIREPAAS_EDGE_RATE_BURST", 200),
	)
	if n := envIntOr("FIREPAAS_EDGE_RATELIMIT_BUCKETS_MAX", 0); n > 0 { // P1-16 容量上限
		limiter.MaxBuckets = n
	}
	handler := edgesvc.NewHandler(edgesvc.Config{
		Catalog: catalog.New(rdb), Routes: routes, Tokens: tokens, Limiter: limiter,
		Counters: counters, AgentTLS: agentTLS, HardConcurrency: hardConcurrency, EdgePorts: edgePorts,
	})

	if err := startMetrics(counters, handler, gauges); err != nil {
		return err
	}
	plainHandler := http.Handler(handler)
	tlsEnabled := tlsPort != "" && serverCertMgr != nil
	if tlsEnabled {
		plainHandler = redirectHandler(tlsPort)
	}

	mainListener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	server := newEdgeServer(plainHandler)
	serve(server, mainListener, "edge serve")

	var extraServers []*http.Server
	mainPort, _ := strconv.Atoi(strings.TrimPrefix(port, ":"))
	for _, p := range extraPorts {
		if p == mainPort {
			continue
		}
		listener, listenErr := net.Listen("tcp", ":"+strconv.Itoa(p))
		if listenErr != nil {
			return fmt.Errorf("listen extra :%d: %w", p, listenErr)
		}
		p := p
		srv := newEdgeServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, edgesvc.WithListenPort(r, p)) },
		))
		extraServers = append(extraServers, srv)
		serve(srv, listener, fmt.Sprintf("edge extra serve port=%d", p))
	}

	var tlsServer *http.Server
	if tlsEnabled {
		tlsCfg := &tls.Config{
			GetCertificate: serverCertMgr.GetCertificate, // 热重载（契约 C-1）
			MinVersion:     tls.VersionTLS12,
			NextProtos:     []string{"h2", "http/1.1"},
		}
		listener, listenErr := net.Listen("tcp", tlsPort)
		if listenErr != nil {
			return fmt.Errorf("listen %s: %w", tlsPort, listenErr)
		}
		tlsServer = newEdgeServer(handler)
		tlsServer.TLSConfig = tlsCfg
		serve(tlsServer, tls.NewListener(listener, tlsCfg), "edge tls serve")
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range extraServers {
		_ = srv.Shutdown(shutdownCtx)
	}
	if tlsServer != nil {
		_ = tlsServer.Shutdown(shutdownCtx)
	}
	return server.Shutdown(shutdownCtx)
}

// loadAgentTLS 装配 edge→agent proxy（:5107）的 mTLS 客户端配置；证书经
// CertManager 热重载并导出到期指标（契约 C-1）。
//
// P0#2 fail-closed：材料缺失即启动失败，不得静默退化为明文 HTTP。唯一例外是
// 显式开发模式 FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true（契约 C-2，对齐 agentd 的
// FIREPAAS_ALLOW_INSECURE_DEV），此时打醒目告警。
func loadAgentTLS(reloadEvery time.Duration, gauges *certExpiryGauges) (*tls.Config, *mtls.CertManager, error) {
	cert := os.Getenv("FIREPAAS_EDGE_TLS_CERT")
	key := os.Getenv("FIREPAAS_EDGE_TLS_KEY")
	ca := os.Getenv("FIREPAAS_EDGE_TLS_CA")
	if cert == "" && key == "" && ca == "" {
		if isTruthy(os.Getenv("FIREPAAS_EDGE_ALLOW_INSECURE_DEV")) {
			slog.Warn(
				"FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true：edge→agent :5107 使用明文 HTTP，仅限本地开发，生产必须配置 FIREPAAS_EDGE_TLS_CERT/KEY/CA",
			)
			return nil, nil, nil
		}
		return nil, nil, errors.New(
			"edge agent mTLS required: set FIREPAAS_EDGE_TLS_CERT/KEY/CA (or FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true for local development only)",
		)
	}
	if cert == "" || key == "" || ca == "" {
		return nil, nil, errors.New("FIREPAAS_EDGE_TLS_CERT/KEY/CA must be set together")
	}
	mgr, err := mtls.NewCertManager(cert, key, reloadEvery, nil, func(expiry time.Time) {
		gauges.set(cert, expiry)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("edge mTLS config: %w", err)
	}
	cfg, err := mgr.ClientTLSConfig(ca, "agentd")
	if err != nil {
		mgr.Close()
		return nil, nil, fmt.Errorf("edge mTLS config: %w", err)
	}
	return cfg, mgr, nil
}

// loadServerCertificates 装配面向公网的 server 证书（热重载 + 到期指标）。
func loadServerCertificates(
	tlsPort string,
	reloadEvery time.Duration,
	gauges *certExpiryGauges,
) (*mtls.CertManager, error) {
	cert, key := os.Getenv("FIREPAAS_EDGE_SERVER_CERT"), os.Getenv("FIREPAAS_EDGE_SERVER_KEY")
	if (cert == "") != (key == "") {
		return nil, errors.New("FIREPAAS_EDGE_SERVER_CERT and FIREPAAS_EDGE_SERVER_KEY must be set together")
	}
	if tlsPort != "" && cert == "" && !isTruthy(os.Getenv("FIREPAAS_ALLOW_INSECURE_DEV")) {
		return nil, errors.New(
			"public edge TLS requires FIREPAAS_EDGE_SERVER_CERT/KEY (set FIREPAAS_ALLOW_INSECURE_DEV=true only for local development)",
		)
	}
	if cert == "" {
		return nil, nil
	}
	mgr, err := mtls.NewCertManager(cert, key, reloadEvery, nil, func(expiry time.Time) {
		gauges.set(cert, expiry)
	})
	if err != nil {
		return nil, fmt.Errorf("edge server cert: %w", err)
	}
	return mgr, nil
}

// certExpiryGauges 汇总各 CertManager 上报的证书到期时间，并在 metrics 端点
// 导出 firepaas_tls_cert_not_after_seconds（gauge，label=file；契约 C-1）。
type certExpiryGauges struct {
	mu    sync.Mutex
	files map[string]time.Time
}

func newCertExpiryGauges() *certExpiryGauges {
	return &certExpiryGauges{files: map[string]time.Time{}}
}

func (g *certExpiryGauges) set(file string, expiry time.Time) {
	g.mu.Lock()
	g.files[file] = expiry
	g.mu.Unlock()
}

func (g *certExpiryGauges) WritePrometheus(w io.Writer) {
	g.mu.Lock()
	files := make([]string, 0, len(g.files))
	expiry := make(map[string]time.Time, len(g.files))
	for f, t := range g.files {
		files = append(files, f)
		expiry[f] = t
	}
	g.mu.Unlock()
	if len(files) == 0 {
		return
	}
	sort.Strings(files)
	_, _ = fmt.Fprint(
		w,
		"# HELP firepaas_tls_cert_not_after_seconds unix timestamp when the managed TLS certificate expires\n# TYPE firepaas_tls_cert_not_after_seconds gauge\n",
	)
	for _, f := range files {
		_, _ = fmt.Fprintf(w, "firepaas_tls_cert_not_after_seconds{file=%q} %d\n", f, expiry[f].Unix())
	}
}

func startMetrics(counters *edgesvc.Counters, handler *edgesvc.Handler, gauges *certExpiryGauges) error {
	port := envOr("FIREPAAS_EDGE_METRICS_PORT", "")
	if port == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counters.WritePrometheus(w)
		handler.WriteInflightPrometheus(w)
		handler.WriteRouteRevisionRejectsPrometheus(w)
		gauges.WritePrometheus(w)
	})
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen metrics :%s: %w", port, err)
	}
	serve(newEdgeServer(mux), listener, "edge metrics serve")
	return nil
}

// newEdgeServer 统一 edge 全部 http.Server 的超时口径：仅 ReadHeaderTimeout
// 与 IdleTimeout。不设 WriteTimeout/ReadTimeout 的原因见顶部常量注释
// （WS/SSE 长连接不得被整体超时切断）。
func newEdgeServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}
}

func serve(server *http.Server, listener net.Listener, label string) {
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error(label, "error", err)
		}
	}()
}

func redirectHandler(tlsPort string) http.Handler {
	suffix := ""
	if p, err := strconv.Atoi(strings.TrimPrefix(tlsPort, ":")); err == nil && p != 443 {
		suffix = ":" + strconv.Itoa(p)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.Redirect(w, r, "https://"+stripPort(r.Host)+suffix+r.URL.RequestURI(), http.StatusPermanentRedirect)
	})
}

func listenerPorts(port, tlsPort string, extra []int) map[int]bool {
	out := map[int]bool{}
	for _, raw := range []string{port, tlsPort} {
		if p, e := strconv.Atoi(strings.TrimPrefix(raw, ":")); e == nil {
			out[p] = true
		}
	}
	for _, p := range extra {
		out[p] = true
	}
	return out
}

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
			lo, e1 := strconv.Atoi(part[:i])
			hi, e2 := strconv.Atoi(part[i+1:])
			if e1 != nil || e2 != nil || lo < 1 || hi < lo || hi > 65535 {
				return nil, fmt.Errorf("bad port range %q", part)
			}
			for p := lo; p <= hi; p++ {
				out = append(out, p)
			}
			continue
		}
		p, e := strconv.Atoi(part)
		if e != nil || p < 1 || p > 65535 {
			return nil, fmt.Errorf("bad port %q", part)
		}
		out = append(out, p)
	}
	return out, nil
}

func stripPort(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport
	}
	return host
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
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			return d
		}
	}
	return def
}

func envFloatOr(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		if _, e := fmt.Sscanf(v, "%g", &f); e == nil {
			return f
		}
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			return n
		}
	}
	return def
}
