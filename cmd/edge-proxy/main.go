// Command edge-proxy composes the edge data-plane HTTP service.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	edgesvc "github.com/zhu327/firepaas/internal/edge"
	"github.com/zhu327/firepaas/internal/security/mtls"
	"github.com/redis/go-redis/v9"
)

const (
	freshTTLDefault        = 5 * time.Second
	staleDefault           = 120 * time.Second
	hardConcurrencyDefault = 256
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
	defer rdb.Close()

	agentTLS, err := loadAgentTLS()
	if err != nil {
		return err
	}
	clientCerts, err := loadServerCertificates(tlsPort)
	if err != nil {
		return err
	}

	extraPorts, err := parseExtraPorts(os.Getenv("FIREPAAS_EDGE_EXTRA_PORTS"))
	if err != nil {
		return fmt.Errorf("FIREPAAS_EDGE_EXTRA_PORTS: %w", err)
	}
	edgePorts := listenerPorts(port, tlsPort, extraPorts)
	tokens := edgesvc.NewTokenClient(envOr("FIREPAAS_API_ADDR", "http://127.0.0.1:8080"), os.Getenv("FIREPAAS_API_TOKEN"), 30*time.Second)
	tokens.SetStaleWindow(staleWindow)
	counters := &edgesvc.Counters{}
	handler := edgesvc.NewHandler(edgesvc.Config{
		Catalog: catalog.New(rdb), Routes: edgesvc.NewRouteCache(freshTTL, staleWindow), Tokens: tokens,
		Limiter:  edgesvc.NewRateLimiter(envFloatOr("FIREPAAS_EDGE_RATE_LIMIT", 100), envFloatOr("FIREPAAS_EDGE_RATE_BURST", 200)),
		Counters: counters, AgentTLS: agentTLS, HardConcurrency: hardConcurrency, EdgePorts: edgePorts,
	})

	if err := startMetrics(counters, handler); err != nil {
		return err
	}
	plainHandler := http.Handler(handler)
	tlsEnabled := tlsPort != "" && len(clientCerts) > 0
	if tlsEnabled {
		plainHandler = redirectHandler(tlsPort)
	}

	mainListener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	server := &http.Server{Handler: plainHandler}
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
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, edgesvc.WithListenPort(r, p)) })}
		extraServers = append(extraServers, srv)
		serve(srv, listener, fmt.Sprintf("edge extra serve port=%d", p))
	}

	var tlsServer *http.Server
	if tlsEnabled {
		tlsCfg := &tls.Config{Certificates: clientCerts, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}
		listener, listenErr := net.Listen("tcp", tlsPort)
		if listenErr != nil {
			return fmt.Errorf("listen %s: %w", tlsPort, listenErr)
		}
		tlsServer = &http.Server{Handler: handler, TLSConfig: tlsCfg}
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

func loadAgentTLS() (*tls.Config, error) {
	cert, key, ca := os.Getenv("FIREPAAS_EDGE_TLS_CERT"), os.Getenv("FIREPAAS_EDGE_TLS_KEY"), os.Getenv("FIREPAAS_EDGE_TLS_CA")
	if cert == "" || key == "" || ca == "" {
		return nil, nil
	}
	cfg, err := mtls.ClientConfig(cert, key, ca, "agentd")
	if err != nil {
		return nil, fmt.Errorf("edge mTLS config: %w", err)
	}
	return cfg, nil
}

func loadServerCertificates(tlsPort string) ([]tls.Certificate, error) {
	cert, key := os.Getenv("FIREPAAS_EDGE_SERVER_CERT"), os.Getenv("FIREPAAS_EDGE_SERVER_KEY")
	if (cert == "") != (key == "") {
		return nil, errors.New("FIREPAAS_EDGE_SERVER_CERT and FIREPAAS_EDGE_SERVER_KEY must be set together")
	}
	if tlsPort != "" && cert == "" && !isTruthy(os.Getenv("FIREPAAS_ALLOW_INSECURE_DEV")) {
		return nil, errors.New("public edge TLS requires FIREPAAS_EDGE_SERVER_CERT/KEY (set FIREPAAS_ALLOW_INSECURE_DEV=true only for local development)")
	}
	if cert == "" {
		return nil, nil
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("edge server cert: %w", err)
	}
	return []tls.Certificate{pair}, nil
}

func startMetrics(counters *edgesvc.Counters, handler *edgesvc.Handler) error {
	port := envOr("FIREPAAS_EDGE_METRICS_PORT", "")
	if port == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counters.WritePrometheus(w)
		handler.WriteInflightPrometheus(w)
	})
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen metrics :%s: %w", port, err)
	}
	serve(&http.Server{Handler: mux}, listener, "edge metrics serve")
	return nil
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
