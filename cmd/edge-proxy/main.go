// Command edge-proxy 是 firepaas 的边缘路由（M1.6 最小版）。
//
// 流量：client → edge(hostname) → Redis route catalog → agent proxy :5107 → VM
// edge 永不读取 slot_ip；只使用 catalog 中的 node_proxy_endpoint。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/redis/go-redis/v9"
)

const (
	headerMachineID   = "X-Firepaas-Machine-ID"
	headerExecutionID = "X-Firepaas-Execution-ID"
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
	redisAddr := envOr("FIREPAAS_REDIS_ADDR", "127.0.0.1:6379")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	cat := catalog.New(rdb)
	edge := &edge{catalog: cat}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	srv := &http.Server{Handler: edge}
	go func() {
		slog.Info("edge-proxy listening", "port", port)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("edge serve", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

type edge struct {
	catalog *catalog.Catalog
	counter atomic.Uint64
}

func (e *edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := stripPort(r.Host)
	route, err := e.catalog.GetRouteForHostname(r.Context(), host)
	if err != nil {
		http.Error(w, "catalog error", http.StatusServiceUnavailable)
		return
	}
	if route == nil || len(route.Backends) == 0 {
		http.Error(w, "no route for hostname", http.StatusNotFound)
		return
	}

	// M1 简单 round-robin；权重/draining 在 M3 发布状态机实现。
	backend := route.Backends[e.counter.Add(1)%uint64(len(route.Backends))]

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = backend.NodeProxyEndpoint
			req.Header.Set(headerMachineID, backend.MachineID)
			req.Header.Set(headerExecutionID, backend.ExecutionID)
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
}

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
