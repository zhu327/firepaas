// Command agentd 是 firepaas 的节点数据面 agent（M1.4）。
//
// 目标形态（mvp-plan §5.5）：
//   - 包装 hypeman lib/instances 与 lib/images，不 import cmd/api 与 providers
//   - 暴露 gRPC InfoService / MachineService（protos/agent/v1）
//   - operation ledger 提供 request hash/result 的原子幂等与重启重放
//
// M1 身份降级（ADR-0006）：先明文 + 主机端口 ACL；mTLS 在 M1.3 补上。
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
	"path/filepath"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/proxy"
	"github.com/example/firepaas/internal/agent/runtime"
	"github.com/example/firepaas/internal/agent/server"
	"github.com/example/firepaas/internal/agent/state"
	"github.com/example/firepaas/internal/security/mtls"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	serviceVersion = "0.1.0-m1"
	serviceCommit  = "dev"
)

func main() {
	if err := run(); err != nil {
		slog.Error("agentd terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	port := envOr("FIREPAAS_AGENT_GRPC_PORT", "5108")
	proxyPort := envOr("FIREPAAS_AGENT_PROXY_PORT", "5107")
	bind := envOr("FIREPAAS_AGENT_BIND", "127.0.0.1")
	nodePool := envOr("FIREPAAS_AGENT_NODE_POOL", "compute")
	nodeID := envOr("FIREPAAS_AGENT_NODE_ID", hostnameOr("firepaas-node"))
	fcVersion := envOr("FIREPAAS_AGENT_FIRECRACKER_VERSION", "v1.14.2")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := runtime.LoadConfig()
	if err != nil {
		return err
	}
	set, err := runtime.Assemble(cfg)
	if err != nil {
		return err
	}

	// 首次启动需要准备内核/initrd（走 HYPEMAN_DOCKER_HUB_MIRROR 补丁）。
	slog.Info("ensuring hypeman system files (first run may take minutes)")
	ensureCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if err := set.System.EnsureSystemFiles(ensureCtx); err != nil {
		return fmt.Errorf("ensure system files: %w", err)
	}

	ledgerPath := envOr("FIREPAAS_AGENT_LEDGER_PATH", filepath.Join(cfg.DataDir, "agent", "ledger.json"))
	ledger, err := state.Open(ledgerPath)
	if err != nil {
		return err
	}

	adapter := machine.New(set.Instances, set.Images)
	infoProvider := info.New(nodeID, serviceVersion, serviceCommit, nodePool, fcVersion, cfg.Network.SubnetCIDR)
	srv := server.New(adapter, ledger, infoProvider)

	lis, err := net.Listen("tcp", net.JoinHostPort(bind, port))
	if err != nil {
		return fmt.Errorf("listen %s:%s: %w", bind, port, err)
	}

	var grpcOpts []grpc.ServerOption
	var proxyTLS *tls.Config
	tlsConf, err := agentServerTLS()
	if err != nil {
		return err
	}
	if tlsConf != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConf)))
		proxyTLS = tlsConf
		slog.Info("agentd mTLS enabled (static certs, ADR-0006 degradation)")
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	pb.RegisterInfoServiceServer(grpcServer, srv)
	pb.RegisterMachineServiceServer(grpcServer, srv)

	errCh := make(chan error, 2)
	go func() {
		slog.Info("agentd gRPC listening", "port", port, "node_id", nodeID, "node_pool", nodePool)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	proxyHandler := proxy.New(adapter)
	proxyServer := &http.Server{Addr: net.JoinHostPort(bind, proxyPort), Handler: proxyHandler, TLSConfig: proxyTLS}
	go func() {
		slog.Info("agentd workload proxy listening", "addr", proxyServer.Addr, "mtls", proxyTLS != nil)
		var err error
		if proxyTLS != nil {
			err = proxyServer.ListenAndServeTLS("", "")
		} else {
			err = proxyServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("agentd shutting down")
		grpcServer.GracefulStop()
		_ = proxyServer.Close()
		return nil
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// agentServerTLS 当三个证书路径都提供时构造服务端 mTLS 配置，否则返回 nil（明文降级）。
func agentServerTLS() (*tls.Config, error) {
	certFile := os.Getenv("FIREPAAS_AGENT_TLS_CERT")
	keyFile := os.Getenv("FIREPAAS_AGENT_TLS_KEY")
	caFile := os.Getenv("FIREPAAS_AGENT_TLS_CA")
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("FIREPAAS_AGENT_TLS_CERT/KEY/CA must be set together")
	}
	return mtls.ServerConfig(certFile, keyFile, caFile)
}

func hostnameOr(def string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return def
	}
	return h
}
