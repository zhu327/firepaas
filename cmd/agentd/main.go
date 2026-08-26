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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/runtime"
	"github.com/example/firepaas/internal/agent/server"
	"github.com/example/firepaas/internal/agent/state"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
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

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterInfoServiceServer(grpcServer, srv)
	pb.RegisterMachineServiceServer(grpcServer, srv)

	errCh := make(chan error, 1)
	go func() {
		slog.Info("agentd gRPC listening", "port", port, "node_id", nodeID, "node_pool", nodePool)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		slog.Info("agentd shutting down")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hostnameOr(def string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return def
	}
	return h
}
