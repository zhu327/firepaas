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

	"github.com/kernel/hypeman/lib/network"

	"github.com/example/firepaas/internal/agent/health"
	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/network/slot"
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
	// generation fence（P0-2）：machine → 已知最高 generation 高水位，
	// 拒绝早于高水位的变更请求（重启保留；machine 删除后仍拒绝旧 re-create）。
	fencesPath := envOr("FIREPAAS_AGENT_FENCES_PATH", filepath.Join(cfg.DataDir, "agent", "fences.json"))
	fences, err := state.OpenFences(fencesPath)
	if err != nil {
		return err
	}

	// ledger/fences 年龄 GC（mvp-plan §5.5 可配置去重窗口，评审 P2-5）：
	// 启动时清理一次，之后每小时一次。
	retention, err := time.ParseDuration(envOr("FIREPAAS_AGENT_LEDGER_RETENTION", "24h"))
	if err != nil || retention <= 0 {
		return fmt.Errorf("invalid FIREPAAS_AGENT_LEDGER_RETENTION: %v", err)
	}
	pruneGC := func() {
		cutoff := time.Now().Add(-retention)
		if n, err := ledger.PruneBefore(cutoff); err != nil {
			slog.Error("ledger gc", "error", err)
		} else if n > 0 {
			slog.Info("ledger gc pruned expired records", "removed", n)
		}
		if n, err := fences.PruneBefore(cutoff); err != nil {
			slog.Error("fences gc", "error", err)
		} else if n > 0 {
			slog.Info("fences gc pruned expired entries", "removed", n)
		}
	}
	pruneGC()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pruneGC()
			}
		}
	}()

	// slot 网络后端（ADR-0004 feature flag：bridge|slot，默认 bridge）。
	// slot 模式启动时先对账：回收孤儿 netns，为存活实例补齐 slot。
	networkBackend := envOr("FIREPAAS_NETWORK_BACKEND", "bridge")
	var slotManager *slot.Manager
	if networkBackend == "slot" {
		slotManager, err = slot.New(slot.Config{
			SubnetCIDR: cfg.Network.SubnetCIDR,
			Gateway:    cfg.Network.SubnetGateway,
			StatePath:  filepath.Join(cfg.DataDir, "agent", "slots.json"),
		})
		if err != nil {
			return err
		}
		if err := slotManager.Load(); err != nil {
			return err
		}
		live := make([]slot.LiveInstance, 0)
		if listed, err := set.Instances.ListInstances(ensureCtx, nil); err == nil {
			for i := range listed {
				inst := &listed[i]
				id := inst.Name
				if id == "" {
					id = inst.Id
				}
				live = append(live, slot.LiveInstance{
					MachineID: id,
					Tap:       network.GenerateTAPName(inst.Id),
					GuestIP:   inst.IP,
				})
			}
		}
		if err := slotManager.Reconcile(ensureCtx, live); err != nil {
			return fmt.Errorf("slot reconcile: %w", err)
		}
		slog.Info("slot network backend active", "slots", slotManager.Count())
	}

	adapter := machine.New(set.Instances, set.Images, slotManager, health.New())
	// 已承诺资源（硬准入输入）：实例 Size / Vcpus 之和（M2.2）。
	listedResources := func() (memMiB uint64, vcpus int) {
		listCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		listed, err := set.Instances.ListInstances(listCtx, nil)
		if err != nil {
			return 0, 0
		}
		var totalBytes int64
		var totalVCPU int
		for i := range listed {
			totalBytes += listed[i].Size
			totalVCPU += listed[i].Vcpus
		}
		if totalBytes > 0 {
			memMiB = uint64(totalBytes) / (1024 * 1024)
		}
		return memMiB, totalVCPU
	}
	memAllocated := func() uint64 { m, _ := listedResources(); return m }
	vcpuAllocated := func() int { _, v := listedResources(); return v }
	infoProvider := info.New(nodeID, serviceVersion, serviceCommit, nodePool, fcVersion, cfg.Network.SubnetCIDR, cfg.DataDir, memAllocated, vcpuAllocated)
	srv := server.New(adapter, ledger, fences, infoProvider)

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
	proxyHandler := http.Handler(proxy.New(adapter))
	if tlsConf != nil {
		// mTLS 已保证“持本 CA 证书才能连”；这里进一步按证书 CN 做最小授权：
		// gRPC（5108）只接受控制面身份，proxy（5107）只接受 edge 身份
		// （评审 P1-2，ADR-0006 的 M1 降级形态）。
		grpcAllowed := mtls.SplitAllowlist(os.Getenv("FIREPAAS_AGENT_GRPC_ALLOWED_CLIENTS"), "control-plane")
		proxyAllowed := mtls.SplitAllowlist(os.Getenv("FIREPAAS_AGENT_PROXY_ALLOWED_CLIENTS"), "edge-proxy")
		grpcOpts = append(grpcOpts,
			grpc.Creds(credentials.NewTLS(tlsConf)),
			grpc.ChainUnaryInterceptor(mtls.UnaryServerIdentityInterceptor(grpcAllowed)),
		)
		proxyTLS = tlsConf
		proxyHandler = mtls.RequireClientIdentity(proxyHandler, proxyAllowed)
		slog.Info("agentd mTLS enabled (static certs, ADR-0006 degradation)",
			"grpc_clients", grpcAllowed, "proxy_clients", proxyAllowed)
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
