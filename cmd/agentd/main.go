// Command agentd 是 firepaas 的节点数据面 agent（M1.4）。
//
// 目标形态（mvp-plan §5.5）：
//   - 包装 hypeman lib/instances 与 lib/images，不 import cmd/api 与 providers
//   - 暴露 gRPC InfoService / MachineService（protos/agent/v1）
//   - operation ledger 提供 request hash/result 的原子幂等与重启重放
//
// mTLS 是生产默认；仅显式 FIREPAAS_ALLOW_INSECURE_DEV=true 可用于本地明文开发。
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
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kernel/hypeman/lib/autostandby"
	"github.com/kernel/hypeman/lib/healthcheck"
	"github.com/kernel/hypeman/lib/hypervisor"
	"github.com/kernel/hypeman/lib/hypervisor/firecracker"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/network"
	"github.com/kernel/hypeman/lib/vm_metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/zhu327/firepaas/internal/agent/egress"
	"github.com/zhu327/firepaas/internal/agent/health"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
	"github.com/zhu327/firepaas/internal/agent/probeflow"
	"github.com/zhu327/firepaas/internal/agent/proxy"
	"github.com/zhu327/firepaas/internal/agent/runtime"
	"github.com/zhu327/firepaas/internal/agent/server"
	"github.com/zhu327/firepaas/internal/agent/standby"
	"github.com/zhu327/firepaas/internal/agent/state"
	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/security/mtls"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	serviceVersion = "0.1.0-m1"
	serviceCommit  = "dev"
	// agentProtocolVersion 是 v1.2-A（ADR-0023）的契约版本标识。
	agentProtocolVersion = "firepaas.agent.v1"
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
	fcVersion := os.Getenv("FIREPAAS_AGENT_FIRECRACKER_VERSION")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// v1.1（F-2/A）：agent 侧 Prometheus 端点。默认只绑定 loopback；跨主机
	// 抓取必须由部署显式设置 FIREPAAS_AGENT_METRICS_BIND，并以网络 ACL 限制
	// Prometheus 所在可信网段。必须在 runtime.Assemble 之前设置全局 meter
	// provider——hypeman 各管理器在构造时从全局 provider 取 meter。
	meter := initAgentMetrics(ctx, envOr("FIREPAAS_AGENT_METRICS_PORT", ""),
		envOr("FIREPAAS_AGENT_METRICS_BIND", "127.0.0.1"))

	cfg, err := runtime.LoadConfig()
	if err != nil {
		return err
	}
	set, err := runtime.Assemble(cfg)
	if err != nil {
		return err
	}
	if fcVersion == "" {
		fcVersion, err = (&firecracker.Starter{}).GetVersion(set.Paths)
		if err != nil {
			return fmt.Errorf("detect firecracker version: %w", err)
		}
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
	// 启动时清理一次，之后每小时一次。fence 侧额外绑定 machine 存活（R2-6）：
	// 活 machine 的高水位必须随 machine 存活，年龄窗口不适用；实例清单
	// 不可得时跳过本轮 fence GC（不清单≠已死，误回收会让过期请求复活）。
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
		liveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		live, err := liveSlotInstances(liveCtx, set.Instances)
		cancel()
		if err != nil {
			slog.Warn("fences gc skipped this round: instance inventory unavailable", "error", err)
			return
		}
		liveSet := make(map[string]struct{}, len(live))
		for _, li := range live {
			liveSet[li.MachineID] = struct{}{}
		}
		// 清单里没有的 machine 视为已删除（prune-if-unknown），高水位超龄即收。
		liveFn := func(machineID string) bool { _, ok := liveSet[machineID]; return ok }
		if n, err := fences.PruneBeforeUnlessLive(cutoff, liveFn); err != nil {
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
	egressPort80 := envIntDefault("FIREPAAS_EGRESS_PROXY_PORT80", 18080)
	egressPort443 := envIntDefault("FIREPAAS_EGRESS_PROXY_PORT443", 18443)
	var slotManager *slot.Manager
	if networkBackend == "slot" {
		slotManager, err = slot.New(slot.Config{
			SubnetCIDR:         cfg.Network.SubnetCIDR,
			Gateway:            cfg.Network.SubnetGateway,
			StatePath:          filepath.Join(cfg.DataDir, "agent", "slots.json"),
			EgressProxyPort80:  egressPort80,
			EgressProxyPort443: egressPort443,
		})
		if err != nil {
			return err
		}
		if err := slotManager.Load(); err != nil {
			return err
		}
		live, err := liveSlotInstances(ensureCtx, set.Instances)
		if err != nil {
			// 实例清单不可得时跳过启动对账：空清单会被 reconcile 解读为
			// “全部 VM 已死”而误删 live slot。降级运行，周期 reconcile 补偿。
			slog.Warn("slot startup reconcile skipped: instance inventory unavailable", "error", err)
		} else if err := slotManager.Reconcile(ensureCtx, live); err != nil {
			return fmt.Errorf("slot reconcile: %w", err)
		}
		slog.Info("slot network backend active", "slots", slotManager.Count())
		startSlotReconcileLoop(ctx, set.Instances, slotManager, slotReconcileInterval())
	}

	tracker := health.New()
	// v1.3-A（ADR-0027）：egress 策略执行层（slot 规则 + 透明代理）。
	// slot 后端下启用；proxy 启动失败即整体退出（fail closed：不能报告
	// egress 能力却不具备执行层）。
	var egressMgr *egress.Manager
	var egressFeatureIDs []string
	if slotManager != nil {
		dnsUpstreams := splitNonEmpty(os.Getenv("FIREPAAS_EGRESS_DNS"), ",")
		resolver, rerr := egress.NewResolver(dnsUpstreams, 0)
		if rerr != nil {
			return fmt.Errorf("egress resolver: %w", rerr)
		}
		reserved, rerr := egress.NewReservedChecker(slot.VethRange, cfg.Network.SubnetCIDR)
		if rerr != nil {
			return fmt.Errorf("egress reserved checker: %w", rerr)
		}
		egressProxy := egress.NewProxy(egressPort80, egressPort443, resolver, reserved, nil)
		if err := egressProxy.Start(ctx); err != nil {
			return fmt.Errorf("egress proxy: %w", err)
		}
		egressMgr = egress.NewManager(egressProxy, slotManager)
		egressFeatureIDs = []string{capabilities.EgressCidrV1, capabilities.EgressDomainV1}
		slog.Info("egress policy enforcement active",
			"proxy_port80", egressPort80, "proxy_port443", egressPort443)
	}
	// v1.1（ADR-0017）：探针连接登记 runner——dial 前 bind 固定本地端口并把
	// 四元组登记进 probeflow registry，供 auto-standby conntrack 过滤器精确
	// 剔除探针流量（不清闲判定，同时不影响真实流量的活跃计数）。
	probeReg := probeflow.NewRegistry(0)
	tracker.SetRunner(health.NewRecordingRunner(
		healthcheck.DefaultProbeRunner{HTTPClient: health.ProbeHTTPClient()}, probeReg))
	adapter := machine.New(set.Instances, set.Images, slotManager, tracker)
	adapter.SetEgressManager(egressMgr)
	// v1.3-D（ADR-0029）：注入 hypeman volumes.Manager（LOCAL_RW）。
	adapter.SetVolumes(set.Volumes)
	// v1.4-D（ADR-0030）：启动时清理崩溃遗留的 dataset 导入 spool（导入授权
	// 窗口最大 15 分钟；超龄文件不可能属于在途导入）。
	if removed, err := machine.CleanupStaleDatasetSpool(20 * time.Minute); err != nil {
		slog.Warn("dataset spool cleanup", "error", err)
	} else if removed > 0 {
		slog.Info("removed stale dataset import spool files", "count", removed)
	}
	// v1.3-A（ADR-0027）：重启后按 hypeman tags + slot 持久化规则重建 egress
	// 代理注册并幂等重放内核规则。
	if egressMgr != nil {
		if err := adapter.RebuildEgress(ctx); err != nil {
			return fmt.Errorf("rebuild egress: %w", err)
		}
	}
	// M5.1：镜像解包大小准入（默认 4096MiB；0 = 不限）。
	if v, err := strconv.ParseInt(envOr("FIREPAAS_IMAGE_MAX_UNPACK_MIB", "4096"), 10, 64); err == nil && v >= 0 {
		adapter.SetMaxUnpackMib(v)
	}
	// v1.2-B（ADR-0024）：默认 one-shot 通道（vsock tmpfs + release gate，
	// 值不落盘）。unsafe-persisted-env 已废弃，保留一个版本：明知 secret
	// 会明文持久化到节点 metadata.json，下版本删除。
	injectionMode := envOr("FIREPAAS_SECRET_INJECTION", machine.SecretInjectionOneShot)
	adapter.SetSecretInjection(injectionMode)
	if injectionMode == machine.SecretInjectionUnsafePersistedEnv {
		slog.Warn("FIREPAAS_SECRET_INJECTION=unsafe-persisted-env is DEPRECATED: " +
			"secrets will be persisted in plaintext node metadata; it will be removed next release")
	} else if injectionMode != machine.SecretInjectionOneShot && injectionMode != machine.SecretInjectionOff {
		slog.Warn("unknown FIREPAAS_SECRET_INJECTION; secret-bearing creates will be rejected", "mode", injectionMode)
	}
	// M4.5：standby 实例的首流量同步唤醑（autoresume）。
	if strings.EqualFold(envOr("FIREPAAS_AGENT_AUTORESUME", "true"), "false") {
		adapter.SetAutoResume(false)
		slog.Info("agent autoresume disabled")
	}
	// v1.1（ADR-0017）：auto-standby 空闲检测控制器（conntrack 驱动，默认开；
	// 策略 per-app 默认关闭）。非 Linux/无 runtime 持久化能力时降级关闭。
	wakes, wakeSeconds := agentWakeMetrics(meter)
	adapter.SetWakeObserver(func(machineID string, took time.Duration) {
		wakes.Add(context.Background(), 1,
			otelmetric.WithAttributes(attribute.String("machine_id", machineID)))
		wakeSeconds.Record(context.Background(), took.Seconds(),
			otelmetric.WithAttributes(attribute.String("machine_id", machineID)))
		slog.Info("autoresume wake", "machine_id", machineID, "took", took.Round(time.Millisecond))
	})
	if strings.EqualFold(envOr("FIREPAAS_AGENT_AUTOSTANDBY", "true"), "true") {
		if err := startAutoStandby(ctx, set.Instances, probeReg, meter); err != nil {
			slog.Warn("auto-standby controller disabled", "error", err)
		}
	} else {
		slog.Info("auto-standby controller disabled by env")
	}
	// v1.1（F-2）：per-VM CPU/RSS 指标（Prometheus 直抓，节点 relabel）。
	// slot netns 下 TAP 网络统计尚无正确采集实现，故不把网络维度作为已交付
	// 指标承诺；详见 v1.1 implementation notes 的 DEFERRED 项。
	if meter != nil {
		vmMgr := vm_metrics.NewManager()
		vmMgr.SetInstanceSource(vm_metrics.NewInstanceManagerAdapter(set.Instances))
		vmMgr.SetLogger(slog.Default().With("component", "vm_metrics"))
		if err := vmMgr.InitializeOTel(meter); err != nil {
			slog.Warn("per-VM metrics disabled", "error", err)
		} else {
			slog.Info("per-VM metrics enabled (direct scrape)")
		}
	}
	// P1-1：探针 worker 独立于 ListMachines gRPC 路径执行（预算内串行，
	// 不再共享 gRPC deadline）；controller 每 5s 的 List 只读缓存。
	probeWorker := health.NewWorker(tracker, func(ctx context.Context) ([]instances.Instance, error) {
		return set.Instances.ListInstances(ctx, nil)
	})
	go func() {
		if err := probeWorker.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("health probe worker stopped", "error", err)
		}
	}()
	// 已承诺资源（硬准入输入）：实例 Size / Vcpus 之和（M2.2）；v1.2-E
	//（ADR-0035）增加 OverlaySize 之和（磁盘 requested 维度）。R2（契约 D-1）：
	// 采集带有效性——见 resourceSampler 注释；inventory 失败 → Unavailable
	// fail-closed，容量上报回退 ≤60s last-known-good。
	// 失败钩子每次调用计指标；warn 日志每分钟最多一条（ServiceInfo 每 5s
	// 消费一次采样器，未节流的告警会淹没日志）。
	var counter otelmetric.Int64Counter
	if meter != nil {
		if c, cerr := meter.Int64Counter("firepaas_agent_inventory_errors_total",
			otelmetric.WithDescription("list-instances failures observed by the admission/capacity sampler")); cerr == nil {
			counter = c
		}
	}
	var hookMu sync.Mutex
	var lastWarn time.Time
	inventoryErrorHook := func(err error) {
		if counter != nil {
			counter.Add(context.Background(), 1)
		}
		hookMu.Lock()
		defer hookMu.Unlock()
		if time.Since(lastWarn) >= time.Minute {
			lastWarn = time.Now()
			slog.Warn("resource inventory unavailable; admission may degrade to fail-closed", "error", err)
		}
	}
	sampler := newResourceSampler(func() (resourceSample, error) {
		listCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		listed, err := set.Instances.ListInstances(listCtx, nil)
		if err != nil {
			return resourceSample{}, err
		}
		var out resourceSample
		var totalBytes int64
		var totalDiskBytes int64
		for i := range listed {
			totalBytes += listed[i].Size
			totalDiskBytes += listed[i].OverlaySize
			out.vcpus += listed[i].Vcpus
		}
		if totalBytes > 0 {
			out.memMiB = uint64(totalBytes) / (1024 * 1024)
		}
		if set.Volumes != nil {
			if volumeBytes, volumeErr := set.Volumes.TotalVolumeBytes(listCtx); volumeErr == nil {
				totalDiskBytes += volumeBytes
			}
		}
		if totalDiskBytes > 0 {
			out.diskMiB = uint64(totalDiskBytes) / (1024 * 1024)
		}
		return out, nil
	}, time.Minute, inventoryErrorHook)
	memAllocated := func() uint64 { m, _ := sampler.current(); return m.memMiB }
	vcpuAllocated := func() int { m, _ := sampler.current(); return m.vcpus }
	infoProvider := info.New(
		nodeID,
		serviceVersion,
		serviceCommit,
		nodePool,
		fcVersion,
		cfg.Network.SubnetCIDR,
		cfg.DataDir,
		memAllocated,
		vcpuAllocated,
	)
	// v1.2-E（ADR-0035）：已承诺磁盘上报（节点投影 + 硬准入输入）。
	infoProvider.SetDiskAllocatedFunc(func() uint64 { m, _ := sampler.current(); return m.diskMiB })
	// R2（契约 D-1）：资源采集有效性（无 live 样本且无 ≤60s LKG → false，
	// create 硬准入 fail-closed；其余 RPC 不消费该判定）。
	infoProvider.SetResourcesValidFunc(func() bool { _, ok := sampler.current(); return ok })
	// v1.1（ADR-0018）：镜像缓存 digest 上报（scheduler 镜像亲和输入）。
	infoProvider.SetImageDigestsFunc(func() []string {
		listCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return adapter.CachedImageDigests(listCtx, 512)
	})
	// v1.2-A（ADR-0023）：runtime capability 投影。只报告本 agent 真实可
	// 兑现的能力；未完成 one-shot secret 安全通道前绝不上报
	// secret.oneshot.v1（fail closed）。默认 guest 运维能力（logs/exec/cp）
	// 由 hypeman guest agent + serial log 提供。
	infoProvider.SetCapabilities(agentProtocolVersion, agentFeatureIDs(
		injectionMode,
		egressFeatureIDs,
		set.Volumes != nil,
		adapter.SnapshotScrubAvailable(),
		adapter.ImageQuarantineAvailable(),
		adapter.VolumeQuarantineAvailable(),
	),
		instances.SnapshotCompatibilityKey(instances.StoredMetadata{
			HypervisorType: hypervisor.TypeFirecracker, HypervisorVersion: fcVersion,
			KernelVersion: string(
				set.System.GetDefaultKernelVersion(),
			), Platform: goruntime.GOOS + "/" + goruntime.GOARCH,
		}))

	// M4（ADR-0006 收口）：proxy credential 验证材料（仅 SHA-256 摘要落盘）。
	credsPath := envOr("FIREPAAS_AGENT_CREDS_PATH", filepath.Join(cfg.DataDir, "agent", "credentials.json"))
	creds, err := state.OpenCreds(credsPath)
	if err != nil {
		return err
	}
	// 兼容开关：默认强制 create 携带 execution-bound credential。
	requireCred := strings.ToLower(envOr("FIREPAAS_PROXY_CREDENTIAL_REQUIRED", "true")) != "false"
	// v1.1（ADR-0018）：PullImage（部署预取）磁盘水位守护（已用比例 ≥ 阈值拒绝）。
	diskWatermark := 0.9
	if v, err := strconv.ParseFloat(envOr("FIREPAAS_PREFETCH_DISK_WATERMARK", "0.9"), 64); err == nil && v > 0 &&
		v <= 1 {
		diskWatermark = v
	}
	srv := server.New(adapter, ledger, fences, infoProvider,
		server.WithCreds(creds), server.WithCredentialRequired(requireCred),
		server.WithDiskWatermark(diskWatermark),
		server.WithAdmissionDiskWatermark(envFloat("FIREPAAS_ADMISSION_DISK_WATERMARK", 0.9)),
		server.WithRuntimeLimits(
			envInt("FIREPAAS_RUNTIME_MAX_SESSIONS", 16),
			int64(envInt("FIREPAAS_RUNTIME_MAX_BYTES", 100<<20)),
			envDur("FIREPAAS_RUNTIME_MAX_DURATION", 15*time.Minute),
			envDur("FIREPAAS_RUNTIME_IDLE_TIMEOUT", time.Minute)))

	lis, err := net.Listen("tcp", net.JoinHostPort(bind, port))
	if err != nil {
		return fmt.Errorf("listen %s:%s: %w", bind, port, err)
	}

	var grpcOpts []grpc.ServerOption
	// v1.2-C：Exec/stdin 帧单条可达 8 MiB（fpctl stdin 上限同值）；默认
	// 4 MiB 会静默拒收大 stdin。消息上限与 maxBytes 流量限额语义不同。
	grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(16<<20))
	var proxyTLS *tls.Config
	tlsConf, certMgr, err := agentServerTLS(meter)
	if err != nil {
		return err
	}
	if certMgr != nil {
		defer certMgr.Close()
	}
	proxyHandler := http.Handler(proxy.NewWithVerifier(adapter, creds))
	if tlsConf != nil {
		// mTLS 已保证“持本 CA 证书才能连”；这里进一步按证书 CN 做最小授权：
		// gRPC（5108）只接受控制面身份，proxy（5107）只接受 edge 身份
		// （评审 P1-2，ADR-0006 的 M1 降级形态）。
		grpcAllowed := mtls.SplitAllowlist(os.Getenv("FIREPAAS_AGENT_GRPC_ALLOWED_CLIENTS"), "control-plane")
		proxyAllowed := mtls.SplitAllowlist(os.Getenv("FIREPAAS_AGENT_PROXY_ALLOWED_CLIENTS"), "edge-proxy")
		grpcOpts = append(grpcOpts,
			grpc.Creds(credentials.NewTLS(tlsConf)),
			grpc.ChainUnaryInterceptor(mtls.UnaryServerIdentityInterceptor(grpcAllowed)),
			grpc.ChainStreamInterceptor(mtls.StreamServerIdentityInterceptor(grpcAllowed)),
		)
		proxyTLS = tlsConf
		proxyHandler = mtls.RequireClientIdentity(proxyHandler, proxyAllowed)
		slog.Info("agentd mTLS enabled (hot-reload certs, ADR-0006 degradation)",
			"grpc_clients", grpcAllowed, "proxy_clients", proxyAllowed)
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	pb.RegisterInfoServiceServer(grpcServer, srv)
	pb.RegisterMachineServiceServer(grpcServer, srv)
	pb.RegisterImageServiceServer(grpcServer, srv)    // v1.1（ADR-0018）：部署预取
	pb.RegisterSnapshotServiceServer(grpcServer, srv) // v1.3-B（ADR-0028）
	pb.RegisterVolumeServiceServer(grpcServer, srv)   // v1.3-D（ADR-0029）

	errCh := make(chan error, 2)
	go func() {
		slog.Info("agentd gRPC listening", "port", port, "node_id", nodeID, "node_pool", nodePool)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// 超时口径与 edge 一致（cmd/edge-proxy newEdgeServer）：仅 ReadHeaderTimeout
	//（Slowloris 防护）与 IdleTimeout（回收空闲 keep-alive）；不设
	// WriteTimeout/ReadTimeout——workload 响应体与 WS/SSE 类长流不得被整体
	// 超时切断。
	proxyServer := &http.Server{
		Addr:              net.JoinHostPort(bind, proxyPort),
		Handler:           proxyHandler,
		TLSConfig:         proxyTLS,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second, // 与 cmd/edge-proxy 同值
	}
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
		// R2-5：GracefulStop 带 deadline（FIREPAAS_AGENT_GRACEFUL_STOP_TIMEOUT，
		// 默认 30s）。GracefulStop 会等在途流结束——Exec/logs/cp 流长可达 15
		// 分钟（runtimeLimits），不绑死节点重启/升级窗口；超时后强制 Stop()
		// 切断全部在途 RPC（流的客户端侧按确定性重试语义处理，幂等性由
		// operation ledger/fence 兜底）。proxy 走直接 Close（服务端推送的
		// workload 响应没有优雅排空语义，close 即可）。
		graceful := envDur("FIREPAAS_AGENT_GRACEFUL_STOP_TIMEOUT", 30*time.Second)
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
			slog.Info("grpc graceful stop completed")
		case <-time.After(graceful):
			slog.Warn("grpc graceful stop deadline reached; forcing stop and abandoning in-flight exec/log streams",
				"timeout", graceful)
			grpcServer.Stop()
		}
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

// envDur 解析时长环境变量（非法/非正值回退默认）。
func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// envFloat 解析 (0,1) 区间的浮点环境变量（非法/越界回退默认并告警）。
func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || f >= 1 {
		slog.Warn("invalid env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return f
}

// envInt 解析正整数环境变量（非法/非正值回退默认并告警）。
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		slog.Warn("invalid env value; using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

// liveSlotInstances 从 hypeman 实例清单构建 slot 对账的存活实例视图。
func liveSlotInstances(ctx context.Context, mgr instances.Manager) ([]slot.LiveInstance, error) {
	listed, err := mgr.ListInstances(ctx, nil)
	if err != nil {
		return nil, err
	}
	live := make([]slot.LiveInstance, 0, len(listed))
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
	return live, nil
}

// slotReconcileInterval 解析 FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL
// （默认 5m；<=0 禁用；非法值回退默认并告警）。
func slotReconcileInterval() time.Duration {
	const def = 5 * time.Minute
	v := os.Getenv("FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL; using default",
			"value", v, "default", def, "error", err)
		return def
	}
	return d
}

// startSlotReconcileLoop 周期性执行 slot 对账（回收孤儿 netns、为存活实例
// 补接线）。错误一律降级为日志（M3 真机事故教训：slot 异常绝不能让 agentd
// 退出）；实例清单不可得时跳过本轮——空清单会让 reconcile 误判 VM 已死
// 而误删 live slot。
func startSlotReconcileLoop(ctx context.Context, mgr instances.Manager, m *slot.Manager, interval time.Duration) {
	if interval <= 0 {
		slog.Info("periodic slot reconcile disabled", "interval", interval)
		return
	}
	slog.Info("periodic slot reconcile enabled", "interval", interval)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				liveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				live, err := liveSlotInstances(liveCtx, mgr)
				cancel()
				if err != nil {
					slog.Warn("slot reconcile skipped: instance inventory unavailable", "error", err)
					continue
				}
				reconcileCtx, cancel := context.WithTimeout(ctx, time.Minute)
				err = m.Reconcile(reconcileCtx, live)
				cancel()
				if err != nil {
					slog.Warn("slot reconcile failed (degraded; retried next round)", "error", err)
				}
			}
		}
	}()
}

// envIntDefault 解析整数环境变量（0 合法，非法/负值回退默认；egress 端口用）。
func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// splitNonEmpty 切分逗号分隔列表（去空白、空项）。
func splitNonEmpty(raw, sep string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, sep) {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// agentFeatureIDs 从实际 secret 注入模式与 egress 装配推导安全能力。环境变量
// 只是默认能力的减法 allowlist，不能在 unsafe/off/unknown 模式伪造
// secret.oneshot.v1，也不能在 egress 未装配时伪造 egress 能力。
func agentFeatureIDs(secretMode string, egressIDs []string, optional ...bool) []string {
	available := []string{
		capabilities.GuestExecV1,
		capabilities.GuestCopyV1,
		capabilities.GuestLogsV1,
	}
	if secretMode == machine.SecretInjectionOneShot {
		available = append(available, capabilities.SecretOneShotV1)
	}
	available = append(available, egressIDs...)
	// v1.4-B：本 agent 的 ListSnapshots/ListVolumes 响应携带 complete 标志
	// 与观测 generation/time（inventory 对账输入）。
	available = append(available, capabilities.LocalInventoryV1)
	// The pinned hypeman exposes checksummed memory/filesystem snapshots and
	// reliable guest freeze/thaw through lib/instances.
	available = append(available, capabilities.SnapshotMemoryV1, capabilities.SnapshotFilesystemV1)
	if len(optional) > 0 && optional[0] {
		// v1.4-A：dataset overlay（per-execution CoW）尚未通过 hypeman capability、
		// disk admission、cleanup 与真机 e2e 验收，不得发布
		// volume.dataset_overlay.v1；API 对 overlay attach 明确拒绝。
		available = append(available, capabilities.VolumeLocalRWV1, capabilities.VolumeDatasetROV1)
	}
	if len(optional) > 1 && optional[1] {
		available = append(available, capabilities.SnapshotScrubV1)
	}
	if len(optional) > 2 && optional[2] {
		available = append(available, capabilities.ImageQuarantineV1)
	}
	if len(optional) > 3 && optional[3] {
		available = append(available, capabilities.VolumeDatasetQuarantineV1)
	}
	raw := os.Getenv("FIREPAAS_AGENT_FEATURE_IDS")
	if raw == "" {
		return available
	}
	allowed := make(map[string]bool, len(available))
	for _, id := range available {
		allowed[id] = true
	}
	out := make([]string, 0, len(available))
	seen := make(map[string]bool, len(available))
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if capabilities.Valid(p) && allowed[p] && !seen[p] {
			out = append(out, p)
			seen[p] = true
		}
	}
	return out
}

// agentServerTLS requires mTLS in production. Local plaintext operation is deliberately
// opt-in so an omitted certificate cannot silently expose control/data-plane RPCs.
// 服务端证书经 CertManager 热重载（P0#5 契约 C-1）：文件轮换无需重启；到期时间
// 导出为 firepaas_tls_cert_not_after_seconds 供告警。
func agentServerTLS(meter otelmetric.Meter) (*tls.Config, *mtls.CertManager, error) {
	certFile := os.Getenv("FIREPAAS_AGENT_TLS_CERT")
	keyFile := os.Getenv("FIREPAAS_AGENT_TLS_KEY")
	caFile := os.Getenv("FIREPAAS_AGENT_TLS_CA")
	if certFile == "" && keyFile == "" && caFile == "" {
		if strings.EqualFold(os.Getenv("FIREPAAS_ALLOW_INSECURE_DEV"), "true") {
			slog.Warn("agentd running without TLS because FIREPAAS_ALLOW_INSECURE_DEV=true")
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf(
			"agent mTLS required: set FIREPAAS_AGENT_TLS_CERT/KEY/CA (or FIREPAAS_ALLOW_INSECURE_DEV=true for local development only)",
		)
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil, fmt.Errorf("FIREPAAS_AGENT_TLS_CERT/KEY/CA must be set together")
	}
	var hook func(time.Time)
	if meter != nil {
		if gauge, err := meter.Float64Gauge("firepaas_tls_cert_not_after_seconds"); err == nil {
			hook = func(expiry time.Time) {
				gauge.Record(context.Background(), float64(expiry.Unix()),
					otelmetric.WithAttributes(attribute.String("file", certFile)))
			}
		}
	}
	cm, err := mtls.NewCertManager(certFile, keyFile,
		envDur("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", time.Minute),
		slog.Default().With("component", "mtls"), hook)
	if err != nil {
		return nil, nil, err
	}
	conf, err := mtls.ServerConfigWithManager(cm, caFile)
	if err != nil {
		cm.Close()
		return nil, nil, err
	}
	return conf, cm, nil
}

func hostnameOr(def string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return def
	}
	return h
}

// ---------------------------------------------------------------------------
// v1.1：auto-standby 装配（ADR-0017）与 agent 指标端点（F-2/A）
// ---------------------------------------------------------------------------

// startAutoStandby 装配 hypeman autostandby Controller：
//   - InstanceStore 适配 instances.Manager（含 runtime 持久化能力断言）；
//   - ConnectionSource 用 probeflow registry 过滤探针连接（slot 后端下探针
//     与代理回流同源段，源段忽略会误伤真实流量——改为精确按连接剔除）。
//
// 策略 per-app 默认关闭；controller 对无策略实例零动作。
func startAutoStandby(
	ctx context.Context,
	mgr instances.Manager,
	probeReg *probeflow.Registry,
	meter otelmetric.Meter,
) error {
	store := standby.NewInstanceStore(mgr)
	if store == nil {
		return fmt.Errorf("instance manager lacks auto-standby runtime persistence")
	}
	maxConcurrent := 4
	if v, err := strconv.Atoi(envOr("FIREPAAS_AUTOSTANDBY_MAX_CONCURRENT", "4")); err == nil && v > 0 {
		maxConcurrent = v
	}
	// 快照同步间隔：hypeman 默认 5min——实例 Created→Running 的转变没有
	// lifecycle 事件（create 事件携带 Created 态，eligible 要求 Running），
	// 控制器只能靠快照发现新就绪实例。缩短到 30s：每次同步 = 一次
	// ListInstances + 一次 conntrack dump + 全实例刷新（廉价）。
	syncInterval := 30 * time.Second
	if v, err := time.ParseDuration(envOr("FIREPAAS_AUTOSTANDBY_SYNC_INTERVAL", "30s")); err == nil && v > 0 {
		syncInterval = v
	}
	opts := autostandby.ControllerOptions{
		Log:                   slog.Default().With("controller", "auto_standby"),
		MaxConcurrentStandbys: maxConcurrent,
		SnapshotSyncInterval:  syncInterval,
	}
	if meter != nil {
		opts.Meter = meter
	}
	ctrl := autostandby.NewController(store,
		standby.NewFilteredSource(autostandby.NewConntrackSource(), probeReg), opts)
	go func() {
		if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("auto-standby controller stopped", "error", err)
		}
	}()
	slog.Info("auto-standby controller enabled (conntrack-driven, per-app policy default off)",
		"max_concurrent_standbys", maxConcurrent, "snapshot_sync_interval", syncInterval)
	return nil
}

// mustNoopHistogram 返回 noop 直方图（meter 构造失败的兜底）。
func mustNoopHistogram() otelmetric.Float64Histogram {
	h, _ := noop.Meter{}.Float64Histogram("firepaas_agent_autostandby_wake_seconds")
	return h
}

// agentWakeMetrics 构造 autoresume 唤醒计数/耗时（v1.1，ADR-0017 metrics）。
// meter 为 nil 时退化为 noop meter（接口含未导出方法，不能自造实现）。
func agentWakeMetrics(meter otelmetric.Meter) (otelmetric.Int64Counter, otelmetric.Float64Histogram) {
	if meter == nil {
		meter = noop.Meter{}
	}
	wakes, err := meter.Int64Counter("firepaas_agent_autostandby_wakes_total",
		otelmetric.WithDescription("autoresume wakeups triggered by first traffic"))
	if err != nil {
		var c otelmetric.Int64Counter
		c, _ = noop.Meter{}.Int64Counter("firepaas_agent_autostandby_wakes_total")
		return c, mustNoopHistogram()
	}
	seconds, err := meter.Float64Histogram("firepaas_agent_autostandby_wake_seconds",
		otelmetric.WithDescription("autoresume wake duration (restore + slot re-attach)"))
	if err != nil {
		return wakes, mustNoopHistogram()
	}
	return wakes, seconds
}

// initAgentMetrics 设置 Prometheus exporter + 全局 meter provider，并在
// metricsBind:metricsPort 上暴露 /metrics。port 为空 = 关闭（返回 nil meter）。
// 调用方必须显式选择非 loopback bind；该端点没有 HTTP 鉴权。
func initAgentMetrics(ctx context.Context, metricsPort, metricsBind string) otelmetric.Meter {
	if metricsPort == "" {
		return nil
	}
	promReg := prometheus.NewRegistry()
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(promReg))
	if err != nil {
		slog.Warn("agent metrics disabled: prometheus exporter", "error", err)
		return nil
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(mp)
	go func() {
		<-ctx.Done()
		_ = mp.Shutdown(context.Background())
	}()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
	lis, err := net.Listen("tcp", net.JoinHostPort(metricsBind, metricsPort))
	if err != nil {
		slog.Warn("agent metrics disabled: listen", "bind", metricsBind, "port", metricsPort, "error", err)
		return nil
	}
	go func() {
		slog.Info("agent metrics listening", "bind", metricsBind, "port", metricsPort)
		if err := http.Serve(lis, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("agent metrics serve", "error", err)
		}
	}()
	return mp.Meter("firepaas-agent")
}
