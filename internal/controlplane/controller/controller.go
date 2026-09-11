// Package controller 实现 M1→M2 的控制面收敛循环（ADR-0003/0014）：
//
//	PG operations（desired）→ 调度（过滤+Best-of-K）→ Redis 预约
//	→ agent gRPC → PG observed → Redis route 投影 → 决策表纠正
//
// M2a 起本循环只在持 leader 锁的 API 实例上运行（ADR-0007）；本包不感知
// leader 机制，由 cmd/api 组装。
package controller

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/placement"
	"github.com/zhu327/firepaas/internal/controlplane/reservations"
	"github.com/zhu327/firepaas/internal/controlplane/routepublisher"
	"github.com/zhu327/firepaas/internal/controlplane/secrets"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config 是 controller 运行参数。
type Config struct {
	DefaultAppPort       int
	LegacyAgentProxyAddr string // 节点视图缺失时的兜底（M1 单节点兼容）
	// DNSStaleWindow（G2c，P1 独立评审）：dns:internal 投影 TTL = serve-stale
	// 预算，与 edge FIREPAAS_EDGE_STALE_WINDOW 同源（默认 120s）；0 = 默认。
	DNSStaleWindow                 time.Duration
	OpPollInterval                 time.Duration    // 默认 1s
	SyncInterval                   time.Duration    // 默认 5s
	RebuildInterval                time.Duration    // 预约/投影重建，默认 30s
	NodeStaleAfter                 time.Duration    // 节点 observed 投影过期阈值，默认 3×SyncInterval
	ReservationCompensationTimeout time.Duration    // PG 派发提交失败后的 Redis 释放上限，默认 5s
	ReconcileGrace                 time.Duration    // ACK 丢失判定宽限，默认 30s
	MaxPlacementAttempts           int              // ResourceExhausted 换节点上限，默认 3
	AgentRPCTimeout                time.Duration    // 默认 2m（未缓存镜像 pull 可达 60s）
	CreateRetryBase                time.Duration    // create FAILED 首次重派退避（P1-3），默认 10s
	CreateRetryMax                 time.Duration    // create FAILED 退避封顶，默认 5m
	MaxCreateRetryAttempts         int              // 同 machine 连续 create FAILED 上限，默认 8；0 取默认
	ClaimStaleAfter                time.Duration    // CLAIMED 滞留回收阈值（P1-1），默认 2×AgentRPCTimeout+60s
	NodeMissingThreshold           int              // 节点连续 List 失败次数才摘路由（P3-9），默认 3
	NodeLossRecreateAfter          time.Duration    // 节点持续失联后换代重建，默认 60s
	RolloutTimeout                 time.Duration    // M3 PREPARING 超时→自动回滚（S3），默认 300s
	RolloutDrainGrace              time.Duration    // M3 CUTOVER 后旧代 drain 期限，默认 30s
	Secrets                        *secrets.Manager // M4：信封加密（nil = secret 引用不可用）
	Traffic                        *traffic.Signer  // M4：execution-bound proxy credential（nil = 不下发）
	// FabricMesh（ADR-0040 T4c）：mesh 启用时的身份/ULA 生命周期接线。
	// Enabled=false（默认）时派发跳过分配，legacy 路径零回归。
	FabricMesh FabricMeshConfig
	// FabricGCInterval（W2 P0）：死亡 execution 的 ULA 泄漏兜底巡检周期，
	// 默认 5m。只释放 positive 死亡证据的行（见 sweepFabricULA），fail-closed。
	FabricGCInterval time.Duration
	// PrefetchTopK（v1.1，ADR-0018）：部署预取向 top-K 候选节点异步 PullImage。
	// 默认 3；0 取默认。失败/超时不阻塞 rollout（尽力而为）。
	PrefetchTopK int
	// AutoscaleInterval（ADR-0041）：并发弹性决策节拍，默认 10s。
	AutoscaleInterval time.Duration
	// AutoscaleQuotaFreeze（ADR-0041）：配额/资源拒绝后扩容冻结时长，
	// 默认 10×AutoscaleInterval（=100s），超时自动解冻重探。
	AutoscaleQuotaFreeze time.Duration
	// EvacuateStepTimeout（v1.1，ADR-0021）：drain+evacuate 单个 machine 迁移的
	// 等待上限（新代 READY 且切流）；超时记事件继续下一个。默认 5m。
	EvacuateStepTimeout time.Duration
	// UserEventsRetention（v1.2-F）：租户事件保留期，默认 168h，上限 720h。
	UserEventsRetention time.Duration
	// SchedulerEventsRetention（review 2026-09-10）：调度/对账事件保留期。
	// 默认 168h，上限 720h；scheduler_events 此前无保留期会无限增长。
	SchedulerEventsRetention time.Duration
	// DispatchWorkers（R2 评审 P1）：operation 派发的工作池大小，默认 4。
	// 串行派发会让一个挂死 agent 的 AgentRPCTimeout 拖倍整批（20 ops × 2m）。
	DispatchWorkers int
	// OperationRetention（R2 加固）：终态 operations 的在表保留期，默认 7d。
	// 幂等键重放只保证在保留窗内同一键命中同一操作；窗口外重试按新 op 处理。
	OperationRetention time.Duration
	// GC（v1.2-F）：引用感知镜像 GC；零值 = DefaultGCConfig()（dry-run）。
	GC    GCConfig
	Scrub ScrubConfig
}

// FabricMeshConfig 是 mesh 身份/地址分配的控制面开关（ADR-0040 T4c）。
type FabricMeshConfig struct {
	// Enabled 为 true 时 create 派发为新 execution 分配稳定身份与 ULA
	//（PG desired；fabric reconciler 随快照下发）。false = 跳过分配。
	Enabled bool
	// CellPrefix 是本 cell /40 ULA（与 fabric reconciler 同值，默认 fd7a:9a55::/40）。
	CellPrefix string
}

// fabricTrustDomain 是 workload 稳定身份的信任域（ADR-0040 §6）。
const fabricTrustDomain = "firepaas.local"

// fabricDefaultService 是无 services 声明时的主 service 名，与
// store.FabricIdentities 的 COALESCE(services->0->>'name','default')
// 同口径（有声明时取 deployments.services 首条 Name，原样保留空串）。
const fabricDefaultService = "default"

// Controller 执行 reconcile。
type Controller struct {
	store     *store.Store
	cat       *catalog.Catalog
	nodes     *nodemanager.Manager
	resv      *reservations.Manager
	placer    *scheduler.Placer
	placement *placement.Service
	metrics   *metrics.Registry
	routes    *routepublisher.Publisher
	cfg       Config

	// nodeListFailures 记录节点连续 List 失败次数（P3-9：单次抖动不摘路由）。
	nodeListFailures map[string]int

	// prefetchedRollouts（v1.1，ADR-0018）：本轮 leader 任期内已下发过预取的
	// rollout（尽力而为的去重；leader 切换后重发一次无害——镜像拉取幂等）。
	prefetchedRollouts map[string]bool

	// evacuatedNodes（v1.1，ADR-0021）：已记过“驱离完成”事件的节点，避免
	// 每 5s 重复记事件。驱离进度不持久化（由剩余 machine 数自然推导）。
	evacuatedNodes map[string]bool

	// reportedOrphans（v1.4-B）：已报过 orphan 事件的本地 artifact
	//（node:type:id），避免每周期重复记事件；orphan bytes 指标每周期重算。
	reportedOrphans map[string]bool

	// machineLocks（R2 评审 P1）：派发路径的进程内 per-machine 互斥——
	// 有界并发下同一批（或跨批 claim）的、面向同一台 machine 的 op 必须串行，
	// 否则两个 worker 会并行派发同机的 pause/resume 或 create+delete。
	machineLocksMu sync.Mutex
	machineLocks   map[string]*machineDispatchLock

	// autoscale（ADR-0041）：leader 进程内决策状态（不持久化；
	// leader 切换/重启后重置 = 推迟缩容，保守方向）。
	autoscaleMu sync.Mutex
	autoscale   *autoscaleLoopState

	// userEventsRetention 在 New 里从 cfg 归一（Config 已含注释）。
	userEventsRetention time.Duration
	// schedulerEventsRetention 同上。
	schedulerEventsRetention time.Duration
	// gc（v1.2-F）：引用感知镜像 GC 配置。
	gc    GCConfig
	scrub ScrubConfig
}

// New 构造 Controller。
func New(st *store.Store, cat *catalog.Catalog, nm *nodemanager.Manager,
	resv *reservations.Manager, placer *scheduler.Placer, reg *metrics.Registry, cfg Config,
) *Controller {
	if cfg.OpPollInterval == 0 {
		cfg.OpPollInterval = time.Second
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 5 * time.Second
	}
	if cfg.RebuildInterval == 0 {
		cfg.RebuildInterval = 30 * time.Second
	}
	if cfg.NodeStaleAfter <= 0 {
		cfg.NodeStaleAfter = 3 * cfg.SyncInterval
	}
	if cfg.ReservationCompensationTimeout <= 0 {
		cfg.ReservationCompensationTimeout = 5 * time.Second
	}
	if cfg.ReconcileGrace == 0 {
		cfg.ReconcileGrace = 30 * time.Second
	}
	if cfg.MaxPlacementAttempts == 0 {
		cfg.MaxPlacementAttempts = 3
	}
	if cfg.AgentRPCTimeout == 0 {
		cfg.AgentRPCTimeout = 2 * time.Minute
	}
	if cfg.CreateRetryBase == 0 {
		cfg.CreateRetryBase = 10 * time.Second
	}
	if cfg.CreateRetryMax == 0 {
		cfg.CreateRetryMax = 5 * time.Minute
	}
	if cfg.UserEventsRetention <= 0 {
		cfg.UserEventsRetention = 168 * time.Hour
	}
	if cfg.SchedulerEventsRetention <= 0 {
		cfg.SchedulerEventsRetention = 168 * time.Hour
	}
	if cfg.GC.Mode == "" {
		cfg.GC = DefaultGCConfig()
	}
	if cfg.GC.Interval <= 0 {
		cfg.GC.Interval = 5 * time.Minute
	}
	if cfg.GC.Grace <= 0 {
		cfg.GC.Grace = 10 * time.Minute
	}
	if cfg.Scrub.Interval <= 0 {
		cfg.Scrub = DefaultScrubConfig()
	}
	if cfg.UserEventsRetention > 720*time.Hour {
		cfg.UserEventsRetention = 720 * time.Hour // v1.2-plan §9：最大 30 天
	}
	if cfg.SchedulerEventsRetention > 720*time.Hour {
		cfg.SchedulerEventsRetention = 720 * time.Hour
	}
	if cfg.MaxCreateRetryAttempts == 0 {
		// M5 评审（e2e-m5 实测暴露）：永久性失败镜像会让同 execution 重派
		// 无限循环（InvalidArgument 虽是终态，但 R3 尾部决策只看 create FAILED
		// 的退避窗口）。上限后停手等人工/rollout 干预，事件流可见。
		cfg.MaxCreateRetryAttempts = 8
	}
	if cfg.FabricGCInterval <= 0 {
		cfg.FabricGCInterval = 5 * time.Minute
	}
	if cfg.ClaimStaleAfter == 0 {
		cfg.ClaimStaleAfter = 2*cfg.AgentRPCTimeout + time.Minute
	}
	if cfg.NodeMissingThreshold == 0 {
		cfg.NodeMissingThreshold = 3
	}
	if cfg.NodeLossRecreateAfter == 0 {
		// 与节点故障检测 <60s 的目标配套：超过一个检测窗口即允许无状态
		// 副本换代重建；origin 在此后恢复会因 execution fencing 被回收。
		cfg.NodeLossRecreateAfter = time.Minute
	}
	if cfg.PrefetchTopK == 0 {
		cfg.PrefetchTopK = 3
	}
	if cfg.EvacuateStepTimeout == 0 {
		cfg.EvacuateStepTimeout = 5 * time.Minute
	}
	if cfg.DispatchWorkers <= 0 {
		cfg.DispatchWorkers = 4
	}
	if cfg.OperationRetention <= 0 {
		cfg.OperationRetention = 7 * 24 * time.Hour
	}
	// P1（独立评审）：dns:internal 投影 TTL 与 edge stale 窗口同源接线
	//（FIREPAAS_DNS_STALE_WINDOW，默认 120s；0 = 默认）。
	routesPub := routepublisher.New(st, cat, cfg.DefaultAppPort, cfg.LegacyAgentProxyAddr)
	routesPub.SetDNSStaleWindow(cfg.DNSStaleWindow)
	return &Controller{
		store: st, cat: cat, nodes: nm, resv: resv, placer: placer,
		placement: placement.New(st, nm, resv, placer, reg, cfg.ReservationCompensationTimeout), metrics: reg,
		routes: routesPub, cfg: cfg,
		nodeListFailures:   map[string]int{},
		prefetchedRollouts: map[string]bool{}, evacuatedNodes: map[string]bool{},
		reportedOrphans: map[string]bool{}, machineLocks: map[string]*machineDispatchLock{},
		userEventsRetention: cfg.UserEventsRetention, gc: cfg.GC, scrub: cfg.Scrub,
		schedulerEventsRetention: cfg.SchedulerEventsRetention,
	}
}

// Run 启动全部独立调和环并等待 ctx 结束。
//
// 每个环一个 goroutine、各自节拍（reconcileLoops）：任一环阻塞/失败/panic
// 不再饿死其他环。此前所有环共用一个 select 串行执行，慢环（多节点 List、
// evacuate 等待、保留期清理）会推迟 op 派发与状态机推进。拆环的代价是原先
// 同一 tick 内的先后关系（syncObserved → reconcileApps）变成并发：所有
// reconcile 都是幂等的，观测投影落后一拍由下一拍收敛；共享内存态按环归属
// （nodeListFailures→observed、prefetchedRollouts→rollout、
// evacuatedNodes→evacuation、reportedOrphans→node-resources、
// autoscale 状态→autoscale），machineLocks 自带互斥。
func (c *Controller) Run(ctx context.Context) error {
	if c.nodeListFailures == nil { // 防御：New 已初始化，测试可直接构造
		c.nodeListFailures = map[string]int{}
	}
	if c.prefetchedRollouts == nil {
		c.prefetchedRollouts = map[string]bool{}
	}
	if c.evacuatedNodes == nil {
		c.evacuatedNodes = map[string]bool{}
	}
	if c.reportedOrphans == nil {
		c.reportedOrphans = map[string]bool{}
	}
	if c.machineLocks == nil { // 防御：测试可直接构造
		c.machineLocks = map[string]*machineDispatchLock{}
	}
	// 有界去重表：超限重置（孤儿事件可重复上报，但不会无限增长）。
	if len(c.reportedOrphans) > 4096 {
		c.reportedOrphans = map[string]bool{}
	}

	// P1-1（启动回收）：单写者不变量——刚获得 leader 锁时，任何 CLAIMED
	// 操作都是前任（已死）留下的孤儿；立即回退为 PENDING，收敛窗口从
	// ClaimStaleAfter（分钟级）降到秒级。重复派发由 agent operation ledger
	// 幂等兜底。
	if n, err := c.store.RequeueStaleClaimed(ctx, 0); err != nil {
		slog.Error("startup requeue stale claimed", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_operation_stale_claims_recovered_total", nil, uint64(n))
		slog.Warn("recovered orphaned CLAIMED operations on leader start", "count", n)
	}
	go c.runPrewarmWorker(ctx)

	// M3：app scale + rollout 状态机首轮立即跑一次，再交给独立环。
	if err := c.reconcileApps(ctx); err != nil {
		slog.Error("reconcile apps (startup)", "error", err)
	}

	loops := c.reconcileLoops()
	var wg sync.WaitGroup
	for _, loop := range loops {
		wg.Add(1)
		go func(loop reconcileLoop) {
			defer wg.Done()
			c.runLoop(ctx, loop)
		}(loop)
	}
	slog.Info("control loops started", "loops", len(loops))
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// reconcileLoop 是一个独立调和环：自有节拍与 goroutine。
type reconcileLoop struct {
	name     string
	interval time.Duration
	runOnce  func(context.Context) error
}

// reconcileLoops 返回全部独立调和环。顺序仅用于日志可读性，不表示执行顺序。
func (c *Controller) reconcileLoops() []reconcileLoop {
	return []reconcileLoop{
		{name: "operations", interval: c.cfg.OpPollInterval, runOnce: c.reconcileOperations},
		{name: "observed", interval: c.cfg.SyncInterval, runOnce: c.syncObserved},
		{name: "rollout", interval: c.cfg.SyncInterval, runOnce: c.reconcileApps},
		{name: "evacuation", interval: c.cfg.SyncInterval, runOnce: c.reconcileEvacuations},
		{name: "node-resources", interval: c.cfg.SyncInterval, runOnce: c.reconcileNodeResources},
		{name: "autoscale", interval: c.autoscaleInterval(), runOnce: c.reconcileAutoscale},
		{name: "route-rebuild", interval: c.cfg.RebuildInterval, runOnce: c.rebuildLeases},
		{name: "stale-claims", interval: 30 * time.Second, runOnce: c.reconcileStaleClaims},
		{name: "retention", interval: time.Minute, runOnce: c.reconcileRetention},
		{name: "image-gc", interval: c.gc.Interval, runOnce: func(ctx context.Context) error {
			c.runGC(ctx)
			return nil
		}},
		{name: "snapshot-scrub", interval: c.scrub.Interval, runOnce: func(ctx context.Context) error {
			c.runScrub(ctx)
			return nil
		}},
		{name: "fabric-gc", interval: c.cfg.FabricGCInterval, runOnce: c.sweepFabricULA},
	}
}

func (c *Controller) runLoop(ctx context.Context, loop reconcileLoop) {
	ticker := time.NewTicker(loop.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runLoopOnce(ctx, loop)
		}
	}
}

// runLoopOnce 执行单环一拍并隔离 panic：单环崩溃不拖垮 API 进程与其他环，
// 记录后等待下一拍重试。进程级崩溃恢复仍由 Nomad/systemd 监督兜底。
func (c *Controller) runLoopOnce(ctx context.Context, loop reconcileLoop) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("reconcile loop panicked", "loop", loop.name, "panic", p)
			c.metrics.Inc("firepaas_controller_loop_panics_total", map[string]string{"loop": loop.name}, 1)
		}
	}()
	if err := loop.runOnce(ctx); err != nil {
		slog.Error("reconcile loop failed", "loop", loop.name, "error", err)
	}
}

// reconcileNodeResources 推进 node-local snapshot/volume 投影。同环串行，
// reportedOrphans 去重表由该环独占；单步失败只记日志，不阻塞其余步骤。
func (c *Controller) reconcileNodeResources(ctx context.Context) error {
	if err := c.reconcileSnapshotSchedules(ctx); err != nil {
		slog.Error("reconcile snapshot schedules", "error", err)
	}
	if err := c.reconcileSnapshotRetention(ctx); err != nil {
		slog.Error("reconcile snapshot retention", "error", err)
	}
	c.reconcileSnapshotNodeState(ctx)
	c.reconcileVolumeNodeState(ctx)
	return nil
}

// reconcileStaleClaims 回收滞留 CLAIMED 操作（P1-1）：阈值
// > AgentRPCTimeout+余量，不会误伤在途派发。
func (c *Controller) reconcileStaleClaims(ctx context.Context) error {
	n, err := c.store.RequeueStaleClaimed(ctx, c.cfg.ClaimStaleAfter)
	if err != nil {
		return err
	}
	if n > 0 {
		c.metrics.Inc("firepaas_operation_stale_claims_recovered_total", nil, uint64(n))
		slog.Warn("recovered stale CLAIMED operations", "count", n)
	}
	return nil
}

// reconcileRetention 执行保留期清理与租约回收（secret lease、事件、快照
// 引用、pin、终态 operation）。每步失败只记日志，不阻塞其余步骤。
func (c *Controller) reconcileRetention(ctx context.Context) error {
	// v1.2-B（ADR-0024）：回收 secret delivery lease——过期（ISSUED/
	// CLAIMED/DELIVERED 超时）→ EXPIRED；execution 已换代/machine 已
	// 删除 → REVOKED。不确定结果（EXPIRED 而机器仍活）由既有 R 路径
	// 收敛（gate 未放行 → VM 停机 → restart/recreate 换新 execution）。
	expired, revoked, err := c.store.ReapSecretLeases(ctx)
	if err != nil {
		slog.Error("reap secret leases", "error", err)
	}
	if expired > 0 {
		c.metrics.Inc("firepaas_secret_leases_total", map[string]string{"result": "expired"}, uint64(expired))
	}
	if revoked > 0 {
		c.metrics.Inc("firepaas_secret_leases_total", map[string]string{"result": "revoked"}, uint64(revoked))
	}
	// v1.2-F（v1.2-plan §9）：user_events 保留期（默认 7d，上限 30d）。
	if n, err := c.store.DeleteUserEventsOlderThan(ctx, time.Now().Add(-c.userEventsRetention)); err != nil {
		slog.Error("user events retention", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_user_events_purged_total", nil, uint64(n))
	}
	// review 2026-09-10：调度事件保留期（默认 168h）。
	if n, err := c.store.DeleteSchedulerEventsOlderThan(ctx,
		time.Now().Add(-c.schedulerEventsRetention)); err != nil {
		slog.Error("scheduler events retention", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_scheduler_events_purged_total", nil, uint64(n))
	}
	// v1.4-A（ADR-0028）：回收已终结 fork/restore 操作的 snapshot 引用，
	// 防止崩溃路径遗留的引用永久阻塞快照删除。
	if n, err := c.store.ReleaseTerminalOperationReferences(ctx); err != nil {
		slog.Error("release terminal operation references", "error", err)
	} else if n > 0 {
		slog.Info("released snapshot references of terminal operations", "count", n)
	}
	// v1.4-C：过期 pin 回收（保护查询已按 expires_at 过滤，此处仅收敛表）。
	if n, err := c.store.DeleteExpiredImagePins(ctx); err != nil {
		slog.Error("delete expired image pins", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_image_pins_expired_total", nil, uint64(n))
	}
	// R2 加固：终态 operations 保留窗清理（默认 7d；幂等键重放只保证
	// 在保留窗内命中原操作，见 store.DeleteTerminalOperationsOlderThan）。
	if n, err := c.store.DeleteTerminalOperationsOlderThan(ctx,
		time.Now().Add(-c.cfg.OperationRetention)); err != nil {
		slog.Error("operations retention purge", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_operations_retention_purged_total", nil, uint64(n))
		slog.Info("purged terminal operations past retention window", "count", n)
	}
	return nil
}

func (c *Controller) nodeViews() []nodeView {
	var out []nodeView
	for _, n := range c.nodes.Nodes() {
		v := nodeView{nomadID: n.NomadNodeID, proxy: n.ProxyAddr, status: n.Status, n: n}
		v.agentID = n.NomadNodeID
		if n.Info != nil && n.Info.NodeId != "" {
			v.agentID = n.Info.NodeId
		}
		out = append(out, v)
	}
	return out
}

func (c *Controller) clientForNodeID(agentID string) *agentclient.Client {
	for _, v := range c.nodeViews() {
		if v.agentID == agentID {
			return c.nodes.ClientFor(v.nomadID)
		}
	}
	return nil
}

// schedulerNodes returns the placement module's coherent node snapshot for
// non-committing prefetch selection.
func (c *Controller) schedulerNodes(ctx context.Context) []scheduler.Node {
	nodes, err := c.placement.SchedulerNodes(ctx)
	if err != nil {
		return nil
	}
	return nodes
}

// copyLivenessRank 是重复副本存活度排序（review M1）：rank 高者优先保留。
// 不能直接用 agentStateUsable——它把 STOPPED 也算 usable（用于阻塞换代），
// 会让 STOPPED 的 home 压过 RUNNING 的重复副本。
func copyLivenessRank(m *pb.Machine) int {
	switch m.GetState() {
	case pb.MachineState_RUNNING:
		return 4
	case pb.MachineState_PAUSED, pb.MachineState_RESUMING:
		return 3
	case pb.MachineState_PAUSING, pb.MachineState_PENDING, pb.MachineState_INITIALIZING:
		return 2
	case pb.MachineState_STOPPING, pb.MachineState_STOPPED:
		return 1
	default: // UNSPECIFIED / DELETED / DELETING
		return 0
	}
}

// agentStateUsable 判断 agent 侧的实例状态是否仍算“活着”。
func agentStateUsable(m *pb.Machine) bool {
	switch m.State {
	case pb.MachineState_PENDING, pb.MachineState_INITIALIZING, pb.MachineState_RUNNING,
		pb.MachineState_PAUSING, pb.MachineState_PAUSED, pb.MachineState_RESUMING,
		pb.MachineState_STOPPING, pb.MachineState_STOPPED:
		return true
	default: // UNSPECIFIED（agent 重启后失联）/ DELETED / DELETING
		return false
	}
}

// deleteErrorConverges 判定 delete/reap 的 FailedPrecondition 是否按
// “目标不在此节点”收敛（决策纯函数，表驱动测试）。agent fencing 是确定性的：
// execution mismatch / stale generation 都证明派发节点上没有这个
// (machine, execution) 的账本；op 对该节点无事可做，继续重试只会活锁
// （多节点验收 finding D3：误派发 reap 无限重试并阻塞 evacuate）。
//   - op 目标非机器当前 execution：旧代/外来代清理，收敛；当前代不受影响，
//     其他节点若持副本由按节点 pin 的 orphan reaps 另行清理。
//   - machine 行已 purge（m==nil）：op 是唯一剩余证据，同样收敛。
//
// 不收敛的例外：op 目标正是机器当前 execution（fencing 污染信号，必须
// 重试由 person 介入检查，不能装作删掉了在跑的机器）。
// 该函数不判断 NotFound（幂等收敛，调用点单独处理）。
func deleteErrorConverges(err error, m *store.Machine, opExecution string) bool {
	if status.Code(err) != codes.FailedPrecondition {
		return false
	}
	if m == nil {
		return true
	}
	return m.CurrentExecutionID != "" && m.CurrentExecutionID != opExecution
}

// recreateAction 依据最近一次终态操作判定下一动作：
//   - create SUCCEEDED 未过 grace → wait（正常初始化/近期成功，防误判重建）；
//   - create SUCCEEDED 已过 grace → recreate（R3 ACK 丢失：状态蒸发）；
//   - create FAILED 未过退避 → wait；已过退避 → retry（同 execution 重派）；
//   - delete SUCCEEDED → recreate（R2/dead-instance 清理完成，换新代）；
//   - delete FAILED → none（由 EnqueueDelete 复活语义在 R2/R5 路径重试）；
//   - 其他（非终态/未知）→ none（在途已由 hasPending 拦截，防御）。
func recreateAction(lastKind, lastStatus string, sinceLast, grace, backoff time.Duration) string {
	switch {
	case lastKind == "create" && lastStatus == "SUCCEEDED":
		if sinceLast < grace {
			return actionWait
		}
		return actionRecreate
	case lastKind == "create" && lastStatus == "FAILED":
		if sinceLast < backoff {
			return actionWait
		}
		return actionRetryCreate
	case (lastKind == "delete" || lastKind == "reap") && lastStatus == "SUCCEEDED":
		return actionRecreate
	default:
		return actionNone
	}
}

// restartExitClassAllows 是 ON_FAILURE/ALWAYS 的 exit class 纯函数决策
// （v1.2-D，ADR-0026）：ON_FAILURE 只重启非零退出；ALWAYS 重启一切退出。
func restartExitClassAllows(mode string, exitCode *int32) bool {
	switch mode {
	case "ALWAYS":
		return true
	case "ON_FAILURE":
		return exitCode != nil && *exitCode != 0
	default:
		return false
	}
}

// isQuotaError 判断是否项目配额类业务错误（终态 FAILED，不 requeue）。
func isQuotaError(err error) bool {
	return err == reservations.ErrProjectQuota
}

// isPermanentAgentError 判断 agent 返回的错误是否不可重试：
// 重试不可能改变结果的操作直接标记 FAILED，避免无限 requeue（M1 评审 P2-3）。
func isPermanentAgentError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, // 请求本身不合法
		codes.AlreadyExists,      // 同 operation_id 不同 request hash（幂等冲突）
		codes.FailedPrecondition, // stale generation fencing
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.NotFound:
		return true
	default:
		return false
	}
}

func quotaRejectionKind(err error) string {
	msg := err.Error()
	for _, k := range []string{"vcpu", "mem", "disk", "machine concurrency"} {
		if strings.Contains(msg, k) {
			return k
		}
	}
	return "unknown"
}
