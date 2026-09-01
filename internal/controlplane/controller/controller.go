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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
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
	"google.golang.org/protobuf/encoding/protojson"
)

// Config 是 controller 运行参数。
type Config struct {
	DefaultAppPort                 int
	LegacyAgentProxyAddr           string           // 节点视图缺失时的兜底（M1 单节点兼容）
	OpPollInterval                 time.Duration    // 默认 1s
	SyncInterval                   time.Duration    // 默认 5s
	RebuildInterval                time.Duration    // 预约/投影重建，默认 30s
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
	// PrefetchTopK（v1.1，ADR-0018）：部署预取向 top-K 候选节点异步 PullImage。
	// 默认 3；0 取默认。失败/超时不阻塞 rollout（尽力而为）。
	PrefetchTopK int
	// EvacuateStepTimeout（v1.1，ADR-0021）：drain+evacuate 单个 machine 迁移的
	// 等待上限（新代 READY 且切流）；超时记事件继续下一个。默认 5m。
	EvacuateStepTimeout time.Duration
	// UserEventsRetention（v1.2-F）：租户事件保留期，默认 168h，上限 720h。
	UserEventsRetention time.Duration
	// GC（v1.2-F）：引用感知镜像 GC；零值 = DefaultGCConfig()（dry-run）。
	GC    GCConfig
	Scrub ScrubConfig
}

// Controller 执行 reconcile。
type Controller struct {
	store     *store.Store
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

	// userEventsRetention 在 New 里从 cfg 归一（Config 已含注释）。
	userEventsRetention time.Duration
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
	if cfg.MaxCreateRetryAttempts == 0 {
		// M5 评审（e2e-m5 实测暴露）：永久性失败镜像会让同 execution 重派
		// 无限循环（InvalidArgument 虽是终态，但 R3 尾部决策只看 create FAILED
		// 的退避窗口）。上限后停手等人工/rollout 干预，事件流可见。
		cfg.MaxCreateRetryAttempts = 8
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
	return &Controller{
		store: st, nodes: nm, resv: resv, placer: placer,
		placement: placement.New(st, nm, resv, placer, reg, cfg.ReservationCompensationTimeout), metrics: reg,
		routes: routepublisher.New(st, cat, cfg.DefaultAppPort, cfg.LegacyAgentProxyAddr), cfg: cfg,
		nodeListFailures:   map[string]int{},
		prefetchedRollouts: map[string]bool{}, evacuatedNodes: map[string]bool{},
		reportedOrphans:     map[string]bool{},
		userEventsRetention: cfg.UserEventsRetention, gc: cfg.GC, scrub: cfg.Scrub,
	}
}

// Run 启动三个循环：操作 reconcile、observed 同步/决策表、预约与投影重建。
func (c *Controller) Run(ctx context.Context) error {
	opTicker := time.NewTicker(c.cfg.OpPollInterval)
	syncTicker := time.NewTicker(c.cfg.SyncInterval)
	rebuildTicker := time.NewTicker(c.cfg.RebuildInterval)
	staleTicker := time.NewTicker(30 * time.Second) // P1-1：CLAIMED 租约回收
	leaseTicker := time.NewTicker(60 * time.Second) // v1.2-B：secret lease 回收
	gcTicker := time.NewTicker(c.gc.Interval)       // v1.2-F：镜像 GC 巡检
	scrubTicker := time.NewTicker(c.scrub.Interval)
	defer opTicker.Stop()
	defer syncTicker.Stop()
	defer rebuildTicker.Stop()
	defer staleTicker.Stop()
	defer leaseTicker.Stop()
	defer gcTicker.Stop()
	defer scrubTicker.Stop()

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
	// 有界去重表：超限重置（孤儿事件可重复上报，但不会无限增长）。
	if len(c.reportedOrphans) > 4096 {
		c.reportedOrphans = map[string]bool{}
	}

	// P1-1（启动回收）：单写者不变量——刚获得 leader 锁时，任何 CLAIMED
	// 操作都是前任（已死）留下的孤儿；立即回退为 PENDING，收敛窗口从
	// ClaimStaleAfter（分钟级）降到秒级。重复派发由 agent operation ledger
	// 幂等兑底。
	if n, err := c.store.RequeueStaleClaimed(ctx, 0); err != nil {
		slog.Error("startup requeue stale claimed", "error", err)
	} else if n > 0 {
		c.metrics.Inc("firepaas_operation_stale_claims_recovered_total", nil, uint64(n))
		slog.Warn("recovered orphaned CLAIMED operations on leader start", "count", n)
	}
	go c.runPrewarmWorker(ctx)

	// M3：app scale + rollout 状态机（5s 节奏与 sync 同拍，首轮立即跑一次）。
	if err := c.reconcileApps(ctx); err != nil {
		slog.Error("reconcile apps (startup)", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-opTicker.C:
			if err := c.reconcileOperations(ctx); err != nil {
				slog.Error("reconcile operations", "error", err)
			}
		case <-syncTicker.C:
			if err := c.syncObserved(ctx); err != nil {
				slog.Error("sync observed", "error", err)
			}
			if err := c.reconcileApps(ctx); err != nil {
				slog.Error("reconcile apps", "error", err)
			}
			// v1.1（ADR-0021）：drain+evacuate 驱离编排（幂等，每拍每节点
			// 至多推进一个 machine）。
			if err := c.reconcileEvacuations(ctx); err != nil {
				slog.Error("reconcile evacuations", "error", err)
			}
			// v1.3-B（ADR-0028）：snapshot 调度与保留（幂等）。
			if err := c.reconcileSnapshotSchedules(ctx); err != nil {
				slog.Error("reconcile snapshot schedules", "error", err)
			}
			if err := c.reconcileSnapshotRetention(ctx); err != nil {
				slog.Error("reconcile snapshot retention", "error", err)
			}
			c.reconcileSnapshotNodeState(ctx)
			c.reconcileVolumeNodeState(ctx)
		case <-rebuildTicker.C:
			if err := c.rebuildLeases(ctx); err != nil {
				slog.Error("rebuild leases", "error", err)
			}
		case <-staleTicker.C:
			// P1-1：回收滞留 CLAIMED（crash/leader 切换时错误路径也失败的操作）。
			// 阈值 > AgentRPCTimeout+余量，不会误伤在途派发。
			n, err := c.store.RequeueStaleClaimed(ctx, c.cfg.ClaimStaleAfter)
			if err != nil {
				slog.Error("requeue stale claimed", "error", err)
			} else if n > 0 {
				c.metrics.Inc("firepaas_operation_stale_claims_recovered_total", nil, uint64(n))
				slog.Warn("recovered stale CLAIMED operations", "count", n)
			}
		case <-leaseTicker.C:
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
		case <-gcTicker.C:
			// v1.2-F（v1.2-plan §9）：引用感知镜像 GC（默认 dry-run；错误内部已记日志）。
			c.runGC(ctx)
		case <-scrubTicker.C:
			c.runScrub(ctx)
		}
	}
}

// rolloutHoldsRecreate 判断该机器是否处于“发布持有”状态（S4/S6）：
// CUTOVER 中的旧代机器、ROLLING_BACK 中的新代机器死亡时不重建。
// v1.2-D（ADR-0026 §6 owner 决策表）：active rollout 负责 target replica
// 的生命周期——PREPARING/CUTOVER/ROLLING_BACK 期间，target deployment
// 的机器死亡由 rollout create retry 收敛，不消耗 restart attempts。
func (c *Controller) rolloutHoldsRecreate(ctx context.Context, m store.Machine) bool {
	if m.DeploymentID == "" {
		return false
	}
	rl, err := c.store.ActiveRolloutForApp(ctx, m.AppID)
	if err != nil {
		return true // fail closed: an unknown rollout owner must suppress restart
	}
	if rl == nil {
		return false
	}
	dep, err := c.store.GetDeployment(ctx, m.DeploymentID)
	if err != nil || dep == nil {
		return true
	}
	return rolloutHoldDecision(rl.Status, dep.Generation, rl.FromGeneration, rl.ToGeneration)
}

// rolloutHoldDecision 是 owner 决策表的纯函数（v1.2-D，ADR-0026 §6）：
// active rollout 负责 target replica 的生命周期；target 的死亡走 rollout
// create retry（S2/S3 超时、回滚），不消耗 restart attempts。
func rolloutHoldDecision(status string, depGen, fromGen, toGen int64) bool {
	switch status {
	case "PREPARING":
		return depGen == toGen
	case "CUTOVER":
		return depGen != toGen
	case "ROLLING_BACK":
		return depGen != fromGen
	default:
		return false
	}
}

// rolloutOwnsReplacement identifies the deployment that an active rollout must
// actively keep at desired replica count. This is intentionally distinct from
// rolloutHoldDecision, which also covers generations being drained/removed.
func rolloutOwnsReplacement(status string, depGen, fromGen, toGen int64) bool {
	switch status {
	case "PREPARING", "CUTOVER":
		return depGen == toGen
	case "ROLLING_BACK":
		return depGen == fromGen
	default:
		return false
	}
}

func (c *Controller) rolloutRepairsMachine(ctx context.Context, m store.Machine) bool {
	rl, err := c.store.ActiveRolloutForApp(ctx, m.AppID)
	if err != nil || rl == nil {
		return false
	}
	dep, err := c.store.GetDeployment(ctx, m.DeploymentID)
	return err == nil && dep != nil && rolloutOwnsReplacement(rl.Status, dep.Generation,
		rl.FromGeneration, rl.ToGeneration)
}

// reconcileApps 是 M3 的 app 层对账（scale + rollout），错误只记日志不中断
// 主循环（单个 app 的脏状态不能拖垮 machine reconcile）。
func (c *Controller) reconcileApps(ctx context.Context) error {
	if err := c.reconcileRollouts(ctx); err != nil {
		return err
	}
	return c.reconcileAppScale(ctx)
}

// ---------------------------------------------------------------------------
// 操作 reconcile（PG outbox → agent）
// ---------------------------------------------------------------------------

func (c *Controller) reconcilePrewarmOperations(ctx context.Context) error {
	ops, err := c.store.ClaimPendingPrewarmOperations(ctx, 2)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err := c.processPrewarm(ctx, op); err != nil {
			_ = c.store.RequeueOperation(context.WithoutCancel(ctx), op.ID, err.Error())
		}
	}
	return nil
}

// runPrewarmWorker is a dedicated bounded lane. Registry latency never blocks
// the main reconcile/select goroutine or observed-state/routing updates.
func (c *Controller) runPrewarmWorker(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.OpPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.reconcilePrewarmOperations(ctx); err != nil {
				slog.Error("reconcile prewarm operations", "error", err)
			}
		}
	}
}

func (c *Controller) reconcileOperations(ctx context.Context) error {
	ops, err := c.store.ClaimPendingOperations(ctx, 20)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err := c.processOperation(ctx, op); err != nil {
			c.metrics.Inc("firepaas_operation_requeues_total", nil, 1)
			slog.Error("process operation", "operation_id", op.ID, "machine_id", op.MachineID,
				"kind", op.Kind, "error", err)
			// RequeueOperation 只回退仍在 CLAIMED 的操作；已终态不会被复活。
			// P1-1：错误路径可能发生在 ctx 取消之后（leader 切换），必须用
			// 不受取消影响的连接写回，否则操作永久滞留 CLAIMED，只能靠
			// RequeueStaleClaimed 在 ClaimStaleAfter 后兜底。
			detached := context.WithoutCancel(ctx)
			_ = c.store.RequeueOperation(detached, op.ID, err.Error())
		}
	}
	if len(ops) > 0 {
		return c.buildRoutes(ctx)
	}
	return nil
}

// processLifecycle 执行 pause/resume 操作（M4.5）。
// 成功：把 observed_state 写为 PAUSED/RUNNING 并落账 SUCCEEDED；
// 失败（无快照等 FailedPrecondition）：pause 可安全重试；resume 视为
// 快照不可用 → 将机器转 R3 重建路径（observed 清空 + desired CREATED，
// 生成新 execution 走 cold-start），本 op 终态 FAILED。
func (c *Controller) processLifecycle(ctx context.Context, op store.Operation) error {
	var req pb.MachineOperationRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	m, err := c.store.GetMachine(ctx, op.MachineID)
	if err != nil || m == nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "machine gone")
		return err
	}
	gen := uint64(m.Generation)
	exec := m.CurrentExecutionID

	client := c.nodes.ClientFor(m.NodeID)
	if client == nil {
		return fmt.Errorf("no agent client for machine %s", op.MachineID)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	defer cancel()

	var pbm *pb.Machine
	// agent 侧 opID = 控制面 op.ID（每次 API 调用唯一）：同一 op 重试命中
	// ledger 重放（正确幂等）；不同 pause/resume 调用各自真执行。此前用
	// "op-pause-{machine}-{exec8}" 固定后缀，同 execution 的后续
	// pause/resume 全被 ledger 重放吞掉——VM 从未真正 standby，sync 循环
	// 又把 observed 回写为 RUNNING，e2e 50 循环在第 N 次撞输竞态（真机
	/// 验收发现）。
	if op.Kind == "pause" {
		pbm, err = client.Pause(rpcCtx, op.MachineID, exec, gen, op.ID)
	} else {
		pbm, err = client.Resume(rpcCtx, op.MachineID, exec, gen, op.ID)
	}
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition && op.Kind == "resume" {
			// 快照不可恢复 → 冷启动重建。
			slog.Warn("resume failed; scheduling cold-start recreate",
				"machine_id", op.MachineID, "error", err)
			c.recreateMachine(ctx, *m, true)
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil,
				"snapshot restore failed; cold-start scheduled")
			return nil
		}
		return fmt.Errorf("agent %s: %w", op.Kind, err)
	}

	// 落账：observed_state 写为目标态（pause→PAUSED / resume→RUNNING）。
	// 以 agent 返回的实际状态为准（幂等路径可能已是目标态），但 M4.5
	// 的 agent 契约保证 Pause/Resume 返回即目标态；防御性兼容 PAUSED/
	// RUNNING 之外的值（不写未知状态）。
	want := "PAUSED"
	if op.Kind == "resume" {
		want = "RUNNING"
	}
	switch pbm.GetState() {
	case pb.MachineState_PAUSED, pb.MachineState_RESUMING:
		want = "PAUSED"
	case pb.MachineState_RUNNING:
		want = "RUNNING"
	}
	if err := c.store.UpdateMachineNodeAndObserved(ctx, op.MachineID, m.NodeID,
		m.CurrentExecutionID, want, m.ObservedSlotIP, m.ObservedReadiness); err != nil {
		return err
	}
	result, _ := protojson.Marshal(pbm)
	c.metrics.Inc("firepaas_operations_total",
		map[string]string{"kind": op.Kind, "result": "succeeded"}, 1)
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")
}

func (c *Controller) processOperation(ctx context.Context, op store.Operation) error {
	// M5 评审（e2e 实测暴露）：机器已进入 DELETED 后，在途 create 不再重派。
	// 否则「删 app + 节点排水/无候选」组合下 create 会永远 PENDING↔CLAIMED
	// 自旋（无候选 → requeue → 再 claim），既耗调度周期又污染 PENDING 积压指标。
	if op.Kind == "create" {
		if m, err := c.store.GetMachine(ctx, op.MachineID); err == nil && m != nil &&
			m.DesiredState == "DELETED" {
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "machine deleted; create cancelled")
			_ = c.resv.Release(ctx, op.ID)
			c.metrics.Inc("firepaas_reconcile_actions_total",
				map[string]string{"kind": "cancel_create_for_deleted"}, 1)
			return nil
		}
	}
	switch op.Kind {
	case "create":
		return c.processCreate(ctx, op)
	case "delete":
		return c.processDelete(ctx, op, true)
	case "reap":
		// reconcile 清理（旧代/死亡残留）：成功后不得推进 desired→DELETED。
		return c.processDelete(ctx, op, false)
	case "pause", "resume":
		return c.processLifecycle(ctx, op)
	case "snapshot_create", "snapshot_delete":
		return c.processSnapshot(ctx, op)
	case "fork", "rescue":
		if op.Kind == "fork" {
			return c.processFork(ctx, op)
		}
		return c.processRescue(ctx, op)
	case "volume_create", "dataset_import", "volume_delete", "volume_attach", "volume_detach":
		return c.processVolume(ctx, op)
	case "image_prewarm":
		return c.processPrewarm(ctx, op)
	default:
		err := fmt.Errorf("unknown operation kind %q", op.Kind)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
}

// processCreate：ACK 丢失补账 → 调度 → 预约 → 派发 → 落账。
// hashRequestPayload 计算 op.Request 的 SHA-256（secret 盲：一次性字段不进
// op.Request）。lease 以它绑定具体 create 载荷（ADR-0024 §3）。
func hashRequestPayload(request []byte) string {
	sum := sha256.Sum256(request)
	return hex.EncodeToString(sum[:])
}

func secretLeaseConfirmsCreate(l *store.SecretLease, op store.Operation, now time.Time) bool {
	return l != nil && l.ProjectID == op.ProjectID && l.MachineID == op.MachineID &&
		l.ExecutionID == op.ExecutionID && l.Generation == int64(op.Generation) &&
		l.OperationID == op.ID && l.RequestHash == hashRequestPayload(op.Request) &&
		l.ExpiresAt.After(now) &&
		(l.State == store.SecretLeaseDelivered || l.State == store.SecretLeaseAcked)
}

func (c *Controller) processCreate(ctx context.Context, op store.Operation) error {
	var req pb.CreateMachineRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}

	// R8：ACK 丢失补账。普通 create 可由 execution-bound RUNNING 证明成功；
	// secret create 还必须有同绑定且已 DELIVERED/ACKED 的 lease，不能仅凭 VM
	// 运行态把“不确定是否投递”误判为成功。
	if m, err := c.store.GetMachine(ctx, op.MachineID); err == nil && m != nil &&
		m.CurrentExecutionID == op.ExecutionID && m.ObservedState == "RUNNING" {
		secretCreate := false
		if req.Spec.GetDeploymentId() != "" {
			if dep, depErr := c.store.GetDeployment(ctx, req.Spec.GetDeploymentId()); depErr != nil {
				return fmt.Errorf("load deployment for create recovery: %w", depErr)
			} else if dep == nil {
				return fmt.Errorf("load deployment for create recovery: deployment %s not found", req.Spec.GetDeploymentId())
			} else {
				secretCreate = len(dep.SecretRefs) != 0
			}
		}
		if !secretCreate {
			c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "ack_lost_reconcile"}, 1)
			_ = c.resv.Release(ctx, op.ID)
			return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", nil, "")
		}
		lease, leaseErr := c.store.SecretLeaseForExecution(ctx, op.MachineID, op.ExecutionID)
		if leaseErr == nil && secretLeaseConfirmsCreate(lease, op, time.Now()) {
			c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "ack_lost_reconcile"}, 1)
			_ = c.resv.Release(ctx, op.ID)
			return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", nil, "")
		}
		if leaseErr == nil {
			nodeID := m.NodeID
			if nodeID == "" {
				nodeID = op.DispatchNodeID
			}
			return c.cleanupUncertainSecretCreate(ctx, op, lease, nodeID,
				"running execution has unconfirmed secret delivery")
		}
		return fmt.Errorf("load secret lease for running execution %s: %w", op.ExecutionID, leaseErr)
	}

	// M4（ADR-0006/0010）：一次性字段在派发时现算，不进 op.Request 持久化。
	// - proxy credential：HMAC 确定性派生 → agent 重试的 request hash 天然一致；
	// - secret_refs → secret_env：按 deployment 固化的引用解析明文。
	// 引用缺失/解密失败视为终态失败（不换节点重试）。
	if c.cfg.Secrets != nil && req.Spec.GetDeploymentId() != "" {
		env, serr := store.ResolveDeploymentSecretRefs(ctx, c.store, c.cfg.Secrets,
			op.ProjectID, req.Spec.GetDeploymentId())
		if serr != nil {
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil,
				"resolve secret refs: "+serr.Error())
			return fmt.Errorf("resolve secret refs: %w", serr)
		}
		for k, v := range env {
			if req.SecretEnv == nil {
				req.SecretEnv = map[string]string{}
			}
			req.SecretEnv[k] = v
		}
	}
	// v1.2-B（ADR-0024）：secret 下发必须绑定 delivery lease（每 execution
	// 至多一条，幂等复用；hash 冲突 = 二次签发 → 终态拒绝，需换 execution）。
	var lease *store.SecretLease
	var leaseID string
	if len(req.SecretEnv) != 0 {
		var lerr error
		lease, lerr = c.store.EnsureSecretLease(ctx, op.ProjectID, op.MachineID,
			op.ExecutionID, int64(op.Generation), op.ID, hashRequestPayload(op.Request), 0)
		if lerr != nil {
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "secret lease: "+lerr.Error())
			return fmt.Errorf("secret lease: %w", lerr)
		}
		leaseID = lease.ID
		req.SecretLeaseId = lease.ID
	}
	if c.cfg.Traffic != nil {
		req.ProxyCredential = c.cfg.Traffic.Token(req.MachineId, req.Spec.GetExecutionId())
	}

	excluded := map[string]bool{}
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxPlacementAttempts; attempt++ {
		choice, err := c.placement.Place(ctx, op, &req, excluded)
		if err != nil {
			// 项目配额是业务终态；其余视为本轮失败（requeue 后重试）。
			if isQuotaError(err) {
				c.metrics.Inc("firepaas_reservations_total", map[string]string{"result": "quota_failed"}, 1)
				c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID, store.UserEventQuotaRejected,
					map[string]any{"reason": quotaRejectionKind(err)})
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			}
			lastErr = err
			break
		}

		client := c.nodes.ClientFor(choice.NomadID)
		if client == nil {
			lastErr = fmt.Errorf("no agent client for node %s", choice.NomadID)
			c.recordEvent(ctx, "reservation", op.MachineID, op.ID, choice.NodeID, "client missing", nil)
			_ = c.resv.Release(ctx, op.ID)
			excluded[choice.NodeID] = true
			continue
		}

		c.metrics.Inc("firepaas_placements_total", nil, 1)
		if leaseID != "" {
			// CLAIMED is a durable pre-send fence. It is deliberately not replayable:
			// after a crash or ambiguous result the only legal next RPC is fenced delete.
			if err := c.store.ClaimSecretLease(ctx, lease); err != nil {
				if errors.Is(err, store.ErrSecretLeaseTerminal) {
					return c.cleanupUncertainSecretCreate(ctx, op, lease, choice.NodeID,
						"secret create dispatch was already claimed; refusing redispatch")
				}
				return fmt.Errorf("claim secret lease: %w", err)
			}
		}
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		machine, err := client.Create(rpcCtx, &req)
		cancel()
		if err != nil {
			c.metrics.Inc("firepaas_agent_rpc_errors_total", map[string]string{"kind": "create"}, 1)
			_ = c.resv.Release(ctx, op.ID)
			if leaseID != "" {
				return c.cleanupUncertainSecretCreate(ctx, op, lease, choice.NodeID,
					"secret create result uncertain: "+status.Code(err).String())
			}
			if status.Code(err) == codes.ResourceExhausted {
				// Secret-free creates retain the existing cross-node retry behavior.
				c.recordEvent(ctx, "reservation", op.MachineID, op.ID, choice.NodeID,
					"agent ResourceExhausted, retrying another node", nil)
				excluded[choice.NodeID] = true
				lastErr = err
				continue
			}
			if isPermanentAgentError(err) {
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			}
			return fmt.Errorf("agent create: %w", err) // 暂时性失败，requeue
		}

		if err := c.resv.Commit(ctx, op.ID); err != nil {
			slog.Warn("reservation commit", "operation_id", op.ID, "error", err)
		}
		// v1.2-B（ADR-0024）：agent 返回即投递完成（同步内联）；状态推进到
		// DELIVERED/ACKED，ACKED 的后续确认由 observed sync 兕底。响应未携带
		// 投递状态 = agent 未走 one-shot 通道，拒绝（fail closed）。
		if leaseID != "" {
			switch machine.GetSecretDeliveryState() {
			case pb.SecretDeliveryState_SECRET_DELIVERY_DELIVERED:
				if err := c.store.MarkSecretLeaseDelivered(ctx, lease); err != nil {
					return fmt.Errorf("mark secret lease delivered: %w", err)
				}
			case pb.SecretDeliveryState_SECRET_DELIVERY_ACKED:
				if err := c.store.MarkSecretLeaseAcked(ctx, lease); err != nil {
					return fmt.Errorf("mark secret lease acked: %w", err)
				}
			default:
				return c.cleanupUncertainSecretCreate(ctx, op, lease, choice.NodeID,
					"agent did not confirm secret delivery")
			}
		}
		if err := c.store.UpdateMachineNodeAndObserved(ctx, machine.MachineId, choice.NodeID,
			machine.ExecutionId, machine.State.String(), machine.SlotIp, machine.Readiness.String()); err != nil {
			return err
		}
		result, _ := protojson.Marshal(machine)
		c.metrics.Inc("firepaas_operations_total", map[string]string{"kind": "create", "result": "succeeded"}, 1)
		c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID, store.UserEventMachineCreated,
			map[string]any{"generation": op.Generation, "node_id": choice.NodeID})
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")
	}
	c.metrics.Inc("firepaas_placements_total", map[string]string{"result": "failed"}, 1)
	if lastErr == nil {
		lastErr = fmt.Errorf("placement attempts exhausted")
	}
	return lastErr
}

func (c *Controller) cleanupUncertainSecretCreate(ctx context.Context, op store.Operation,
	lease *store.SecretLease, nodeID, cause string,
) error {
	delID := "op-secret-cleanup-" + op.ID
	del := &pb.DeleteMachineRequest{
		MachineId: op.MachineID, ExecutionId: op.ExecutionID,
		Generation: uint64(op.Generation), OperationId: delID,
	}
	raw, err := protojson.Marshal(del)
	if err != nil {
		return err
	}
	// Recovery may have run placement again before discovering the durable CLAIMED
	// fence. Preserve the node recorded by the original dispatch in that case.
	if op.DispatchNodeID != "" {
		nodeID = op.DispatchNodeID
	} else {
		op.DispatchNodeID = nodeID
	}
	// Use a detached context: leader cancellation after the RPC must not leave the
	// create CLAIMED and therefore eligible for generic stale-claim recovery.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.AgentRPCTimeout)
	defer cancel()
	if err := c.store.MarkSecretCreateUncertainAndEnqueueCleanup(persistCtx, op, lease,
		delID, raw, cause); err != nil {
		return fmt.Errorf("persist uncertain secret create cleanup: %w", err)
	}
	_ = c.resv.Release(persistCtx, op.ID)
	c.recordEvent(persistCtx, "reconcile", op.MachineID, delID, nodeID,
		"secret create uncertain; fenced cleanup enqueued", nil)
	c.metrics.Inc("firepaas_reconcile_actions_total",
		map[string]string{"kind": "secret_create_uncertain_cleanup"}, 1)
	return nil
}

func (c *Controller) processDelete(ctx context.Context, op store.Operation, markDeleted bool) error {
	var del pb.DeleteMachineRequest
	if err := protojson.Unmarshal(op.Request, &del); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}

	m, _ := c.store.GetMachine(ctx, del.MachineId)
	nodeID := ""
	if m != nil {
		nodeID = m.NodeID
	}
	if nodeID == "" {
		nodeID = op.DispatchNodeID
	}
	client := (*agentclient.Client)(nil)
	if nodeID != "" {
		client = c.clientForNodeID(nodeID)
	}
	if client == nil {
		// Secret create 的不确定 execution 不能按“节点失联即已删除”收敛；
		// 必须等 fenced delete 获得确定结果，之后才允许换代并签发新 lease。
		if lease, err := c.store.SecretLeaseForExecution(ctx, del.MachineId, del.ExecutionId); err == nil &&
			lease.State == store.SecretLeaseUncertain {
			return fmt.Errorf("secret cleanup waiting for node %s", nodeID)
		}
		// 普通清理维持既有语义：agent 侧残留由 orphan 决策表在节点恢复后处理。
		c.recordEvent(ctx, "reconcile", del.MachineId, op.ID, nodeID,
			"delete converged without agent (node unreachable)", nil)
	} else {
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		err := client.Delete(rpcCtx, &del)
		cancel()
		if err != nil {
			c.metrics.Inc("firepaas_agent_rpc_errors_total", map[string]string{"kind": "delete"}, 1)
			switch {
			case status.Code(err) == codes.NotFound:
				// agent 侧已不存在（节点数据被清理）：幂等成功收敛。
				slog.Warn("delete target missing at agent; converging as deleted",
					"machine_id", del.MachineId, "execution_id", del.ExecutionId)
			case isPermanentAgentError(err):
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			default:
				return fmt.Errorf("agent delete: %w", err)
			}
		}
	}
	// 只有“删当前 execution”的用户 delete 才推进 desired→DELETED；reap 删的是
	// 旧代/死亡残留，desired 必须保持 CREATED（R2/R5 清理路径，M2 决策表）。
	if markDeleted && m != nil && del.ExecutionId == m.CurrentExecutionID && m.DesiredState != "DELETED" {
		if err := c.store.MarkMachineDeleted(ctx, del.MachineId); err != nil {
			return err
		}
	}
	// The agent has authoritatively deleted this exact execution (or the node is
	// gone under the documented orphan-cleanup path), so its attachment claims
	// can no longer protect or consume volume quota. Scope by execution to avoid
	// an old delete releasing a replacement execution's mounts.
	if _, err := c.store.ReleaseTerminalExecutionAttachments(ctx, del.MachineId, del.ExecutionId); err != nil {
		return err
	}
	c.metrics.Inc("firepaas_operations_total", map[string]string{"kind": "delete", "result": "succeeded"}, 1)
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

// ---------------------------------------------------------------------------
// 放置：scheduler（先过滤后打分）→ Redis 预约
// ---------------------------------------------------------------------------

type nodeView struct {
	agentID string
	nomadID string
	proxy   string
	status  string
	n       nodemanager.Node
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

// requiredFeaturesForDeployment 返回 deployment 的启动必需能力
// （v1.2-A，ADR-0023）。列中已固化的平台推导值优先；legacy 行 fail closed。
// v1.3-A：egress 能力与已固化列合并（不可因已有 secret 列而丢失 egress 硬过滤）。
func requiredFeaturesForDeployment(d *store.Deployment) []string {
	if d == nil {
		return nil
	}
	return placement.RequiredFeatures(d.RequiredFeatures, len(d.SecretRefs) > 0, d.EgressPolicy)
}

// imageDigestOf 提取 image_ref 的 digest 后缀（非 digest-pinned 返回空）。
func imageDigestOf(imageRef string) string { return placement.ImageDigest(imageRef) }

func machineQuotaExceeded(usage, limit int64) bool {
	return placement.MachineQuotaExceeded(usage, limit)
}

// ---------------------------------------------------------------------------
// observed 同步与决策表（R1–R7）
// ---------------------------------------------------------------------------

func (c *Controller) syncObserved(ctx context.Context) error {
	// v1.2-D（ADR-0026）：TTL 到期先摘 route（desired→DELETED，buildRoutes
	// 立即排除），再下发 fenced delete。controller 停机跨过到期点后，恢复
	// 时这里立即处理（绝对 expires_at，不依赖计时器）。
	if err := c.expireMachines(ctx); err != nil {
		slog.Error("expire machines", "error", err)
	}
	views := c.nodeViews()
	seen := map[string]*pb.Machine{}
	agentByMachine := map[string]string{} // machine → agent node id

	for _, v := range views {
		client := c.nodes.ClientFor(v.nomadID)
		if client == nil {
			// M5 诊断：之前静默跳过掩盖了 node client 未建立的问题。
			c.nodeListFailures[v.agentID]++
			if c.nodeListFailures[v.agentID]%5 == 1 {
				slog.Warn("no agent client for node view", "node", v.agentID,
					"nomad_id", v.nomadID, "status", v.status, "consecutive", c.nodeListFailures[v.agentID])
			}
			continue
		}
		listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		machines, err := client.List(listCtx, "")
		cancel()
		if err != nil {
			c.metrics.Inc("firepaas_agent_rpc_errors_total", map[string]string{"kind": "list"}, 1)
			// P3-9：单次失败只可能是瞬时抖动（agent 重启/网络闪断）；连续
			// NodeMissingThreshold 次失败才把节点上的 machine 摘路由，避免
			// backend 随抖动来回抖。真正失联时 nodemanager 会在 20s 内把
			// 节点置 UNKNOWN，R4 路径兑底。
			c.nodeListFailures[v.agentID]++
			if c.nodeListFailures[v.agentID] < c.cfg.NodeMissingThreshold {
				slog.Warn("agent list failed (transient)", "node", v.agentID,
					"consecutive", c.nodeListFailures[v.agentID], "error", err)
				continue
			}
			// 节点持续失联：把该节点上的 machine 保守置 UNKNOWN（摘路由）。
			rows, _ := c.store.ListMachinesOnNode(ctx, v.agentID)
			for _, m := range rows {
				_ = c.store.MarkMachineObservedMissing(ctx, m.ID)
				c.recordEvent(ctx, "reconcile", m.ID, "", v.agentID, "node unreachable, observed UNKNOWN", nil)
			}
			continue
		}
		delete(c.nodeListFailures, v.agentID)
		for _, m := range machines {
			seen[m.MachineId] = m
			agentByMachine[m.MachineId] = v.agentID
			c.processAgentMachine(ctx, m, v)
		}
	}

	pgMachines, err := c.store.ListMachines(ctx, "")
	if err != nil {
		return err
	}
	for _, m := range pgMachines {
		c.processPGMachine(ctx, m, seen, agentByMachine)
	}
	if err := c.buildRoutes(ctx); err != nil {
		return err
	}
	c.publishGauges(ctx, views, pgMachines)
	return nil
}

// KickRouteRebuild 立即执行一次路由重建（P3-13，M5 评审）：显式重投影端点
// 调用它抢占 5s syncTicker，而不只是 wipe 后等待。用后台 context（不占
// leader 循环的取消窗口），重建完成即返回耗时。
func (c *Controller) KickRouteRebuild() (time.Duration, error) {
	start := time.Now()
	ctx := context.Background()
	if err := c.buildRoutes(ctx); err != nil {
		return time.Since(start), err
	}
	return time.Since(start), nil
}

// publishGauges：M5.2/M5.3 观测 gauge 快照（每 sync 周期刷新，供 /metrics + 告警规则）。
func (c *Controller) publishGauges(ctx context.Context, views []nodeView, machines []store.Machine) {
	unhealthy := 0
	for _, v := range views {
		if v.status != "READY" {
			unhealthy++
		}
	}
	c.metrics.Set("firepaas_nodes_unhealthy", nil, uint64(unhealthy))
	c.metrics.Set("firepaas_nodes_total", nil, uint64(len(views)))
	// v1.2-E/F（ADR-0035 / v1.2-plan §9）：节点磁盘 requested/总量 gauge
	//（label=node_id，有界集合；水位与 GC 触发的观测面）。
	for _, v := range views {
		if v.n.Info == nil || v.n.Info.Capacity == nil {
			continue
		}
		labels := map[string]string{"node_id": v.agentID}
		c.metrics.Set("firepaas_node_disk_total_mib", labels, v.n.Info.Capacity.DiskTotalMib)
		if v.n.Info.Usage != nil {
			c.metrics.Set("firepaas_node_disk_allocated_mib", labels, v.n.Info.Usage.DiskAllocatedMib)
			c.metrics.Set("firepaas_node_disk_used_mib", labels, v.n.Info.Usage.DiskUsedMib)
		}
	}

	// P2-8：先清 family 再 Set——机器状态消失后旧 {state=...} 序列不得残留。
	c.metrics.ResetFamily("firepaas_machines_observed")
	byState := map[string]uint64{}
	for _, m := range machines {
		if m.DesiredState == "DELETED" {
			continue
		}
		byState[m.ObservedState]++
	}
	for state, n := range byState {
		c.metrics.Set("firepaas_machines_observed", map[string]string{"state": state}, n)
	}
	if n, err := c.store.CountOperations(ctx, "PENDING"); err == nil {
		c.metrics.Set("firepaas_operations_pending", nil, uint64(n))
	}
}

// processAgentMachine：agent 视角 → PG（orphan/旧 execution/正常 observed）。
func (c *Controller) processAgentMachine(ctx context.Context, m *pb.Machine, v nodeView) {
	pg, err := c.store.GetMachine(ctx, m.MachineId)
	if err != nil || pg == nil {
		// R6：agent 有、PG 无 → orphan delete。
		project := "dev"
		if m.GetSpec() != nil && m.GetSpec().ProjectId != "" {
			project = m.GetSpec().ProjectId
		}
		hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.MachineId)
		if err != nil || hasPending {
			return
		}
		c.enqueueOrphanDelete(ctx, project, m)
		return
	}

	if pg.CurrentExecutionID != m.ExecutionId {
		// R2 由 PG 视角统一处理（见 processPGMachine）：这里只记观测，
		// 不污染当前 observed，也不在此下单（避免与在途 create 竞争）。
		c.recordEvent(ctx, "reconcile", m.MachineId, "", v.agentID,
			"agent holds stale execution", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "stale_execution_seen"}, 1)
		return
	}

	// R1：正常观测。v1.1（ADR-0017）：RUNNING→PAUSED 迁移即 auto-standby
	// 生效事件（agent 侧 conntrack 驱动，无 operation），metrics+审计留痕。
	if pg.ObservedState == "RUNNING" && m.State == pb.MachineState_PAUSED {
		c.metrics.Inc("firepaas_machine_standby_total", nil, 1)
		c.recordEvent(ctx, "autostandby", m.MachineId, "", v.agentID,
			"machine entered standby (idle); readiness frozen, wake on next request", nil)
	}
	_ = c.store.UpdateMachineObserved(ctx, m.MachineId, m.ExecutionId,
		m.State.String(), m.SlotIp, m.Readiness.String())

	// v1.3-A（ADR-0027）：egress 拒绝摘要入 PG（counter 语义；agent 上报
	// 当前 execution 的聚合计数）。
	if ea := m.GetEgressAudit(); ea != nil && ea.GetPolicyGeneration() > 0 {
		if err := c.recordEgressAudit(ctx, pg, m, ea); err != nil {
			slog.Warn("record egress audit", "machine_id", m.MachineId, "error", err)
		}
	}

	// v1.2-B（ADR-0024）：观察到 entrypoint 启动（guest 已消费 secret）→
	// lease ACKED。只查带 ACKED 报告的机器（非 secret 机器为 NONE，跳过）。
	if m.GetSecretDeliveryState() == pb.SecretDeliveryState_SECRET_DELIVERY_ACKED {
		if lease, err := c.store.SecretLeaseForExecution(ctx, m.MachineId, m.ExecutionId); err == nil {
			if lease.State != store.SecretLeaseAcked {
				if err := c.store.MarkSecretLeaseAcked(ctx, lease); err != nil {
					slog.Warn("ack secret lease", "lease", lease.ID, "error", err)
				} else {
					// v1.2-F：secret 交付事件（不含 secret 键名/值，只含状态）。
					c.userEvent(ctx, lease.ProjectID, pg.AppID, m.MachineId, store.UserEventSecretDelivered,
						map[string]any{"generation": pg.Generation})
				}
			}
		}
	}

	// v1.2-D（ADR-0026）：restart stable window 从新 execution READY 开始；
	// 连续 READY 满窗口后清零 attempts（pause 不重置）。
	if pg.RestartAttempts > 0 && (m.Readiness == pb.MachineReadiness_READY ||
		m.Readiness == pb.MachineReadiness_UNCONFIGURED) {
		now := time.Now()
		if pg.RestartStableSince == nil {
			if err := c.store.SetRestartStableSince(ctx, m.MachineId, &now); err != nil {
				slog.Error("set restart stable since", "machine_id", m.MachineId, "error", err)
			}
		} else if window := time.Duration(pg.RestartStableWindowSeconds) * time.Second; window > 0 &&
			now.Sub(*pg.RestartStableSince) >= window {
			if err := c.store.ResetRestartAttempts(ctx, m.MachineId); err != nil {
				slog.Error("reset restart attempts", "machine_id", m.MachineId, "error", err)
			} else {
				c.recordEvent(ctx, "restart", m.MachineId, "", v.agentID,
					"stable window reached, restart attempts reset", nil)
				c.metrics.Inc("firepaas_reconcile_actions_total",
					map[string]string{"kind": "restart_attempts_reset"}, 1)
			}
		}
	} else if pg.RestartAttempts > 0 && pg.RestartStableSince != nil &&
		m.State != pb.MachineState_PAUSED {
		// Any non-ready observation interrupts continuity. PAUSED deliberately
		// freezes the READY window rather than resetting it.
		if err := c.store.SetRestartStableSince(ctx, m.MachineId, nil); err != nil {
			slog.Error("reset interrupted restart stable window", "machine_id", m.MachineId, "error", err)
		}
	}
}

// processPGMachine：PG 视角 → agent（ACK 丢失/节点失联/desired 删除）。
func (c *Controller) processPGMachine(ctx context.Context, m store.Machine,
	seen map[string]*pb.Machine, agentByMachine map[string]string,
) {
	agent, hasAgent := seen[m.ID]
	nodeID := m.NodeID
	if nodeID == "" && hasAgent {
		nodeID = agentByMachine[m.ID]
	}

	if m.DesiredState == "DELETED" {
		if !hasAgent {
			return // 已收敛；route 由 buildRoutes 清理
		}
		// R5：desired 已删除但 agent 残留 → 补 delete 操作。
		hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
		if err != nil || hasPending {
			return
		}
		exec := m.CurrentExecutionID
		if exec == "" {
			exec = agent.ExecutionId
		}
		c.recordEvent(ctx, "reconcile", m.ID, "", nodeID, "desired DELETED but agent has machine", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "desired_deleted"}, 1)
		_ = c.enqueueDelete(ctx, m.ID, exec, m.Generation,
			"op-reap-"+m.ID+"-"+exec, nodeID, "desired deleted")
		return
	}

	if m.DesiredState != "CREATED" && m.DesiredState != "RUNNING" {
		return
	}

	// R2：agent 持有旧 execution → 先清理旧代（必要时作废在途 create），
	// 待 delete 完成后下一轮再重建。
	if hasAgent && agent.ExecutionId != m.CurrentExecutionID {
		c.supersedePendingCreateAndReap(ctx, m, agent, nodeID)
		return
	}

	// 同 execution 但本地实例已死（agentd 重启带走 VM 的已知行为）：
	// 先删掉同代残留，delete 完成后下一轮 R3 重建。若已有在途 create
	// （它必然撞“实例名已存在”），先作废它，否则 reap 永远排不上。
	if hasAgent && agent.ExecutionId == m.CurrentExecutionID && !agentStateUsable(agent) {
		pending, err := c.store.PendingOperationForMachine(ctx, m.ID)
		if err != nil {
			return
		}
		if pending != nil {
			_ = c.store.CompleteOperation(ctx, pending.ID, "FAILED", nil,
				"superseded: dead instance of current execution, reap first")
			_ = c.resv.Release(ctx, pending.ID)
			c.recordEvent(ctx, "reconcile", m.ID, pending.ID, nodeID,
				"superseded pending op; dead instance cleanup", nil)
			c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "supersede_pending_dead"}, 1)
			return
		}
		c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
			"agent holds dead instance of current execution, reap first", nil)
		_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
			"op-reap-"+m.ID+"-"+m.CurrentExecutionID, nodeID, "dead instance")
		return
	}
	if hasAgent {
		// v1.2-D（ADR-0026）：agent 观测到 STOPPED = 本 execution 已退出。
		// 只有符合 restart policy 的“意外退出”才换代重启；TTL/manual delete
		// 的 machine 到不了这里（desired 已 DELETED 走 R5）。
		if agent.ExecutionId == m.CurrentExecutionID && agent.State == pb.MachineState_STOPPED {
			if c.rolloutRepairsMachine(ctx, m) {
				// The rollout owner must do real repair, not merely suppress restart.
				// Reap the stopped execution first; after it disappears the rollout
				// owner below creates the replacement without colliding at the agent.
				hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
				if err == nil && !hasPending {
					_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
						"op-rollout-reap-"+m.ID+"-"+m.CurrentExecutionID,
						nodeID, "rollout target stopped; reap before replacement")
				}
				return
			}
			if c.rolloutHoldsRecreate(ctx, m) {
				return // draining/removal generation: rollout intentionally does not repair
			}
			if c.maybeRestartMachine(ctx, m, agent) {
				return
			}
		}
		return // 当前 execution 存活：R1 已处理 observed
	}

	// R4：节点失联时立即摘路由；持续超过有界窗口才换代重建。旧节点恢复
	// 后会因 execution 不匹配走 R2 清理，避免无限期 hold 影响可用性。
	if nodeID != "" {
		v := c.viewForAgent(nodeID)
		if v == nil || v.status != "HEALTHY" {
			_ = c.store.MarkMachineObservedMissing(ctx, m.ID)
			hasLocalVolume, volumeErr := c.store.MachineHasLocalRWAttachment(ctx, m.ID)
			if volumeErr != nil || hasLocalVolume {
				c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
					"node unhealthy with LOCAL_RW volume; recreate blocked", nil)
				return
			}
			if m.LastObservedAt != nil && time.Since(*m.LastObservedAt) >= c.cfg.NodeLossRecreateAfter {
				hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
				if err == nil && !hasPending && !c.rolloutHoldsRecreate(ctx, m) {
					c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
						"node loss timeout elapsed, recreate on a new execution", nil)
					c.recreateMachine(ctx, m, true)
					return
				}
			}
			c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
				"node unhealthy, holding recreate within node-loss window", nil)
			return
		}
	}

	// 有在途操作：等它收敛（终态后再判下一动作）。
	hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
	if err != nil || hasPending {
		return
	}

	// S4/S6（ADR-0015）：发布 CUTOVER/回滚期间，非目标代的机器死亡不重建——
	// drain/rollback 会按 ordinal 回收它；重建只会制造马上要删的浪费。
	if c.rolloutRepairsMachine(ctx, m) {
		c.recreateMachine(ctx, m, true)
		return
	}
	if c.rolloutHoldsRecreate(ctx, m) {
		c.recordEvent(ctx, "rollout", m.ID, "", nodeID,
			"non-target generation machine missing; drain/rollback owns lifecycle", nil)
		return
	}

	// R3 尾部决策（P1-3）：只有 ACK 丢失（create 已成功、agent 却没有）
	// 或清理完成（reap delete SUCCEEDED）才换代重建；create FAILED 走
	// 同 execution 的退避重派，不推动 generation，消除无限换代循环。
	last, err := c.store.GetLatestOperationForMachine(ctx, m.ID)
	if err != nil || last == nil {
		return // 无操作历史：等首次派发（Ensure 已建 op，不在此下单）
	}
	attempts := 0
	if last.Kind == "create" && last.Status == "FAILED" {
		if n, err := c.store.FailedCreateAttempts(ctx, m.ID); err == nil {
			attempts = n
		}
	}
	// 重试上限（M5 评审）：同 machine 连续 create FAILED 达上限后停止重派，
	// 记事件等人工/rollout 干预；否则坏镜像 → 永久 5 分钟节奏的无限循环。
	if last.Kind == "create" && last.Status == "FAILED" && attempts >= c.cfg.MaxCreateRetryAttempts {
		slog.Error("create retry budget exhausted; giving up until operator/rollout intervention",
			"machine_id", m.ID, "attempts", attempts)
		c.recordEvent(ctx, "reconcile", m.ID, last.ID, m.NodeID,
			"create retry budget exhausted", []byte(`{"attempts":`+fmt.Sprint(attempts)+`}`))
		c.metrics.Inc("firepaas_reconcile_actions_total",
			map[string]string{"kind": "create_retry_exhausted"}, 1)
		return
	}
	action := recreateAction(last.Kind, last.Status, time.Since(last.UpdatedAt),
		c.cfg.ReconcileGrace, c.createRetryDelay(attempts))
	if action == actionRetryCreate {
		// A lease row proves this execution carried secrets. CLAIMED/UNCERTAIN are
		// especially important after leader recovery: never route them back to Create.
		if _, err := c.store.SecretLeaseForExecution(ctx, m.ID, m.CurrentExecutionID); err == nil {
			action = actionRecreate
		}
	}
	switch action {
	case actionRecreate:
		c.recreateMachine(ctx, m, true)
	case actionRetryCreate:
		c.recreateMachine(ctx, m, false)
	}
}

// supersedePendingCreateAndReap：agent 有旧 execution 时，作废仍在途的
// 新代 create（否则它永远抢占“pending 操作”名额，R2 无法下单），再入队
// 旧代 delete；delete 完成后下一轮 sync 走 R3 重建。
func (c *Controller) supersedePendingCreateAndReap(ctx context.Context, m store.Machine,
	agent *pb.Machine, nodeID string,
) {
	pending, err := c.store.PendingOperationForMachine(ctx, m.ID)
	if err != nil {
		return
	}
	if pending != nil {
		// 只作废在途 create（它指向的新代永远无法落地）；在途 delete 是
		// 清理未身的动作，误杀会制造 FAILED→复活→再误杀的乒乓（P1-2）。
		if pending.Kind != "create" {
			return
		}
		_ = c.store.CompleteOperation(ctx, pending.ID, "FAILED", nil,
			"superseded: agent holds stale execution, reap first")
		_ = c.resv.Release(ctx, pending.ID)
		c.recordEvent(ctx, "reconcile", m.ID, pending.ID, nodeID,
			"superseded pending create; stale execution cleanup", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "supersede_pending"}, 1)
		return // 本轮只作废；下一轮再入队 delete（避免与 op 循环竞争）
	}
	c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
		"agent holds stale execution, enqueue delete", nil)
	c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "stale_execution"}, 1)
	_ = c.enqueueDelete(ctx, m.ID, agent.ExecutionId, m.Generation,
		"op-orphan-"+m.ID+"-"+agent.ExecutionId, nodeID, "stale execution")
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

// recreate 尾部动作常量（P1-3，决策纯函数便于表驱动测试）。
const (
	actionNone        = "none"
	actionWait        = "wait"
	actionRecreate    = "recreate"     // 换代重建：新 execution + generation+1
	actionRetryCreate = "retry_create" // 同 execution 重派：不推动 generation
)

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

// createRetryDelay 是 create FAILED 的指数退避：base·2^(n-1)，封顶 max。
// n=1 → base（首次重派快速收敛，不影Ⅱ 2 分钟验收）；封顶后有界刷库。
func (c *Controller) createRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	base, maxDelay := c.cfg.CreateRetryBase, c.cfg.CreateRetryMax
	if base <= 0 || maxDelay <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempts; i++ {
		if d > maxDelay/2 { // 再翻倍必然超限：封顶
			return maxDelay
		}
		d *= 2
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}

// recreateMachine 按模式重建/重派（P1-3）：
//   - bump=true（ACK 丢失/清理完成）：新 execution + generation+1，
//     opID 派生自新 execution（原有语义）；
//   - bump=false（create FAILED 重派）：复用当前 execution/generation
//     （该 execution 从未在 agent 成功落地，fence 水位不动），opID 带尝试
//     序号；Ensure 的 GREATEST 兒底保证 generation 单调不回退。
func (c *Controller) recreateMachine(ctx context.Context, m store.Machine, bump bool) {
	if attached, err := c.store.MachineHasLocalRWAttachment(ctx, m.ID); err != nil || attached {
		c.recordEvent(ctx, "reconcile", m.ID, "", m.NodeID,
			"recreate blocked by LOCAL_RW attachment", nil)
		return
	}
	// v1.2-B（ADR-0024 §6）：当前 execution 的 lease 已终态（EXPIRED/REVOKED/
	// ACKED）时，同 execution 重派注定失败（lease 不可重签）。create FAILED
	// 的重试路径必须强制换代：销毁旧 execution、以新 execution 重签 lease。
	if !bump && m.CurrentExecutionID != "" {
		if lease, err := c.store.SecretLeaseForExecution(ctx, m.ID, m.CurrentExecutionID); err == nil {
			switch lease.State {
			case store.SecretLeaseExpired, store.SecretLeaseRevoked, store.SecretLeaseAcked:
				bump = true
				c.recordEvent(ctx, "restart", m.ID, "", m.NodeID,
					"secret lease "+string(lease.State)+": recreate forces new execution (ADR-0024)", nil)
			}
		}
	}
	var exec string
	var gen int64
	var opID string
	if bump {
		exec = "exec-" + uuid.NewString()
		gen = m.Generation + 1
		opID = "op-" + exec
	} else {
		if m.CurrentExecutionID == "" {
			// 防御：无 execution 可复用时退回换代路径。
			c.recreateMachine(ctx, m, true)
			return
		}
		attempts, err := c.store.FailedCreateAttempts(ctx, m.ID)
		if err != nil {
			slog.Error("failed create attempts", "machine_id", m.ID, "error", err)
			return
		}
		exec = m.CurrentExecutionID
		gen = m.Generation
		// opID 必须全局唯一：仅用 (exec, attempts) 会在“上次重试已 SUCCEEDED、
		// 本轮又出现 FAILED”时与历史 op 撞幂等键（request hash 不同 → 永久冲突
		// 循环，真机验收发现的死循环）。uuid 后缀保证永不复用。
		opID = fmt.Sprintf("op-retry-%s-%d-%s", exec, attempts+1, uuid.NewString()[:8])
	}
	req := &pb.CreateMachineRequest{
		MachineId:   m.ID,
		Generation:  uint64(gen),
		OperationId: opID,
		Spec: &pb.MachineSpec{
			ProjectId:      "", // 由 Ensure 的 project 参数统一（见下）
			AppId:          m.AppID,
			DeploymentId:   m.DeploymentID,
			ExecutionId:    exec,
			ReplicaOrdinal: uint32(m.ReplicaOrdinal),
			Hostname:       m.Hostname,
			ImageRef:       m.ImageRef,
			Vcpu:           uint64(m.RequestedVCPU),
			MemMib:         uint64(m.RequestedMemMIB),
			Env:            m.Env,
			Network:        &pb.NetworkSpec{IngressPort: uint64(m.IngressPort)},
		},
	}
	// P2-6：还原放置约束。调度器从请求的 spec.placement 读取
	// node_pool/labels/反亲和；不还原则重建副本的反亲和全部失效，
	// 多副本 app 节点故障重建后可能全落同节点（违反 ADR-0009）。
	if len(m.Placement) > 0 {
		var pl pb.PlacementConstraints
		if err := protojson.Unmarshal(m.Placement, &pl); err != nil {
			slog.Warn("unmarshal stored placement, dropping constraints",
				"machine_id", m.ID, "error", err)
		} else {
			req.Spec.Placement = &pl
		}
	}
	// v1.1（ADR-0017/0022）：还原 deployment 固化的 auto_standby/services
	//（R3/evacuate 重建与首次 create 共用同一请求体派生路径，幂等链路一致）。
	if dep, derr := c.store.GetDeployment(ctx, m.DeploymentID); derr == nil && dep != nil {
		applyDeploymentSpecExtras(dep, req.Spec)
	}
	project, err := c.store.ProjectForApp(ctx, m.AppID)
	if err != nil || project == "" {
		project = "dev"
	}
	req.Spec.ProjectId = project

	raw, err := protojson.Marshal(req)
	if err != nil {
		slog.Error("marshal recreate request", "machine_id", m.ID, "error", err)
		return
	}
	_, err = c.store.EnsureAppAndEnqueueCreate(ctx, project, m.AppID, m.Hostname, m.ImageRef,
		m.RequestedVCPU, m.RequestedMemMIB,
		int64(agentv1.EffectiveDiskMib(req.Spec.GetDiskMib())), m.IngressPort,
		m.ID, m.DeploymentID, exec, opID, gen, m.ReplicaOrdinal, raw, []byte(m.Placement))
	if err != nil {
		slog.Error("enqueue recreate", "machine_id", m.ID, "error", err)
		return
	}
	if bump {
		c.recordEvent(ctx, "reconcile", m.ID, opID, m.NodeID, "ack lost, recreate with new execution", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "ack_lost_recreate"}, 1)
	} else {
		c.recordEvent(ctx, "reconcile", m.ID, opID, m.NodeID,
			"create failed, retry same execution with backoff", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "create_retry"}, 1)
	}
	slog.Info("reconcile recreate", "machine_id", m.ID, "execution_id", exec,
		"operation_id", opID, "generation", gen, "bump_generation", bump)
}

// enqueueOrphanDelete / enqueueDelete 补 delete 操作。
func (c *Controller) enqueueOrphanDelete(ctx context.Context, project string, m *pb.Machine) {
	// R6 的 fence 安全 generation：优先用 agent 观测值（mapMachine 从
	// instance tag 回读）；缺失时回退 1（agent 无 fence 记录的 machine
	// 任意 generation 均放行）。旧代码固定 1，对已换代（gen≥2）的残留
	// 必然 FailedPrecondition → FAILED → 永远无法清理（P1-2）。
	gen := m.GetGeneration()
	if gen == 0 {
		gen = 1
	}
	req := &pb.DeleteMachineRequest{
		MachineId:   m.MachineId,
		ExecutionId: m.ExecutionId,
		Generation:  gen,
		OperationId: "op-orphan-" + m.MachineId + "-" + m.ExecutionId,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return
	}
	_, err = c.store.EnqueueReapDelete(ctx, project, m.MachineId, m.ExecutionId,
		req.OperationId, int64(req.Generation), raw)
	if err != nil {
		slog.Error("enqueue orphan delete", "machine_id", m.MachineId, "error", err)
		return
	}
	c.recordEvent(ctx, "reconcile", m.MachineId, req.OperationId, "",
		"orphan at agent, enqueue delete", nil)
	c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "orphan_delete"}, 1)
}

func (c *Controller) enqueueDelete(
	ctx context.Context,
	machineID, executionID string,
	generation int64,
	opID, nodeID, reason string,
) error {
	req := &pb.DeleteMachineRequest{
		MachineId:   machineID,
		ExecutionId: executionID,
		Generation:  uint64(generation),
		OperationId: opID,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	project := "dev"
	if pg, err := c.store.GetMachine(ctx, machineID); err == nil && pg != nil {
		if p, err := c.store.ProjectForApp(ctx, pg.AppID); err == nil && p != "" {
			project = p
		}
	}
	op, err := c.store.EnqueueReapDelete(ctx, project, machineID, executionID, opID, generation, raw)
	if err != nil {
		return err
	}
	if nodeID != "" {
		// delete 派发走 dispatch_node_id（machine 行可能还没有 node_id）。
		_ = c.store.UpdateOperationDispatchNode(ctx, op.ID, nodeID)
	}
	c.recordEvent(ctx, "reconcile", machineID, opID, nodeID, reason, nil)
	return nil
}

func (c *Controller) viewForAgent(agentID string) *nodeView {
	for _, v := range c.nodeViews() {
		if v.agentID == agentID {
			return &v
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// v1.2-D（ADR-0026）：TTL 到期回收与 restart 决策
// ---------------------------------------------------------------------------

// expireMachines 处理到期 machine：先摘 route（desired→DELETED，buildRoutes
// 排除），再下发 fenced delete。幂等：opID 确定性派生，已存在则跳过。
func (c *Controller) expireMachines(ctx context.Context) error {
	expired, err := c.store.ListExpiredMachines(ctx, time.Now())
	if err != nil {
		return err
	}
	for _, m := range expired {
		if m.DesiredState == "DELETED" {
			continue
		}
		detached, err := c.store.ExpiredRouteDetached(ctx, m.ID, m.CurrentExecutionID)
		if err != nil {
			slog.Error("check expired route detach", "machine_id", m.ID, "error", err)
			continue
		}
		if !detached {
			// First pass only establishes durable intent. buildRoutes later in this
			// sync removes the PG/Redis projection; delete is forbidden this round.
			if err := c.store.MarkExpiredRouteDetached(ctx, m.ID); err != nil {
				slog.Error("mark expired route detached", "machine_id", m.ID, "error", err)
			}
			continue
		}
		if err := c.store.MarkMachineDeleted(ctx, m.ID); err != nil {
			slog.Error("mark expired machine deleted", "machine_id", m.ID, "error", err)
			continue
		}
		exec := m.CurrentExecutionID
		if exec == "" {
			exec = "exec-none"
		}
		opID := "op-ttl-" + m.ID + "-" + exec
		req := &pb.DeleteMachineRequest{
			MachineId: m.ID, ExecutionId: exec,
			Generation: uint64(m.Generation), OperationId: opID,
		}
		raw, err := protojson.Marshal(req)
		if err != nil {
			continue
		}
		project := "dev"
		if p, err := c.store.ProjectForApp(ctx, m.AppID); err == nil && p != "" {
			project = p
		}
		c.userEvent(ctx, project, m.AppID, m.ID, store.UserEventMachineExpired, nil)
		c.metrics.Inc("firepaas_machine_expiry_total", nil, 1)
		op, err := c.store.EnqueueDelete(ctx, project, m.ID, exec, opID, m.Generation, raw)
		if err != nil {
			if errors.Is(err, store.ErrRequestConflict) {
				slog.Warn("ttl delete idempotency conflict", "machine_id", m.ID, "error", err)
				continue
			}
			slog.Error("enqueue ttl delete", "machine_id", m.ID, "error", err)
			continue
		}
		if m.NodeID != "" {
			_ = c.store.UpdateOperationDispatchNode(ctx, op.ID, m.NodeID)
		}
		c.recordEvent(ctx, "expiry", m.ID, opID, m.NodeID, "machine expired, route detached and delete enqueued", nil)
		c.metrics.Inc("firepaas_machine_expirations_total", nil, 1)
	}
	return nil
}

// nodeEvacuating 判断 machine 所在节点是否处于 evacuate 驱离中（唯一 owner
// 决策表：active evacuate 负责 source replacement，restart 不得并发补副本）。
func (c *Controller) nodeEvacuating(ctx context.Context, nodeID string) bool {
	if nodeID == "" {
		return false
	}
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		return true // fail closed: evacuation ownership could not be determined
	}
	for i := range nodes {
		if (nodes[i].ID == nodeID || nodes[i].NomadNodeID == nodeID) &&
			nodes[i].Draining && nodes[i].Evacuate {
			return true
		}
	}
	return false
}

// maybeRestartMachine 执行 v1.2-D restart 决策（返回 true = 已入队 restart，
// 本轮不再走其它路径）。
func (c *Controller) maybeRestartMachine(ctx context.Context, m store.Machine, agent *pb.Machine) bool {
	if m.RestartMode != "ON_FAILURE" && m.RestartMode != "ALWAYS" {
		return false
	}
	if m.RestartBlocked {
		return false
	}
	// 唯一 owner 决策表：active rollout 负责 target replica；active evacuate
	// 负责 source replacement。
	if c.rolloutHoldsRecreate(ctx, m) {
		c.recordEvent(ctx, "restart", m.ID, "", m.NodeID, "rollout holds lifecycle, restart suppressed", nil)
		return false
	}
	if c.nodeEvacuating(ctx, m.NodeID) {
		c.recordEvent(ctx, "restart", m.ID, "", m.NodeID, "evacuation owns lifecycle, restart suppressed", nil)
		return false
	}
	// ON_FAILURE 的权威 exit class 来自 execution-bound agent exit report。
	if !restartExitClassAllows(m.RestartMode, agent.ExitCode) {
		c.recordEvent(ctx, "restart", m.ID, "", m.NodeID, "normal exit, ON_FAILURE does not restart", nil)
		return false
	}
	// 固定 backoff 必须在换 execution/generation 前建立。首次看到该完整
	// failed execution 只持久化 deadline；后续轮次到点后才允许 CAS 换代。
	backoff := time.Duration(m.RestartBackoffSeconds) * time.Second
	if backoff <= 0 {
		backoff = 10 * time.Second
	}
	prepared, err := c.store.PrepareRestartBackoff(ctx, m.ID, m.CurrentExecutionID,
		m.Generation, time.Now().Add(backoff))
	if err != nil {
		slog.Error("prepare restart backoff", "machine_id", m.ID, "error", err)
		return true
	}
	if prepared {
		return true
	}
	if m.RestartNextAttemptAt == nil || time.Now().Before(*m.RestartNextAttemptAt) {
		return true
	}
	if m.RestartAttempts >= m.RestartMaxAttempts {
		_ = c.store.BlockRestart(ctx, m.ID, true)
		c.recordEvent(ctx, "restart", m.ID, "", m.NodeID,
			"restart attempts exhausted, machine RESTART_BLOCKED", nil)
		c.metrics.Inc("firepaas_restarts_total", map[string]string{"result": "blocked"}, 1)
		if project, perr := c.store.ProjectForApp(ctx, m.AppID); perr == nil {
			c.userEvent(ctx, project, m.AppID, m.ID, store.UserEventMachineRestartBlock,
				map[string]any{"attempts": m.RestartAttempts})
		}
		return false
	}
	c.restartMachine(ctx, m)
	return true
}

// restartMachine 换代重启：新 execution + generation+1，重新调度/准入/
// readiness；opID 幂等键包含 machine、failed execution 与 attempt ordinal
// （ADR-0026 §8）。
func (c *Controller) restartMachine(ctx context.Context, m store.Machine) {
	exec := "exec-" + uuid.NewString()
	gen := m.Generation + 1
	failedExecution := m.CurrentExecutionID
	if failedExecution == "" {
		failedExecution = "missing-" + uuid.NewString()
	}
	attempt, err := c.store.RestartAttemptNumber(ctx, m.ID, m.CurrentExecutionID, m.Generation)
	if err != nil {
		return
	}
	// Keep the complete failed execution in the durable idempotency key. A
	// truncated suffix is not a sufficient fence across long-lived machines.
	opID := fmt.Sprintf("op-restart-%s-%s-%d", m.ID, failedExecution, attempt)

	req := &pb.CreateMachineRequest{
		MachineId: m.ID, Generation: uint64(gen), OperationId: opID,
		Spec: &pb.MachineSpec{
			AppId: m.AppID, DeploymentId: m.DeploymentID, ExecutionId: exec,
			ReplicaOrdinal: uint32(m.ReplicaOrdinal), Hostname: m.Hostname,
			ImageRef: m.ImageRef, Vcpu: uint64(m.RequestedVCPU),
			MemMib: uint64(m.RequestedMemMIB), DiskMib: uint64(m.RequestedDiskMIB), Env: m.Env,
			Network: &pb.NetworkSpec{IngressPort: uint64(m.IngressPort)},
		},
	}
	if len(m.Placement) > 0 {
		var pl pb.PlacementConstraints
		if err := protojson.Unmarshal(m.Placement, &pl); err == nil {
			req.Spec.Placement = &pl
		}
	}
	dep, err := c.store.GetDeployment(ctx, m.DeploymentID)
	if err != nil || dep == nil {
		slog.Error("resolve restart deployment", "machine_id", m.ID, "error", err)
		return
	}
	applyDeploymentSpecExtras(dep, req.Spec)
	project, err := c.store.ProjectForApp(ctx, m.AppID)
	if err != nil || project == "" {
		slog.Error("resolve restart project", "machine_id", m.ID, "error", err)
		return
	}
	req.Spec.ProjectId = project
	raw, err := protojson.Marshal(req)
	if err != nil {
		slog.Error("marshal restart request", "machine_id", m.ID, "error", err)
		return
	}
	nextAt := time.Now().Add(time.Duration(m.RestartBackoffSeconds) * time.Second)
	if _, err := c.store.EnqueueRestartCAS(ctx, project, m.ID, m.CurrentExecutionID,
		exec, opID, m.Generation, raw, nextAt); err != nil {
		if errors.Is(err, store.ErrMachineLifecycleClosed) {
			c.recordEvent(ctx, "restart", m.ID, opID, m.NodeID,
				"restart CAS lost to delete, owner, or another reconciler", nil)
			return
		}
		slog.Error("enqueue restart", "machine_id", m.ID, "error", err)
		return
	}
	c.recordEvent(ctx, "restart", m.ID, opID, m.NodeID,
		fmt.Sprintf("attempt %d/%d: new execution %s", attempt, m.RestartMaxAttempts, exec), nil)
	c.metrics.Inc("firepaas_restarts_total", map[string]string{"result": "restarted"}, 1)
	if project, perr := c.store.ProjectForApp(ctx, m.AppID); perr == nil {
		c.userEvent(ctx, project, m.AppID, m.ID, store.UserEventMachineRestarted,
			map[string]any{"attempt": attempt, "max_attempts": m.RestartMaxAttempts, "generation": gen})
	}
	slog.Info("machine restart scheduled", "machine_id", m.ID, "execution_id", exec,
		"operation_id", opID, "attempt", attempt)
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

// rebuildLeases：预约全量重建（P2-2）+ 节点投影 stale 标记（P3-6c）+ route
// 投影全量重建。流程：
//
//  1. ListInFlightOperations 取活跃 create 集合；
//  2. PruneStaleOps 删除非活跃 resv:op 键；
//  3. Reset 原子重建（清零全部 node/project hash + 从存活 op 键重放在途
//     承诺），修复 op 键 TTL 先过期、hash 增量永久残留的节点假满；
//     重建不依赖重新 Acquire——Acquire 的幂等早退会跳过 hash 入账。
//  4. MarkStaleNodes 把 last_seen 超时节点置 UNKNOWN；
//  5. buildRoutes。
//
// 只在单写者（M2a leader）周期内执行。
func (c *Controller) rebuildLeases(ctx context.Context) error {
	inFlight, err := c.store.ListInFlightOperations(ctx)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, op := range inFlight {
		if op.Kind != "create" {
			continue
		}
		active[op.ID] = true
	}
	pruned, err := c.resv.PruneStaleOps(ctx, active)
	if err != nil {
		return err
	}
	cleared, err := c.resv.Reset(ctx)
	if err != nil {
		return err
	}
	if pruned > 0 || cleared > 0 {
		slog.Info("reservation rebuild", "pruned_stale_ops", pruned, "cleared_hashes", cleared)
		c.metrics.Inc("firepaas_reservation_rebuilds_total", nil, 1)
	}

	// P3-6c：节点从 Nomad 消失后 PG 投影永远保留旧状态；按 SyncInterval 的
	// 3 倍作为 stale 阈值（容忍两轮同步失败）。
	if n, err := c.store.MarkStaleNodes(ctx, 3*c.cfg.SyncInterval); err != nil {
		slog.Warn("mark stale nodes", "error", err)
	} else if n > 0 {
		slog.Warn("marked stale nodes UNKNOWN", "count", n)
	}
	return c.buildRoutes(ctx)
}

// ---------------------------------------------------------------------------
// route 投影（R7：全量重建 + prune）
// ---------------------------------------------------------------------------

func (c *Controller) buildRoutes(ctx context.Context) error {
	proxyByNode := make(map[string]string)
	for _, view := range c.nodeViews() {
		proxyByNode[view.agentID] = view.proxy
	}
	return c.routes.Rebuild(ctx, proxyByNode)
}

func (c *Controller) recordEvent(ctx context.Context, kind, machineID, opID, nodeID, reason string, details []byte) {
	ev := store.SchedulerEvent{
		Kind: kind, MachineID: machineID, OperationID: opID, NodeID: nodeID, Reason: reason,
	}
	if len(details) > 0 {
		ev.Details = details
	}
	if err := c.store.RecordSchedulerEvent(ctx, ev); err != nil {
		slog.Error("record scheduler event", "error", err)
	}
}

// userEvent（v1.2-F）：append-only 租户事件（ADR 无明文、v1.2-plan §9）。
// details 只放脱敏摘要；失败只记日志，不阻塞业务路径。
func (c *Controller) userEvent(ctx context.Context, project, app, machine, typ string, details map[string]any) {
	if project == "" {
		project = "dev"
	}
	var raw []byte
	if len(details) > 0 {
		raw, _ = json.Marshal(details)
	}
	if err := c.store.RecordUserEvent(ctx, store.UserEvent{
		ProjectID: project, AppID: app, MachineID: machine, Type: typ, Details: raw,
	}); err != nil {
		slog.Warn("record user event", "type", typ, "machine_id", machine, "error", err)
	}
}

// recordEgressAudit（v1.3-A，ADR-0027）：把 agent 上报的 per-execution
// egress 决策计数聚合成 PG 拒绝摘要。deny_buckets 只保留计数与
// protocol:port（无 Host/SNI），不构成高基数。
func (c *Controller) recordEgressAudit(
	ctx context.Context,
	pg *store.Machine,
	m *pb.Machine,
	ea *pb.EgressAuditStats,
) error {
	buckets := make([]map[string]any, 0, len(ea.GetDenyBuckets()))
	for _, b := range ea.GetDenyBuckets() {
		buckets = append(buckets, map[string]any{
			"protocol": b.GetProtocol(), "port": b.GetPort(), "denied": b.GetDenied(),
		})
	}
	bucketsJSON, _ := json.Marshal(buckets)
	project := "dev"
	if pg != nil {
		if app, err := c.store.GetApp(ctx, pg.AppID); err == nil && app != nil {
			project = app.ProjectID
		}
	}
	return c.store.UpsertEgressDenySummary(ctx, store.EgressDenySummary{
		ProjectID:          project,
		AppID:              pg.AppID,
		MachineID:          m.MachineId,
		ExecutionID:        m.ExecutionId,
		PolicyGeneration:   int64(ea.GetPolicyGeneration()),
		AllowedConnections: int64(ea.GetAllowedConnections()),
		DeniedConnections:  int64(ea.GetDeniedConnections()),
		LimitRejections:    int64(ea.GetLimitRejections()),
		DenyBuckets:        bucketsJSON,
	})
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
