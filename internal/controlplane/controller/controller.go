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
	"fmt"
	"log/slog"
	"time"

	"github.com/example/firepaas/internal/controlplane/agentclient"
	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/example/firepaas/internal/controlplane/nodemanager"
	"github.com/example/firepaas/internal/controlplane/reservations"
	"github.com/example/firepaas/internal/controlplane/store"
	"github.com/example/firepaas/internal/observability/metrics"
	"github.com/example/firepaas/internal/scheduler"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Config 是 controller 运行参数。
type Config struct {
	DefaultAppPort       int
	LegacyAgentProxyAddr string        // 节点视图缺失时的兜底（M1 单节点兼容）
	OpPollInterval       time.Duration // 默认 1s
	SyncInterval         time.Duration // 默认 5s
	RebuildInterval      time.Duration // 预约/投影重建，默认 30s
	ReconcileGrace       time.Duration // ACK 丢失判定宽限，默认 30s
	MaxPlacementAttempts int           // ResourceExhausted 换节点上限，默认 3
	AgentRPCTimeout      time.Duration // 默认 2m（未缓存镜像 pull 可达 60s）
	CreateRetryBase      time.Duration // create FAILED 首次重派退避（P1-3），默认 10s
	CreateRetryMax       time.Duration // create FAILED 退避封顶，默认 5m
	ClaimStaleAfter      time.Duration // CLAIMED 滞留回收阈值（P1-1），默认 2×AgentRPCTimeout+60s
	NodeMissingThreshold int           // 节点连续 List 失败次数才摘路由（P3-9），默认 3
}

// Controller 执行 reconcile。
type Controller struct {
	store   *store.Store
	catalog *catalog.Catalog
	nodes   *nodemanager.Manager
	resv    *reservations.Manager
	placer  *scheduler.Placer
	metrics *metrics.Registry
	cfg     Config

	// nodeListFailures 记录节点连续 List 失败次数（P3-9：单次抖动不摘路由）。
	nodeListFailures map[string]int
}

// New 构造 Controller。
func New(st *store.Store, cat *catalog.Catalog, nm *nodemanager.Manager,
	resv *reservations.Manager, placer *scheduler.Placer, reg *metrics.Registry, cfg Config) *Controller {
	if cfg.OpPollInterval == 0 {
		cfg.OpPollInterval = time.Second
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 5 * time.Second
	}
	if cfg.RebuildInterval == 0 {
		cfg.RebuildInterval = 30 * time.Second
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
	if cfg.ClaimStaleAfter == 0 {
		cfg.ClaimStaleAfter = 2*cfg.AgentRPCTimeout + time.Minute
	}
	if cfg.NodeMissingThreshold == 0 {
		cfg.NodeMissingThreshold = 3
	}
	return &Controller{store: st, catalog: cat, nodes: nm, resv: resv, placer: placer, metrics: reg, cfg: cfg,
		nodeListFailures: map[string]int{}}
}

// Run 启动三个循环：操作 reconcile、observed 同步/决策表、预约与投影重建。
func (c *Controller) Run(ctx context.Context) error {
	opTicker := time.NewTicker(c.cfg.OpPollInterval)
	syncTicker := time.NewTicker(c.cfg.SyncInterval)
	rebuildTicker := time.NewTicker(c.cfg.RebuildInterval)
	staleTicker := time.NewTicker(30 * time.Second) // P1-1：CLAIMED 租约回收
	defer opTicker.Stop()
	defer syncTicker.Stop()
	defer rebuildTicker.Stop()
	defer staleTicker.Stop()

	if c.nodeListFailures == nil { // 防御：New 已初始化，测试可直接构造
		c.nodeListFailures = map[string]int{}
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
		}
	}
}

// ---------------------------------------------------------------------------
// 操作 reconcile（PG outbox → agent）
// ---------------------------------------------------------------------------

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

func (c *Controller) processOperation(ctx context.Context, op store.Operation) error {
	switch op.Kind {
	case "create":
		return c.processCreate(ctx, op)
	case "delete":
		return c.processDelete(ctx, op, true)
	case "reap":
		// reconcile 清理（旧代/死亡残留）：成功后不得推进 desired→DELETED。
		return c.processDelete(ctx, op, false)
	default:
		err := fmt.Errorf("unknown operation kind %q", op.Kind)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
}

// processCreate：ACK 丢失补账 → 调度 → 预约 → 派发 → 落账。
func (c *Controller) processCreate(ctx context.Context, op store.Operation) error {
	var req pb.CreateMachineRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}

	// R8：ACK 丢失补账——machine 已按本次 execution RUNNING，直接补 SUCCEEDED。
	if m, err := c.store.GetMachine(ctx, op.MachineID); err == nil && m != nil &&
		m.CurrentExecutionID == op.ExecutionID && m.ObservedState == "RUNNING" {
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "ack_lost_reconcile"}, 1)
		_ = c.resv.Release(ctx, op.ID)
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", nil, "")
	}

	excluded := map[string]bool{}
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxPlacementAttempts; attempt++ {
		view, err := c.placeFor(ctx, op, &req, excluded)
		if err != nil {
			// 项目配额是业务终态；其余视为本轮失败（requeue 后重试）。
			if isQuotaError(err) {
				c.metrics.Inc("firepaas_reservations_total", map[string]string{"result": "quota_failed"}, 1)
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			}
			lastErr = err
			break
		}

		client := c.nodes.ClientFor(view.nomadID)
		if client == nil {
			lastErr = fmt.Errorf("no agent client for node %s", view.nomadID)
			c.recordEvent(ctx, "reservation", op.MachineID, op.ID, view.agentID, "client missing", nil)
			_ = c.resv.Release(ctx, op.ID)
			excluded[view.agentID] = true
			continue
		}

		c.metrics.Inc("firepaas_placements_total", nil, 1)
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		machine, err := client.Create(rpcCtx, &req)
		cancel()
		if err != nil {
			c.metrics.Inc("firepaas_agent_rpc_errors_total", map[string]string{"kind": "create"}, 1)
			_ = c.resv.Release(ctx, op.ID)
			if status.Code(err) == codes.ResourceExhausted {
				// ADR-0002：换节点重试，最多 3 次。
				c.recordEvent(ctx, "reservation", op.MachineID, op.ID, view.agentID,
					"agent ResourceExhausted, retrying another node", nil)
				excluded[view.agentID] = true
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
		if err := c.store.UpdateMachineNodeAndObserved(ctx, machine.MachineId, view.agentID,
			machine.ExecutionId, machine.State.String(), machine.SlotIp, machine.Readiness.String()); err != nil {
			return err
		}
		result, _ := protojson.Marshal(machine)
		c.metrics.Inc("firepaas_operations_total", map[string]string{"kind": "create", "result": "succeeded"}, 1)
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")
	}
	c.metrics.Inc("firepaas_placements_total", map[string]string{"result": "failed"}, 1)
	if lastErr == nil {
		lastErr = fmt.Errorf("placement attempts exhausted")
	}
	return lastErr
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
		// 节点失联：desired=DELETED 已可收敛；agent 侧残留由 orphan 决策表
		// 在节点恢复后清理（R4/R6）。记事件，审计可解释。
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
	_ = c.catalog.RemoveLocation(ctx, del.MachineId, del.ExecutionId)
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

// placeFor 执行一次完整放置（过滤+打分+预约），成功返回节点视图。
func (c *Controller) placeFor(ctx context.Context, op store.Operation, req *pb.CreateMachineRequest, excluded map[string]bool) (*nodeView, error) {
	views := c.nodeViews()
	allocated, err := c.store.AllocatedByNode(ctx)
	if err != nil {
		return nil, err
	}
	pending, err := c.store.PendingUsageByNode(ctx)
	if err != nil {
		return nil, err
	}
	pendingByNode := map[string]store.PendingUsage{}
	for _, p := range pending {
		pendingByNode[p.NodeID] = p
	}
	deployNodes, err := c.store.MachineNodesByDeployment(ctx)
	if err != nil {
		return nil, err
	}

	spec := req.GetSpec()
	nodes := make([]scheduler.Node, 0, len(views))
	for _, v := range views {
		n := scheduler.Node{ID: v.agentID, Status: v.status, Pool: v.n.NodePool}
		if v.n.Info != nil {
			info := v.n.Info
			n.Labels = info.Labels
			if n.Pool == "" {
				n.Pool = info.Labels["node_pool"]
			}
			if info.Capacity != nil {
				n.CPUTotal = info.Capacity.VcpuTotal
				n.MemTotalMib = info.Capacity.MemTotalMib
			}
			if info.Usage != nil {
				n.MemAllocated = info.Usage.MemAllocatedMib
				n.CPUPercent = info.Usage.CpuPercent
				n.MemUsedMib = info.Usage.MemUsedMib
			}
		}
		if a, ok := allocated[v.agentID]; ok {
			n.CPUAllocated = uint64(a.VCPU)
			if n.MemAllocated == 0 {
				n.MemAllocated = uint64(a.MemMib)
			}
		}
		if p, ok := pendingByNode[v.agentID]; ok {
			n.CPUPending = uint64(p.VCPU)
			n.MemPending = uint64(p.MemMib)
		}
		nodes = append(nodes, n)
	}

	var wantPool string
	var wantLabels map[string]string
	antiAffinity := false
	if spec.GetPlacement() != nil {
		wantPool = spec.GetPlacement().NodePool
		wantLabels = spec.GetPlacement().Labels
		antiAffinity = spec.GetPlacement().AntiAffinity == pb.PlacementConstraints_DEPLOYMENT
	}
	vcpu, memMib := spec.GetVcpu(), spec.GetMemMib()
	if vcpu == 0 {
		vcpu = 1
	}
	if memMib == 0 {
		memMib = 512
	}
	req2 := scheduler.Request{
		VCPU:                    vcpu,
		MemMib:                  memMib,
		DeploymentID:            spec.GetDeploymentId(),
		Pool:                    wantPool,
		Labels:                  wantLabels,
		AntiAffinity:            antiAffinity,
		ExistingDeploymentNodes: deployNodes[spec.GetDeploymentId()],
		ExcludedNodes:           excluded,
	}
	pl, err := c.placer.Place(req2, nodes, nil)
	if err != nil {
		for _, ev := range pl.Events {
			c.recordEvent(ctx, "filter_rejection", op.MachineID, op.ID, ev.NodeID, ev.Reason, nil)
		}
		return nil, err
	}
	for _, ev := range pl.Events {
		kind := "placement"
		if ev.Kind == "filter_rejection" {
			kind = "filter_rejection"
			c.metrics.Inc("firepaas_filter_rejections_total", map[string]string{"reason": ev.Reason}, 1)
		}
		c.recordEvent(ctx, kind, op.MachineID, op.ID, ev.NodeID, ev.Reason, nil)
	}

	// 找到被选中节点的视图（预约/派发需要 nomad ID 与容量）。
	var chosen *nodeView
	for i := range views {
		if views[i].agentID == pl.NodeID {
			chosen = &views[i]
			break
		}
	}
	if chosen == nil {
		return nil, fmt.Errorf("scheduler returned unknown node %s", pl.NodeID)
	}
	if chosen.n.Info == nil || chosen.n.Info.Capacity == nil {
		return nil, fmt.Errorf("node %s has no capacity info yet", pl.NodeID)
	}

	vCPUQuota, memQuota, err := c.store.ProjectQuota(ctx, op.ProjectID)
	if err != nil {
		return nil, err
	}
	err = c.resv.Acquire(ctx, op.ID, chosen.agentID, op.ProjectID,
		vcpu, memMib, chosen.n.Info.Capacity.VcpuTotal, chosen.n.Info.Capacity.MemTotalMib,
		uint64(vCPUQuota), uint64(memQuota))
	if err != nil {
		c.metrics.Inc("firepaas_reservations_total", map[string]string{"result": "failed"}, 1)
		c.recordEvent(ctx, "reservation", op.MachineID, op.ID, chosen.agentID, err.Error(), nil)
		return nil, err
	}
	c.metrics.Inc("firepaas_reservations_total", map[string]string{"result": "acquired"}, 1)

	// P2-1：记录派发节点。PG pending 记账（PendingUsageByNode）依赖
	// dispatch_node_id；旧实现只在 delete 路径写它，生产中调度器 pending
	// 恒为 0，硬上限只剩 agent 准入单层兑底。必须在派发前写入，这样
	// 下一轮 placeFor 就能看到本轮在途承诺。
	if err := c.store.UpdateOperationDispatchNode(ctx, op.ID, chosen.agentID); err != nil {
		return nil, err
	}
	return chosen, nil
}

// ---------------------------------------------------------------------------
// observed 同步与决策表（R1–R7）
// ---------------------------------------------------------------------------

func (c *Controller) syncObserved(ctx context.Context) error {
	views := c.nodeViews()
	seen := map[string]*pb.Machine{}
	agentByMachine := map[string]string{} // machine → agent node id

	for _, v := range views {
		client := c.nodes.ClientFor(v.nomadID)
		if client == nil {
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
	return c.buildRoutes(ctx)
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

	// R1：正常观测。
	_ = c.store.UpdateMachineObserved(ctx, m.MachineId, m.ExecutionId,
		m.State.String(), m.SlotIp, m.Readiness.String())
}

// processPGMachine：PG 视角 → agent（ACK 丢失/节点失联/desired 删除）。
func (c *Controller) processPGMachine(ctx context.Context, m store.Machine,
	seen map[string]*pb.Machine, agentByMachine map[string]string) {

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
		return // 当前 execution 存活：R1 已处理 observed
	}

	// R4：节点失联/不可用 → 保守 UNKNOWN，不重建。
	if nodeID != "" {
		v := c.viewForAgent(nodeID)
		if v == nil || v.status != "HEALTHY" {
			_ = c.store.MarkMachineObservedMissing(ctx, m.ID)
			c.recordEvent(ctx, "reconcile", m.ID, "", nodeID,
				"node unhealthy, hold recreate until recovery", nil)
			return
		}
	}

	// 有在途操作：等它收敛（终态后再判下一动作）。
	hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
	if err != nil || hasPending {
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
	switch recreateAction(last.Kind, last.Status, time.Since(last.UpdatedAt),
		c.cfg.ReconcileGrace, c.createRetryDelay(attempts)) {
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
	agent *pb.Machine, nodeID string) {
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
		m.RequestedVCPU, m.RequestedMemMIB, m.IngressPort, m.ID, m.DeploymentID,
		exec, opID, gen, m.ReplicaOrdinal, raw, []byte(m.Placement))
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

func (c *Controller) enqueueDelete(ctx context.Context, machineID, executionID string, generation int64, opID, nodeID, reason string) error {
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
	machines, err := c.store.ActiveRouteMachines(ctx)
	if err != nil {
		return err
	}
	views := c.nodeViews()
	proxyByNode := map[string]string{}
	for _, v := range views {
		proxyByNode[v.agentID] = v.proxy
	}

	type routeKey struct {
		hostname string
		port     int
	}
	grouped := map[routeKey]*store.RouteRow{}
	for _, m := range machines {
		port := m.IngressPort
		if port == 0 {
			port = c.cfg.DefaultAppPort
		}
		key := routeKey{hostname: m.Hostname, port: port}
		route := grouped[key]
		if route == nil {
			route = &store.RouteRow{Hostname: m.Hostname, Port: port, AppID: m.AppID}
			grouped[key] = route
		}
		if m.Generation > route.Generation {
			route.Generation = m.Generation
		}
		proxy := proxyByNode[m.NodeID]
		if proxy == "" {
			proxy = c.cfg.LegacyAgentProxyAddr
		}
		if proxy == "" {
			// 节点视图缺失：该 backend 暂时不可达，不写入投影（受控收敛）。
			continue
		}
		route.Backends = append(route.Backends, store.RouteBackendRow{
			MachineID:         m.ID,
			ExecutionID:       m.CurrentExecutionID,
			NodeProxyEndpoint: proxy,
			AppPort:           port,
			Weight:            100,
			Readiness:         m.ObservedReadiness,
			Draining:          false,
		})
	}

	active := make([]store.RouteRow, 0, len(grouped))
	for _, route := range grouped {
		active = append(active, *route)
	}
	if err := c.store.SyncRoutes(ctx, active); err != nil {
		return err
	}

	keepRoutes := make(map[string]bool, len(active))
	keepHosts := make(map[string]bool, len(active))
	for _, r := range active {
		keepRoutes[fmt.Sprintf("route:%s:%d", r.Hostname, r.Port)] = true
		keepHosts[r.Hostname] = true
		catalogRoute := catalog.Route{
			RouteGeneration: r.Generation,
			Backends:        make([]catalog.Backend, 0, len(r.Backends)),
		}
		for _, b := range r.Backends {
			catalogRoute.Backends = append(catalogRoute.Backends, catalog.Backend{
				MachineID:         b.MachineID,
				ExecutionID:       b.ExecutionID,
				NodeProxyEndpoint: b.NodeProxyEndpoint,
				AppPort:           b.AppPort,
				Readiness:         b.Readiness,
				Weight:            b.Weight,
				Draining:          b.Draining,
			})
		}
		if err := c.catalog.PublishRoute(ctx, r.Hostname, r.Port, catalogRoute); err != nil {
			return err
		}
	}
	return c.catalog.PruneRoutes(ctx, keepRoutes, keepHosts)
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
