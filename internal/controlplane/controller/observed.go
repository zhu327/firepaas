package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/placement"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type nodeView struct {
	agentID string
	nomadID string
	proxy   string
	status  string
	n       nodemanager.Node
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

const (
	// nodeObservedListTimeout 是单节点 observed List 的上限（保持既有 10s）。
	nodeObservedListTimeout = 10 * time.Second
	// nodeObservedListParallel 是 observed List 的并发上限（P1#10）：串行
	// 抓取时 N 个失联节点会把主 select 循环堵住 ~10N 秒，饿死 op 派发/
	// rollout/TTL 处理；有界并发后整轮开销 ≈ ⌈N/并发⌉×单节点超时。
	nodeObservedListParallel = 8
)

// nodeListOutcome 是单节点 List 抓取结果。fetch 阶段各元素只由对应
// goroutine 写，合并阶段单线程消费（单写者不变量不受影响）。
type nodeListOutcome struct {
	view     nodeView
	machines []*pb.Machine
	err      error // List RPC 失败（含超时）
	noClient bool  // 无 agent client（nodemanager 尚未建好连接）
}

// fetchNodeObservations 有界并发地抓取各节点 List：每节点独立超时，失联/
// 挂起节点只影响自己的 outcome（per-node failure isolation），不再串行
// 拖住整轮。结果按 views 原序返回，合并与决策由调用方单线程执行。
// clientFor/list 为依赖注入 seam（生产走 agent gRPC，测试注入 fake agent）。
func fetchNodeObservations(ctx context.Context, views []nodeView,
	clientFor func(nodeView) *agentclient.Client,
	list func(ctx context.Context, v nodeView, client *agentclient.Client) ([]*pb.Machine, error),
	timeout time.Duration, maxParallel int,
) []nodeListOutcome {
	outcomes := make([]nodeListOutcome, len(views))
	if maxParallel > len(views) {
		maxParallel = len(views)
	}
	if maxParallel < 1 {
		maxParallel = 1
	}
	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup
	for i, v := range views {
		outcomes[i].view = v
		client := clientFor(v)
		if client == nil {
			outcomes[i].noClient = true
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, v nodeView, client *agentclient.Client) {
			defer wg.Done()
			defer func() { <-sem }()
			listCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			machines, err := list(listCtx, v, client)
			outcomes[i].machines, outcomes[i].err = machines, err
		}(i, v, client)
	}
	wg.Wait()
	return outcomes
}

func (c *Controller) syncObserved(ctx context.Context) error {
	// v1.2-D（ADR-0026）：TTL 到期先摘 route（desired→DELETED，buildRoutes
	// 立即排除），再下发 fenced delete。controller 停机跨过到期点后，恢复
	// 时这里立即处理（绝对 expires_at，不依赖计时器）。
	if err := c.expireMachines(ctx); err != nil {
		slog.Error("expire machines", "error", err)
	}
	views := c.nodeViews()
	// P1#10：抓取阶段有界并发（失联节点不串行堵整轮）；合并阶段维持
	// 单线程决策，节点顺序与失败处理语义与原串行实现一致。
	outcomes := fetchNodeObservations(ctx, views,
		func(v nodeView) *agentclient.Client { return c.nodes.ClientFor(v.nomadID) },
		func(ctx context.Context, _ nodeView, client *agentclient.Client) ([]*pb.Machine, error) {
			return client.List(ctx, "")
		},
		nodeObservedListTimeout, nodeObservedListParallel)
	// D3/D5 验收发现：failover+旧节点恢复会让同一 machine id 同时出现在两个
	// 节点（旧代/新代副本并存）。按机器 id 单槽位去重会随机丢掉一个副本，且
	// R2 清理会用 PG node_id 把 reap 派发到错误节点（无限重试活锁）。改为
	// 按（machine, 节点）保留全部副本：旧代/外来代副本的 reap 一律 pin 在
	// 观察到它的节点。
	copies := map[string]map[string]*pb.Machine{} // machine → agent node id → 观测副本

	for _, o := range outcomes {
		v := o.view
		if o.noClient {
			// M5 诊断：之前静默跳过掩盖了 node client 未建立的问题。
			c.nodeListFailures[v.agentID]++
			if c.nodeListFailures[v.agentID]%5 == 1 {
				slog.Warn("no agent client for node view", "node", v.agentID,
					"nomad_id", v.nomadID, "status", v.status, "consecutive", c.nodeListFailures[v.agentID])
			}
			continue
		}
		if o.err != nil {
			c.metrics.Inc("firepaas_agent_rpc_errors_total", map[string]string{"kind": "list"}, 1)
			// P3-9：单次失败只可能是瞬时抖动（agent 重启/网络闪断）；连续
			// NodeMissingThreshold 次失败才把节点上的 machine 摘路由，避免
			// backend 随抖动来回抖。真正失联时 nodemanager 会在 20s 内把
			// 节点置 UNKNOWN，R4 路径兑底。
			c.nodeListFailures[v.agentID]++
			if c.nodeListFailures[v.agentID] < c.cfg.NodeMissingThreshold {
				slog.Warn("agent list failed (transient)", "node", v.agentID,
					"consecutive", c.nodeListFailures[v.agentID], "error", o.err)
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
		for _, m := range o.machines {
			if copies[m.MachineId] == nil {
				copies[m.MachineId] = map[string]*pb.Machine{}
			}
			copies[m.MachineId][v.agentID] = m
			c.processAgentMachine(ctx, m, v)
		}
	}

	pgMachines, err := c.store.ListMachines(ctx, "")
	if err != nil {
		return err
	}
	for _, m := range pgMachines {
		c.processPGMachine(ctx, m, copies[m.ID])
	}
	if err := c.buildRoutes(ctx); err != nil {
		return err
	}
	c.publishGauges(ctx, views, pgMachines)
	return nil
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
		c.enqueueOrphanDelete(ctx, project, m, v.agentID, 1)
		return
	}

	if pg.CurrentExecutionID != m.ExecutionId {
		// 非当前 execution 的副本一律按节点 pin reap（D3/D5/D6 + 复验修正）：
		// 存活副本有双脑风险；UNSPECIFIED/DELETED 死条目虽不转发流量，仍占
		// 实例名/slot 网络分配，同节点同名重建会永久撞名。二者都必须靠
		// re-arm 到真实删除。这里不查 per-machine pending：目标 execution
		// 与当前 create/delete 不同，若等在途 create 先收敛会形成“create
		// 等腾名、reap 等 create”的死锁；machine dispatch lock+op 幂等仍
		// 保证串行派发与账本安全。
		c.recordEvent(ctx, "reconcile", m.MachineId, "", v.agentID,
			"agent holds stale execution", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "stale_execution_seen"}, 1)
		project := "dev"
		if m.GetSpec() != nil && m.GetSpec().ProjectId != "" {
			project = m.GetSpec().ProjectId
		}
		c.enqueueOrphanDelete(ctx, project, m, v.agentID, pg.Generation)
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
// copies 为 (machine, 节点) 的全量观测副本：failover+旧节点恢复、第二写者
// 等会让同一 machine id 在多节点并存不同 execution。清理动作必须 pin 在
// 持有副本的节点（D3）；当前 execution 的生存判定优先 home 节点、任意节点
// 的存活副本都算数（防止误换代）；非当前代副本（foreign）的下单由
// processAgentMachine 按节点 pin 完成，本函数不重复。
// reapDuplicateLiveCopy 回收“同一 execution 在多个节点存活”的重复副本
// （review 2026-09-10）。返回 true 表示本轮已下单，调用方应结束本轮决策。
//
// 存活副本的选择顺序（review H1/M1）：
//  1. operation.dispatch_node_id（ledger 归属节点，重放幂等的锚点）；
//  2. m.NodeID（PG 登记的 home）；
//  3. 其余按 agent ID 确定性排序。
//
// 同等优先级内优先存活副本（RUNNING/PAUSED）：不能为了保留 STOPPED 的 home
// 而删掉 RUNNING 的重复副本（会造成停服 + 重建 churn）。
//
// 不做 agentStateUsable 过滤的“只删不存活”分支：同一 execution 出现在任何
// 其它节点都是分叉，STOPPED/UNSPECIFIED 副本同样占实例名并会经
// processAgentMachine 覆盖 observed 状态。
func (c *Controller) reapDuplicateLiveCopy(ctx context.Context, m store.Machine,
	copies map[string]*pb.Machine,
) bool {
	if m.CurrentExecutionID == "" {
		return false
	}
	ids := make([]string, 0, len(copies))
	for id, cp := range copies {
		if cp != nil && cp.ExecutionId == m.CurrentExecutionID {
			ids = append(ids, id)
		}
	}
	if len(ids) < 2 {
		return false
	}
	sort.Strings(ids)

	pref := make([]string, 0, len(ids))
	addPref := func(id string) {
		if id == "" || !slices.Contains(ids, id) || slices.Contains(pref, id) {
			return
		}
		pref = append(pref, id)
	}
	if dispatchNode, err := c.store.LatestCreateDispatchNode(ctx, m.ID, m.CurrentExecutionID); err == nil {
		addPref(dispatchNode)
	}
	addPref(m.NodeID)
	for _, id := range ids {
		addPref(id)
	}
	survivor := pref[0]
	bestRank := copyLivenessRank(copies[survivor])
	for _, id := range pref[1:] {
		if r := copyLivenessRank(copies[id]); r > bestRank {
			survivor, bestRank = id, r
		}
	}

	pending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
	if err != nil || pending {
		return false
	}
	for _, id := range ids {
		if id == survivor {
			continue
		}
		c.recordEvent(ctx, "reconcile", m.ID, "", id,
			"duplicate live execution on another node; reaping (survivor="+survivor+")", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "duplicate_execution"}, 1)
		_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
			"op-dup-"+m.ID+"-"+m.CurrentExecutionID+"-"+id, id, "duplicate live execution")
		return true
	}
	return false
}

func (c *Controller) processPGMachine(ctx context.Context, m store.Machine,
	copies map[string]*pb.Machine,
) {
	agentIDs := make([]string, 0, len(copies))
	for id := range copies {
		agentIDs = append(agentIDs, id)
	}
	sort.Strings(agentIDs) // 决策确定性：同状态不同遍历顺序产出一致动作
	var homeCopy, liveCopy *pb.Machine
	liveNodeID := ""
	foreignCount := 0
	for _, id := range agentIDs {
		cp := copies[id]
		if id == m.NodeID {
			homeCopy = cp
		}
		if cp.ExecutionId == m.CurrentExecutionID {
			if liveCopy == nil || id == m.NodeID {
				liveCopy, liveNodeID = cp, id
			}
			continue
		}
		if agentStateUsable(cp) {
			// 只有存活的外来副本才阻塞换代重建；崩溃恢复账本的死条目由
			// agent 保留（dedup/fence），清不掉也不应卡死 R3（验收复验的
			// evacuate wedge 根因）。
			foreignCount++
		}
	}
	hasAgent := len(copies) > 0
	nodeID := m.NodeID
	if nodeID == "" && liveCopy != nil {
		// 只从“当前 execution 的存活副本”回填节点视图；外来/死条目的节点
		// 不是本代的节点，错误回填会让 R4 误判节点失联而永久挂起重建。
		nodeID = liveNodeID
	}

	if m.DesiredState == "DELETED" {
		if !hasAgent {
			return // 已收敛；route 由 buildRoutes 清理
		}
		// R5：desired 已删除但 agent 残留 → 按副本所在节点 pin delete。当前代
		// 副本优先；外来/分歧代副本同样下单（D6：否则 delete 对 PG 记录的旧
		// execution 幂等成功，真实在跑的 execution 被泄漏）。一轮最多一个
		// op（pending 守卫），opID 按 execution 区分，下轮继续清下一副本。
		hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
		if err != nil || hasPending {
			return
		}
		c.recordEvent(ctx, "reconcile", m.ID, "", nodeID, "desired DELETED but agent has machine", nil)
		c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "desired_deleted"}, 1)
		if liveCopy != nil {
			_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
				"op-reap-"+m.ID+"-"+m.CurrentExecutionID, liveNodeID, "desired deleted")
			return
		}
		for _, id := range agentIDs {
			cp, ok := copies[id]
			if !ok || cp.ExecutionId == m.CurrentExecutionID {
				continue
			}
			// opID 与 R6/R2 同按 (machine, execution, node) 作用域；generation
			// 取副本观测值、回退 PG 代际，与 enqueueOrphanDelete 同一规则。
			gen := int64(cp.GetGeneration())
			if gen == 0 {
				gen = m.Generation
			}
			if gen == 0 {
				gen = 1
			}
			_ = c.enqueueDelete(ctx, m.ID, cp.ExecutionId, gen,
				"op-orphan-"+m.ID+"-"+cp.ExecutionId+"-"+id, id, "desired deleted: foreign execution")
			return
		}
		return
	}

	if m.DesiredState != "CREATED" && m.DesiredState != "RUNNING" {
		return
	}

	// R2：home（PG 登记）节点持旧 execution → 先清理旧代（必要时作废在途
	// create），待 delete 完成后下一轮再重建；非 home 节点副本已由
	// processAgentMachine 按节点 pin 下单，不在此重复。
	if homeCopy != nil && homeCopy.ExecutionId != m.CurrentExecutionID {
		c.supersedePendingCreateAndReap(ctx, m, homeCopy, m.NodeID)
		return
	}

	// R2.5（review 2026-09-10）：同一 execution 在两个节点同时存活（leader
	// 切换后原 op 被重派发到另一节点造成）。observed 状态会在两个 agent 之间
	// 抖动，且多出的副本不受任何 delete 收敛。存活副本的选择顺序见
	// reapDuplicateLiveCopy；一轮最多一个 op，pending 守卫防堆积。
	if c.reapDuplicateLiveCopy(ctx, m, copies) {
		return
	}

	if liveCopy == nil {
		// 当前 execution 无处存活：外来副本的 pinned reap 由 processAgentMachine
		// 已下单或将有 pending 占位，不能边清边建（同节点撞名与换代乒乓）；
		// 等清理收敛后的无副本轮次走 R4/R3 尾部。
		if foreignCount > 0 {
			return
		}
	} else if !agentStateUsable(liveCopy) {
		pending, err := c.store.PendingOperationForMachine(ctx, m.ID)
		if err != nil {
			return
		}
		if pending != nil {
			// 只作废在途 create（换代的那个）：在途 delete/reap 正是清理
			// 死实例的动作，误杀会造成 FAILED→重启→FAILED 乒乓（P1-2 在
			// supersedePendingCreateAndReap 的同一教训；评审 P3）。
			if pending.Kind != "create" {
				return
			}
			_ = c.store.CompleteOperation(ctx, pending.ID, "FAILED", nil,
				"superseded: dead instance of current execution, reap first")
			_ = c.resv.Release(ctx, pending.ID)
			c.recordEvent(ctx, "reconcile", m.ID, pending.ID, nodeID,
				"superseded pending op; dead instance cleanup", nil)
			c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "supersede_pending_dead"}, 1)
			return
		}
		c.recordEvent(ctx, "reconcile", m.ID, "", liveNodeID,
			"agent holds dead instance of current execution, reap first", nil)
		_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
			"op-reap-"+m.ID+"-"+m.CurrentExecutionID, liveNodeID, "dead instance")
		return
	} else {
		// v1.2-D（ADR-0026）：agent 观测到 STOPPED = 本 execution 已退出。
		// 只有符合 restart policy 的“意外退出”才换代重启；TTL/manual delete
		// 的 machine 到不了这里（desired 已 DELETED 走 R5）。
		if liveCopy.State == pb.MachineState_STOPPED {
			if c.rolloutRepairsMachine(ctx, m) {
				// The rollout owner must do real repair, not merely suppress restart.
				// Reap the stopped execution first; after it disappears the rollout
				// owner below creates the replacement without colliding at the agent.
				hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
				if err == nil && !hasPending {
					_ = c.enqueueDelete(ctx, m.ID, m.CurrentExecutionID, m.Generation,
						"op-rollout-reap-"+m.ID+"-"+m.CurrentExecutionID,
						liveNodeID, "rollout target stopped; reap before replacement")
				}
				return
			}
			if c.rolloutHoldsRecreate(ctx, m) {
				return // draining/removal generation: rollout intentionally does not repair
			}
			if c.maybeRestartMachine(ctx, m, liveCopy) {
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
		"op-orphan-"+m.ID+"-"+agent.ExecutionId+"-"+nodeID, nodeID, "stale execution")
}

// v1.2-D（ADR-0026）：TTL 到期回收与 restart 决策。
//
// recreate 尾部动作常量（P1-3，决策纯函数便于表驱动测试）。
const (
	actionNone        = "none"
	actionWait        = "wait"
	actionRecreate    = "recreate"     // 换代重建：新 execution + generation+1
	actionRetryCreate = "retry_create" // 同 execution 重派：不推动 generation
)

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
		// 同源还原 health_check / egress：首次 create（enqueueAppMachineCreate）
		// 会携带，重建路径漏掉会让重试/换代副本永久丢失探针（readiness 恒
		// UNCONFIGURED，不达 READY 门控）与 egress 策略——真机 G2c 验收
		// 抓到（agent 重启后 recreate 的 dns-* 机器探针全丢）。
		if len(dep.HealthCheck) > 0 && string(dep.HealthCheck) != "null" {
			var h pb.HealthCheckSpec
			if err := protojson.Unmarshal(dep.HealthCheck, &h); err == nil {
				req.Spec.HealthCheck = &h
			}
		}
		if len(dep.EgressPolicy) > 0 && string(dep.EgressPolicy) != "null" {
			var ep pb.EgressPolicySpec
			if err := protojson.Unmarshal(dep.EgressPolicy, &ep); err == nil {
				if req.Spec.Network == nil {
					req.Spec.Network = &pb.NetworkSpec{}
				}
				req.Spec.Network.Egress = &ep
			}
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

// enqueueOrphanDelete / enqueueDelete 补 delete 操作。nodeID 必传观测节点——
// machine 行已消失是 orphan 的常态，沒有 dispatch_node 时 processDelete 会
// 走“节点不可达”伪装成功分支，不发起任何删除 RPC（真机验收复现：ops
// SUCCEEDED 但 VM 照旧运行）。
func (c *Controller) enqueueOrphanDelete(
	ctx context.Context,
	project string,
	m *pb.Machine,
	nodeID string,
	genFallback int64,
) {
	// R6 的 fence 安全 generation：优先用 agent 观测值（mapMachine 从
	// instance tag 回读）；缺失时回退调用方给的 PG 代际，再回退 1（agent
	// 无 fence 记录的 machine 任意 generation 均放行）。opID 按
	// (machine, execution, node) 作用域（评审 P1）：多节点并存副本需要
	// 每个节点独立的 reap，且被围栏拒绝后收敛的 op 不能永久占用幂等键
	// 阻断对正确节点的派发。
	gen := m.GetGeneration()
	if gen == 0 {
		gen = uint64(genFallback)
	}
	if gen == 0 {
		gen = 1
	}
	req := &pb.DeleteMachineRequest{
		MachineId:   m.MachineId,
		ExecutionId: m.ExecutionId,
		Generation:  gen,
		OperationId: "op-orphan-" + m.MachineId + "-" + m.ExecutionId + "-" + nodeID,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return
	}
	op, err := c.store.EnqueueReapDelete(ctx, project, m.MachineId, m.ExecutionId,
		req.OperationId, int64(req.Generation), raw)
	if err != nil {
		slog.Error("enqueue orphan delete", "machine_id", m.MachineId, "error", err)
		return
	}
	if op.Status != "PENDING" && op.Status != "CLAIMED" {
		// 复验发现：agent 恢复窗口内 delete 会在运行时视图未装齐时返回成功
		//（实例“尚未出现”按阶段完成收敛），实例随后恢复列出——同名实例名
		// 网络分配被永久占用，重建撞名循环。终态 op 的幂等键命中不能的；
		// 副本仍被观测 = 上次效果落空，必须派生新 op 重执（按分钟分桶限流）。
		req.OperationId = req.OperationId + "-re" + time.Now().Format("0601021504")
		raw2, err := protojson.Marshal(req)
		if err != nil {
			return
		}
		op, err = c.store.EnqueueReapDelete(ctx, project, m.MachineId, m.ExecutionId,
			req.OperationId, int64(req.Generation), raw2)
		if err != nil {
			slog.Error("re-arm orphan delete", "machine_id", m.MachineId, "error", err)
			return
		}
		c.recordEvent(ctx, "reconcile", m.MachineId, req.OperationId, nodeID,
			"orphan still observed after terminal op; re-armed delete", nil)
	}
	if nodeID != "" {
		_ = c.store.UpdateOperationDispatchNode(ctx, op.ID, nodeID)
	}
	c.recordEvent(ctx, "reconcile", m.MachineId, req.OperationId, nodeID,
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
	if op.Status != "PENDING" && op.Status != "CLAIMED" {
		// 与 enqueueOrphanDelete 同一效应检查（评审复验的“恢复窗口假成功”）：
		// 终态命中说明上次已无效果终止而副本仍被观测 → 派生新 op 重执。
		opID = opID + "-re" + time.Now().Format("0601021504")
		req = &pb.DeleteMachineRequest{
			MachineId:   machineID,
			ExecutionId: executionID,
			Generation:  uint64(generation),
			OperationId: opID,
		}
		raw, err = protojson.Marshal(req)
		if err != nil {
			return err
		}
		op, err = c.store.EnqueueReapDelete(ctx, project, machineID, executionID, opID, generation, raw)
		if err != nil {
			return err
		}
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
		if errors.Is(err, store.ErrRequestConflict) {
			slog.Warn("ttl delete idempotency conflict", "machine_id", m.ID, "error", err)
			continue
		}
		if err != nil {
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
