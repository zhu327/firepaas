package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/ulanet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

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

// ---------------------------------------------------------------------------
// 操作 reconcile（PG outbox → agent）
// ---------------------------------------------------------------------------

func (c *Controller) reconcileOperations(ctx context.Context) error {
	ops, err := c.store.ClaimPendingOperations(ctx, 20)
	if err != nil {
		return err
	}
	// R2 评审 P1（有界并发派发）：串行派发意味着一个挂死的 agent 会把
	// 整批 20 个 op 拖到 20×AgentRPCTimeout（理论上 40 分钟才轮到下一轮
	// tick）。改为有界工作池（默认 4）；同一台 machine 的 op 经 per-machine
	// 锁串行；账本语义不变（每 op 仍是本进程的单写者：claim→process→
	// complete/requeue 都只在持有 machine 锁的一个 worker 里发生）。
	// buildRoutes 等全部 worker 完成后统一执行（路由按批次末尾的一致
	// 状态构建，与串行时代一致）。
	if len(ops) > 0 {
		dispatchBounded(ctx, ops, c.cfg.DispatchWorkers, c.lockMachine, c.dispatchOne)
		return c.buildRoutes(ctx)
	}
	return nil
}

// dispatchBounded：把 ops 分发给固定个数的 worker，process 在持有
// per-machine 锁的状态下串行执行同机 op。workers<=1 退化为串行（与原语义
// 完全一致）。本函数只为测试与生层复用，不含记账（由 process 回调负责）。
func dispatchBounded(ctx context.Context, ops []store.Operation, workers int,
	lock func(machineID string) func(), process func(ctx context.Context, op store.Operation),
) {
	if workers <= 1 || len(ops) <= 1 {
		for _, op := range ops {
			unlock := lock(op.MachineID)
			process(ctx, op)
			unlock()
		}
		return
	}
	ch := make(chan store.Operation)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range ch {
				unlock := lock(op.MachineID)
				process(ctx, op)
				unlock()
			}
		}()
	}
	for _, op := range ops {
		ch <- op
	}
	close(ch)
	wg.Wait()
}

// machineDispatchLock 是 per-machine 派发互斥 + 引用计数（在途持锁/等待
// 者数），供使用后安全摘除：只有 refs==0 才从 map 删除，摘除时必有
// machineLocksMu，与 refs++ 同临界区，不存在“摘除后仍有人持有旧锁”
// 的窗口（评审 R2 P2：机器 map 不能只增不减）。
type machineDispatchLock struct {
	mu   sync.Mutex
	refs int
}

// lockMachine 获取该 machine 的派发互斥（进程内；不同机互不阻塞）。
// 返回的 unlock 同时归还引用并在无人使用时回收条目。
func (c *Controller) lockMachine(machineID string) func() {
	c.machineLocksMu.Lock()
	m, ok := c.machineLocks[machineID]
	if !ok {
		m = &machineDispatchLock{}
		c.machineLocks[machineID] = m
	}
	m.refs++
	c.machineLocksMu.Unlock()
	m.mu.Lock()
	return func() {
		m.mu.Unlock()
		c.machineLocksMu.Lock()
		m.refs--
		if m.refs == 0 && c.machineLocks[machineID] == m {
			delete(c.machineLocks, machineID)
		}
		c.machineLocksMu.Unlock()
	}
}

// dispatchOne 处理单个已 CLAIMED 的操作（在 per-machine 锁内被调用）。
func (c *Controller) dispatchOne(ctx context.Context, op store.Operation) {
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

// processLifecycle 执行 pause/resume 操作（M4.5）。
// 成功：把 observed_state 写为 PAUSED/RUNNING 并落账 SUCCEEDED；
// 失败（无快照等 FailedPrecondition）：pause 可安全重试；resume 视为
// 快照不可用 → 将机器转 R3 重建路径（observed 清空 + desired CREATED，
// 生成新 execution 走 cold-start），本 op 终态 FAILED。
//
// R2 评审 P0#3（fenced 派发）：本 op 只能作用于入队时的 (execution,
// generation)——两个固定点校验：
//  1. 派发前：机器当前 fence 与 op 不一致 → SUPERSEDED 终态（不发起 RPC。
//     旧 pause 作用于新 execution 会快照到不该被睡眠的新代 VM）。
//  2. 落账时：observed 写回按同一 fence 对 CAS；CAS 失败不记 SUCCEEDED
//     （op 置 SUPERSEDED；agent 侧效果由新代的对账路径接管）。
func (c *Controller) processLifecycle(ctx context.Context, op store.Operation) error {
	var req pb.MachineOperationRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	m, err := c.store.GetMachine(ctx, op.MachineID)
	if err != nil {
		return err // PG 抖动：requeue 重试（不能误判终态）
	}
	if m == nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "machine gone")
		return nil
	}
	exec, gen := op.ExecutionID, op.Generation
	if m.CurrentExecutionID != exec || m.Generation != gen {
		c.metrics.Inc("firepaas_operations_total",
			map[string]string{"kind": op.Kind, "result": "superseded"}, 1)
		slog.Warn("lifecycle op superseded by fence drift",
			"operation_id", op.ID, "kind", op.Kind, "machine_id", op.MachineID,
			"op_execution", exec, "op_generation", gen,
			"current_execution", m.CurrentExecutionID, "current_generation", m.Generation)
		_ = c.store.CompleteOperation(ctx, op.ID, "SUPERSEDED", nil,
			"machine fence moved past this operation (no dispatch)")
		return nil
	}

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
		pbm, err = client.Pause(rpcCtx, op.MachineID, exec, uint64(gen), op.ID)
	} else {
		pbm, err = client.Resume(rpcCtx, op.MachineID, exec, uint64(gen), op.ID)
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
	okCAS, err := c.store.UpdateMachineObservedWithFenceCAS(ctx, op.MachineID, m.NodeID,
		exec, gen, want, m.ObservedSlotIP, m.ObservedReadiness)
	if err != nil {
		return err
	}
	if !okCAS {
		// RPC 与落账之间机器换代（fence 漂移）：绝不记 SUCCEEDED。RPC 已
		// 对旧代执行完毕，按 SUPERSEDED 收敛；新代的观测由 syncObserved 接管。
		c.metrics.Inc("firepaas_operations_total",
			map[string]string{"kind": op.Kind, "result": "superseded"}, 1)
		slog.Warn("lifecycle op result discarded by fence CAS",
			"operation_id", op.ID, "kind", op.Kind, "machine_id", op.MachineID,
			"op_execution", exec, "op_generation", gen)
		_ = c.store.CompleteOperation(ctx, op.ID, "SUPERSEDED", nil,
			"machine fence drifted during dispatch; result discarded")
		return nil
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

	// 验收修复（2026-09-11）：已删机器的存量 create 必须终态收敛，不能
	// 无限重试。app 删除入队 delete 后，旧 create 仍 PENDING；无此守卫它们
	// 每次派发都走 placement→agent（健康环境白白起 VM 再删；异常环境永久
	// poison 挤占派发队头）。复活通道换新 execution 并回写 desired=CREATED，
	// 不命中此分支；终态用 SUPERSEDED（FAILED 会被 resurrectFailed 复活）。
	if m, err := c.store.GetMachine(ctx, op.MachineID); err != nil {
		return err // PG 抖动：requeue 重试（不能误判终态）
	} else if m == nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "machine gone")
		return nil
	} else if m.DesiredState == "DELETED" {
		_ = c.store.CompleteOperation(ctx, op.ID, "SUPERSEDED", nil,
			"machine deleted; dropping stale create (no dispatch)")
		return nil
	}

	// P0（R2 评审）：secrets 主密钥 fail-closed。deployment 带了 secret_refs而
	// 控制面未配置 master key（cfg.Secrets==nil）时，绝不能继续派发——跳过
	// 校验的 VM 会以"缺 secret"的形态上线，运维侧察觉之前流量已受损。
	// 在任何 placement/RPC 之前终止：op FAILED + 高亮日志 + 指标 + 用户事件。
	if c.cfg.Secrets == nil && req.Spec.GetDeploymentId() != "" {
		refs, refsErr := store.DeploymentSecretRefs(ctx, c.store, req.Spec.GetDeploymentId())
		if refsErr != nil {
			return fmt.Errorf("load deployment secret refs: %w", refsErr) // 暂态：requeue
		}
		if len(refs) > 0 {
			reason := "deployment has secret_refs but FIREPAAS_SECRETS_MASTER_KEY is not configured; " +
				"refusing to create a VM without its secrets (fail-closed)"
			slog.Error("create dispatch fail-closed: secrets master key missing",
				"operation_id", op.ID, "machine_id", op.MachineID,
				"deployment_id", req.Spec.GetDeploymentId())
			c.metrics.Inc("firepaas_secret_create_failclosed_total", nil, 1)
			c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID,
				store.UserEventSecretCreateRejected, map[string]any{
					"reason": "secrets master key missing; dispatch fail-closed",
				})
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, reason)
			return nil // 终态：不重试（配置不变不会改变结果）
		}
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
			c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID, store.UserEventMachineCreated,
				map[string]any{"generation": op.Generation, "node_id": m.NodeID, "recovered": true})
			return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", nil, "")
		}
		lease, leaseErr := c.store.SecretLeaseForExecution(ctx, op.MachineID, op.ExecutionID)
		if leaseErr == nil && secretLeaseConfirmsCreate(lease, op, time.Now()) {
			c.metrics.Inc("firepaas_reconcile_actions_total", map[string]string{"kind": "ack_lost_reconcile"}, 1)
			_ = c.resv.Release(ctx, op.ID)
			c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID, store.UserEventMachineCreated,
				map[string]any{"generation": op.Generation, "node_id": m.NodeID, "recovered": true})
			if lease.State == store.SecretLeaseAcked {
				c.userEvent(ctx, op.ProjectID, req.Spec.GetAppId(), op.MachineID, store.UserEventSecretDelivered,
					map[string]any{"generation": op.Generation, "recovered": true})
			}
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
				// ADR-0041 §4.5：本 app 配额拒绝 → 冻结扩容（缩容保持开放）。
				c.noteQuotaRejection(req.Spec.GetAppId())
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			}
			// ADR-0041 §4.5：连续 resources 拒绝 → 冻结扩容（配额同语义）。
			c.notePlacementFailure(ctx, op, req.Spec.GetAppId())
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
		if c.cfg.FabricMesh.Enabled {
			// ADR-0040 T4c：mesh 下为新 execution 分配稳定身份与 ULA（PG
			// desired；fabric reconciler 随快照下发到节点）。分配失败 = 暂态
			// （op 重入列）；节点未入 mesh = 跳过走 legacy（灰度 fail-open）。
			// 位置在 lease CLAIM 与 agent RPC 之前：失败时无任何外部副作用，
			// 重试干净。AllocateULA 对同 machine+execution 幂等收敛。
			if err := c.ensureFabricAllocation(ctx, choice.NodeID, op, &req); err != nil {
				// 已有分配绑定其它节点（前次尝试落在那）→ 本 execution 的
				// ULA 不变（§7 稳定域），改回那台重试而非在别的节点重分配
				// （重分配会与已推送快照竞争，真机 spike 抓到 guest 拿到旧
				// ULA 而库内已换新，路由永不可达）。
				var pinned *store.ErrFabricNodePinned
				if errors.As(err, &pinned) && !excluded[pinned.NodeID] {
					_ = c.resv.Release(ctx, op.ID)
					excluded[choice.NodeID] = true
					c.recordEvent(ctx, "reservation", op.MachineID, op.ID, pinned.NodeID,
						"fabric allocation pinned to node, retrying there", nil)
					continue
				}
				_ = c.resv.Release(ctx, op.ID)
				return fmt.Errorf("fabric allocation: %w", err)
			}
		}
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
		// ADR-0041 §4.5：成功派发证明有容量，清 resources 连续拒绝计数。
		c.notePlacementSuccess(req.Spec.GetAppId())
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")
	}
	c.metrics.Inc("firepaas_placements_total", map[string]string{"result": "failed"}, 1)
	if lastErr == nil {
		lastErr = fmt.Errorf("placement attempts exhausted")
	}
	return lastErr
}

// fabricServiceName 取 deployments.services 首条 Name（ADR-0022 主 service），
// 与 store.FabricIdentities 的 COALESCE(services->0->>'name','default')
// 同口径：无声明（nil/空）= 'default'；有声明则原样取首条 Name（含空串，
// 与 jsonb ->> 语义一致——store 层 Name 无 omitempty，空串会原样落库）。
func fabricServiceName(dep *store.Deployment) string {
	if dep == nil || len(dep.Services) == 0 {
		return fabricDefaultService
	}
	return dep.Services[0].Name
}

// ensureFabricAllocation 在 mesh 启用时为一次 create 派发分配稳定身份与
// ULA（ADR-0040 §6-§8，T4c），供 fabric reconciler 随节点快照下发。
//
// 幂等：同 machine+execution 的重放收敛到同一 /128（store 唯一索引仲裁），
// 先 EnsureWorkloadIdentity 再 AllocateULA——顺序反转会让快照查询 fail closed
// （FabricIdentities 要求分配必有身份行）。
//
// 节点尚未入 mesh（无 wg_peers 行）时返回 nil 跳过，派发走 legacy 路径
// （灰度 fail-open）；其他错误上抛使 op 重入列（暂态，不终态 FAILED）。
func (c *Controller) ensureFabricAllocation(
	ctx context.Context,
	nodeID string,
	op store.Operation,
	req *pb.CreateMachineRequest,
) error {
	peers, err := c.store.ListWGPeers(ctx)
	if err != nil {
		return err
	}
	var nodePrefix *netip.Prefix
	for i := range peers {
		if peers[i].NodeID == nodeID {
			p := peers[i].NodePrefix
			nodePrefix = &p
			break
		}
	}
	if nodePrefix == nil {
		slog.Debug("fabric allocation skipped: node not in mesh",
			"machine_id", op.MachineID, "node_id", nodeID)
		return nil
	}
	cell, err := ulanet.ValidatePrefix(c.cfg.FabricMesh.CellPrefix, 40)
	if err != nil {
		return fmt.Errorf("cell prefix: %w", err)
	}
	project := req.Spec.GetProjectId()
	if project == "" {
		project = op.ProjectID
	}
	service := fabricDefaultService
	if depID := req.Spec.GetDeploymentId(); depID != "" {
		dep, derr := c.store.GetDeployment(ctx, depID)
		if derr != nil {
			return fmt.Errorf("load deployment for fabric identity: %w", derr)
		}
		service = fabricServiceName(dep)
	}
	if _, err := c.store.EnsureWorkloadIdentity(ctx, fabricTrustDomain, project, req.Spec.GetAppId(), service); err != nil {
		return fmt.Errorf("ensure workload identity: %w", err)
	}
	ula, err := c.store.AllocateULA(
		ctx,
		cell,
		*nodePrefix,
		project,
		nodeID,
		op.MachineID,
		op.ExecutionID,
		op.Generation,
	)
	if err != nil {
		return fmt.Errorf("allocate ULA: %w", err)
	}
	slog.Info("fabric ULA allocated", "machine_id", op.MachineID,
		"execution_id", op.ExecutionID, "node_id", nodeID, "ula", ula.String())
	return nil
}

// sweepFabricULA 释放已死亡 execution 的 ULA 行（W2 P0 兜底：create 终态失败、
// AlreadyExists/FailedPrecondition 等无法证明“无 VM”的错误码路径、mesh 关闭期
// 残留、mismatch 收敛跳过释放的残留）。
//
// fail-closed：任一整表查询失败整轮跳过；只在 positive 死亡证据下释放：
//   - machine 行不存在（部署已删）且无该 (machine,execution) 的在途 op；
//   - machine desired=DELETED 且无该 (machine,execution) 的在途 op（终删
//     后残留；含 current 仍指被删 execution 的常规终删情况）；
//   - machine desired=CREATED 但当前 execution 不是该行，且无在途 op
//     （create 终态失败/旧代 superseded 的残留；同 execution 重试中的 op
//     受 in-flight 保护——无论重试复用还是重 mint execution 都正确）。
//
// 当前 execution 的行永不释放（重试/重入列安全）。
// 不做 wg_peers 自动 GC：短暂失联节点被摘除后重注册可能换 /64，全网重编号
// 抖动；退役用 store.DeleteWGPeer 显式执行。
func (c *Controller) sweepFabricULA(ctx context.Context) error {
	rows, err := c.store.ListActiveULAs(ctx, "", "")
	if err != nil {
		return fmt.Errorf("fabric gc list: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	liveOps, err := c.store.ListInFlightOperations(ctx)
	if err != nil {
		return fmt.Errorf("fabric gc live ops: %w", err)
	}
	live := make(map[string]bool, len(liveOps))
	for _, op := range liveOps {
		if op.MachineID != "" && op.ExecutionID != "" {
			live[op.MachineID+"\x00"+op.ExecutionID] = true
		}
	}
	var released int
	for _, row := range rows {
		key := row.MachineID + "\x00" + row.ExecutionID
		if live[key] {
			continue
		}
		m, err := c.store.GetMachine(ctx, row.MachineID)
		if err != nil {
			return fmt.Errorf("fabric gc get machine: %w", err)
		}
		if m == nil {
			// 部署已删：无未来 op 会引用该 execution（execution 服务端 mint）。
		} else if m.DesiredState == "DELETED" {
			// 终删：含 current 仍指被删 execution 的常规终删残留（delete
			// 完成后不清 current_execution_id）；在途 op 已被 liveOps 保护。
		} else if m.CurrentExecutionID == row.ExecutionID {
			continue // 现役 execution：重试/重入列安全起见永不释放。
		} else if m.DesiredState != "CREATED" {
			continue // 未知 desired：保守跳过。
		}
		if err := c.store.ReleaseULA(ctx, row.MachineID, row.ExecutionID); err != nil {
			slog.Warn("fabric gc release", "machine_id", row.MachineID,
				"execution_id", row.ExecutionID, "error", err)
			continue
		}
		released++
	}
	if released > 0 {
		slog.Info("fabric gc released stale ULAs", "count", released)
	}
	return nil
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
	// Pinned reap/delete 必须优先使用 operation.dispatch_node_id：该节点是
	// 观察到目标 execution 的位置。若先取 PG machine.node_id，failover 后
	// 旧 execution 的 reap 会被发往新 execution 所在节点，围栏拒绝后又被
	// 收敛为 SUCCEEDED，形成“证据显示已清理、实例名仍泄漏”的假成功。
	nodeID := op.DispatchNodeID
	if nodeID == "" && m != nil {
		nodeID = m.NodeID
	}
	client := (*agentclient.Client)(nil)
	if nodeID != "" {
		client = c.clientForNodeID(nodeID)
	}
	mismatchConverged := false
	if client == nil {
		// Secret create 的不确定 execution 不能按“节点失联即已删除”收敛；
		// 必须等 fenced delete 获得确定结果，之后才允许换代并签发新 lease。
		if lease, err := c.store.SecretLeaseForExecution(ctx, del.MachineId, del.ExecutionId); err == nil &&
			lease.State == store.SecretLeaseUncertain {
			// 但“不确定”只在 create 确实发到过 agent 才成立：机器从未分配
			// 过节点（create 在 placement/即时拒绝阶段失败）时，任何 agent 都不可能
			// 有该 execution 的工件——继续等节点等于永久活锁（真机验收复现：
			// badmode 注入拒绝场景机器行无 node_id，cleanup 永远无法完成删除）。
			// 另一充分收敛条件：uncertain-cleanup reap 已 SUCCEEDED（意味着确定
			// delete RPC 已在目标 agent 得到应答）——lease 遍历为终态的全部证据
			// 为节点重启/失联而清理代理退避。
			discharged, derr := c.store.SecretCleanupDischarged(ctx, lease.OperationID)
			if derr != nil {
				return derr
			}
			if !discharged {
				dispatched, derr := c.store.CreateDispatchedToAgent(ctx, del.MachineId, del.ExecutionId)
				if derr != nil {
					return fmt.Errorf("secret cleanup dispatch lookup: %w", derr)
				}
				if dispatched {
					return fmt.Errorf("secret cleanup waiting for node %s", nodeID)
				}
				slog.Info("uncertain secret create never dispatched; converging delete",
					"machine_id", del.MachineId, "execution_id", del.ExecutionId)
			} else {
				slog.Info("uncertain secret create cleanup discharged; converging delete",
					"machine_id", del.MachineId, "execution_id", del.ExecutionId)
			}
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
			case deleteErrorConverges(err, m, op.ExecutionID):
				// 目标 execution 已换代（机器行当前 execution 已迁移）且机器整体
				// desired=DELETED：name 作用域的所有可清资材归当前 execution 的
				// 删除链接所有，旧代 delete 无独立残留可清——收敛而非无限重试
				// （真机验收发现的 reap 活锁：generation 屏障拒绝旧 fenced delete）。
				slog.Info("delete of superseded execution converges (machine moved on)",
					"machine_id", del.MachineId, "execution_id", del.ExecutionId,
					"operation_id", op.ID)
				c.recordEvent(ctx, "reconcile", del.MachineId, op.ID, nodeID,
					"superseded execution delete converged", nil)
				// 机器仍存活的 mismatch 收敛只证明“该节点无此 execution”：
				// attachment 释放（下文中）只对终删语义安全；分歧副本可能仍在
				// 其他节点挂载，释放留给真实删除成功/NotFound 的路径（评审 P1）。
				mismatchConverged = m != nil && m.DesiredState != "DELETED"
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
	// 例外：存活机器的 fencing-mismatch 收敛（可能仍有分歧副本挂载在别的节点）
	// 不放配额——真实删除/NotFound 的 op 才释放（评审 P1 双挂载风险）。
	if !mismatchConverged {
		if _, err := c.store.ReleaseTerminalExecutionAttachments(ctx, del.MachineId, del.ExecutionId); err != nil {
			return err
		}
		// ADR-0040 T4c：execution 权威删除后释放其 ULA（行保留供审计/重放，
		// 下一轮快照即不再携带）。尽力而为：失败不阻塞 delete op——快照侧
		// 以 released_at 为准收敛，ReleaseULA 本身幂等。
		// W2：不再以 FabricMesh.Enabled 为门控——mesh 关闭期发生的删除同样
		// 产生幽灵行（开关翻转即泄漏）；ReleaseULA 对无行/已释放幂等成功。
		if err := c.store.ReleaseULA(ctx, del.MachineId, del.ExecutionId); err != nil {
			slog.Warn("fabric ULA release", "machine_id", del.MachineId,
				"execution_id", del.ExecutionId, "error", err)
		}
	}
	c.metrics.Inc("firepaas_operations_total", map[string]string{"kind": "delete", "result": "succeeded"}, 1)
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}
