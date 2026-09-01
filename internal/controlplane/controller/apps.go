// apps.go：M3 app/deployment/replica 对账 + rollout 发布状态机（ADR-0015）。
//
// 对账顺序（controller 主循环在 machine reconcile 之后调用）：
//  1. reconcileRollouts：推进 PREPARING→CUTOVER→COMPLETE / 失败→ROLLING_BACK；
//  2. reconcileAppScale：把每个 app 的目标 deployment（活跃 rollout 的 to
//     代，否则 ACTIVE 代）的 ordinal 集对账到 desired_replicas（缺失建、
//     多余删）。
//
// machine_id 采用 {app}-r{ordinal}-g{generation} 稳定推导（迁移 0006 放宽
// 唯一键为 (app_id, replica_ordinal, generation)，发布窗口内新旧代同 ordinal
// 共存）。machine 行当前 execution 是 create 幂等键稳定性的锚：重试沿用行内
// execution；复活墓碑行（desired=DELETED）时换新 execution（旧 observed 已随
// 换代清除逻辑作废，R8 不会短路）。
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/id"
	"google.golang.org/protobuf/encoding/protojson"
)

// parsePGTime 解析 PG timestamp 文本（两种形态：
// `2026-08-26 18:49:37.22+08` 或 RFC3339）。解析失败返回零值。
func parsePGTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07", s); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999+07", s); err == nil {
		return t
	}
	return time.Time{}
}

const (
	rolloutDefaultTimeout    = 300 * time.Second // PREPARING 超时 → 自动回滚（S3）
	rolloutDefaultDrainGrace = 30 * time.Second  // CUTOVER 后旧代 drain 期限
	rolloutMaxRetries        = 3                 // create 终态 FAILED 重试上限（S2）
)

func (c *Controller) rolloutTimeout() time.Duration {
	if c.cfg.RolloutTimeout > 0 {
		return c.cfg.RolloutTimeout
	}
	return rolloutDefaultTimeout
}

func (c *Controller) rolloutDrainGrace() time.Duration {
	if c.cfg.RolloutDrainGrace > 0 {
		return c.cfg.RolloutDrainGrace
	}
	return rolloutDefaultDrainGrace
}

// reconcileRollouts 推进所有活跃 rollout 的状态机（ADR-0015 决策表 S1-S6）。
func (c *Controller) reconcileRollouts(ctx context.Context) error {
	rollouts, err := c.store.ListActiveRollouts(ctx)
	if err != nil {
		return err
	}
	for i := range rollouts {
		r := &rollouts[i]
		app, err := c.store.GetApp(ctx, r.AppID)
		if err != nil {
			return err
		}
		if app == nil {
			continue // app 已删（级联删除 rollout）
		}
		if err := c.reconcileRollout(ctx, app, r); err != nil {
			slog.Error("reconcile rollout", "app_id", r.AppID, "error", err)
		}
	}
	return nil
}

func (c *Controller) reconcileRollout(ctx context.Context, app *store.App, r *store.Rollout) error {
	toDep, err := c.deploymentForGeneration(ctx, app.ID, r.ToGeneration)
	if err != nil || toDep == nil {
		return fmt.Errorf("rollout target deployment missing: %w", err)
	}
	machines, err := c.store.ListMachinesForApp(ctx, app.ID, false)
	if err != nil {
		return err
	}
	// 按目标 deployment 判定（machine.generation 是 fence 计数器，会被 R3
	// 换代重建 +1，不能作为发布轴）。
	toMachines := filterMachines(machines, func(m store.Machine) bool { return m.DeploymentID == toDep.ID })

	switch r.Status {
	case "PREPARING":
		// S3 超时 → 回滚；S2 重试耗尽 → 回滚；全部 READY → 切流。
		started := parsePGTime(r.StartedAt)
		if !started.IsZero() && time.Since(started) > c.rolloutTimeout() {
			c.recordEvent(ctx, "rollout", "", r.ID, "", "preparing timeout, rollback", nil)
			return c.startRollback(ctx, r)
		}
		for _, m := range toMachines {
			if failedCreateAttemptsExhausted(ctx, c, m.ID) {
				c.recordEvent(ctx, "rollout", m.ID, r.ID, "", "create retries exhausted, rollback", nil)
				return c.startRollback(ctx, r)
			}
		}
		// v1.1（ADR-0018）：部署预取——PREPARING 派发 create 前，向 top-K
		// 候选节点异步 PullImage（尽力而为，失败/超时不阻塞 rollout）。
		c.maybePrefetchRolloutImage(ctx, r, toDep)
		// v1.1-F：rolling 按 ordinal 把 route 切到新代，但不能在此回收旧代。
		// buildRoutes 已将旧代标为 draining；edge 仍可能在 fresh cache 窗口内
		// 使用旧 route/token。旧代必须保留至全部新代已 READY、CUTOVER 的
		// drain grace 到期后再删除，避免 route/token 的短暂空窗。
		if allReady(toMachines, app.DesiredReplicas) {
			deadline := time.Now().Add(c.rolloutDrainGrace()).UTC().Format(time.RFC3339Nano)
			if err := c.store.RolloutToCutover(ctx, app.ID, deadline); err != nil {
				return err
			}
			c.recordEvent(ctx, "rollout", "", r.ID, "", "cutover: new generation ready, old draining", nil)
			c.rolloutUserEvent(ctx, r, "cutover", nil)
			c.metrics.Inc(
				"firepaas_rollout_transitions_total",
				map[string]string{"from": "PREPARING", "to": "CUTOVER"},
				1,
			)
		}

	case "CUTOVER":
		// S4：旧代死亡不重建（drain 期限后统一回收）；S5：新代死亡由
		// machine reconcile（M2 R1-R8）按目标代重建。
		if r.DrainDeadline == nil {
			return nil
		}
		deadline := parsePGTime(*r.DrainDeadline)
		if deadline.IsZero() || time.Now().Before(deadline) {
			return nil
		}
		// 防御性重检：即使 rollout 已进入 CUTOVER，也不能因新代随后掉出
		// route-serving 就删除旧代。这样 edge 只能在新代已可用、且 drain
		// grace 已覆盖 route/token cache 后失去旧 execution。
		if !rollingOldGenerationDeleteAllowed(r.Status, toMachines, app.DesiredReplicas) {
			return nil
		}
		// 回收旧代机器（用户 delete 语义：成功即 desired=DELETED）。
		for _, m := range machines {
			if m.DeploymentID == toDep.ID {
				continue
			}
			_ = c.enqueueUserDelete(ctx, m, "rollout drain: recycle old generation")
		}
		// P2-3：rollout 完成 + deployment ACTIVE/SUPERSEDED 同事务，中途崩溃
		// 不再留下「ACTIVE 指向旧代」的不自愈状态。
		fromID, fromStatus := "", ""
		if fromDep, err := c.deploymentForGeneration(ctx, app.ID, r.FromGeneration); err == nil && fromDep != nil {
			fromID, fromStatus = fromDep.ID, "SUPERSEDED"
		}
		if err := c.store.CompleteRolloutWithStatus(ctx, app.ID, false, toDep.ID, "ACTIVE", fromID, fromStatus); err != nil {
			return err
		}
		c.recordEvent(ctx, "rollout", "", r.ID, "", "complete: old generation recycled", nil)
		c.rolloutUserEvent(ctx, r, "complete", nil)
		c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": "CUTOVER", "to": "COMPLETE"}, 1)

	case "ROLLING_BACK":
		// S6：先确认旧代已可立即服务，再删除 to 代。rolling 可能已回收
		// 已切 ordinal 的旧代；targetDeployment 会持久地补建它们，因此不能
		// 在此抢先 COMPLETE 或造成旧代全部不可服务。
		fromDep, err := c.deploymentForGeneration(ctx, app.ID, r.FromGeneration)
		if err != nil || fromDep == nil {
			return fmt.Errorf("rollback source deployment missing: %w", err)
		}
		fromMachines := filterMachines(machines, func(m store.Machine) bool { return m.DeploymentID == fromDep.ID })
		if !allReady(fromMachines, app.DesiredReplicas) {
			return nil
		}
		remaining := 0
		for _, m := range toMachines {
			if m.DesiredState != "DELETED" {
				_ = c.enqueueUserDelete(ctx, m, "rollback: remove new generation")
				remaining++
			}
		}
		if remaining == 0 {
			if err := c.store.CompleteRolloutWithStatus(ctx, app.ID, true, toDep.ID, "FAILED", fromDep.ID, "ACTIVE"); err != nil {
				return err
			}
			c.recordEvent(ctx, "rollout", "", r.ID, "", "rollback complete; previous generation retained", nil)
			c.metrics.Inc(
				"firepaas_rollout_transitions_total",
				map[string]string{"from": "ROLLING_BACK", "to": "COMPLETE_FAILED"},
				1,
			)
		}
	}
	return nil
}

func (c *Controller) startRollback(ctx context.Context, r *store.Rollout) error {
	if err := c.store.RolloutToRollback(ctx, r.AppID); err != nil {
		return err
	}
	c.recordEvent(ctx, "rollout", "", r.ID, "", "start rollback to previous generation", nil)
	c.rolloutUserEvent(ctx, r, "rollback_started", nil)
	c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": r.Status, "to": "ROLLING_BACK"}, 1)
	return nil
}

// rolloutUserEvent（v1.2-F）：rollout 状态迁移的租户事件。
func (c *Controller) rolloutUserEvent(ctx context.Context, r *store.Rollout, status string, details map[string]any) {
	app, err := c.store.GetApp(ctx, r.AppID)
	if err != nil || app == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	details["status"] = status
	details["from_generation"] = r.FromGeneration
	details["to_generation"] = r.ToGeneration
	c.userEvent(ctx, app.ProjectID, r.AppID, "", store.UserEventRolloutUpdated, details)
}

// reconcileAppScale 把 app 的目标 deployment 对账到 desired_replicas。
func (c *Controller) reconcileAppScale(ctx context.Context) error {
	apps, err := c.store.ListApps(ctx)
	if err != nil {
		return err
	}
	for i := range apps {
		app := &apps[i]
		if err := c.reconcileApp(ctx, app); err != nil {
			slog.Error("reconcile app scale", "app_id", app.ID, "error", err)
		}
	}
	return nil
}

func (c *Controller) reconcileApp(ctx context.Context, app *store.App) error {
	// P0-1：已删除的 app 不补建副本。机器由 deleteApp 经 outbox 下发的
	// delete 操作收敛（R5 会补齐 agent 侧残留）。ListApps 已过滤墓碑行，
	// 这里是双保险（防未来新调用方漏过滤）。
	if app.Deleted {
		return nil
	}
	target, err := c.targetDeployment(ctx, app)
	if err != nil {
		return err
	}
	if target == nil {
		// 没有 deployment（例如 app 刚建、API 还没插 deployment）：等待。
		return nil
	}

	machines, err := c.store.ListMachinesForApp(ctx, app.ID, false)
	if err != nil {
		return err
	}
	// 按 deployment 判定存在性（而非 generation）：R3 换代重建会把
	// machine.generation+1（fence 语义），按 generation 判断会误判“缺失
	// ordinal”并与 M2 的 recreate 抢幂等键（真机事故：双重 create 风暴）。
	have := map[int]bool{}
	for _, m := range machines {
		if m.DeploymentID == target.ID {
			have[m.ReplicaOrdinal] = true
		}
	}

	// rolling 发布的建新节奏以 active rollout 的 target deployment 为准，
	// 不可从 backend 的 deployment 推断。查询失败必须 fail closed，避免在
	// rollout 存在但读取故障时越过 batch 门控。
	rollingLimit := -1
	rl, err := c.store.ActiveRolloutForApp(ctx, app.ID)
	if err != nil {
		return fmt.Errorf("get active rollout for rolling scale: %w", err)
	}
	if rl != nil && rl.Status == "PREPARING" && rl.ToGeneration == target.Generation &&
		target.EffectiveStrategy() == "rolling" {
		// A recreate/retry can temporarily leave more than one row for an
		// ordinal. Batch exposure is defined by distinct ordinals, not rows.
		cutOrdinals := map[int]bool{}
		for _, m := range machines {
			if m.DeploymentID == target.ID && machineServing(m) {
				cutOrdinals[m.ReplicaOrdinal] = true
			}
		}
		rollingLimit = len(cutOrdinals) + rollingBatchSize(app.DesiredReplicas)
	}

	// 缺失 ordinal（目标代）→ 建。
	for ordinal := 0; ordinal < app.DesiredReplicas; ordinal++ {
		if have[ordinal] {
			continue
		}
		if rollingLimit >= 0 && ordinal >= rollingLimit {
			continue // 当前批次未切流，不建下一批
		}
		if err := c.enqueueAppMachineCreate(ctx, app, target, ordinal); err != nil {
			slog.Error("enqueue app machine create", "app_id", app.ID, "ordinal", ordinal, "error", err)
		}
	}

	// 超出 N 的目标代 ordinal → 删（scale down）。
	for _, m := range machines {
		if m.DeploymentID != target.ID || m.ReplicaOrdinal < app.DesiredReplicas {
			continue
		}
		if m.DesiredState == "DELETED" {
			continue
		}
		_ = c.enqueueUserDelete(ctx, m, "scale down")
	}
	return nil
}

// targetDeployment 返回 scale 对账的目标：活跃 rollout 的目标代。
// PREPARING/CUTOVER → to 代（发布中 scale 作用于新代）；
// ROLLING_BACK → from 代（回滚即回归旧代，绝不能重建即将删除的新代）。
func (c *Controller) targetDeployment(ctx context.Context, app *store.App) (*store.Deployment, error) {
	active, err := c.store.ActiveRolloutForApp(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	if active != nil {
		gen := active.ToGeneration
		if active.Status == "ROLLING_BACK" {
			gen = active.FromGeneration
		}
		return c.deploymentForGeneration(ctx, app.ID, gen)
	}
	return c.store.ActiveDeploymentForApp(ctx, app.ID)
}

func (c *Controller) deploymentForGeneration(ctx context.Context, appID string, gen int64) (*store.Deployment, error) {
	deps, err := c.store.ListDeployments(ctx, appID)
	if err != nil {
		return nil, err
	}
	for i := range deps {
		if deps[i].Generation == gen {
			return &deps[i], nil
		}
	}
	return nil, nil
}

// enqueueAppMachineCreate 为 (app, deployment, ordinal) 建立 machine + create 操作。
// execution 稳定性：live 行复用其 current_execution_id；墓碑行/无行换新
// execution（换代清除逻辑会作废旧 observed，避免 R8 短路）。
func (c *Controller) enqueueAppMachineCreate(
	ctx context.Context,
	app *store.App,
	dep *store.Deployment,
	ordinal int,
) error {
	machineID := fmt.Sprintf("%s-r%d-g%d", app.ID, ordinal, dep.Generation)
	executionID := "exec-" + id.New()
	if existing, err := c.store.GetMachine(ctx, machineID); err == nil && existing != nil {
		if existing.DesiredState != "DELETED" && existing.CurrentExecutionID != "" {
			executionID = existing.CurrentExecutionID
		}
	}

	spec := &pb.MachineSpec{
		ProjectId:      app.ProjectID,
		AppId:          app.ID,
		DeploymentId:   dep.ID,
		ReplicaOrdinal: uint32(ordinal),
		ExecutionId:    executionID,
		Hostname:       app.Hostname,
		ImageRef:       dep.ImageRef,
		Vcpu:           uint64(dep.VCPU),
		MemMib:         uint64(dep.MemMIB),
		Env:            dep.Env,
		Network:        &pb.NetworkSpec{IngressPort: uint64(dep.Port)},
	}
	// v1.1（ADR-0017/0022）：auto_standby / services 策略随 deployment 固化。
	applyDeploymentSpecExtras(dep, spec)
	if len(dep.Placement) > 0 && string(dep.Placement) != "null" {
		var p pb.PlacementConstraints
		if err := protojson.Unmarshal(dep.Placement, &p); err == nil {
			spec.Placement = &p
		}
	}
	if len(dep.HealthCheck) > 0 && string(dep.HealthCheck) != "null" {
		var h pb.HealthCheckSpec
		if err := protojson.Unmarshal(dep.HealthCheck, &h); err == nil {
			spec.HealthCheck = &h
		}
	}
	// v1.3-A（ADR-0027）：egress policy 随 deployment 固化；nil = 历史
	// CIDR-only 语义。policy_generation 已等于 deployment generation。
	if len(dep.EgressPolicy) > 0 && string(dep.EgressPolicy) != "null" {
		var ep pb.EgressPolicySpec
		if err := protojson.Unmarshal(dep.EgressPolicy, &ep); err != nil {
			return fmt.Errorf("decode deployment egress policy: %w", err)
		}
		if err := agentv1.ValidateEgressPolicy(&ep); err != nil {
			return fmt.Errorf("validate deployment egress policy: %w", err)
		}
		if spec.Network == nil {
			spec.Network = &pb.NetworkSpec{}
		}
		spec.Network.Egress = &ep
	}

	req := &pb.CreateMachineRequest{
		MachineId:  machineID,
		Spec:       spec,
		Generation: uint64(dep.Generation),
		// 幂等键必须 execution 作用域：M2 recreate 会换代（新 execution），
		// 同一 opID 携不同请求体会撞“幂等键复用”冲突。
		OperationId: "op-" + machineID + "-" + executionID[len(executionID)-8:],
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	op, err := c.store.EnsureAppAndEnqueueCreate(ctx, app.ProjectID, app.ID, app.Hostname,
		dep.ImageRef, dep.VCPU, dep.MemMIB,
		int64(agentv1.EffectiveDiskMib(spec.GetDiskMib())), dep.Port,
		machineID, dep.ID, executionID,
		req.OperationId, dep.Generation, ordinal, raw, placementJSONFor(spec.Placement))
	if err != nil {
		return err
	}
	if op.Status == "PENDING" || op.Status == "CLAIMED" {
		c.recordEvent(ctx, "scale", machineID, op.ID, "", "app controller enqueued create", nil)
	}
	return nil
}

// enqueueUserDelete 是 app 语义的删除（kind=delete，成功即 desired=DELETED）。
// opID 必须嵌入 execution 后缀（P0-2）：墓碑行复活会换新 execution，若 opID
// 只用 machineID，scale down→up→down 的第二次缩容会撞「同幂等键不同请求体」
// 的 ErrRequestConflict，永远 409（与 M2 recreateMachine 撞键问题同类）。
func (c *Controller) enqueueUserDelete(ctx context.Context, m store.Machine, reason string) error {
	opID := userDeleteOpID(m.ID, m.CurrentExecutionID)
	req := &pb.DeleteMachineRequest{
		MachineId:   m.ID,
		ExecutionId: m.CurrentExecutionID,
		Generation:  uint64(m.Generation),
		OperationId: opID,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	project := "dev"
	if pg, err := c.store.GetApp(ctx, m.AppID); err == nil && pg != nil {
		project = pg.ProjectID
	}
	op, err := c.store.EnqueueDelete(ctx, project, m.ID, m.CurrentExecutionID,
		opID, m.Generation, raw)
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			// 同 execution 下的重复删除请求体必然一致；冲突只来自异常路径，
			// 记事件供审计，不中断对账循环。
			c.recordEvent(ctx, "scale", m.ID, opID, "", "user delete idempotency conflict: "+err.Error(), nil)
			return nil
		}
		return err
	}
	if op.Status == "PENDING" {
		c.recordEvent(ctx, "scale", m.ID, op.ID, "", reason, nil)
	}
	return nil
}

// userDeleteOpID 是用户语义 delete 的幂等键：machine + execution 后缀
// （execution 尾部 8 字符，与 create 路径 op-{id}-{exec8} 对齐）。
func userDeleteOpID(machineID, executionID string) string {
	return store.UserDeleteOpID(machineID, executionID)
}

// allReady 判定目标代全部 ordinal 就绪（S3 切流条件）：
// observed RUNNING 且 readiness ∈ {READY, UNCONFIGURED}（ADR-0008）。
// v1.1（ADR-0017）：PAUSED（standby）同样计入——standby 是可服务态，
// readiness 冻结在入睡前值，首请求经 autoresume 唤醒（<5s SLO）；
// PREPARING 期间新代副本无真实流量，idle 到点 standby 不阻塞切流。
func allReady(machines []store.Machine, replicas int) bool {
	ready := map[int]bool{}
	for _, m := range machines {
		switch m.ObservedState {
		case "RUNNING", "PAUSED":
		default:
			continue
		}
		switch m.ObservedReadiness {
		case "READY", "UNCONFIGURED":
			ready[m.ReplicaOrdinal] = true
		}
	}
	for i := 0; i < replicas; i++ {
		if !ready[i] {
			return false
		}
	}
	return true
}

// machineServing 判定单台 machine 是否可服务（route/滚动切流的口径）。
func machineServing(m store.Machine) bool {
	switch m.ObservedState {
	case "RUNNING", "PAUSED":
	default:
		return false
	}
	switch m.ObservedReadiness {
	case "READY", "UNCONFIGURED":
		return true
	}
	return false
}

// applyDeploymentSpecExtras 把 deployment 的 v1.1 扩展字段（auto_standby、
// services）翻译进 MachineSpec。单端口 deployment 不动 spec（请求体字节级
// 不变，存量幂等链路零回归）；多 services 时主 service 端口写入
// network.ingress_port，全部 services 写入 spec.services。
func applyDeploymentSpecExtras(dep *store.Deployment, spec *pb.MachineSpec) {
	if dep == nil || spec == nil {
		return
	}
	if len(dep.AutoStandby) > 0 && string(dep.AutoStandby) != "null" {
		var as pb.AutoStandbyPolicy
		if err := protojson.Unmarshal(dep.AutoStandby, &as); err == nil {
			spec.AutoStandby = &as
		}
	}
	if len(dep.Services) > 1 {
		spec.Services = make([]*pb.ServiceSpec, 0, len(dep.Services))
		for _, s := range dep.Services {
			name := s.Name
			if name == "" {
				name = fmt.Sprintf("port-%d", s.InternalPort)
			}
			spec.Services = append(spec.Services, &pb.ServiceSpec{Name: name, InternalPort: uint32(s.InternalPort)})
		}
		if spec.Network == nil {
			spec.Network = &pb.NetworkSpec{IngressPort: uint64(dep.Services[0].InternalPort)}
		} else {
			spec.Network.IngressPort = uint64(dep.Services[0].InternalPort)
		}
	} else if len(dep.Services) == 1 && spec.Network != nil && spec.Network.IngressPort == 0 {
		// 单 service 声明（新 API）：主端口即该 service 端口。
		spec.Network.IngressPort = uint64(dep.Services[0].InternalPort)
	}
}

func filterMachines(ms []store.Machine, keep func(store.Machine) bool) []store.Machine {
	out := make([]store.Machine, 0, len(ms))
	for _, m := range ms {
		if keep(m) {
			out = append(out, m)
		}
	}
	return out
}

// failedCreateAttemptsExhausted 判断 machine 的 create 是否已重试耗尽（S2）。
// 用 operations.attempts（每次 claim +1，跨 resurrect 累计）而不是数 FAILED 行：
// app controller 的幂等复活会在 FAILED/PENDING 之间来回翻转单行状态。
func failedCreateAttemptsExhausted(ctx context.Context, c *Controller, machineID string) bool {
	// 信号 1：最近一次 create 的 claim 次数（app controller 幂等复活路径，
	// 单行在 FAILED/PENDING 之间翻转，attempts 跨复活累计）。
	attempts, status, err := c.store.LatestCreateAttempt(ctx, machineID)
	if err == nil && status != "SUCCEEDED" && attempts >= rolloutMaxRetries {
		return true
	}
	// 信号 2：最近一次 SUCCEEDED 之后的 FAILED 行数（R3 退避重派路径，
	// 每次重试是新 op 行）。
	if n, err := c.store.FailedCreateAttempts(ctx, machineID); err == nil && n >= rolloutMaxRetries {
		return true
	}
	return false
}

func placementJSONFor(p *pb.PlacementConstraints) []byte {
	if p == nil {
		return nil
	}
	raw, err := protojson.Marshal(p)
	if err != nil {
		return nil
	}
	return raw
}

// ---------------------------------------------------------------------------
// v1.1-F：rolling batch 发布策略（ADR-0015 状态机扩展，不新增状态）
// ---------------------------------------------------------------------------

// rollingBatchSize 计算滚动批次大小：max(1, 25%·目标副本)。
func rollingBatchSize(replicas int) int {
	b := replicas / 4
	if b < 1 {
		b = 1
	}
	return b
}

// rollingOldGenerationDeleteAllowed keeps the per-ordinal route cut separate
// from lifecycle deletion. CUTOVER's elapsed drain deadline supplies the
// edge-cache propagation grace; this predicate additionally requires every
// target ordinal to remain route-serving at the moment old executions are
// deleted. It is strategy-neutral so blue/green receives the same protection.
func rollingOldGenerationDeleteAllowed(rolloutStatus string, target []store.Machine, replicas int) bool {
	return rolloutStatus == "CUTOVER" && allReady(target, replicas)
}

// ---------------------------------------------------------------------------
// v1.1-B：部署预取（ADR-0018）
// ---------------------------------------------------------------------------

// maybePrefetchRolloutImage 在 rollout PREPARING 首次对账时向 top-K 候选节点
// 异步下发 PullImage。尽力而为：失败/超时只记调度事件（prefetch_failed），
// 不阻塞 rollout、不计入重试预算；镜像拉取幂等（leader 切换重发无害）。
func (c *Controller) maybePrefetchRolloutImage(ctx context.Context, r *store.Rollout, toDep *store.Deployment) {
	if c.prefetchedRollouts[r.ID] {
		return
	}
	c.prefetchedRollouts[r.ID] = true
	// 镜像 digest（准入已保证 digest-pinned；无 @ 后缀时不预取）。
	ref := toDep.ImageRef
	i := strings.LastIndex(ref, "@")
	if i < 0 || i == len(ref)-1 {
		return
	}
	digest := ref[i+1:]
	go c.prefetchImage(ctx, r.ID, ref, digest, toDep)
}

// prefetchImage runs under the current leader reconcile context. It may outlive
// one loop iteration, but leader loss cancels every RPC and event write; no
// Background context may let a former leader keep prefetching.
func (c *Controller) prefetchImage(ctx context.Context, rolloutID, imageRef, digest string, toDep *store.Deployment) {
	nodes, req, err := c.prefetchCandidates(ctx, toDep)
	if err != nil {
		c.recordEvent(ctx, "prefetch_failed", "", rolloutID, "", "prefetch candidates: "+err.Error(), nil)
		return
	}
	top := c.placer.PrefetchTopK(req, nodes, c.cfg.PrefetchTopK)
	if len(top) == 0 {
		c.recordEvent(ctx, "prefetch", "", rolloutID, "", "prefetch skipped: no hard placement candidates", nil)
		return
	}
	var wg sync.WaitGroup
	for _, n := range top {
		v := c.viewForAgent(n.ID)
		if v == nil {
			continue
		}
		client := c.nodes.ClientFor(v.nomadID)
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(n scheduler.Node, client *agentclient.Client) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if _, err := client.PullImage(pctx, imageRef); err != nil {
				c.recordEvent(ctx, "prefetch_failed", "", rolloutID, n.ID, "prefetch failed: "+err.Error(), nil)
				c.metrics.Inc("firepaas_prefetch_total", map[string]string{"result": "failed"}, 1)
				return
			}
			c.recordEvent(
				ctx,
				"prefetch",
				"",
				rolloutID,
				n.ID,
				"prefetch succeeded: image cached for placement affinity",
				nil,
			)
			c.metrics.Inc("firepaas_prefetch_total", map[string]string{"result": "succeeded"}, 1)
		}(n, client)
	}
	wg.Wait()
	c.recordEvent(ctx, "prefetch", "", rolloutID, "",
		fmt.Sprintf("prefetch dispatched to top-%d hard placement candidates (digest %s)", len(top), digest), nil)
}

// prefetchCandidates builds the exact hard-candidate request used for create
// placement: pool/labels/resources/deployment anti-affinity and image digest.
func (c *Controller) prefetchCandidates(
	ctx context.Context,
	dep *store.Deployment,
) ([]scheduler.Node, scheduler.Request, error) {
	nodes := c.schedulerNodes(ctx)
	var placement pb.PlacementConstraints
	if len(dep.Placement) > 0 && string(dep.Placement) != "null" {
		if err := protojson.Unmarshal(dep.Placement, &placement); err != nil {
			return nil, scheduler.Request{}, fmt.Errorf("decode deployment placement: %w", err)
		}
	}
	deployNodes, err := c.store.MachineNodesByDeployment(ctx)
	if err != nil {
		return nil, scheduler.Request{}, err
	}
	return nodes, scheduler.Request{
		VCPU: uint64(dep.VCPU), MemMib: uint64(dep.MemMIB), DeploymentID: dep.ID,
		Pool: placement.NodePool, Labels: placement.Labels,
		AntiAffinity:            placement.AntiAffinity == pb.PlacementConstraints_DEPLOYMENT,
		ExistingDeploymentNodes: deployNodes[dep.ID], ImageDigest: imageDigestOf(dep.ImageRef),
	}, nil
}
