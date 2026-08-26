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
	"fmt"
	"log/slog"
	"time"

	"github.com/example/firepaas/internal/controlplane/store"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/example/firepaas/shared/pkg/id"
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
		if allReady(toMachines, app.DesiredReplicas) {
			deadline := time.Now().Add(c.rolloutDrainGrace()).UTC().Format(time.RFC3339Nano)
			if err := c.store.RolloutToCutover(ctx, app.ID, deadline); err != nil {
				return err
			}
			c.recordEvent(ctx, "rollout", "", r.ID, "", "cutover: new generation ready, old draining", nil)
			c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": "PREPARING", "to": "CUTOVER"}, 1)
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
		// 回收旧代机器（用户 delete 语义：成功即 desired=DELETED）。
		for _, m := range machines {
			if m.DeploymentID == toDep.ID {
				continue
			}
			_ = c.enqueueUserDelete(ctx, m, "rollout drain: recycle old generation")
		}
		if err := c.store.CompleteRollout(ctx, app.ID, false); err != nil {
			return err
		}
		_ = c.store.SetDeploymentStatus(ctx, toDep.ID, "ACTIVE")
		if fromDep, err := c.deploymentForGeneration(ctx, app.ID, r.FromGeneration); err == nil && fromDep != nil {
			_ = c.store.SetDeploymentStatus(ctx, fromDep.ID, "SUPERSEDED")
		}
		c.recordEvent(ctx, "rollout", "", r.ID, "", "complete: old generation recycled", nil)
		c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": "CUTOVER", "to": "COMPLETE"}, 1)

	case "ROLLING_BACK":
		// S6：删除 to 代机器；全部清完 → COMPLETE(failed=true)。
		remaining := 0
		for _, m := range toMachines {
			_ = c.enqueueUserDelete(ctx, m, "rollback: remove new generation")
			remaining++
		}
		if remaining == 0 {
			if err := c.store.CompleteRollout(ctx, app.ID, true); err != nil {
				return err
			}
			_ = c.store.SetDeploymentStatus(ctx, toDep.ID, "FAILED")
			c.recordEvent(ctx, "rollout", "", r.ID, "", "rollback complete", nil)
			c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": "ROLLING_BACK", "to": "COMPLETE_FAILED"}, 1)
		}
	}
	return nil
}

func (c *Controller) startRollback(ctx context.Context, r *store.Rollout) error {
	if err := c.store.RolloutToRollback(ctx, r.AppID); err != nil {
		return err
	}
	c.recordEvent(ctx, "rollout", "", r.ID, "", "start rollback to previous generation", nil)
	c.metrics.Inc("firepaas_rollout_transitions_total", map[string]string{"from": r.Status, "to": "ROLLING_BACK"}, 1)
	return nil
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
	// 删除后的墓碑（replicas=0）不再对账。
	if app.DesiredReplicas <= 0 {
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

	// 缺失 ordinal（目标代）→ 建。
	for ordinal := 0; ordinal < app.DesiredReplicas; ordinal++ {
		if have[ordinal] {
			continue
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
func (c *Controller) enqueueAppMachineCreate(ctx context.Context, app *store.App, dep *store.Deployment, ordinal int) error {
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
		dep.ImageRef, dep.VCPU, dep.MemMIB, dep.Port, machineID, dep.ID, executionID,
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
func (c *Controller) enqueueUserDelete(ctx context.Context, m store.Machine, reason string) error {
	req := &pb.DeleteMachineRequest{
		MachineId:   m.ID,
		ExecutionId: m.CurrentExecutionID,
		Generation:  uint64(m.Generation),
		OperationId: "op-del-" + m.ID,
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
		req.OperationId, m.Generation, raw)
	if err != nil {
		return err
	}
	if op.Status == "PENDING" {
		c.recordEvent(ctx, "scale", m.ID, op.ID, "", reason, nil)
	}
	return nil
}

// allReady 判定目标代全部 ordinal 就绪（S3 切流条件）：
// observed RUNNING 且 readiness ∈ {READY, UNCONFIGURED}（ADR-0008）。
func allReady(machines []store.Machine, replicas int) bool {
	ready := map[int]bool{}
	for _, m := range machines {
		if m.ObservedState != "RUNNING" {
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
