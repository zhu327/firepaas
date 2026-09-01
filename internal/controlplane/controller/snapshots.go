// snapshots.go：v1.3-B（ADR-0028）snapshot 控制面：操作派发、observed 状态
// 收敛（UNAVAILABLE↔READY）、schedule 调度（deterministic jitter）与
// retention（max_count/max_age，只清理同 schedule 产物，不删手工 checkpoint）。
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/id"
	"google.golang.org/protobuf/encoding/protojson"
)

// processSnapshot 处理 kind ∈ {snapshot_create, snapshot_delete} 的操作。
func (c *Controller) processSnapshot(ctx context.Context, op store.Operation) error {
	switch op.Kind {
	case "snapshot_create":
		return c.processSnapshotCreate(ctx, op)
	case "snapshot_delete":
		return c.processSnapshotDelete(ctx, op)
	default:
		return fmt.Errorf("unknown snapshot operation kind %q", op.Kind)
	}
}

func (c *Controller) processSnapshotCreate(ctx context.Context, op store.Operation) error {
	var req pb.CreateSnapshotRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	snapClient := agentclient.NewSnapshotClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	info, err := snapClient.CreateSnapshot(rpcCtx, &req)
	cancel()
	if err != nil {
		return fmt.Errorf("agent create snapshot: %w", err)
	}
	checksum := checksumOf(info)
	if info.GetCompatibilityKey() != "" &&
		info.GetCompatibilityKey() != snapshotCompatibilityKey(c, op.DispatchNodeID) {
		err := fmt.Errorf(
			"snapshot compatibility key differs from node: artifact=%q node=%q",
			info.GetCompatibilityKey(),
			snapshotCompatibilityKey(c, op.DispatchNodeID),
		)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	if info.GetSizeBytes() == 0 || checksum == "" || info.GetCompressionState() == "compressing" ||
		info.GetCompressionState() == "error" {
		err := fmt.Errorf("snapshot artifact incomplete: size=%d checksum_present=%t compression_state=%q",
			info.GetSizeBytes(), checksum != "", info.GetCompressionState())
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	level := int(-1)
	snap, uerr := c.store.UpdateSnapshotArtifact(ctx, req.GetSnapshotId(),
		int64(info.GetSizeBytes()), checksum, info.GetCompressionState(),
		info.GetCompressionAlgorithm(), info.GetFilesystemConsistency(), &level, true)
	if uerr != nil {
		// Agent ACK without durable observed convergence must remain retryable;
		// otherwise the operation can succeed while PG never publishes READY.
		return fmt.Errorf("update snapshot artifact: %w", uerr)
	} else if snap != nil && snap.Status == "READY" {
		c.userEvent(ctx, snap.ProjectID, "", snap.SourceMachineID, "snapshot.ready",
			map[string]any{"snapshot_id": snap.ID, "kind": snap.Kind, "size_bytes": snap.SizeBytes})
	}
	result, _ := protojson.Marshal(info)
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")
}

func checksumOf(info *pb.SnapshotInfo) string {
	if info == nil {
		return ""
	}
	return info.GetArtifactSha256()
}

func (c *Controller) processSnapshotDelete(ctx context.Context, op store.Operation) error {
	var req pb.DeleteSnapshotRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	// The operation row is the durable identity; re-keyed delete attempts
	// (v1.4-A terminal re-enqueue) dispatch under the row ID so agent ledger
	// replay stays consistent with the PG outbox key.
	req.OperationId = op.ID
	referenced, err := c.store.SnapshotReferenced(ctx, req.GetSnapshotId())
	if err != nil {
		return err
	}
	if referenced {
		// References follow their owning operation's terminal state and are
		// reaped by ReleaseTerminalOperationReferences. A delete blocked by an
		// active consumer must stay retryable instead of terminally failing.
		return fmt.Errorf("snapshot has active fork/restore references; delete deferred")
	}
	cur, err := c.store.GetSnapshot(ctx, req.GetSnapshotId())
	if err != nil {
		return err
	}
	if cur == nil {
		// 业务行缺失（理论上不可达）：不得静默返回，否则 op 滞留 CLAIMED。
		return fmt.Errorf("snapshot %s disappeared during delete", req.GetSnapshotId())
	}
	if cur.Status == "DELETED" {
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
	}
	if cur.Status != "DELETING" {
		if _, err := c.store.TransitionSnapshot(ctx, req.GetSnapshotId(), cur.Status, "DELETING"); err != nil {
			// A concurrent worker may have already advanced the row to DELETING;
			// re-read and continue instead of failing the operation.
			cur, rerr := c.store.GetSnapshot(ctx, req.GetSnapshotId())
			if rerr != nil || cur == nil || cur.Status != "DELETING" {
				if rerr == nil && cur != nil {
					return c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, store.ErrSnapshotStatusConflict.Error())
				}
				return err
			}
		}
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client != nil {
		snapClient := agentclient.NewSnapshotClient(client.RawConn())
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		err := snapClient.DeleteSnapshot(rpcCtx, &req)
		cancel()
		if err != nil {
			// agent 暂不可达：保持 DELETING，等 next cycle 重试。
			return err
		}
	} else {
		// Missing client is a transient reachability/configuration condition. Keep
		// DELETING and the outbox PENDING until the origin agent can acknowledge.
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	if _, err := c.store.TransitionSnapshot(ctx, req.GetSnapshotId(), "DELETING", "DELETED"); err != nil {
		if err == store.ErrSnapshotStatusConflict {
			// A concurrent worker already converged the row to DELETED.
			return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
		}
		return err
	}
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

// EnqueueSnapshotCreate 为 API 构造 snapshot_create 操作（幂等键 = operation
// ID，调用方保证稳定）。nodeID 必须已由调度/机器行确定。
func (c *Controller) EnqueueSnapshotCreate(ctx context.Context, projectID, machineID,
	executionID string, generation uint64, snapID, kind, name, compression, scheduleID string,
	level *int, retentionClass string, nodeID string,
) error {
	req := &pb.CreateSnapshotRequest{
		MachineId:   machineID,
		ExecutionId: executionID,
		Generation:  generation,
		OperationId: "op-snap-" + snapID,
		SnapshotId:  snapID,
		Kind:        pb.SnapshotKind_SNAPSHOT_MEMORY,
		Name:        name,
	}
	if kind == "FILESYSTEM" {
		req.Kind = pb.SnapshotKind_SNAPSHOT_FILESYSTEM
	}
	req.Compression = &pb.SnapshotCompressionSpec{
		Algorithm: pb.SnapshotCompressionSpec_ALGORITHM_UNSPECIFIED,
		Level:     -1,
	}
	switch compression {
	case "zstd":
		req.Compression.Algorithm = pb.SnapshotCompressionSpec_ZSTD
	case "lz4":
		req.Compression.Algorithm = pb.SnapshotCompressionSpec_LZ4
	}
	if level != nil {
		req.Compression.Level = int32(*level)
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	_, op, err := c.store.CreateSnapshotAndEnqueue(ctx, store.Snapshot{
		ID: snapID, ProjectID: projectID, SourceMachineID: machineID,
		SourceExecutionID: executionID, SourceGeneration: int64(generation), Kind: kind, NodeID: nodeID,
		CompatibilityKey: snapshotCompatibilityKey(c, nodeID), Compression: compression, CompressionLevel: level,
		CompressionState: "none", RetentionClass: retentionClass, ScheduleID: scheduleID,
	}, store.EnqueueOperationParams{
		OperationID: req.OperationId, ProjectID: projectID, MachineID: machineID,
		ExecutionID: executionID, Generation: int64(generation), Kind: "snapshot_create",
		Request: raw, DispatchNodeID: nodeID,
	})
	if err == nil && op.Status == "PENDING" {
		c.recordEvent(ctx, "snapshot", machineID, op.ID, nodeID, "snapshot_create enqueued", nil)
	}
	return err
}

func snapshotCompatibilityKey(c *Controller, nodeID string) string {
	if v := c.viewForAgent(nodeID); v != nil && v.n.Info != nil {
		return v.n.Info.SnapshotCompatibilityKey
	}
	return ""
}

// EnqueueSnapshotDelete 为 API 构造 snapshot_delete 操作。
func (c *Controller) EnqueueSnapshotDelete(ctx context.Context, projectID, machineID,
	executionID string, generation uint64, snapID, nodeID string,
) error {
	req := &pb.DeleteSnapshotRequest{
		SnapshotId: snapID, MachineId: machineID, ExecutionId: executionID,
		Generation: generation, OperationId: "op-snap-del-" + snapID,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	op, err := c.store.BeginSnapshotDeleteAndEnqueue(ctx, snapID, store.EnqueueOperationParams{
		OperationID: req.OperationId, ProjectID: projectID, MachineID: machineID,
		ExecutionID: executionID, Generation: int64(generation), Kind: "snapshot_delete",
		Request: raw, DispatchNodeID: nodeID,
	})
	if err != nil {
		return err
	}
	if op.Status == "PENDING" {
		c.recordEvent(ctx, "snapshot", machineID, op.ID, nodeID, "snapshot_delete enqueued", nil)
	}
	// 引用检查 + 删除前状态迁移放在 processSnapshotDelete（agent 成功后才 DELETED）。
	return nil
}

// reconcileSnapshotSchedules 领取到期 schedule 并派发 checkpoint 操作
// （deterministic jitter：schedule ID hash 在 interval 内确定性偏移，宕机后
// 跳到未来周期，不补跑全部历史）。
func (c *Controller) reconcileSnapshotSchedules(ctx context.Context) error {
	schedules, err := c.store.ClaimDueSnapshotSchedules(ctx, time.Now(), 32)
	if err != nil {
		return err
	}
	for _, sc := range schedules {
		m, err := c.store.GetMachine(ctx, sc.MachineID)
		if err != nil || m == nil || m.DesiredState == "DELETED" {
			continue
		}
		if m.CurrentExecutionID == "" || m.NodeID == "" {
			continue
		}
		if m.ObservedState != "RUNNING" && m.ObservedState != "PAUSED" {
			continue
		}
		snapID := "snap-" + id.New()
		if err := c.EnqueueSnapshotCreate(ctx, sc.ProjectID, sc.MachineID,
			m.CurrentExecutionID, uint64(m.Generation), snapID, "MEMORY",
			"", sc.Compression, sc.ID, sc.CompressionLevel, sc.ID, m.NodeID); err != nil {
			slog.Error("enqueue scheduled snapshot", "schedule", sc.ID, "error", err)
		}
	}
	return nil
}

// reconcileSnapshotRetention 对每个 schedule 应用 max_count/max_age：
// 只清理同 schedule 产物（READY/UNAVAILABLE），绝不删除手工 checkpoint。
// 删除前检查 restore/fork 引用由 v1.3.2 引入后接上（当前无引用者）。
func (c *Controller) reconcileSnapshotRetention(ctx context.Context) error {
	// 用 apps 扫描 schedule：schedule 数量小，避免另建全表扫描。
	schedules, err := c.store.ListAllSnapshotSchedules(ctx)
	if err != nil {
		return err
	}
	for _, sc := range schedules {
		snaps, err := c.store.ListSnapshotsForRetention(ctx, sc.ID)
		if err != nil {
			slog.Error("list snapshots for retention", "schedule", sc.ID, "error", err)
			continue
		}
		now := time.Now()
		if sc.MaxCount > 0 && len(snaps) > sc.MaxCount {
			// created_at DESC：超出 max_count 的旧快照删除。
			for _, snap := range snaps[sc.MaxCount:] {
				c.enqueueSnapshotRetentionDelete(ctx, sc, &snap)
			}
		}
		if sc.MaxAgeSeconds > 0 {
			cutoff := now.Add(-time.Duration(sc.MaxAgeSeconds) * time.Second)
			for i := range snaps {
				if snaps[i].CreatedAt.Before(cutoff) {
					c.enqueueSnapshotRetentionDelete(ctx, sc, &snaps[i])
				}
			}
		}
	}
	return nil
}

func (c *Controller) enqueueSnapshotRetentionDelete(
	ctx context.Context,
	sc store.SnapshotSchedule,
	snap *store.Snapshot,
) {
	if snap.Status != "READY" && snap.Status != "UNAVAILABLE" {
		return
	}
	referenced, err := c.store.SnapshotReferenced(ctx, snap.ID)
	if err != nil {
		slog.Warn("check snapshot retention references", "snapshot_id", snap.ID, "error", err)
		return
	}
	if referenced {
		return
	}
	if err := c.EnqueueSnapshotDelete(ctx, sc.ProjectID, snap.SourceMachineID,
		snap.SourceExecutionID, uint64(snap.SourceGeneration), snap.ID, snap.NodeID); err != nil {
		slog.Warn("enqueue snapshot retention delete", "snapshot_id", snap.ID, "error", err)
	}
}

// reconcileSnapshotNodeState 节点失联/恢复时推进 UNAVAILABLE↔READY，并对
// HEALTHY 节点执行 v1.4-B inventory 对账：完整列表可推导 MISSING（产物不
// 存在→ UNAVAILABLE + integrity=MISSING，不自动 LOST）；存在→ METADATA_
// VERIFIED；旧 agent（complete=false）只产生 UNKNOWN。本地 orphan 只报告。
func (c *Controller) reconcileSnapshotNodeState(ctx context.Context) {
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		slog.Error("list nodes for snapshot state", "error", err)
		return
	}
	for _, n := range nodes {
		switch n.Status {
		case "HEALTHY":
			client := c.clientForNodeID(n.ID)
			if client == nil {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
			inv, ierr := agentclient.NewSnapshotClient(client.RawConn()).ListSnapshots(rpcCtx, "", "")
			cancel()
			if ierr != nil {
				slog.Warn("inventory snapshots before availability restore", "node", n.ID, "error", ierr)
				continue
			}
			c.reconcileSnapshotIntegrity(ctx, n, inv)
		case "UNKNOWN":
			if changed, err := c.store.MarkSnapshotsUnavailable(ctx, n.ID); err != nil {
				slog.Warn("mark snapshots unavailable", "node", n.ID, "error", err)
			} else if changed > 0 {
				slog.Warn("snapshots marked UNAVAILABLE", "node", n.ID, "count", changed)
			}
		}
	}
}

// reconcileSnapshotIntegrity 对账 PG snapshot 与 origin-node inventory
// （v1.4-B）。complete=false（旧 agent/未升级）时只保留 UNKNOWN，绝不推导
// MISSING；查询失败在上游已 fail closed（不进入本函数）。
func (c *Controller) reconcileSnapshotIntegrity(ctx context.Context, n store.Node, inv *pb.ListSnapshotsResponse) {
	pgSnaps, err := c.store.ListSnapshotsOnNode(ctx, n.ID)
	if err != nil {
		slog.Error("list snapshots for integrity reconcile", "node", n.ID, "error", err)
		return
	}
	// 旧 agent（complete=false，未上报 inventory 能力）只能产生 UNKNOWN，
	// 不得推导 MISSING；只记录 inventory 支持度观测。
	obs := inv.GetObservation()
	supported := hasFeature(n.FeatureIDs, capabilities.LocalInventoryV1) && inv.GetComplete() &&
		obs != nil && obs.GetComplete() && obs.GetEpoch() != "" && obs.GetGeneration() > 0 && obs.GetObservedAtUnix() > 0 &&
		inv.GetObservationGeneration() == obs.GetGeneration() && inv.GetObservedAtUnix() == obs.GetObservedAtUnix()
	c.metrics.Set(
		"firepaas_local_inventory_support",
		map[string]string{"node_id": n.ID, "type": "snapshot"},
		boolU64(supported),
	)
	if !supported {
		return
	}
	items := make(map[string]store.SnapshotInventoryItem, len(inv.GetSnapshots()))
	present := make(map[string]*pb.SnapshotInfo, len(inv.GetSnapshots()))
	for _, item := range inv.GetSnapshots() {
		if item == nil || item.GetId() == "" || present[item.GetId()] != nil {
			return
		}
		present[item.GetId()] = item
		kind := strings.TrimPrefix(item.GetKind().String(), "SNAPSHOT_")
		items[item.GetId()] = store.SnapshotInventoryItem{
			SizeBytes:        int64(item.GetSizeBytes()),
			Checksum:         item.GetArtifactSha256(),
			Kind:             kind,
			CompatibilityKey: item.GetCompatibilityKey(),
		}
	}
	accepted, transitions, err := c.store.ApplySnapshotInventoryObservation(
		ctx,
		store.InventoryObservation{
			NodeID:       n.ID,
			ResourceType: "snapshot", Epoch: obs.GetEpoch(), Generation: obs.GetGeneration(), AgentObservedAt: time.Unix(obs.GetObservedAtUnix(), 0), ItemCount: len(items),
		},
		items,
	)
	if err != nil {
		slog.Warn("apply snapshot inventory observation", "node", n.ID, "error", err)
		return
	}
	if !accepted {
		return
	}
	for _, transition := range transitions {
		c.metrics.Inc(
			"firepaas_local_integrity_transitions_total",
			map[string]string{"type": "snapshot", "integrity": transition.To},
			1,
		)
		if transition.To == "MISSING" {
			c.userEvent(
				ctx,
				transition.ProjectID,
				"",
				transition.MachineID,
				"snapshot.integrity.missing",
				map[string]any{"snapshot_id": transition.ID, "node_id": n.ID},
			)
		}
	}
	pgIDs := make(map[string]bool, len(pgSnaps))
	for _, snap := range pgSnaps {
		pgIDs[snap.ID] = true
	}
	// orphan：agent 本地存在、PG 无对应行（仅报告，不删除）。
	var orphanBytes uint64
	for id, item := range present {
		if pgIDs[id] || item == nil {
			continue
		}
		orphanBytes += item.GetSizeBytes()
		if !c.reportedOrphans["snapshot:"+n.ID+":"+id] {
			c.reportedOrphans["snapshot:"+n.ID+":"+id] = true
			c.recordEvent(ctx, "inventory", "", "", n.ID,
				fmt.Sprintf("orphan snapshot artifact %s (%d bytes); report-only", id, item.GetSizeBytes()), nil)
		}
	}
	c.refreshIntegrityMetrics(ctx)
	c.metrics.Set("firepaas_local_orphan_bytes", map[string]string{"type": "snapshot", "node_id": n.ID}, orphanBytes)
}

// processFork 处理 kind=fork 操作（v1.3-C）：从 READY snapshot 在 origin node
// fork 新 debug machine。控制面 API 已保证同 project、TTL 必填、无 volume、
// 无 public route。
func (c *Controller) processFork(ctx context.Context, op store.Operation) error {
	var req pb.ForkSnapshotRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	if err := c.store.AcquireSnapshotReference(ctx, req.GetSnapshotId(), op.ID, "fork"); err != nil {
		return err
	}
	snap, err := c.store.GetSnapshot(ctx, req.GetSnapshotId())
	if err != nil || snap == nil {
		return fmt.Errorf("snapshot %s unavailable during fork", req.GetSnapshotId())
	}
	if snap.Status != "READY" || snap.Integrity == "MISSING" || snap.Integrity == "CORRUPT" {
		err := fmt.Errorf("snapshot cannot be forked: status=%s integrity=%s", snap.Status, snap.Integrity)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	view := c.viewForAgent(op.DispatchNodeID)
	required := SnapshotCapability(snap.Kind)
	if view == nil || view.n.Info == nil || !hasFeature(view.n.Info.FeatureIds, required) {
		err := fmt.Errorf("fork requires node capability %s", required)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	// execution-bound 凭证：HMAC 确定性派生（与 create 同源）。
	if c.cfg.Traffic != nil {
		req.ProxyCredential = c.cfg.Traffic.Token(req.GetMachineId(), req.GetExecutionId())
	}
	snapClient := agentclient.NewSnapshotClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	resp, err := snapClient.ForkSnapshot(rpcCtx, &req)
	cancel()
	if err != nil {
		return fmt.Errorf("agent fork snapshot: %w", err)
	}
	if resp.GetMachine() == nil {
		return fmt.Errorf("agent fork returned no machine")
	}
	if resp.GetMachine().GetExecutionId() != req.GetExecutionId() ||
		resp.GetMachine().GetGeneration() != req.GetGeneration() {
		return fmt.Errorf("agent fork returned stale identity: execution=%q generation=%d",
			resp.GetMachine().GetExecutionId(), resp.GetMachine().GetGeneration())
	}
	// 新 machine 行（EPHEMERAL/DEBUG 语义由 API 层写 desired/expiry；这里
	// 只记录 fork 结果）。
	if err := c.store.UpdateMachineNodeAndObserved(ctx, req.GetMachineId(), op.DispatchNodeID,
		req.GetExecutionId(), resp.GetMachine().GetState().String(), resp.GetMachine().GetSlotIp(),
		resp.GetMachine().GetReadiness().String()); err != nil {
		return fmt.Errorf("record forked execution: %w", err)
	}
	result, _ := protojson.Marshal(resp)
	c.userEvent(ctx, op.ProjectID, "", req.GetMachineId(), "machine.forked",
		map[string]any{"snapshot_id": req.GetSnapshotId()})
	if err := c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, ""); err != nil {
		return err
	}
	return c.store.ReleaseSnapshotReference(ctx, req.GetSnapshotId(), op.ID)
}

// releaseSnapshotReferenceOnFailure drops the consumer protection when a
// fork/rescue operation terminally fails, so a failed consumer cannot block
// snapshot deletion forever (v1.4-A).
func (c *Controller) releaseSnapshotReferenceOnFailure(ctx context.Context, snapshotID, operationID string) {
	if err := c.store.ReleaseSnapshotReference(ctx, snapshotID, operationID); err != nil {
		slog.Warn("release snapshot reference after failure", "snapshot_id", snapshotID,
			"operation_id", operationID, "error", err)
	}
}

// RestoreDecision 是 restore 兼容性判定的结构化结果（v1.4-D preflight 与
// 实际 rescue 派发共用同一决策函数，保证 preflight 结论与执行一致）。
type RestoreDecision struct {
	MemoryCompatible bool   // memory 模式可用（kind+完整 key 匹配）
	ResolvedMode     string // 请求模式的解析结果（memory/filesystem）
	Degradable       bool   // auto 在当前差异下能否降级 filesystem
	Reason           string // 不兼容/降级原因（可观测，不含敏感信息）
}

// RestoreCapability validates the resolved restore mode against action-time
// node capabilities. It is shared by API preflight and controller dispatch.
func RestoreCapability(d RestoreDecision, memoryCap, filesystemCap bool) (string, bool) {
	if d.ResolvedMode == "memory" {
		return capabilities.SnapshotMemoryV1, memoryCap && d.MemoryCompatible
	}
	return capabilities.SnapshotFilesystemV1, filesystemCap
}

// AvailableRestoreModes reports modes that are both semantically usable and
// advertised by the target node.
func AvailableRestoreModes(d RestoreDecision, memoryCap, filesystemCap bool) []string {
	modes := make([]string, 0, 2)
	if d.MemoryCompatible && memoryCap {
		modes = append(modes, "memory")
	}
	if filesystemCap {
		modes = append(modes, "filesystem")
	}
	return modes
}

// SnapshotCapability returns the node feature required to consume a snapshot
// without restore-mode conversion (fork).
func SnapshotCapability(kind string) string {
	if strings.EqualFold(kind, "MEMORY") {
		return capabilities.SnapshotMemoryV1
	}
	return capabilities.SnapshotFilesystemV1
}

// DecideRestore 依据 PG 权威事实（kind、compatibility key）与目标节点 key
// 判定 restore 兼容性。memory 需要完整 key 匹配；auto 只对列明的兼容性
// 差异降级 filesystem（ADR-0028 §6）。预检与实际派发必须使用同一函数。
func DecideRestore(snapKind, snapKey, targetKey, mode string) RestoreDecision {
	snapKind = strings.ToUpper(snapKind)
	mode = strings.ToLower(mode)
	compatibleMemory := snapKind == "MEMORY" && snapKey != "" && targetKey != "" && snapKey == targetKey
	d := RestoreDecision{MemoryCompatible: compatibleMemory}
	switch mode {
	case "memory":
		d.ResolvedMode = "memory"
		if !compatibleMemory {
			switch {
			case snapKind != "MEMORY":
				d.Reason = "snapshot kind is not MEMORY"
			case snapKey == "":
				d.Reason = "snapshot has no compatibility key"
			case targetKey == "":
				d.Reason = "target node has no compatibility key"
			default:
				d.Reason = "compatibility key mismatch"
			}
		}
	case "auto":
		if compatibleMemory {
			d.ResolvedMode = "memory"
		} else {
			d.ResolvedMode = "filesystem"
			d.Degradable = true
			if snapKind != "MEMORY" {
				d.Reason = "snapshot kind is not MEMORY; auto resolves to filesystem"
			} else {
				d.Reason = "compatibility key mismatch; auto degrades to filesystem"
			}
		}
	default:
		d.ResolvedMode = "filesystem"
	}
	return d
}

// processRescue 处理 kind=rescue 操作（v1.3-C）：restore_mode 语义见 proto。
func (c *Controller) processRescue(ctx context.Context, op store.Operation) error {
	var req pb.RestoreSnapshotRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	// The rescue API acquired this reference in the same transaction as the
	// execution CAS and outbox insert. Re-acquiring is intentionally idempotent
	// for recovery of operations created by older callers.
	if err := c.store.AcquireSnapshotReference(ctx, req.GetSnapshotId(), op.ID, "restore"); err != nil {
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	snap, err := c.store.GetSnapshot(ctx, req.GetSnapshotId())
	if err != nil {
		return err
	}
	if snap == nil {
		return fmt.Errorf("snapshot %s disappeared during rescue", req.GetSnapshotId())
	}
	if snap.Status != "READY" || snap.Integrity == "MISSING" || snap.Integrity == "CORRUPT" {
		err := fmt.Errorf("snapshot cannot be restored: status=%s integrity=%s", snap.Status, snap.Integrity)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	targetKey := snapshotCompatibilityKey(c, op.DispatchNodeID)
	mode := strings.ToLower(req.GetRestoreMode())
	decision := DecideRestore(snap.Kind, snap.CompatibilityKey, targetKey, mode)
	view := c.viewForAgent(op.DispatchNodeID)
	memoryCap, filesystemCap := false, false
	if view != nil && view.n.Info != nil {
		memoryCap = hasFeature(view.n.Info.FeatureIds, capabilities.SnapshotMemoryV1)
		filesystemCap = hasFeature(view.n.Info.FeatureIds, capabilities.SnapshotFilesystemV1)
	}
	requiredFeature, capable := RestoreCapability(decision, memoryCap, filesystemCap)
	if !capable {
		err := fmt.Errorf("restore mode %s requires node capability %s", decision.ResolvedMode, requiredFeature)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	if mode == "memory" && !decision.MemoryCompatible {
		err := fmt.Errorf("memory restore compatibility mismatch: kind=%q source=%q target=%q",
			snap.Kind, snap.CompatibilityKey, targetKey)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	if mode == "auto" && !decision.MemoryCompatible {
		// ADR-0028 permits auto fallback only for compatibility. Snapshot kind and
		// complete compatibility-key mismatch are the authoritative PG inputs.
		req.RestoreMode = "filesystem"
	}
	// Pass the target key; the agent independently validates it against artifact
	// metadata and fails closed when upstream cannot expose that metadata.
	req.CompatibilityKey = targetKey
	// Restore creates a new execution identity, so rotate the execution-bound
	// traffic credential exactly as create/fork do. It remains request-only.
	if c.cfg.Traffic != nil {
		req.ProxyCredential = c.cfg.Traffic.Token(req.GetMachineId(), req.GetExecutionId())
	}
	snapClient := agentclient.NewSnapshotClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	resp, err := snapClient.RestoreSnapshot(rpcCtx, &req)
	cancel()
	if err != nil {
		return fmt.Errorf("agent restore snapshot: %w", err)
	}
	if resp.GetMachine() == nil {
		err := fmt.Errorf("agent restore returned no machine")
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	if resp.GetMachine().GetExecutionId() != req.GetExecutionId() ||
		resp.GetMachine().GetGeneration() != req.GetGeneration() {
		err := fmt.Errorf("agent restore returned stale identity: execution=%q generation=%d",
			resp.GetMachine().GetExecutionId(), resp.GetMachine().GetGeneration())
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		c.releaseSnapshotReferenceOnFailure(ctx, req.GetSnapshotId(), op.ID)
		return err
	}
	if err := c.store.UpdateMachineNodeAndObserved(ctx, req.GetMachineId(), op.DispatchNodeID,
		req.GetExecutionId(), resp.GetMachine().GetState().String(), resp.GetMachine().GetSlotIp(),
		resp.GetMachine().GetReadiness().String()); err != nil {
		return fmt.Errorf("record restored execution: %w", err)
	}
	result, _ := protojson.Marshal(resp)
	c.userEvent(ctx, op.ProjectID, "", req.GetMachineId(), "machine.rescued",
		map[string]any{"snapshot_id": req.GetSnapshotId(), "restore_mode": resp.GetRestoreModeUsed()})
	if err := c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, ""); err != nil {
		return err
	}
	return c.store.ReleaseSnapshotReference(ctx, req.GetSnapshotId(), op.ID)
}
