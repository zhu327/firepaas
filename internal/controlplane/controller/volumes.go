// volumes.go：v1.3-D（ADR-0029）volume 操作派发。
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/security/redact"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// processVolume 处理 volume_create/volume_delete/volume_attach/volume_detach。
func (c *Controller) processVolume(ctx context.Context, op store.Operation) error {
	switch op.Kind {
	case "volume_create":
		return c.processVolumeCreate(ctx, op)
	case "dataset_import":
		return c.processDatasetImport(ctx, op)
	case "volume_delete":
		return c.processVolumeDelete(ctx, op)
	case "volume_attach":
		return c.processVolumeAttach(ctx, op)
	case "volume_detach":
		return c.processVolumeDetach(ctx, op)
	default:
		return fmt.Errorf("unknown volume operation kind %q", op.Kind)
	}
}

// reconcileVolumeNodeState projects node reachability onto node-local volume
// availability and reconciles v1.4-B integrity observations. It never changes
// locality or creates replacement data.
func (c *Controller) reconcileVolumeNodeState(ctx context.Context) {
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		return
	}
	for _, n := range nodes {
		volumes, verr := c.store.ListVolumesOnNode(ctx, n.ID)
		if verr != nil {
			slog.Error("list volumes on node", "node", n.ID, "error", verr)
			continue
		}
		if n.Status != "HEALTHY" {
			if len(volumes) > 0 {
				_, _ = c.store.MarkVolumesUnavailable(ctx, n.ID)
			}
			continue
		}
		if len(volumes) == 0 {
			continue
		}
		client := c.clientForNodeID(n.ID)
		if client == nil {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
		inv, ierr := agentclient.NewVolumeClient(client.RawConn()).ListVolumes(rpcCtx)
		cancel()
		if ierr != nil {
			continue
		}
		c.reconcileVolumeIntegrity(ctx, n, inv, volumes)
	}
}

// reconcileVolumeIntegrity 对账 PG volume 与 origin-node inventory（v1.4-B）。
// 旧 agent（complete=false/无能力）只能产生 UNKNOWN，不推导 MISSING；
// presence 恢复 READY 的既有语义保留（inventory 证明产物存在）。
func (c *Controller) reconcileVolumeIntegrity(ctx context.Context, n store.Node, inv *pb.ListVolumesResponse, volumes []store.Volume) {
	obs := inv.GetObservation()
	supported := hasFeature(n.FeatureIDs, capabilities.LocalInventoryV1) && inv.GetComplete() &&
		obs != nil && obs.GetComplete() && obs.GetEpoch() != "" && obs.GetGeneration() > 0 && obs.GetObservedAtUnix() > 0 &&
		inv.GetObservationGeneration() == obs.GetGeneration() && inv.GetObservedAtUnix() == obs.GetObservedAtUnix()
	c.metrics.Set("firepaas_local_inventory_support", map[string]string{"node_id": n.ID, "type": "volume"}, boolU64(supported))
	if !supported {
		return
	}
	items := make(map[string]store.VolumeInventoryItem, len(inv.GetVolumes()))
	present := make(map[string]*pb.VolumeInfo, len(inv.GetVolumes()))
	for _, item := range inv.GetVolumes() {
		if item == nil || item.GetId() == "" || present[item.GetId()] != nil {
			return
		}
		present[item.GetId()] = item
		items[item.GetId()] = store.VolumeInventoryItem{SizeBytes: int64(item.GetSizeBytes()), Mode: item.GetMode(), ContentDigest: item.GetContentDigest(), Sealed: item.GetSealed(), MetadataHealth: item.GetMetadataHealth()}
	}
	accepted, transitions, err := c.store.ApplyVolumeInventoryObservation(ctx, store.InventoryObservation{NodeID: n.ID, ResourceType: "volume", Epoch: obs.GetEpoch(), Generation: obs.GetGeneration(), AgentObservedAt: time.Unix(obs.GetObservedAtUnix(), 0), ItemCount: len(items)}, items)
	if err != nil {
		slog.Warn("apply volume inventory observation", "node", n.ID, "error", err)
		return
	}
	if !accepted {
		return
	}
	for _, transition := range transitions {
		c.metrics.Inc("firepaas_local_integrity_transitions_total", map[string]string{"type": "volume", "integrity": transition.To}, 1)
		if transition.To == "MISSING" {
			c.userEvent(ctx, transition.ProjectID, "", "", "volume.integrity.missing", map[string]any{"volume_id": transition.ID, "node_id": n.ID})
		}
	}
	pgIDs := make(map[string]bool, len(volumes))
	for _, volume := range volumes {
		pgIDs[volume.ID] = true
	}
	// orphan 报告（只报告，不删除；v1.4-B 无自动回收）。
	var orphanBytes uint64
	for id, item := range present {
		if pgIDs[id] || item == nil {
			continue
		}
		orphanBytes += item.GetSizeBytes()
		if !c.reportedOrphans["volume:"+n.ID+":"+id] {
			c.reportedOrphans["volume:"+n.ID+":"+id] = true
			c.recordEvent(ctx, "inventory", "", "", n.ID,
				fmt.Sprintf("orphan volume artifact %s (%d bytes); report-only", id, item.GetSizeBytes()), nil)
		}
	}
	c.refreshIntegrityMetrics(ctx)
	c.metrics.Set("firepaas_local_orphan_bytes", map[string]string{"type": "volume", "node_id": n.ID}, orphanBytes)
}

func (c *Controller) refreshIntegrityMetrics(ctx context.Context) {
	rows, err := c.store.Pool().Query(ctx, `SELECT 'snapshot',integrity,count(*) FROM snapshots WHERE status NOT IN ('DELETED','LOST') GROUP BY integrity
		UNION ALL SELECT 'volume',integrity,count(*) FROM volumes WHERE state<>'DELETED' GROUP BY integrity`)
	if err != nil {
		return
	}
	defer rows.Close()
	c.metrics.ResetFamily("firepaas_local_integrity")
	for rows.Next() {
		var typ, state string
		var count uint64
		if rows.Scan(&typ, &state, &count) == nil {
			c.metrics.Set("firepaas_local_integrity", map[string]string{"type": typ, "integrity": state}, count)
		}
	}
}

func boolU64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (c *Controller) processVolumeCreate(ctx context.Context, op store.Operation) error {
	var req pb.CreateVolumeRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	nodeID := op.DispatchNodeID
	if nodeID == "" {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, "LOCAL_RW origin node is not fixed")
		return fmt.Errorf("LOCAL_RW origin node is not fixed")
	}
	client := c.clientForNodeID(nodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", nodeID)
	}
	view := c.viewForAgent(nodeID)
	if view == nil || view.n.Info == nil || view.n.Info.Capacity == nil {
		return fmt.Errorf("node %s has no capacity info", nodeID)
	}
	sizeMib := (req.GetSizeBytes() + 1024*1024 - 1) / (1024 * 1024)
	_, _, diskQuota, err := c.store.ProjectQuota(ctx, op.ProjectID)
	if err != nil {
		return err
	}
	if err := c.resv.Acquire(ctx, op.ID, nodeID, op.ProjectID, 0, 0, sizeMib,
		view.n.Info.Capacity.VcpuTotal, view.n.Info.Capacity.MemTotalMib,
		view.n.Info.Capacity.DiskTotalMib, 0, 0, uint64(diskQuota), 0, 0); err != nil {
		return err
	}
	defer func() { _ = c.resv.Release(context.WithoutCancel(ctx), op.ID) }()
	volClient := agentclient.NewVolumeClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	_, err = volClient.CreateVolume(rpcCtx, &req)
	cancel()
	if err != nil {
		return fmt.Errorf("agent create volume: %w", err)
	}
	if err := c.resv.Commit(ctx, op.ID); err != nil {
		// Redis projection is rebuildable; PG completion remains authoritative.
	}
	if err := c.store.TransitionVolume(ctx, req.GetVolumeId(), "CREATING", "READY"); err != nil {
		if err := c.store.TransitionVolume(ctx, req.GetVolumeId(), "UNAVAILABLE", "READY"); err != nil {
			return err
		}
	}
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

func (c *Controller) processDatasetImport(ctx context.Context, op store.Operation) error {
	var req pb.ImportDatasetRequest
	// v1.4：持久化请求可携带控制面观测元数据（如 source_url_digest），
	// proto 信封按未知字段容忍，不得因元数据拒发导入。
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(op.Request, &req); err != nil {
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	view := c.viewForAgent(op.DispatchNodeID)
	if view == nil || view.n.Info == nil || !hasFeature(view.n.Info.FeatureIds, capabilities.VolumeDatasetROV1) {
		return fmt.Errorf("node lacks %s", capabilities.VolumeDatasetROV1)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	resp, err := agentclient.NewVolumeClient(client.RawConn()).ImportDataset(rpcCtx, &req)
	if err != nil {
		// Deterministic validation failures are terminal; retrying the same signed
		// URL/archive cannot succeed and would leave imports pending forever.
		if code := status.Code(err); code == codes.InvalidArgument || code == codes.FailedPrecondition || code == codes.PermissionDenied {
			// The schema has no FAILED volume state. Keep the unsealed artifact in
			// CREATING with import_status=failed; delete accepts this safe state.
			if failErr := c.store.FailDatasetImport(ctx, req.GetVolumeId()); failErr != nil {
				return failErr
			}
			message := "agent import dataset: " + redact.RedactText(err.Error())
			return c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, message)
		}
		return fmt.Errorf("agent import dataset: %w", err)
	}
	if !resp.GetSealed() || resp.GetContentDigest() != req.GetExpectedDigest() {
		return fmt.Errorf("agent returned unsealed or mismatched dataset")
	}
	if err := c.store.SealDataset(ctx, req.GetVolumeId(), resp.GetContentDigest(), int64(resp.GetSizeBytes())); err != nil {
		return err
	}
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

func hasFeature(ids []string, wanted string) bool {
	for _, id := range ids {
		if id == wanted {
			return true
		}
	}
	return false
}

func (c *Controller) processVolumeDelete(ctx context.Context, op store.Operation) error {
	var req pb.DeleteVolumeRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	// Store retries terminal deletes under a suffixed operation row ID. Use that
	// row ID as the agent idempotency key, just like snapshot deletion.
	req.OperationId = op.ID
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		// The materialization is still unconfirmed on its authoritative node.
		// Keep the PG tombstone in DELETING and retry when the client returns.
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	volClient := agentclient.NewVolumeClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	err := volClient.DeleteVolume(rpcCtx, &req)
	cancel()
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	if err := c.store.TransitionVolume(ctx, req.GetVolumeId(), "DELETING", "DELETED"); err != nil {
		return err
	}
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

func (c *Controller) processVolumeAttach(ctx context.Context, op store.Operation) error {
	var req pb.AttachVolumeRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	volClient := agentclient.NewVolumeClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	_, err := volClient.AttachVolume(rpcCtx, &req)
	cancel()
	if err != nil {
		return fmt.Errorf("agent attach volume: %w", err)
	}
	if err := c.store.MarkVolumeAttachmentAttached(ctx, req.GetVolumeId(), req.GetMachineId(), req.GetExecutionId()); err != nil {
		return err
	}
	c.userEvent(ctx, op.ProjectID, "", req.GetMachineId(), "volume.attached",
		map[string]any{"volume_id": req.GetVolumeId()})
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}

func (c *Controller) processVolumeDetach(ctx context.Context, op store.Operation) error {
	var req pb.DetachVolumeRequest
	if err := protojson.Unmarshal(op.Request, &req); err != nil {
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
	client := c.clientForNodeID(op.DispatchNodeID)
	if client == nil {
		return fmt.Errorf("no agent client for node %s", op.DispatchNodeID)
	}
	volClient := agentclient.NewVolumeClient(client.RawConn())
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
	_, err := volClient.DetachVolume(rpcCtx, &req)
	cancel()
	if err != nil {
		return err
	}
	if err := c.store.CompleteVolumeDetach(ctx, req.GetVolumeId(), req.GetMachineId(), req.GetExecutionId()); err != nil {
		return err
	}
	return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")
}
