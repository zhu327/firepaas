package controller

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

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
