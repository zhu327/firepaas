// evacuate.go：v1.1（ADR-0021）节点排水驱离编排。
//
// 每次只驱离一个 node、一个 machine。步骤状态保存在 nodes 表而非内存：
// 删除源 execution 后由既有 R3 重建；仅 replacement 已在非源节点、可服务且
// route-ready 时才清步骤并推进下一台。这样 leader 切换不会丢失等待状态。
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/example/firepaas/internal/controlplane/store"
	"github.com/example/firepaas/shared/pkg/id"
)

func (c *Controller) reconcileEvacuations(ctx context.Context) error {
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		return err
	}
	for i := range nodes {
		n := &nodes[i]
		if !n.Draining || !n.Evacuate {
			continue
		}
		// The store's unique partial index guarantees this is at most one even
		// if a stale database predates the index; do not run concurrent nodes.
		if err := c.reconcileEvacuateNode(ctx, n); err != nil {
			slog.Error("reconcile evacuate", "node_id", n.ID, "error", err)
		}
		return nil
	}
	return nil
}

func (c *Controller) reconcileEvacuateNode(ctx context.Context, node *store.Node) error {
	if node.EvacuationMachineID != "" {
		return c.reconcileEvacuationStep(ctx, node)
	}

	machines, err := c.store.ListMachinesOnNode(ctx, node.ID)
	if err != nil {
		return err
	}
	if len(machines) == 0 {
		if !c.evacuatedNodes[node.ID] {
			c.evacuatedNodes[node.ID] = true
			c.recordEvent(ctx, "evacuate_complete", "", "", node.ID,
				"node evacuated: zero machines remain, safe for maintenance/upgrade", nil)
			c.metrics.Inc("firepaas_evacuate_total", map[string]string{"result": "complete"}, 1)
		}
		return nil
	}
	c.evacuatedNodes[node.ID] = false
	sort.Slice(machines, func(i, j int) bool {
		if machines[i].AppID != machines[j].AppID {
			return machines[i].AppID < machines[j].AppID
		}
		return machines[i].ReplicaOrdinal < machines[j].ReplicaOrdinal
	})
	for i := range machines {
		m := machines[i]
		rl, err := c.store.ActiveRolloutForApp(ctx, m.AppID)
		if err != nil {
			// A failed active-rollout lookup must never race a rollout.
			return err
		}
		if rl != nil {
			c.recordEvent(ctx, "evacuate_skip", m.ID, "", node.ID,
				"active rollout holds machine; retry next round", nil)
			continue
		}
		claimed, err := c.store.StartEvacuationStep(ctx, node.ID, m.ID)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		c.recordEvent(ctx, "evacuate", m.ID, "", node.ID,
			"evacuation step claimed; waiting for replacement before source delete", nil)
		return nil
	}
	return nil
}

func (c *Controller) reconcileEvacuationStep(ctx context.Context, node *store.Node) error {
	m, err := c.store.GetMachine(ctx, node.EvacuationMachineID)
	if err != nil {
		return err
	}
	// The source row may have disappeared through an unrelated app delete.
	if m == nil || m.DesiredState == "DELETED" {
		return c.store.ClearEvacuationStep(ctx, node.ID, node.EvacuationMachineID)
	}
	if m.NodeID != node.ID && c.evacuationReplacementRouteReady(*m) {
		// buildRoutes runs before evacuation on each sync and derives eligibility
		// from this same predicate, including a usable node proxy.
		c.recordEvent(ctx, "evacuate", m.ID, "", node.ID,
			"replacement serving and route-ready on another node; step complete", nil)
		c.metrics.Inc("firepaas_evacuate_total", map[string]string{"result": "step"}, 1)
		return c.store.ClearEvacuationStep(ctx, node.ID, m.ID)
	}
	if node.EvacuationStartedAt != nil && time.Since(*node.EvacuationStartedAt) >= c.cfg.EvacuateStepTimeout {
		// Timeout is a durable progress fence, not permission to skip this
		// machine. After delete-then-recreate the source may already be gone;
		// clearing the marker here would incorrectly advance to another source.
		// Keep waiting for the persisted replacement and emit an actionable
		// timeout event (the normal R3 retry path remains responsible for repair).
		c.recordEvent(ctx, "evacuate_timeout", m.ID, "", node.ID,
			"replacement did not become serving before step timeout; holding step", nil)
		c.metrics.Inc("firepaas_evacuate_total", map[string]string{"result": "timeout"}, 1)
		return nil
	}
	if m.NodeID != node.ID {
		return nil // replacement exists but is not route-ready yet
	}
	hasPending, err := c.store.HasPendingOperationForMachine(ctx, m.ID)
	if err != nil || hasPending {
		return err
	}
	// Start replacement only after the step has been persistently claimed. Reap
	// alone leaves the row on its old execution until a later generic R3 pass;
	// fence it now so the persisted desired state unambiguously requires a new
	// execution, which scheduler placement will keep off the draining source.
	exec := m.CurrentExecutionID
	if exec == "" {
		return nil
	}
	if err := c.store.ResetMachineForRecreate(ctx, m.ID, "exec-evac-"+id.New(), m.Generation+1); err != nil {
		return fmt.Errorf("fence evacuation replacement: %w", err)
	}
	c.recordEvent(ctx, "evacuate", m.ID, "", node.ID,
		"evacuation replacement fenced; scheduling on a non-draining node", nil)
	return nil
}

// evacuationDeleteOperationID accepts any execution string; unlike an unchecked
// exec[len(exec)-8:] slice it is safe for legacy/partially-written short IDs.
func evacuationDeleteOperationID(machineID, executionID string) string {
	suffix := executionID
	if len(suffix) > 8 {
		suffix = suffix[len(suffix)-8:]
	}
	return "op-evac-" + machineID + "-" + suffix
}

// evacuationReplacementRouteReady mirrors the route projection's hard gates.
// An empty readiness is deliberately not accepted: it must never advance a
// drain merely because the agent has not reported health yet.
func (c *Controller) evacuationReplacementRouteReady(m store.Machine) bool {
	if !machineServing(m) {
		return false
	}
	v := c.viewForAgent(m.NodeID)
	return v != nil && (v.proxy != "" || c.cfg.LegacyAgentProxyAddr != "")
}
