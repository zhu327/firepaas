// Package placement prepares coherent control-plane node snapshots and commits
// machine placement decisions. It owns scheduler input derivation, authoritative
// project quota checks, the soft Redis reservation, and dispatch-node persistence.
// Agents retain final admission and are never called from this package.
package placement

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/reservations"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Choice identifies the selected live node without exposing controller-owned
// node types. NomadID is used only by the controller to resolve an agent client.
type Choice struct {
	NodeID  string
	NomadID string
	Proxy   string
}

type liveNode struct {
	NodeID  string
	NomadID string
	Proxy   string
	Status  string
	Node    nodemanager.Node
}

// Service owns machine placement preparation and commitment.
const defaultCompensationTimeout = 5 * time.Second

type Service struct {
	store               *store.Store
	nodes               *nodemanager.Manager
	resv                *reservations.Manager
	placer              *scheduler.Placer
	metrics             *metrics.Registry
	compensationTimeout time.Duration
}

func New(st *store.Store, nodes *nodemanager.Manager, resv *reservations.Manager,
	placer *scheduler.Placer, reg *metrics.Registry, compensationTimeout time.Duration,
) *Service {
	return &Service{
		store: st, nodes: nodes, resv: resv, placer: placer, metrics: reg,
		compensationTimeout: compensationTimeout,
	}
}

func (s *Service) liveNodes() []liveNode {
	discovered := s.nodes.Nodes()
	out := make([]liveNode, 0, len(discovered))
	for _, n := range discovered {
		id := n.NomadNodeID
		if n.Info != nil && n.Info.NodeId != "" {
			id = n.Info.NodeId
		}
		out = append(out, liveNode{NodeID: id, NomadID: n.NomadNodeID, Proxy: n.ProxyAddr, Status: n.Status, Node: n})
	}
	return out
}

// SchedulerNodes builds one scheduler snapshot from one discovery snapshot and
// the corresponding PG projections. Callers use it for non-committing policy
// such as deployment prefetch.
func (s *Service) SchedulerNodes(ctx context.Context) ([]scheduler.Node, error) {
	live := s.liveNodes()
	allocated, err := s.store.AllocatedByNode(ctx)
	if err != nil {
		return nil, err
	}
	pending, err := s.store.PendingUsageByNode(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	return assembleSchedulerNodes(live, allocated, pendingMap(pending), stored), nil
}

func pendingMap(rows []store.PendingUsage) map[string]store.PendingUsage {
	out := make(map[string]store.PendingUsage, len(rows))
	for _, row := range rows {
		out[row.NodeID] = row
	}
	return out
}

func assembleSchedulerNodes(live []liveNode, allocated map[string]store.Allocated,
	pending map[string]store.PendingUsage, stored []store.Node,
) []scheduler.Node {
	draining := map[string]bool{}
	images := map[string]map[string]bool{}
	features := map[string]map[string]bool{}
	for _, n := range stored {
		if n.Draining {
			draining[n.ID] = true
			draining[n.NomadNodeID] = true
		}
		if len(n.ImageCache) > 0 {
			set := make(map[string]bool, len(n.ImageCache))
			for _, digest := range n.ImageCache {
				set[digest] = true
			}
			images[n.ID] = set
		}
		if len(n.FeatureIDs) > 0 {
			features[n.ID] = capabilities.SetOf(n.FeatureIDs)
		}
	}
	out := make([]scheduler.Node, 0, len(live))
	for _, view := range live {
		n := scheduler.Node{
			ID: view.NodeID, Status: view.Status, Pool: view.Node.NodePool,
			Draining: draining[view.NodeID] ||
				draining[view.NomadID], CachedImageDigests: images[view.NodeID], FeatureIDs: features[view.NodeID],
		}
		if info := view.Node.Info; info != nil {
			if n.FeatureIDs == nil && len(info.FeatureIds) > 0 {
				n.FeatureIDs = capabilities.SetOf(info.FeatureIds)
			}
			n.Labels = info.Labels
			if n.Pool == "" {
				n.Pool = info.Labels["node_pool"]
			}
			if info.Capacity != nil {
				n.CPUTotal, n.MemTotalMib, n.DiskTotalMib = info.Capacity.VcpuTotal, info.Capacity.MemTotalMib, info.Capacity.DiskTotalMib
			}
			if info.Usage != nil {
				n.MemAllocated, n.DiskAllocated = info.Usage.MemAllocatedMib, info.Usage.DiskAllocatedMib
				n.CPUPercent, n.MemUsedMib = info.Usage.CpuPercent, info.Usage.MemUsedMib
			}
		}
		if a, ok := allocated[view.NodeID]; ok {
			n.CPUAllocated = uint64(a.VCPU)
			if n.MemAllocated == 0 {
				n.MemAllocated = uint64(a.MemMib)
			}
			if n.DiskAllocated == 0 {
				n.DiskAllocated = uint64(a.DiskMib)
			}
		}
		if p, ok := pending[view.NodeID]; ok {
			n.CPUPending, n.MemPending, n.DiskPending = uint64(p.VCPU), uint64(p.MemMib), uint64(p.DiskMib)
		}
		out = append(out, n)
	}
	return out
}

// Place chooses a node, persists scheduler events, checks PG-authoritative
// quotas, acquires the Redis soft reservation, and records the dispatch node.
func (s *Service) Place(ctx context.Context, op store.Operation, req *pb.CreateMachineRequest,
	excluded map[string]bool,
) (*Choice, error) {
	live := s.liveNodes()
	allocated, err := s.store.AllocatedByNode(ctx)
	if err != nil {
		return nil, err
	}
	pending, err := s.store.PendingUsageByNode(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	deployNodes, err := s.store.MachineNodesByDeployment(ctx)
	if err != nil {
		return nil, err
	}

	spec := req.GetSpec()
	vcpu, memMib := spec.GetVcpu(), spec.GetMemMib()
	if vcpu == 0 {
		vcpu = 1
	}
	if memMib == 0 {
		memMib = 512
	}
	var pool string
	var labels map[string]string
	var antiAffinity bool
	if p := spec.GetPlacement(); p != nil {
		pool, labels = p.GetNodePool(), p.GetLabels()
		antiAffinity = p.GetAntiAffinity() == pb.PlacementConstraints_DEPLOYMENT
	}
	var required []string
	if spec.GetDeploymentId() != "" {
		dep, err := s.store.GetDeployment(ctx, spec.GetDeploymentId())
		if err != nil {
			return nil, fmt.Errorf("deployment required features: %w", err)
		}
		if dep != nil {
			required = RequiredFeatures(dep.RequiredFeatures, len(dep.SecretRefs) > 0, dep.EgressPolicy)
		}
	}
	localNode, err := s.store.MachineLocalRWNode(ctx, op.MachineID)
	if err != nil {
		return nil, fmt.Errorf("volume locality: %w", err)
	}
	if localNode != "" {
		required = append(required, capabilities.VolumeLocalRWV1)
	}

	schedReq := scheduler.Request{
		VCPU: vcpu, MemMib: memMib,
		DiskMib: agentv1.EffectiveDiskMib(spec.GetDiskMib()), DeploymentID: spec.GetDeploymentId(),
		Pool: pool, Labels: labels, AntiAffinity: antiAffinity,
		ExistingDeploymentNodes: deployNodes[spec.GetDeploymentId()], ExcludedNodes: excluded,
		ImageDigest: ImageDigest(spec.GetImageRef()), RequiredFeatures: required, RequiredNodeID: localNode,
	}
	result, err := s.placer.Place(schedReq, assembleSchedulerNodes(live, allocated, pendingMap(pending), stored), nil)
	s.recordSchedulerEvents(ctx, op, result.Events)
	if err != nil {
		return nil, err
	}

	var chosen *liveNode
	for i := range live {
		if live[i].NodeID == result.NodeID {
			chosen = &live[i]
			break
		}
	}
	if chosen == nil {
		return nil, fmt.Errorf("scheduler returned unknown node %s", result.NodeID)
	}
	if chosen.Node.Info == nil || chosen.Node.Info.Capacity == nil {
		return nil, fmt.Errorf("node %s has no capacity info yet", result.NodeID)
	}

	quotaVCPU, quotaMem, quotaDisk, err := s.store.ProjectQuota(ctx, op.ProjectID)
	if err != nil {
		return nil, err
	}
	usedVCPU, usedMem, usedDisk, err := s.store.ProjectUsage(ctx, op.ProjectID)
	if err != nil {
		return nil, err
	}
	if usedVCPU > quotaVCPU || usedMem > quotaMem || usedDisk > quotaDisk {
		return nil, reservations.ErrProjectQuota
	}
	detail, err := s.store.GetProjectQuotaDetail(ctx, op.ProjectID)
	if err != nil {
		return nil, err
	}
	if detail.MachineConcurrency > 0 {
		usage, err := s.store.ProjectMachineUsage(ctx, op.ProjectID)
		if err != nil {
			return nil, err
		}
		if MachineQuotaExceeded(usage, detail.MachineConcurrency) {
			return nil, reservations.ErrProjectQuota
		}
	}
	capacity := chosen.Node.Info.Capacity
	diskMib := agentv1.EffectiveDiskMib(spec.GetDiskMib())
	if err := s.resv.Acquire(ctx, op.ID, chosen.NodeID, op.ProjectID, vcpu, memMib, diskMib,
		capacity.VcpuTotal, capacity.MemTotalMib, capacity.DiskTotalMib,
		uint64(quotaVCPU), uint64(quotaMem), uint64(quotaDisk), uint64(detail.MachineConcurrency)); err != nil {
		s.metric("firepaas_reservations_total", map[string]string{"result": "failed"})
		s.recordEvent(ctx, op, "reservation", chosen.NodeID, err.Error())
		return nil, err
	}
	s.metric("firepaas_reservations_total", map[string]string{"result": "acquired"})
	if err := commitDispatch(ctx, op.ID, chosen.NodeID, s.compensationTimeout,
		s.store.UpdateOperationDispatchNode, s.resv.Release); err != nil {
		return nil, err
	}
	return &Choice{NodeID: chosen.NodeID, NomadID: chosen.NomadID, Proxy: chosen.Proxy}, nil
}

func (s *Service) recordSchedulerEvents(ctx context.Context, op store.Operation, events []scheduler.Event) {
	for _, ev := range events {
		kind := "placement"
		if ev.Kind == "filter_rejection" {
			kind = "filter_rejection"
			s.metric("firepaas_filter_rejections_total", map[string]string{"reason": ev.Reason})
		}
		s.recordEvent(ctx, op, kind, ev.NodeID, ev.Reason)
	}
}

func (s *Service) recordEvent(ctx context.Context, op store.Operation, kind, nodeID, reason string) {
	if err := s.store.RecordSchedulerEvent(ctx, store.SchedulerEvent{
		ProjectID: op.ProjectID, Kind: kind,
		MachineID: op.MachineID, OperationID: op.ID, NodeID: nodeID, Reason: reason,
	}); err != nil {
		slog.Error("record scheduler event", "error", err)
	}
}

func (s *Service) metric(name string, labels map[string]string) {
	if s.metrics != nil {
		s.metrics.Inc(name, labels, 1)
	}
}

func commitDispatch(ctx context.Context, opID, nodeID string, compensationTimeout time.Duration,
	persist func(context.Context, string, string) error,
	release func(context.Context, string) error,
) error {
	if err := persist(ctx, opID, nodeID); err != nil {
		if compensationTimeout <= 0 {
			compensationTimeout = defaultCompensationTimeout
		}
		// The reservation is a soft projection; compensate even if the request was
		// canceled, but bound Redis work so a failed dependency cannot strand this path.
		compensationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensationTimeout)
		defer cancel()
		if releaseErr := release(compensationCtx, opID); releaseErr != nil {
			slog.Warn(
				"release reservation after dispatch persistence failure",
				"operation_id",
				opID,
				"error",
				releaseErr,
			)
		}
		return err
	}
	return nil
}

func MachineQuotaExceeded(usage, limit int64) bool { return limit > 0 && usage > limit }

func ImageDigest(imageRef string) string {
	if i := strings.LastIndex(imageRef, "@"); i >= 0 && i < len(imageRef)-1 {
		return imageRef[i+1:]
	}
	return ""
}

// RequiredFeatures derives startup correctness capabilities from persisted
// deployment semantics. Invalid persisted egress fails closed.
func RequiredFeatures(requiredFeatures []string, hasSecretRefs bool, egressPolicy []byte) []string {
	var out []string
	if len(requiredFeatures) > 0 {
		out = append(out, requiredFeatures...)
	} else if hasSecretRefs {
		out = append(out, capabilities.SecretOneShotV1)
	}
	if len(egressPolicy) > 0 && string(egressPolicy) != "null" {
		var policy pb.EgressPolicySpec
		if err := protojson.Unmarshal(egressPolicy, &policy); err != nil ||
			agentv1.ValidateEgressPolicy(&policy) != nil {
			out = append(out, "egress.invalid-policy")
		} else {
			out = append(out, capabilities.EgressCidrV1)
			if len(policy.GetAllowedDomains()) > 0 {
				out = append(out, capabilities.EgressDomainV1)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
