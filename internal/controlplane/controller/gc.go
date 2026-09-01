// gc.go implements reference-aware, fail-closed node image cache GC.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

var imageDigestPattern = regexp.MustCompile(`^sha256:[a-fA-F0-9]{64}$`)

// GCConfig controls image cache GC. Delete mode is deliberately opt-in.
type GCConfig struct {
	Mode      string
	MinAge    time.Duration
	HighWater float64
	LowWater  float64
	Interval  time.Duration
	Grace     time.Duration
}

func DefaultGCConfig() GCConfig {
	// Destructive behavior is opt-in. Operators must first inspect dry-run
	// evidence and complete a quarantine/rollback rehearsal.
	return GCConfig{
		Mode:      "off",
		MinAge:    time.Hour,
		HighWater: 0.85,
		LowWater:  0.70,
		Interval:  5 * time.Minute,
		Grace:     10 * time.Minute,
	}
}

func validGCConfig(cfg GCConfig) bool {
	return (cfg.Mode == "off" || cfg.Mode == "dry-run" || cfg.Mode == "delete") &&
		cfg.MinAge >= 0 && cfg.Interval > 0 && cfg.Grace > 0 && cfg.LowWater >= 0 &&
		cfg.LowWater < cfg.HighWater && cfg.HighWater <= 1
}

// runGC collects authoritative PG roots and observed local instance roots. Any
// incomplete or unparseable root set disables deletion for that node.
func (c *Controller) runGC(ctx context.Context) {
	cfg := c.gc
	if cfg.Mode == "off" {
		return
	}
	if !validGCConfig(cfg) {
		slog.Error("gc disabled: unsafe configuration", "mode", cfg.Mode)
		return
	}
	pgRoots, err := c.gcPGRoots(ctx)
	if err != nil {
		slog.Error("gc pg roots", "error", err)
		return
	}
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		slog.Error("gc list nodes", "error", err)
		return
	}
	for _, n := range nodes {
		client := c.clientForNodeID(n.ID)
		if cfg.Mode == "delete" && client != nil && nodeHasFeature(n, capabilities.ImageQuarantineV1) {
			c.reconcileImageQuarantines(ctx, n, client)
		}
		if n.Status != "HEALTHY" || len(n.ImageCache) == 0 {
			continue
		}
		if err := c.store.GCMarkSeen(ctx, n.ID, n.ImageCache); err != nil {
			slog.Error("gc mark seen", "node", n.ID, "error", err)
			continue
		}
		if n.DiskTotalMib == 0 {
			continue
		}
		frac := float64(n.DiskUsedMib) / float64(n.DiskTotalMib)
		if frac < cfg.HighWater {
			continue
		}
		if client == nil {
			slog.Warn("gc skipped: no agent client", "node", n.ID)
			continue
		}
		roots, err := gcAgentRoots(ctx, client, pgRoots)
		if err != nil {
			slog.Error("gc live roots", "node", n.ID, "error", err)
			continue
		}
		cache, err := gcCacheRefs(ctx, client)
		if err != nil {
			slog.Error("gc cache references", "node", n.ID, "error", err)
			continue
		}
		if err := validateReportedCache(n.ImageCache, cache); err != nil {
			slog.Error("gc reported cache", "node", n.ID, "error", err)
			continue
		}
		// v1.4-C：节点作用域 pin（project+digest+selector 命中本节点）与在途
		// prewarm 的 digest 是额外 roots；任一查询失败即放弃该节点本轮删除。
		pinned, err := c.store.PinnedDigestsForNode(ctx, n.ID, n.NodePool)
		if err != nil {
			slog.Error("gc pinned digests", "node", n.ID, "error", err)
			continue
		}
		nodeRoots := make(map[string]bool, len(roots)+len(pinned))
		for digest := range roots {
			nodeRoots[digest] = true
		}
		for digest := range pinned {
			nodeRoots[digest] = true
		}
		candidates := c.gcCandidates(ctx, n, nodeRoots, cfg)
		if len(candidates) > 0 {
			c.gcSweepNode(ctx, n, candidates, cache, frac, cfg, client)
		}
	}
}

func nodeHasFeature(n store.Node, feature string) bool {
	for _, f := range n.FeatureIDs {
		if f == feature {
			return true
		}
	}
	return false
}

func (c *Controller) reconcileImageQuarantines(ctx context.Context, n store.Node, client *agentclient.Client) {
	claims, err := c.store.ListActiveLocalGCClaims(ctx)
	if err != nil {
		return
	}
	remote, err := client.ListImageQuarantines(ctx)
	if err != nil {
		return
	}
	byID := map[string]*pb.ImageQuarantine{}
	for _, q := range remote {
		byID[q.GetClaimId()] = q
	}
	for _, claim := range claims {
		if claim.NodeID != n.ID || claim.ArtifactType != "image" {
			continue
		}
		q := byID[claim.ID]
		if q == nil {
			// A successful full agent listing is authoritative for quarantine
			// manifests. CLAIMED + absent proves the ambiguous RPC failed before a
			// durable manifest/rename, so release the claim; QUARANTINED remains
			// fail-closed because absence there is an integrity anomaly.
			if claim.Status == "CLAIMED" {
				_ = c.store.AbortLocalGCClaim(ctx, claim.ID, "agent authoritative quarantine inventory absent")
			}
			continue
		}
		if q.GetDigest() != claim.ArtifactKey {
			continue
		}
		// Quarantine may have committed remotely before its ACK reached the
		// controller. Adopt the returned capability token after claim+digest
		// identity validation, then let the next pass use normal token fencing.
		if claim.Status == "CLAIMED" && q.GetState() == "active" && q.GetToken() != "" {
			_ = c.store.MarkLocalGCQuarantined(ctx, claim.ID, q.GetToken())
			continue
		}
		if (claim.Status != "QUARANTINED" && claim.Status != "FINALIZING") ||
			!store.VerifyLocalGCToken(claim, q.GetToken()) {
			continue
		}
		// ACK loss/crash recovery: remote terminal state is authoritative only
		// after claim token and digest identity validation.
		if q.GetState() == "rolled_back" {
			_ = c.store.CompleteLocalGCClaim(ctx, claim.ID, "ROLLED_BACK", "agent rollback reconciled")
			continue
		}
		if q.GetState() == "finalized" {
			_ = c.store.CompleteLocalGCClaim(ctx, claim.ID, "DELETED", "")
			continue
		}
		if q.GetState() != "active" || time.Now().Before(claim.GraceUntil) {
			continue
		}
		roots, e := c.gcPGRoots(ctx)
		if e != nil {
			continue
		}
		live, e := gcAgentRoots(ctx, client, roots)
		if e != nil {
			continue
		}
		pinned, e := c.store.PinnedDigestsForNode(ctx, n.ID, n.NodePool)
		if e != nil {
			continue
		}
		for d := range pinned {
			live[d] = true
		}
		action := &pb.ImageQuarantineActionRequest{
			ClaimId:     claim.ID,
			Token:       q.GetToken(),
			OperationId: claim.ID + "-finalize",
		}
		if live[claim.ArtifactKey] {
			action.OperationId = claim.ID + "-rollback"
			if client.RollbackImageQuarantine(ctx, action) == nil {
				_ = c.store.CompleteLocalGCClaim(ctx, claim.ID, "ROLLED_BACK", "new root appeared")
			}
			continue
		}
		// Persist the durable fence before the remote call. If the RPC times out,
		// FINALIZING continues blocking pins/root admission until remote state is
		// reconciled; transaction rollback cannot reopen the race.
		if err := c.store.SetLocalGCFinalizing(ctx, claim.ID); err != nil {
			continue
		}
		claim.Status = "FINALIZING"
		_ = c.store.FinalizeImageGCClaim(ctx, claim, func() error {
			// The database lock is intentionally held across this final agent-side
			// reference check/removal; bound it tightly so an unhealthy agent cannot
			// turn the fence into a control-plane-wide write outage. Timeout remains
			// QUARANTINED and is reconciled from remote terminal state next pass.
			finalizeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return client.FinalizeImageQuarantine(finalizeCtx, action)
		})
	}
}

func (c *Controller) gcPGRoots(ctx context.Context) (map[string]bool, error) {
	refs, err := c.store.GCRootImages(ctx)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]bool, len(refs))
	for _, ref := range refs {
		digest, ok := parsePinnedImageDigest(ref)
		if !ok {
			return nil, fmt.Errorf("unresolvable PG image reference %q", ref)
		}
		roots[digest] = true
	}
	return roots, nil
}

func gcAgentRoots(ctx context.Context, client *agentclient.Client, base map[string]bool) (map[string]bool, error) {
	machines, err := client.List(ctx, "")
	if err != nil {
		return nil, err
	}
	roots := make(map[string]bool, len(base)+len(machines))
	for digest := range base {
		roots[digest] = true
	}
	for _, m := range machines {
		if m == nil || m.Spec == nil {
			return nil, fmt.Errorf("live machine has no image specification")
		}
		digest, ok := parsePinnedImageDigest(m.Spec.ImageRef)
		if !ok {
			return nil, fmt.Errorf("unresolvable live image reference for machine %q", m.MachineId)
		}
		roots[digest] = true
	}
	return roots, nil
}

func parsePinnedImageDigest(ref string) (string, bool) {
	digest := imageDigestOf(ref)
	if digest == "" && imageDigestPattern.MatchString(ref) {
		digest = ref
	}
	return digest, imageDigestPattern.MatchString(digest)
}

type gcCachedImage struct {
	ref     string
	sizeMib int64
}

func gcCacheRefs(ctx context.Context, client *agentclient.Client) (map[string]gcCachedImage, error) {
	images, err := client.ListImages(ctx)
	if err != nil {
		return nil, err
	}
	refs := make(map[string]gcCachedImage, len(images)*2)
	for _, img := range images {
		if img == nil || img.ImageRef == "" {
			return nil, fmt.Errorf("cached image has no full reference")
		}
		// GC accounting must use observed ListImages size_mib. Guessing an
		// unknown size can stop too early or delete far beyond the low watermark.
		if img.SizeMib == 0 || img.SizeMib > math.MaxInt64 {
			return nil, fmt.Errorf("cached image %q has unknown or invalid size_mib", img.ImageRef)
		}
		entry := gcCachedImage{ref: img.ImageRef, sizeMib: int64(img.SizeMib)}
		nameDigest, nameOK := parsePinnedImageDigest(img.ImageRef)
		manifestOK := imageDigestPattern.MatchString(img.Digest)
		if !nameOK && !manifestOK {
			return nil, fmt.Errorf("cached image %q has no resolvable digest", img.ImageRef)
		}
		for _, digest := range []string{nameDigest, img.Digest} {
			if !imageDigestPattern.MatchString(digest) {
				continue
			}
			if previous, ok := refs[digest]; ok && previous != entry {
				return nil, fmt.Errorf("cached digest %q resolves to inconsistent entries", digest)
			}
			refs[digest] = entry
		}
	}
	return refs, nil
}

func validateReportedCache(digests []string, refs map[string]gcCachedImage) error {
	for _, digest := range digests {
		if !imageDigestPattern.MatchString(digest) {
			return fmt.Errorf("unparseable reported cache digest %q", digest)
		}
		entry, ok := refs[digest]
		if !ok || entry.ref == "" || entry.sizeMib <= 0 {
			return fmt.Errorf("reported cache digest %q has no sized deletable reference", digest)
		}
	}
	return nil
}

func (c *Controller) gcCandidates(ctx context.Context, n store.Node, roots map[string]bool, cfg GCConfig) []string {
	firstSeen, err := c.store.GCFirstSeen(ctx, n.ID, n.ImageCache)
	if err != nil {
		slog.Error("gc first seen", "node", n.ID, "error", err)
		return nil
	}
	var out []string
	now := time.Now()
	for _, digest := range n.ImageCache {
		if !imageDigestPattern.MatchString(digest) || roots[digest] {
			continue
		}
		seen, ok := firstSeen[digest]
		if !ok || now.Sub(seen) < cfg.MinAge {
			continue
		}
		out = append(out, digest)
	}
	return out
}

func gcDeletionBudgetMib(totalMib int64, usedFraction, lowWater float64) int64 {
	if totalMib <= 0 || usedFraction <= lowWater {
		return 0
	}
	return int64(float64(totalMib) * (usedFraction - lowWater))
}

func gcConsumeBudgetMib(budget, observedSizeMib int64) int64 {
	return budget - observedSizeMib
}

// gcSweepNode re-confirms both PG and live-agent roots immediately before every
// destructive call. A failed confirmation aborts the node sweep.
func (c *Controller) gcSweepNode(
	ctx context.Context,
	n store.Node,
	candidates []string,
	cache map[string]gcCachedImage,
	frac float64,
	cfg GCConfig,
	client *agentclient.Client,
) {
	dry := cfg.Mode != "delete" || !nodeHasFeature(n, capabilities.ImageQuarantineV1)
	c.metrics.Inc("firepaas_gc_candidates", map[string]string{"node": n.ID}, uint64(len(candidates)))
	if dry {
		for _, digest := range candidates {
			slog.Info("gc dry-run candidate", "node", n.ID, "digest", digest)
		}
		return
	}

	deleted := 0
	budget := gcDeletionBudgetMib(n.DiskTotalMib, frac, cfg.LowWater)
	for _, digest := range candidates {
		if budget <= 0 {
			break
		}
		pgRoots, err := c.gcPGRoots(ctx)
		if err != nil {
			slog.Error("gc delete confirmation failed", "node", n.ID, "error", err)
			break
		}
		roots, err := gcAgentRoots(ctx, client, pgRoots)
		if err != nil {
			slog.Error("gc live delete confirmation failed", "node", n.ID, "error", err)
			break
		}
		// v1.4-C：删除前重确认节点作用域 pin/prefetch roots（同一临界区内）。
		pinned, err := c.store.PinnedDigestsForNode(ctx, n.ID, n.NodePool)
		if err != nil {
			slog.Error("gc pinned delete confirmation failed", "node", n.ID, "error", err)
			break
		}
		for digest := range pinned {
			roots[digest] = true
		}
		confirmedCache, err := gcCacheRefs(ctx, client)
		if err != nil {
			slog.Error("gc cache delete confirmation failed", "node", n.ID, "error", err)
			break
		}
		if roots[digest] {
			continue
		}
		confirmed, ok := confirmedCache[digest]
		original, originallyKnown := cache[digest]
		if !ok || !originallyKnown || confirmed != original || confirmed.ref == "" || confirmed.sizeMib <= 0 {
			slog.Error(
				"gc delete confirmation failed",
				"node",
				n.ID,
				"digest",
				digest,
				"error",
				"sized cache reference unavailable",
			)
			break
		}
		claimID := fmt.Sprintf("gc-%s-%d", n.ID, time.Now().UnixNano())
		claimToken := fmt.Sprintf("%s:%s:%d", n.ID, digest, time.Now().UnixNano())
		claim := store.LocalGCClaim{
			ID: claimID, NodeID: n.ID, ArtifactType: "image", ArtifactKey: digest,
			Revision: digest, SizeBytes: confirmed.sizeMib * 1024 * 1024, GraceUntil: time.Now().Add(cfg.Grace), Reason: "image cache high watermark",
		}
		if err := c.store.ClaimImageForGC(ctx, claim, claimToken); err != nil {
			c.recordEvent(ctx, "gc", "", "", n.ID, fmt.Sprintf("claim image %s blocked", digest), nil)
			continue
		}
		q, err := client.QuarantineImage(
			ctx,
			&pb.QuarantineImageRequest{
				ImageRef:         confirmed.ref,
				ClaimId:          claimID,
				OperationId:      claimID + "-quarantine",
				ExpectedRevision: digest,
			},
		)
		if err != nil {
			// RPC failure is ambiguous: the agent may have durably renamed before
			// its ACK was lost. Keep CLAIMED so ListImageQuarantines can adopt or
			// safely resolve it on the next pass.
			continue
		}
		if q.GetState() != "active" || q.GetToken() == "" || q.GetDigest() != digest {
			_ = c.store.AbortLocalGCClaim(ctx, claimID, "agent proved quarantine was not active")
			continue
		}
		if err := c.store.MarkLocalGCQuarantined(ctx, claimID, q.GetToken()); err != nil {
			_ = client.RollbackImageQuarantine(
				ctx,
				&pb.ImageQuarantineActionRequest{
					ClaimId:     claimID,
					Token:       q.GetToken(),
					OperationId: claimID + "-rollback",
				},
			)
			continue
		}
		budget = gcConsumeBudgetMib(budget, confirmed.sizeMib)
	}
	if deleted > 0 {
		c.metrics.Inc("firepaas_gc_deleted", map[string]string{"node": n.ID, "mode": "delete"}, uint64(deleted))
		slog.Info("gc sweep", "node", n.ID, "mode", "delete", "candidates", len(candidates), "deleted", deleted)
	}
}
