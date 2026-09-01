package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ScrubConfig struct {
	Enabled  bool
	Interval time.Duration
	Budget   int
}

func DefaultScrubConfig() ScrubConfig {
	return ScrubConfig{Enabled: false, Interval: time.Hour, Budget: 1}
}
func validScrubConfig(c ScrubConfig) bool { return c.Interval > 0 && c.Budget > 0 }

// runScrub verifies at most Budget immutable snapshots per pass. Results are
// committed with revision CAS, so a stale result can never bless new content.
func (c *Controller) runScrub(ctx context.Context) {
	cfg := c.scrub
	if !cfg.Enabled {
		return
	}
	if !validScrubConfig(cfg) {
		slog.Error("scrub disabled: unsafe configuration")
		return
	}
	nodes, e := c.store.ListNodes(ctx)
	if e != nil {
		return
	}
	remaining := cfg.Budget
	for _, n := range nodes {
		if remaining == 0 {
			return
		}
		if n.Status != "HEALTHY" || !nodeHasFeature(n, capabilities.SnapshotScrubV1) {
			continue
		}
		client := c.clientForNodeID(n.ID)
		if client == nil {
			continue
		}
		snaps, e := c.store.ListSnapshotsOnNode(ctx, n.ID)
		if e != nil {
			continue
		}
		sc := agentclient.NewSnapshotClient(client.RawConn())
		for _, snap := range snaps {
			if remaining == 0 {
				return
			}
			if snap.Checksum == "" || snap.Status == "DELETED" || snap.Status == "LOST" {
				continue
			}
			remaining--
			rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.AgentRPCTimeout)
			res, err := sc.ScrubSnapshot(rpcCtx, &pb.ScrubSnapshotRequest{SnapshotId: snap.ID, ExpectedRevision: snap.Checksum})
			cancel()
			if err != nil {
				if status.Code(err) == codes.DataLoss {
					_, _ = c.store.ApplySnapshotScrub(ctx, snap.ID, snap.Checksum, "CORRUPT", "content checksum mismatch")
				}
				continue
			}
			if res.GetRevision() != snap.Checksum || res.GetIntegrity() != "CONTENT_VERIFIED" {
				continue
			}
			_, _ = c.store.ApplySnapshotScrub(ctx, snap.ID, snap.Checksum, "CONTENT_VERIFIED", "")
		}
	}
}
