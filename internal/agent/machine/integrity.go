package machine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/tags"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// SnapshotScrubAvailable reports whether the installed hypeman exposes its
// lock-aware, path-free integrity API.
func (a *Adapter) SnapshotScrubAvailable() bool {
	_, ok := a.instances.(instances.SnapshotIntegrityManager)
	return ok
}

// ScrubSnapshot resolves the firepaas stable ID without exposing a local path,
// then delegates content verification to hypeman under its snapshot lock.
func (a *Adapter) ScrubSnapshot(
	ctx context.Context,
	snapshotID, expectedRevision string,
) (*pb.ScrubSnapshotResponse, error) {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, ErrSnapshotUnsupported
	}
	integrity, ok := a.instances.(instances.SnapshotIntegrityManager)
	if !ok {
		return nil, ErrSnapshotUnsupported
	}
	list, err := sp.ListSnapshots(ctx, &instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: snapshotID}})
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot for scrub: %w", err)
	}
	if len(list) != 1 {
		return nil, ErrSnapshotNotFound
	}
	if expectedRevision == "" || list[0].ArtifactSHA256 != expectedRevision {
		return nil, fmt.Errorf("%w: snapshot revision changed", ErrSnapshotIncompatible)
	}
	result, err := integrity.ScrubSnapshot(ctx, list[0].Id)
	if err != nil {
		return nil, err
	}
	if result.ArtifactSHA256 != expectedRevision {
		return nil, fmt.Errorf("%w: scrub result revision changed", ErrSnapshotIncompatible)
	}
	state := "UNKNOWN"
	if result.State == instances.SnapshotIntegrityVerified {
		state = "CONTENT_VERIFIED"
	}
	return &pb.ScrubSnapshotResponse{SnapshotId: snapshotID, Revision: result.ArtifactSHA256, Integrity: state}, nil
}

func IsSnapshotCorrupt(err error) bool {
	return err != nil && strings.Contains(err.Error(), "snapshot checksum mismatch") &&
		!errors.Is(err, ErrSnapshotNotFound) && !errors.Is(err, ErrSnapshotUnsupported) &&
		!errors.Is(err, ErrSnapshotIncompatible)
}
