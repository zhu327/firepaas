package store

import (
	"context"
	"fmt"
)

// ApplySnapshotScrub writes a content result only while the immutable revision
// still matches the revision scrubbed by the agent.
func (s *Store) ApplySnapshotScrub(ctx context.Context, id, expectedRevision, integrity, reason string) (bool, error) {
	if integrity != "CONTENT_VERIFIED" && integrity != "CORRUPT" {
		return false, fmt.Errorf("invalid scrub integrity %q", integrity)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE snapshots SET integrity=$3,integrity_reason=$4,integrity_observed_at=now(),
		status=CASE WHEN $3='CORRUPT' AND status='READY' THEN 'UNAVAILABLE' WHEN $3='CONTENT_VERIFIED' AND status='UNAVAILABLE' THEN 'READY' ELSE status END,updated_at=now()
		WHERE id=$1 AND checksum=$2 AND status NOT IN ('DELETED','LOST')`, id, expectedRevision, integrity, reason)
	if err != nil {
		return false, fmt.Errorf("apply snapshot scrub: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
