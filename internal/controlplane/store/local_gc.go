package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrLocalGCClaimConflict = errors.New("local GC claim conflicts with an active root")

type LocalGCClaim struct {
	ID, NodeID, ArtifactType, ArtifactKey, ProjectID string
	Status, TokenHash, Revision, Reason              string
	SizeBytes                                        int64
	GraceUntil                                       time.Time
}

func localGCLockKey(nodeID, artifactKey string) string {
	return "local-gc:" + nodeID + ":" + artifactKey
}

// ClaimImageForGC serializes deletion intent with pin creation. The returned
// clear token is never persisted; only its digest is stored.
func (s *Store) ClaimImageForGC(ctx context.Context, claim LocalGCClaim, token string) error {
	if claim.ID == "" || claim.NodeID == "" || claim.ArtifactKey == "" || token == "" ||
		!claim.GraceUntil.After(time.Now()) {
		return fmt.Errorf("invalid local GC claim")
	}
	sum := sha256.Sum256([]byte(token))
	claim.TokenHash = hex.EncodeToString(sum[:])
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, localGCLockKey(claim.NodeID, claim.ArtifactKey)); err != nil {
			return err
		}
		var rooted bool
		if err := tx.QueryRow(ctx, localGCImageRootSQL, claim.NodeID, claim.ArtifactKey).Scan(&rooted); err != nil {
			return err
		}
		if rooted {
			return ErrLocalGCClaimConflict
		}
		_, err := tx.Exec(
			ctx,
			`INSERT INTO local_gc_claims
			(id,node_id,artifact_type,artifact_key,project_id,status,claim_token_hash,artifact_revision,size_bytes,grace_until,reason)
			VALUES($1,$2,'image',$3,$4,'CLAIMED',$5,$6,$7,$8,$9)`,
			claim.ID,
			claim.NodeID,
			claim.ArtifactKey,
			claim.ProjectID,
			claim.TokenHash,
			claim.Revision,
			claim.SizeBytes,
			claim.GraceUntil,
			claim.Reason,
		)
		return err
	})
}

func (s *Store) MarkLocalGCQuarantined(ctx context.Context, id, token string) error {
	if token == "" {
		return fmt.Errorf("empty quarantine token")
	}
	sum := sha256.Sum256([]byte(token))
	tag, err := s.pool.Exec(
		ctx,
		`UPDATE local_gc_claims SET status='QUARANTINED',claim_token_hash=$2,updated_at=now() WHERE id=$1 AND status='CLAIMED'`,
		id,
		hex.EncodeToString(sum[:]),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLocalGCClaimConflict
	}
	return nil
}

// FinalizeImageGCClaim holds the node+digest admission lock and PostgreSQL
// relation locks while the agent performs its final lock-held reference guard
// and removal. New pins/prewarms and app/deployment/machine roots cannot commit
// between this authoritative recheck and the remote finalize.
func (s *Store) FinalizeImageGCClaim(ctx context.Context, claim LocalGCClaim, finalize func() error) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, localGCLockKey(claim.NodeID, claim.ArtifactKey)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `LOCK TABLE apps, deployments, machines, operations, image_prewarm_targets, image_pins IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return err
		}
		var current LocalGCClaim
		if err := tx.QueryRow(ctx, `SELECT id,node_id,artifact_type,artifact_key,project_id,status,claim_token_hash,artifact_revision,size_bytes,grace_until,reason
			FROM local_gc_claims WHERE id=$1 FOR UPDATE`, claim.ID).Scan(&current.ID, &current.NodeID, &current.ArtifactType,
			&current.ArtifactKey, &current.ProjectID, &current.Status, &current.TokenHash, &current.Revision,
			&current.SizeBytes, &current.GraceUntil, &current.Reason); err != nil {
			return err
		}
		if current.NodeID != claim.NodeID || current.ArtifactKey != claim.ArtifactKey ||
			current.Revision != claim.Revision ||
			current.TokenHash != claim.TokenHash ||
			(current.Status != "QUARANTINED" && current.Status != "FINALIZING") {
			return ErrLocalGCClaimConflict
		}
		var rooted bool
		if err := tx.QueryRow(ctx, localGCImageRootSQL, claim.NodeID, claim.ArtifactKey).Scan(&rooted); err != nil {
			return err
		}
		if rooted {
			return ErrLocalGCClaimConflict
		}
		// FINALIZING is committed before the remote call by SetLocalGCFinalizing.
		// It remains an active root-admission fence across timeout/ACK loss.
		if err := finalize(); err != nil {
			return err
		}
		tag, err := tx.Exec(
			ctx,
			`UPDATE local_gc_claims SET status='DELETED',updated_at=now() WHERE id=$1 AND status='FINALIZING'`,
			claim.ID,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrLocalGCClaimConflict
		}
		return nil
	})
}

const localGCImageRootSQL = `SELECT EXISTS(
	SELECT 1 FROM image_pins p JOIN nodes n ON n.id=$1
	WHERE p.image_digest=$2 AND p.expires_at>now()
	AND (p.selector='node:'||n.id OR p.selector='node_pool:'||n.node_pool)
	UNION ALL
	SELECT 1 FROM operations o JOIN image_prewarm_targets t ON t.operation_id=o.id
	WHERE t.node_id=$1 AND t.status='PENDING' AND o.kind='image_prewarm'
	AND o.status IN ('PENDING','CLAIMED') AND o.request->>'digest'=$2
	UNION ALL
	SELECT 1 FROM apps WHERE deleted_at IS NULL AND (image_ref=$2 OR image_ref LIKE '%@'||$2)
	UNION ALL
	SELECT 1 FROM deployments WHERE status IN ('PREPARING','ACTIVE') AND (image_ref=$2 OR image_ref LIKE '%@'||$2)
	UNION ALL
	SELECT 1 FROM machines WHERE desired_state IN ('CREATED','RUNNING') AND (image_ref=$2 OR image_ref LIKE '%@'||$2)
	UNION ALL
	SELECT 1 FROM operations WHERE kind='create' AND status IN ('PENDING','CLAIMED')
	AND (request->'spec'->>'image_ref'=$2 OR request->'spec'->>'image_ref' LIKE '%@'||$2))`

func (s *Store) SetLocalGCFinalizing(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE local_gc_claims SET status='FINALIZING',updated_at=now()
		WHERE id=$1 AND status IN ('QUARANTINED','FINALIZING')`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLocalGCClaimConflict
	}
	return nil
}

func (s *Store) CompleteLocalGCClaim(ctx context.Context, id, status, reason string) error {
	if status != "DELETED" && status != "ROLLED_BACK" {
		return fmt.Errorf("invalid local GC completion")
	}
	tag, err := s.pool.Exec(
		ctx,
		`UPDATE local_gc_claims SET status=$2,reason=$3,updated_at=now() WHERE id=$1 AND status IN ('QUARANTINED','FINALIZING','ROLLBACK_REQUESTED')`,
		id,
		status,
		reason,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLocalGCClaimConflict
	}
	return nil
}

func VerifyLocalGCToken(claim LocalGCClaim, token string) bool {
	sum := sha256.Sum256([]byte(token))
	return token != "" && claim.TokenHash == hex.EncodeToString(sum[:])
}

func (s *Store) ListActiveLocalGCClaims(ctx context.Context) ([]LocalGCClaim, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT id,node_id,artifact_type,artifact_key,project_id,status,claim_token_hash,artifact_revision,size_bytes,grace_until,reason FROM local_gc_claims WHERE status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED') ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LocalGCClaim
	for rows.Next() {
		var c LocalGCClaim
		if err := rows.Scan(&c.ID, &c.NodeID, &c.ArtifactType, &c.ArtifactKey, &c.ProjectID, &c.Status, &c.TokenHash, &c.Revision, &c.SizeBytes, &c.GraceUntil, &c.Reason); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) AbortLocalGCClaim(ctx context.Context, id, reason string) error {
	_, err := s.pool.Exec(ctx, `UPDATE local_gc_claims SET status='ABORTED',reason=$2,updated_at=now()
		WHERE id=$1 AND status='CLAIMED'`, id, reason)
	return err
}

// ActiveLocalGCClaim reports whether pin/prewarm admission must wait. It takes
// the same advisory lock as claim/finalize, closing the root-check/delete TOCTOU.
func (s *Store) ActiveLocalGCClaim(ctx context.Context, nodeID, digest string) (bool, error) {
	var active bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, localGCLockKey(nodeID, digest)); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM local_gc_claims WHERE node_id=$1 AND artifact_key=$2
			AND status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED'))`, nodeID, digest).Scan(&active)
	})
	return active, err
}
