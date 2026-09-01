package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrStaleInventoryObservation = errors.New("stale inventory observation")

type IntegrityTransition struct {
	ID        string
	ProjectID string
	MachineID string
	From      string
	To        string
}

type SnapshotInventoryItem struct {
	SizeBytes        int64
	Checksum         string
	Kind             string
	CompatibilityKey string
}

type VolumeInventoryItem struct {
	SizeBytes      int64
	Mode           string
	ContentDigest  string
	Sealed         bool
	MetadataHealth string
}

type InventoryObservation struct {
	ID              int64
	NodeID          string
	ResourceType    string
	Epoch           string
	Generation      uint64
	AgentObservedAt time.Time
	ReceivedAt      time.Time
	ItemCount       int
}

func validInventoryObservation(o InventoryObservation) bool {
	if o.NodeID == "" || (o.ResourceType != "snapshot" && o.ResourceType != "volume") ||
		o.Epoch == "" || o.Generation == 0 || o.AgentObservedAt.IsZero() || o.ItemCount < 0 {
		return false
	}
	now := time.Now().UTC()
	return !o.AgentObservedAt.After(now.Add(5*time.Minute)) && !o.AgentObservedAt.Before(now.Add(-24*time.Hour))
}

// acceptInventoryObservationTx serializes sequence acceptance and returns the
// durable observation identity. Callers apply every lifecycle/integrity effect
// in this same transaction, so an accepted observation can never be partial.
func acceptInventoryObservationTx(ctx context.Context, tx pgx.Tx, o *InventoryObservation) error {
	if !validInventoryObservation(*o) {
		return ErrStaleInventoryObservation
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		"inventory:"+o.NodeID+":"+o.ResourceType); err != nil {
		return err
	}
	var epoch string
	var generation int64
	err := tx.QueryRow(ctx, `SELECT epoch,generation FROM local_inventory_observations
		WHERE node_id=$1 AND resource_type=$2 ORDER BY id DESC LIMIT 1`,
		o.NodeID, o.ResourceType).Scan(&epoch, &generation)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		if epoch == o.Epoch && int64(o.Generation) <= generation {
			return ErrStaleInventoryObservation
		}
		if epoch != o.Epoch {
			var retired bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM local_inventory_observations
				WHERE node_id=$1 AND resource_type=$2 AND epoch=$3)`, o.NodeID, o.ResourceType, o.Epoch).Scan(&retired); err != nil {
				return err
			}
			if retired {
				return ErrStaleInventoryObservation
			}
		}
	}
	if err := tx.QueryRow(ctx, `INSERT INTO local_inventory_observations
		(node_id,resource_type,epoch,generation,agent_observed_at,item_count)
		VALUES($1,$2,$3,$4,$5,$6) RETURNING id,received_at`, o.NodeID, o.ResourceType,
		o.Epoch, int64(o.Generation), o.AgentObservedAt.UTC(), o.ItemCount).Scan(&o.ID, &o.ReceivedAt); err != nil {
		return fmt.Errorf("record inventory observation: %w", err)
	}
	return nil
}

// AcceptInventoryObservation is retained for callers that only record an
// observation. Integrity reconciliation must use Apply*InventoryObservation.
func (s *Store) AcceptInventoryObservation(ctx context.Context, o InventoryObservation) (bool, error) {
	err := s.inTx(ctx, func(tx pgx.Tx) error { return acceptInventoryObservationTx(ctx, tx, &o) })
	if errors.Is(err, ErrStaleInventoryObservation) {
		return false, nil
	}
	return err == nil, err
}
