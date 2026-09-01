// volumes.go：v1.3-D（ADR-0029）/ v1.3-E（ADR-0030）volume 业务事实。
// PG 是 volume/attachment 权威；agent 是本地 materialization 观测。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Volume 是 volumes 表行。
type Volume struct {
	ID            string
	ProjectID     string
	Name          string
	Mode          string // LOCAL_RW | DATASET_RO
	NodeID        string // origin node（硬钉）
	SizeBytes     int64
	State         string // CREATING|READY|UNAVAILABLE|DELETING|DELETED
	ContentDigest string
	ImportStatus  string
	// Integrity（v1.4-B）是正交的产物完整性观测，不复用业务状态表达。
	Integrity           string // UNKNOWN|METADATA_VERIFIED|CONTENT_VERIFIED|MISSING|CORRUPT
	IntegrityObservedAt *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// VolumeAttachment 是 volume_attachments 表行（绑定 execution）。
type VolumeAttachment struct {
	VolumeID         string
	MachineID        string
	ExecutionID      string
	MountPath        string
	Readonly         bool
	OverlaySizeBytes int64
	Status           string // PENDING|ATTACHED|DETACHED
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Volume errors are stable sentinels for API conflict/quota mapping.
var (
	ErrVolumeStateConflict = errors.New("volume state conflict")
	ErrVolumeSingleWriter  = errors.New("LOCAL_RW volume already has an active attachment")
	ErrVolumeLocality      = errors.New("volume locality mismatch")
	ErrVolumeQuota         = errors.New("project disk quota exceeded")
	ErrDatasetDigest       = errors.New("dataset digest is immutable")
)

const volumeCols = `id, project_id, name, mode, node_id, size_bytes, state,
	content_digest, import_status, integrity, integrity_observed_at, created_at, updated_at`

// CreateVolume 创建 LOCAL_RW 空卷（CREATING → agent 落盘 → READY）。
// Deprecated: API mutations should use CreateLocalRWVolumeAndEnqueue so the
// business row and outbox operation cannot be split by a crash.
func (s *Store) CreateVolume(ctx context.Context, v Volume) (*Volume, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO volumes(id, project_id, name, mode, node_id, size_bytes, state)
		VALUES($1,$2,$3,'LOCAL_RW',$4,$5,'CREATING')
		ON CONFLICT (id) DO UPDATE SET updated_at=volumes.updated_at
		RETURNING `+volumeCols,
		v.ID, v.ProjectID, v.Name, v.NodeID, v.SizeBytes)
	out, err := scanVolume(row)
	if err != nil {
		return nil, fmt.Errorf("create volume: %w", err)
	}
	return out, nil
}

func scanVolume(sc scanner) (*Volume, error) {
	var v Volume
	if err := sc.Scan(&v.ID, &v.ProjectID, &v.Name, &v.Mode, &v.NodeID,
		&v.SizeBytes, &v.State, &v.ContentDigest, &v.ImportStatus,
		&v.Integrity, &v.IntegrityObservedAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}

// GetVolume 按 ID 查询（nil = 不存在）。
func (s *Store) GetVolume(ctx context.Context, id string) (*Volume, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+volumeCols+` FROM volumes WHERE id=$1`, id)
	out, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get volume: %w", err)
	}
	return out, nil
}

// ListVolumes 按 project 列出（projectID 空 = 全部）。
func (s *Store) ListVolumes(ctx context.Context, projectID string) ([]Volume, error) {
	q := `SELECT ` + volumeCols + ` FROM volumes`
	args := []any{}
	if projectID != "" {
		q += ` WHERE project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY created_at`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	defer rows.Close()
	var out []Volume
	for rows.Next() {
		v, err := scanVolume(rows)
		if err != nil {
			return nil, fmt.Errorf("scan volume: %w", err)
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// SetVolumeIntegrity 记录正交的产物完整性观测（v1.4-B）。
func (s *Store) SetVolumeIntegrity(ctx context.Context, id, integrity string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE volumes SET integrity=$2, integrity_observed_at=now(), updated_at=now()
		WHERE id=$1 AND state <> 'DELETED'`, id, integrity)
	if err != nil {
		return fmt.Errorf("set volume integrity: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVolumeStateConflict
	}
	return nil
}

// ApplyVolumeIntegrity atomically records integrity and the matching
// availability transition. Presence alone never heals CORRUPT.
func (s *Store) ApplyVolumeIntegrity(ctx context.Context, id, integrity string) (bool, error) {
	if integrity != "MISSING" && integrity != "CORRUPT" && integrity != "METADATA_VERIFIED" {
		return false, fmt.Errorf("invalid volume integrity %q", integrity)
	}
	var changed bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var oldIntegrity, state string
		if err := tx.QueryRow(ctx, `SELECT integrity,state FROM volumes WHERE id=$1 FOR UPDATE`, id).Scan(&oldIntegrity, &state); err != nil {
			return err
		}
		if oldIntegrity == "CORRUPT" && integrity == "METADATA_VERIFIED" {
			return nil
		}
		newState := state
		if (integrity == "MISSING" || integrity == "CORRUPT") && state == "READY" {
			newState = "UNAVAILABLE"
		} else if integrity == "METADATA_VERIFIED" && state == "UNAVAILABLE" {
			newState = "READY"
		}
		changed = oldIntegrity != integrity || state != newState
		_, err := tx.Exec(ctx, `UPDATE volumes SET integrity=$2,integrity_observed_at=now(),state=$3,updated_at=now()
			WHERE id=$1 AND state<>'DELETED'`, id, integrity, newState)
		return err
	})
	return changed, err
}

// ApplyVolumeInventoryObservation atomically accepts a complete observation and
// applies availability/integrity consequences. DATASET_RO requires sealed
// digest metadata; LOCAL_RW requires healthy materialization metadata.
func (s *Store) ApplyVolumeInventoryObservation(
	ctx context.Context,
	o InventoryObservation,
	items map[string]VolumeInventoryItem,
) (bool, []IntegrityTransition, error) {
	if o.ResourceType != "volume" || o.ItemCount != len(items) {
		return false, nil, nil
	}
	var transitions []IntegrityTransition
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := acceptInventoryObservationTx(ctx, tx, &o); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT id,project_id,mode,state,size_bytes,content_digest,integrity
			FROM volumes WHERE node_id=$1 AND state IN ('CREATING','READY','UNAVAILABLE') FOR UPDATE`, o.NodeID)
		if err != nil {
			return err
		}
		type row struct {
			id, project, mode, state, digest, integrity string
			size                                        int64
		}
		var all []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.project, &r.mode, &r.state, &r.size, &r.digest, &r.integrity); err != nil {
				rows.Close()
				return err
			}
			all = append(all, r)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, r := range all {
			if r.state == "CREATING" {
				continue
			}
			item, present := items[r.id]
			integrity, reason := "MISSING", "absent from complete inventory"
			if present {
				integrity, reason = "METADATA_VERIFIED", ""
				if item.SizeBytes <= 0 || item.Mode == "" || item.MetadataHealth == "" ||
					(r.mode == "DATASET_RO" && item.ContentDigest == "") {
					integrity, reason = "UNKNOWN", "incomplete materialization metadata"
				} else {
					matches := item.SizeBytes == r.size && item.Mode == r.mode && item.MetadataHealth == "HEALTHY" &&
						item.ContentDigest == r.digest && item.Sealed == (r.mode == "DATASET_RO")
					if !matches {
						integrity, reason = "CORRUPT", "materialization metadata mismatch"
					}
				}
			}
			if r.integrity == "CORRUPT" && (integrity == "METADATA_VERIFIED" || integrity == "UNKNOWN") {
				integrity = "CORRUPT"
				reason = "awaiting successful content scrub"
			}
			newState := r.state
			if (integrity == "MISSING" || integrity == "CORRUPT") && r.state == "READY" {
				newState = "UNAVAILABLE"
			}
			if integrity == "METADATA_VERIFIED" && r.state == "UNAVAILABLE" {
				newState = "READY"
			}
			if r.integrity != integrity || r.state != newState {
				transitions = append(
					transitions,
					IntegrityTransition{ID: r.id, ProjectID: r.project, From: r.integrity, To: integrity},
				)
			}
			if _, err := tx.Exec(ctx, `UPDATE volumes SET integrity=$2,integrity_reason=$3,state=$4,
				integrity_observed_at=$5,inventory_epoch=$6,inventory_generation=$7,inventory_received_at=$8,
				inventory_observation_id=$9,updated_at=now() WHERE id=$1`, r.id, integrity, reason, newState,
				o.AgentObservedAt, o.Epoch, int64(o.Generation), o.ReceivedAt, o.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, ErrStaleInventoryObservation) {
		return false, nil, nil
	}
	return err == nil, transitions, err
}

// ListVolumesOnNode 返回节点上的非终态卷（inventory 对账输入）。
func (s *Store) ListVolumesOnNode(ctx context.Context, nodeID string) ([]Volume, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+volumeCols+` FROM volumes
		WHERE node_id=$1 AND state IN ('CREATING','READY','UNAVAILABLE')`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list volumes on node: %w", err)
	}
	defer rows.Close()
	var out []Volume
	for rows.Next() {
		v, err := scanVolume(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// BeginVolumeDeleteAndEnqueue atomically tombstones the business fact and
// records the node-pinned delete outbox command. A crash can therefore never
// leave DELETING without work to dispatch (or work without DELETING).
func (s *Store) BeginVolumeDeleteAndEnqueue(
	ctx context.Context,
	id string,
	p EnqueueOperationParams,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var projectID, nodeID, state string
		if err := tx.QueryRow(ctx, `SELECT project_id,node_id,state FROM volumes WHERE id=$1 FOR UPDATE`, id).Scan(&projectID, &nodeID, &state); err != nil {
			return err
		}
		if p.Kind != "volume_delete" || p.ProjectID != projectID || p.DispatchNodeID != nodeID {
			return ErrVolumeStateConflict
		}
		if state == "DELETED" {
			return ErrVolumeStateConflict
		}
		// Match snapshot deletion semantics: replay an in-flight attempt and
		// re-key after a terminal attempt so DELETING cannot wedge forever.
		base := p.OperationID
		for attempt := 1; ; attempt++ {
			candidate := base
			if attempt > 1 {
				candidate = fmt.Sprintf("%s-r%d", base, attempt)
			}
			existing, err := selectOperationByKey(ctx, tx, p.ProjectID, candidate)
			if err != nil {
				return err
			}
			if existing == nil {
				p.OperationID = candidate
				break
			}
			if existing.Kind != "volume_delete" {
				return ErrRequestConflict
			}
			if existing.Status == "PENDING" || existing.Status == "CLAIMED" {
				if !jsonEqual(existing.Request, p.Request) {
					return ErrRequestConflict
				}
				op = *existing
				return nil
			}
		}
		// CREATING is allowed so a deterministically failed import can be
		// cleaned up with the current schema (which has no FAILED state).
		if state != "READY" && state != "UNAVAILABLE" && state != "CREATING" && state != "DELETING" {
			return ErrVolumeStateConflict
		}
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM volume_attachments WHERE volume_id=$1 AND status<>'DETACHED')`, id).Scan(&active); err != nil {
			return err
		}
		if active {
			return ErrVolumeStateConflict
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		if state != "DELETING" {
			if _, err := tx.Exec(ctx, `UPDATE volumes SET state='DELETING',updated_at=now() WHERE id=$1`, id); err != nil {
				return err
			}
		}
		op = *created
		return nil
	})
	return op, err
}

// TransitionVolume 推进 volume 状态（终态 DELETED 不可逆）。
func (s *Store) TransitionVolume(ctx context.Context, id, from, to string) error {
	if to == "DELETED" {
		_, err := s.pool.Exec(ctx, `UPDATE volumes SET state='DELETED', deleted_at=now(),
			updated_at=now() WHERE id=$1 AND state <> 'DELETED'`, id)
		if err != nil {
			return fmt.Errorf("transition volume: %w", err)
		}
		return nil
	}
	tag, err := s.pool.Exec(ctx, `UPDATE volumes SET state=$3, updated_at=now()
		WHERE id=$1 AND ($2='' OR state=$2) AND state<>'DELETED'`, id, from, to)
	if err != nil {
		return fmt.Errorf("transition volume: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVolumeStateConflict
	}
	return nil
}

// MarkVolumesUnavailable / MarkVolumesAvailable：节点失联/恢复。
func (s *Store) MarkVolumesUnavailable(ctx context.Context, nodeID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE volumes SET state='UNAVAILABLE', updated_at=now()
		WHERE node_id=$1 AND state IN ('CREATING','READY')`, nodeID)
	if err != nil {
		return 0, fmt.Errorf("mark volumes unavailable: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// MarkVolumesAvailable 恢复 READY。
func (s *Store) MarkVolumesAvailable(ctx context.Context, nodeID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE volumes SET state='READY', updated_at=now()
		WHERE node_id=$1 AND state='UNAVAILABLE'`, nodeID)
	if err != nil {
		return 0, fmt.Errorf("mark volumes available: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// CreateLocalRWVolumeAndEnqueue atomically reserves project disk quota, fixes
// the origin node, creates the volume business fact, and inserts its outbox op.
// node_id is deliberately mandatory: a later controller fallback could create
// the local data on a different node after a retry/leader change.
func (s *Store) CreateLocalRWVolumeAndEnqueue(
	ctx context.Context,
	v Volume,
	p EnqueueOperationParams,
) (*Volume, Operation, error) {
	var out *Volume
	var op Operation
	if v.ID == "" || v.ProjectID == "" || v.NodeID == "" || v.SizeBytes <= 0 ||
		p.OperationID == "" || p.ProjectID != v.ProjectID || p.Kind != "volume_create" || p.DispatchNodeID != v.NodeID {
		return nil, op, fmt.Errorf("create LOCAL_RW volume: invalid atomic command")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := reserveVolumeQuota(ctx, tx, v.ProjectID, v.SizeBytes); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state)
			VALUES($1,$2,$3,'LOCAL_RW',$4,$5,'CREATING') RETURNING `+volumeCols,
			v.ID, v.ProjectID, v.Name, v.NodeID, v.SizeBytes)
		var err error
		out, err = scanVolume(row)
		if err != nil {
			return fmt.Errorf("create volume: %w", err)
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return out, op, err
}

// CreateDatasetAndEnqueue atomically creates an immutable dataset import fact
// and its node-pinned outbox operation. The digest cannot be changed after insert.
func (s *Store) CreateDatasetAndEnqueue(
	ctx context.Context,
	v Volume,
	p EnqueueOperationParams,
) (*Volume, Operation, error) {
	var out *Volume
	var op Operation
	if v.ID == "" || v.ProjectID == "" || v.NodeID == "" || v.SizeBytes <= 0 || v.ContentDigest == "" ||
		p.Kind != "dataset_import" || p.ProjectID != v.ProjectID || p.DispatchNodeID != v.NodeID {
		return nil, op, errors.New("create DATASET_RO: invalid atomic command")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := reserveVolumeQuota(ctx, tx, v.ProjectID, v.SizeBytes); err != nil {
			return err
		}
		row := tx.QueryRow(
			ctx,
			`INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state,content_digest,import_status)
			VALUES($1,$2,$3,'DATASET_RO',$4,$5,'CREATING',$6,'importing') RETURNING `+volumeCols,
			v.ID,
			v.ProjectID,
			v.Name,
			v.NodeID,
			v.SizeBytes,
			v.ContentDigest,
		)
		var err error
		out, err = scanVolume(row)
		if err != nil {
			return err
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return out, op, err
}

// SealDataset atomically publishes a verified base. A different digest can
// never overwrite the originally authorized digest.
// FailDatasetImport records a terminal import failure without publishing the
// volume. The current migration lacks a FAILED volume state, so CREATING plus
// import_status=failed is the safe deletable representation.
func (s *Store) FailDatasetImport(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE volumes SET import_status='failed',updated_at=now()
		WHERE id=$1 AND mode='DATASET_RO' AND state='CREATING'`, id)
	if err != nil {
		return fmt.Errorf("fail dataset import: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrVolumeStateConflict
	}
	return nil
}

func (s *Store) SealDataset(ctx context.Context, id, digest string, size int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE volumes SET state='READY',import_status='sealed',size_bytes=$3,updated_at=now()
		WHERE id=$1 AND mode='DATASET_RO' AND state='CREATING' AND content_digest=$2`, id, digest, size)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrDatasetDigest
	}
	return nil
}

// ClaimDatasetAttachmentAndEnqueue allows same-project readonly multi-attach.
// Overlay storage is separately accounted and always execution scoped.
func (s *Store) ClaimDatasetAttachmentAndEnqueue(
	ctx context.Context,
	att VolumeAttachment,
	p EnqueueOperationParams,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var projectID, nodeID, state, mode string
		if err := tx.QueryRow(ctx, `SELECT project_id,node_id,state,mode FROM volumes WHERE id=$1 FOR UPDATE`, att.VolumeID).Scan(&projectID, &nodeID, &state, &mode); err != nil {
			return err
		}
		if mode != "DATASET_RO" || state != "READY" || !att.Readonly || att.OverlaySizeBytes < 0 ||
			p.ProjectID != projectID ||
			p.DispatchNodeID != nodeID {
			return ErrVolumeStateConflict
		}
		if att.OverlaySizeBytes > 0 {
			if err := reserveVolumeQuota(ctx, tx, projectID, att.OverlaySizeBytes); err != nil {
				return err
			}
		}
		var machineNode, execution string
		var generation int64
		if err := tx.QueryRow(ctx, `SELECT coalesce(node_id,''),current_execution_id,generation FROM machines WHERE id=$1 FOR UPDATE`, att.MachineID).Scan(&machineNode, &execution, &generation); err != nil {
			return err
		}
		if machineNode != nodeID || execution != att.ExecutionID || generation != p.Generation {
			return ErrVolumeLocality
		}
		if _, err := tx.Exec(ctx, `INSERT INTO volume_attachments(volume_id,machine_id,execution_id,mount_path,readonly,overlay_size_bytes,status)
			VALUES($1,$2,$3,$4,true,$5,'PENDING')`, att.VolumeID, att.MachineID, att.ExecutionID, att.MountPath, att.OverlaySizeBytes); err != nil {
			return err
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

// WritableActiveAttachments returns attachment roots that forbid memory checkpoint.
func (s *Store) WritableActiveAttachments(
	ctx context.Context,
	machineID, executionID string,
) ([]VolumeAttachment, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT volume_id,machine_id,execution_id,mount_path,readonly,overlay_size_bytes,status,created_at,updated_at
		FROM volume_attachments WHERE machine_id=$1 AND execution_id=$2 AND status<>'DETACHED' AND (NOT readonly OR overlay_size_bytes>0)`,
		machineID,
		executionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VolumeAttachment
	for rows.Next() {
		var a VolumeAttachment
		if err := rows.Scan(&a.VolumeID, &a.MachineID, &a.ExecutionID, &a.MountPath, &a.Readonly, &a.OverlaySizeBytes, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ClaimLocalRWAttachmentAndEnqueue atomically takes the single-writer claim and
// inserts the fenced attach outbox operation. The volume row lock serializes
// concurrent claimers even before the recommended partial unique index exists.
func (s *Store) ClaimLocalRWAttachmentAndEnqueue(
	ctx context.Context,
	att VolumeAttachment,
	p EnqueueOperationParams,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var projectID, mode, nodeID, state string
		if err := tx.QueryRow(ctx, `SELECT project_id,mode,node_id,state FROM volumes WHERE id=$1 FOR UPDATE`, att.VolumeID).
			Scan(&projectID, &mode, &nodeID, &state); err != nil {
			return err
		}
		if mode != "LOCAL_RW" || state != "READY" {
			return ErrVolumeStateConflict
		}
		if p.ProjectID != projectID || p.Kind != "volume_attach" || p.MachineID != att.MachineID ||
			p.ExecutionID != att.ExecutionID || p.DispatchNodeID != nodeID {
			return ErrVolumeLocality
		}
		var machineNode, execution string
		var generation int64
		if err := tx.QueryRow(ctx, `SELECT coalesce(node_id,''),current_execution_id,generation FROM machines WHERE id=$1 FOR UPDATE`, att.MachineID).
			Scan(&machineNode, &execution, &generation); err != nil {
			return err
		}
		if machineNode != nodeID || execution != att.ExecutionID || generation != p.Generation {
			return ErrVolumeLocality
		}
		var active int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM volume_attachments WHERE volume_id=$1 AND status<>'DETACHED'`, att.VolumeID).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return ErrVolumeSingleWriter
		}
		if _, err := tx.Exec(ctx, `INSERT INTO volume_attachments(volume_id,machine_id,execution_id,mount_path,readonly,overlay_size_bytes,status)
			VALUES($1,$2,$3,$4,false,0,'PENDING')`, att.VolumeID, att.MachineID, att.ExecutionID, att.MountPath); err != nil {
			return err
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

// BeginDetachVolumeAndEnqueue changes ATTACHED to DETACHING before dispatch and
// atomically records the outbox operation. Only agent acknowledgement may call
// CompleteVolumeDetach and make the claim reusable.
func (s *Store) BeginDetachVolumeAndEnqueue(
	ctx context.Context,
	volumeID, machineID, executionID string,
	p EnqueueOperationParams,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE volume_attachments SET status='DETACHING',updated_at=now()
			WHERE volume_id=$1 AND machine_id=$2 AND execution_id=$3 AND status='ATTACHED'`, volumeID, machineID, executionID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrVolumeStateConflict
		}
		created, err := insertVolumeOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

func reserveVolumeQuota(ctx context.Context, tx pgx.Tx, projectID string, extraBytes int64) error {
	var quotaMib int64
	if err := tx.QueryRow(ctx, `SELECT disk_mib_quota FROM projects WHERE id=$1 FOR UPDATE`, projectID).Scan(&quotaMib); err != nil {
		return fmt.Errorf("lock project quota: %w", err)
	}
	var usedMib int64
	if err := tx.QueryRow(ctx, `SELECT
		coalesce((SELECT sum(coalesce(nullif(m.requested_disk_mib,0),10240)) FROM machines m JOIN apps a ON a.id=m.app_id
			WHERE a.project_id=$1 AND m.desired_state IN ('CREATED','RUNNING')),0)
		+ coalesce((SELECT sum((size_bytes+1048575)/1048576) FROM volumes WHERE project_id=$1 AND state<>'DELETED'),0)
		+ coalesce((SELECT sum((a.overlay_size_bytes+1048575)/1048576) FROM volume_attachments a JOIN volumes v ON v.id=a.volume_id
			WHERE v.project_id=$1 AND a.status<>'DETACHED'),0)`, projectID).Scan(&usedMib); err != nil {
		return fmt.Errorf("project disk quota usage: %w", err)
	}
	extraMib := (extraBytes + 1048575) / 1048576
	if quotaMib > 0 && usedMib+extraMib > quotaMib {
		return ErrVolumeQuota
	}
	return nil
}

func insertVolumeOperation(ctx context.Context, tx pgx.Tx, p EnqueueOperationParams) (*Operation, error) {
	existing, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !jsonEqual(existing.Request, p.Request) {
			return nil, ErrRequestConflict
		}
		return existing, nil
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO operations(id,project_id,machine_id,execution_id,generation,kind,idempotency_key,status,request,dispatch_node_id)
		VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb,NULLIF($8,''))`,
		p.OperationID,
		p.ProjectID,
		p.MachineID,
		p.ExecutionID,
		p.Generation,
		p.Kind,
		string(p.Request),
		p.DispatchNodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("enqueue %s: %w", p.Kind, err)
	}
	return selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
}

// UpsertVolumeAttachment 绑定 attachment（execution 作用域）。
// Deprecated: use ClaimLocalRWAttachmentAndEnqueue for LOCAL_RW.
func (s *Store) UpsertVolumeAttachment(ctx context.Context, att VolumeAttachment) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO volume_attachments(volume_id, machine_id, execution_id,
			mount_path, readonly, overlay_size_bytes, status)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (volume_id, machine_id, execution_id) DO UPDATE SET
			mount_path=EXCLUDED.mount_path, readonly=EXCLUDED.readonly,
			overlay_size_bytes=EXCLUDED.overlay_size_bytes,
			status=EXCLUDED.status, updated_at=now()`,
		att.VolumeID, att.MachineID, att.ExecutionID, att.MountPath,
		att.Readonly, att.OverlaySizeBytes, att.Status)
	if err != nil {
		return fmt.Errorf("upsert volume attachment: %w", err)
	}
	return nil
}

func (s *Store) MarkVolumeAttachmentAttached(ctx context.Context, volumeID, machineID, executionID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE volume_attachments SET status='ATTACHED',updated_at=now()
		WHERE volume_id=$1 AND machine_id=$2 AND execution_id=$3 AND status IN ('PENDING','ATTACHED')`, volumeID, machineID, executionID)
	if err != nil {
		return fmt.Errorf("mark attachment attached: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVolumeStateConflict
	}
	return nil
}

// ActiveAttachment 返回 volume 当前 active（非 DETACHED）的 attachment
// （LOCAL_RW 单写：多于一个 = 违约，调用方拒绝新 attach）。
func (s *Store) ActiveAttachments(ctx context.Context, volumeID string) ([]VolumeAttachment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT volume_id, machine_id, execution_id, mount_path, readonly,
			overlay_size_bytes, status, created_at, updated_at
		FROM volume_attachments WHERE volume_id=$1 AND status <> 'DETACHED'`, volumeID)
	if err != nil {
		return nil, fmt.Errorf("active attachments: %w", err)
	}
	defer rows.Close()
	var out []VolumeAttachment
	for rows.Next() {
		var a VolumeAttachment
		if err := rows.Scan(&a.VolumeID, &a.MachineID, &a.ExecutionID, &a.MountPath,
			&a.Readonly, &a.OverlaySizeBytes, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan attachment: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// MachineLocalRWNode returns the hard locality claim for a machine. Multiple
// different origin nodes are treated as a conflict and fail closed.
func (s *Store) MachineLocalRWNode(ctx context.Context, machineID string) (string, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT DISTINCT v.node_id FROM volume_attachments a JOIN volumes v ON v.id=a.volume_id
		WHERE a.machine_id=$1 AND a.status<>'DETACHED' AND v.mode='LOCAL_RW' AND v.state<>'DELETED'`,
		machineID,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var node string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return "", err
		}
		if node != "" && node != n {
			return "", ErrVolumeLocality
		}
		node = n
	}
	return node, rows.Err()
}

// ReleaseTerminalExecutionAttachments releases attachment claims after the
// caller has established that a machine execution is terminal. It is idempotent.
func (s *Store) ReleaseTerminalExecutionAttachments(ctx context.Context, machineID, executionID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE volume_attachments SET status='DETACHED',updated_at=now()
		WHERE machine_id=$1 AND execution_id=$2 AND status<>'DETACHED'`, machineID, executionID)
	if err != nil {
		return 0, fmt.Errorf("release terminal execution attachments: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) MachineHasLocalRWAttachment(ctx context.Context, machineID string) (bool, error) {
	var yes bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM volume_attachments a JOIN volumes v ON v.id=a.volume_id
		WHERE a.machine_id=$1 AND a.status<>'DETACHED' AND v.mode='LOCAL_RW' AND v.state<>'DELETED')`, machineID).Scan(&yes)
	return yes, err
}

// CompleteVolumeDetach is called only after the agent confirms detach.
func (s *Store) CompleteVolumeDetach(ctx context.Context, volumeID, machineID, executionID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE volume_attachments SET status='DETACHED',updated_at=now()
		WHERE volume_id=$1 AND machine_id=$2 AND execution_id=$3 AND status='DETACHING'`, volumeID, machineID, executionID)
	if err != nil {
		return fmt.Errorf("complete volume detach: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVolumeStateConflict
	}
	return nil
}

// DetachVolumeAttachment 把指定 execution 的 attachment 置 DETACHED（幂等）。
// Deprecated: API should use BeginDetachVolumeAndEnqueue; controller should use CompleteVolumeDetach.
func (s *Store) DetachVolumeAttachment(ctx context.Context, volumeID, machineID, executionID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE volume_attachments SET status='DETACHED', updated_at=now()
		WHERE volume_id=$1 AND machine_id=$2 AND execution_id=$3`, volumeID, machineID, executionID)
	if err != nil {
		return fmt.Errorf("detach volume attachment: %w", err)
	}
	return nil
}
