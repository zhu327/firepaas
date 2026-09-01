// snapshots.go：v1.3-B（ADR-0028）snapshot 资源持久层。
// PG 只拥有元数据与操作事实；artifact 仍在 agent 本地（v1.3 不承诺跨节点
// durability）。状态机与 schedule retention 见迁移 0024。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Snapshot 是 snapshots 表行。
type Snapshot struct {
	ID                    string
	ProjectID             string
	SourceMachineID       string
	SourceExecutionID     string
	SourceGeneration      int64
	Kind                  string // MEMORY | FILESYSTEM
	Status                string // CREATING|READY|DELETING|DELETED|UNAVAILABLE|LOST
	NodeID                string
	CompatibilityKey      string
	SizeBytes             int64
	Checksum              string
	Compression           string // none|zstd|lz4
	CompressionLevel      *int
	CompressionState      string // none|compressing|compressed|error
	FilesystemConsistency string // clean|crash-consistent
	RetentionClass        string
	ScheduleID            string // 空 = 手工 checkpoint
	ExpiresAt             *time.Time
	LostAt                *time.Time
	// Integrity（v1.4-B）是正交的产物完整性观测，不复用业务状态表达。
	Integrity           string // UNKNOWN|METADATA_VERIFIED|CONTENT_VERIFIED|MISSING|CORRUPT
	IntegrityObservedAt *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SnapshotSchedule 是 snapshot_schedules 表行。
type SnapshotSchedule struct {
	ID               string
	ProjectID        string
	AppID            string
	MachineID        string
	IntervalSeconds  int
	JitterSeconds    int
	MaxCount         int
	MaxAgeSeconds    int
	Compression      string
	CompressionLevel *int
	Enabled          bool
	NextRunAt        *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// ErrSnapshotStatusConflict 表示快照状态不允许该迁移（非幂等重复）。
var (
	ErrSnapshotStatusConflict = errors.New("snapshot status conflict")
	ErrRescueConflict         = errors.New("rescue execution conflict")
)

const snapshotCols = `id, project_id, source_machine_id, source_execution_id, source_generation, kind, status,
	node_id, compatibility_key, size_bytes, checksum, compression, compression_level,
	compression_state, filesystem_consistency, retention_class, schedule_id,
	expires_at, lost_at, integrity, integrity_observed_at, created_at, updated_at`

func scanSnapshot(sc scanner) (*Snapshot, error) {
	var s Snapshot
	err := sc.Scan(&s.ID, &s.ProjectID, &s.SourceMachineID, &s.SourceExecutionID, &s.SourceGeneration,
		&s.Kind, &s.Status, &s.NodeID, &s.CompatibilityKey, &s.SizeBytes, &s.Checksum,
		&s.Compression, &s.CompressionLevel, &s.CompressionState, &s.FilesystemConsistency,
		&s.RetentionClass, &s.ScheduleID, &s.ExpiresAt, &s.LostAt, &s.Integrity,
		&s.IntegrityObservedAt, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// CreateSnapshot 插入 CREATING 快照（幂等：同 ID 已存在时返回既有行）。
func (s *Store) CreateSnapshot(ctx context.Context, snap Snapshot) (*Snapshot, error) {
	var level any
	if snap.CompressionLevel != nil {
		level = *snap.CompressionLevel
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO snapshots(id, project_id, source_machine_id, source_execution_id, source_generation,
			kind, status, node_id, compatibility_key, compression, compression_level,
			compression_state, retention_class, schedule_id)
		VALUES($1,$2,$3,$4,$5,$6,'CREATING',$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO UPDATE SET updated_at=snapshots.updated_at
		RETURNING `+snapshotCols,
		snap.ID, snap.ProjectID, snap.SourceMachineID, snap.SourceExecutionID, snap.SourceGeneration,
		snap.Kind, snap.NodeID, snap.CompatibilityKey, snap.Compression, level,
		snap.CompressionState, snap.RetentionClass, snap.ScheduleID)
	out, err := scanSnapshot(row)
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}
	return out, nil
}

// CreateSnapshotAndEnqueue atomically persists the CREATING business fact and
// its node-pinned outbox operation. Neither side is visible without the other.
func (s *Store) CreateSnapshotAndEnqueue(ctx context.Context, snap Snapshot, p EnqueueOperationParams) (*Snapshot, Operation, error) {
	var out *Snapshot
	var op Operation
	if snap.ID == "" || snap.ProjectID == "" || snap.SourceMachineID == "" || snap.SourceExecutionID == "" ||
		snap.SourceGeneration < 1 || snap.NodeID == "" || p.OperationID == "" || p.ProjectID != snap.ProjectID ||
		p.MachineID != snap.SourceMachineID || p.ExecutionID != snap.SourceExecutionID ||
		p.Generation != snap.SourceGeneration || p.Kind != "snapshot_create" || p.DispatchNodeID != snap.NodeID {
		return nil, op, errors.New("create snapshot: invalid atomic command")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var level any
		if snap.CompressionLevel != nil {
			level = *snap.CompressionLevel
		}
		row := tx.QueryRow(ctx, `INSERT INTO snapshots(id,project_id,source_machine_id,source_execution_id,source_generation,
			kind,status,node_id,compatibility_key,compression,compression_level,compression_state,retention_class,schedule_id)
			VALUES($1,$2,$3,$4,$5,$6,'CREATING',$7,$8,$9,$10,$11,$12,$13) RETURNING `+snapshotCols,
			snap.ID, snap.ProjectID, snap.SourceMachineID, snap.SourceExecutionID, snap.SourceGeneration,
			snap.Kind, snap.NodeID, snap.CompatibilityKey, snap.Compression, level,
			snap.CompressionState, snap.RetentionClass, snap.ScheduleID)
		var err error
		out, err = scanSnapshot(row)
		if err != nil {
			return fmt.Errorf("create snapshot: %w", err)
		}
		created, err := insertSnapshotOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return out, op, err
}

// GetSnapshot 按 ID 查询（不存在返回 nil）。
func (s *Store) GetSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+snapshotCols+` FROM snapshots WHERE id=$1`, id)
	out, err := scanSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get snapshot: %w", err)
	}
	return out, nil
}

// ListSnapshots 按 project 返回快照（projectID 空 = 全部，admin 用）。
func (s *Store) ListSnapshots(ctx context.Context, projectID string) ([]Snapshot, error) {
	q := `SELECT ` + snapshotCols + ` FROM snapshots`
	args := []any{}
	if projectID != "" {
		q += ` WHERE project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY created_at DESC`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		out = append(out, *snap)
	}
	return out, rows.Err()
}

// TransitionSnapshot 状态迁移（终态 DELETED/LOST 不可逆）。返回更新后的行；
// 目标状态非法或不可迁移时返回 ErrSnapshotStatusConflict。
func (s *Store) TransitionSnapshot(ctx context.Context, id, from, to string) (*Snapshot, error) {
	allowed := map[string]map[string]bool{
		"CREATING":    {"READY": true, "DELETING": true, "UNAVAILABLE": true, "LOST": true},
		"READY":       {"DELETING": true, "UNAVAILABLE": true, "LOST": true},
		"UNAVAILABLE": {"READY": true, "DELETING": true, "LOST": true},
		"DELETING":    {"DELETED": true, "LOST": true},
	}
	if from == "" || !allowed[from][to] {
		return nil, ErrSnapshotStatusConflict
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET status=$3,
			lost_at=CASE WHEN $3='LOST' THEN now() ELSE lost_at END,
			updated_at=now()
		WHERE id=$1 AND status=$2`, id, from, to)
	if err != nil {
		return nil, fmt.Errorf("transition snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrSnapshotStatusConflict
	}
	return s.GetSnapshot(ctx, id)
}

// UpdateSnapshotArtifact 写 agent 观测的产物事实（size/checksum/compression/
// consistency），并推进 CREATING→READY（原子）。半成品（compressing 且无
// checksum）不得 READY：调用方必须在 capture 完成后才携带完整事实。
func (s *Store) UpdateSnapshotArtifact(ctx context.Context, id string,
	sizeBytes int64, checksum, compressionState, algorithm, consistency string,
	compressionLevel *int, ready bool) (*Snapshot, error) {
	var level any
	if compressionLevel != nil {
		level = *compressionLevel
	}
	complete := ready && sizeBytes > 0 && checksum != "" &&
		compressionState != "compressing" && compressionState != "error"
	row := s.pool.QueryRow(ctx, `
		UPDATE snapshots SET size_bytes=$2, checksum=$3, compression_state=$4,
			compression=$5, filesystem_consistency=$6, compression_level=$7,
			status=CASE WHEN $8 AND status='CREATING' THEN 'READY' ELSE status END,
			updated_at=now()
		WHERE id=$1 AND status NOT IN ('DELETED','LOST')
		RETURNING `+snapshotCols,
		id, sizeBytes, checksum, compressionState, algorithm, consistency, level, complete)
	out, err := scanSnapshot(row)
	if err != nil {
		return nil, fmt.Errorf("update snapshot artifact: %w", err)
	}
	return out, nil
}

// MarkSnapshotsUnavailable 把指定节点上的非终态快照置 UNAVAILABLE。
func (s *Store) MarkSnapshotsUnavailable(ctx context.Context, nodeID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET status='UNAVAILABLE', updated_at=now()
		WHERE node_id=$1 AND status='READY'`, nodeID)
	if err != nil {
		return 0, fmt.Errorf("mark snapshots unavailable: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// MarkSnapshotsAvailable is intentionally conservative: node health alone is
// not artifact inventory. Call MarkInventoriedSnapshotsAvailable after a
// successful authoritative ListSnapshots response.
func (s *Store) MarkSnapshotsAvailable(ctx context.Context, nodeID string) (int, error) {
	return 0, nil
}

// MarkInventoriedSnapshotsAvailable restores only artifacts proven present by
// authoritative agent inventory; missing artifacts remain UNAVAILABLE.
func (s *Store) MarkInventoriedSnapshotsAvailable(ctx context.Context, nodeID string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET status='READY', updated_at=now()
		WHERE node_id=$1 AND status='UNAVAILABLE' AND id=ANY($2)
			AND size_bytes > 0 AND checksum <> ''
			AND compression_state NOT IN ('compressing','error')`, nodeID, ids)
	if err != nil {
		return 0, fmt.Errorf("mark snapshots available: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ListSnapshotsForRetention 返回 schedule 产物中可被保留策略清理的行
// （READY/UNAVAILABLE；手工 checkpoint 无 schedule 不参与）。
func (s *Store) ListSnapshotsForRetention(ctx context.Context, scheduleID string) ([]Snapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+snapshotCols+` FROM snapshots
		WHERE schedule_id=$1 AND status IN ('READY','UNAVAILABLE')
		ORDER BY created_at DESC`, scheduleID)
	if err != nil {
		return nil, fmt.Errorf("list snapshots for retention: %w", err)
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		out = append(out, *snap)
	}
	return out, rows.Err()
}

// BeginSnapshotDeleteAndEnqueue atomically checks active consumers, moves the
// business fact to DELETING, and writes the delete outbox operation.
//
// v1.4-A: a retry against an already-DELETING snapshot must converge
// idempotently. An in-flight delete operation replays by key; a terminal
// (FAILED) attempt is re-enqueued under a fresh suffixed key so the snapshot
// never wedges in DELETING, and an orphaned DELETING row (no live operation)
// accepts a fresh delete as well.
func (s *Store) BeginSnapshotDeleteAndEnqueue(ctx context.Context, snapshotID string, p EnqueueOperationParams) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var projectID, machineID, executionID, nodeID, status string
		var generation int64
		if err := tx.QueryRow(ctx, `SELECT project_id,source_machine_id,source_execution_id,source_generation,node_id,status
			FROM snapshots WHERE id=$1 FOR UPDATE`, snapshotID).
			Scan(&projectID, &machineID, &executionID, &generation, &nodeID, &status); err != nil {
			return err
		}
		if p.ProjectID != projectID || p.MachineID != machineID || p.ExecutionID != executionID ||
			p.Generation != generation || p.DispatchNodeID != nodeID || p.Kind != "snapshot_delete" {
			return ErrSnapshotStatusConflict
		}
		if status == "DELETED" {
			return ErrSnapshotStatusConflict
		}
		// Deterministic idempotent replay: an in-flight delete (PENDING/CLAIMED)
		// for this snapshot returns the existing operation unchanged. Terminal
		// attempts fall through and re-enqueue under a suffixed key.
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
			if existing.Kind != "snapshot_delete" {
				// 同键异类操作（键冲突/残留）不得被当作删除重放。
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
		var referenced bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM snapshot_references
			WHERE snapshot_id=$1 AND released_at IS NULL)`, snapshotID).Scan(&referenced); err != nil {
			return err
		}
		// DELETING is legal here only for a fresh (re-keyed) attempt after a
		// terminal failure or an orphaned row; the state itself is unchanged.
		if referenced || (status != "READY" && status != "UNAVAILABLE" && status != "CREATING" && status != "DELETING") {
			return ErrSnapshotStatusConflict
		}
		if status != "DELETING" {
			if _, err := tx.Exec(ctx, `UPDATE snapshots SET status='DELETING',updated_at=now() WHERE id=$1`, snapshotID); err != nil {
				return err
			}
		}
		created, err := insertSnapshotOperation(ctx, tx, p)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

// RescueReplacementParams describes one atomic rescue execution replacement.
// OldExecutionID/OldGeneration are the CAS guard; Request must address the new
// identity. The snapshot is rechecked under the same transaction as the
// machine transition and operation enqueue.
type RescueReplacementParams struct {
	ProjectID               string
	MachineID               string
	OldExecutionID          string
	OldGeneration           int64
	NewExecutionID          string
	OperationID             string
	SnapshotID              string
	Request                 []byte
	DispatchNodeID          string
	RequiredFeature         string
	TargetCompatibilityKey  string
	RequireMemoryCompatible bool
}

// EnqueueRescueReplacement atomically advances the machine identity, clears
// observed state, removes the old route backend, protects the READY snapshot,
// and writes the rescue outbox operation. A failed dispatch is recoverable by
// retrying that operation; stale callers cannot repeat the CAS.
func (s *Store) EnqueueRescueReplacement(ctx context.Context, p RescueReplacementParams) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		existing, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
		if err != nil {
			return err
		}
		if existing != nil {
			if !jsonEqual(existing.Request, p.Request) || existing.Kind != "rescue" ||
				existing.MachineID != p.MachineID || existing.ExecutionID != p.NewExecutionID {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}
		var snapshotProject, snapshotNode, snapshotKind, snapshotCompatibilityKey, status, integrity string
		if err := tx.QueryRow(ctx, `SELECT project_id,node_id,kind,compatibility_key,status,integrity FROM snapshots WHERE id=$1 FOR UPDATE`, p.SnapshotID).
			Scan(&snapshotProject, &snapshotNode, &snapshotKind, &snapshotCompatibilityKey, &status, &integrity); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrSnapshotStatusConflict
			}
			return err
		}
		if status != "READY" || integrity == "MISSING" || integrity == "CORRUPT" ||
			snapshotProject != p.ProjectID || snapshotNode != p.DispatchNodeID {
			return ErrSnapshotStatusConflict
		}
		// New callers provide an action-time capability/key fence. Keep empty
		// fields compatible with legacy internal callers while the API always
		// uses the strict path.
		if p.RequiredFeature != "" {
			var nodeStatus string
			var nodeDraining, featurePresent bool
			var nodeCompatibilityKey string
			if err := tx.QueryRow(ctx, `SELECT status,draining,snapshot_compatibility_key,
				coalesce(feature_ids,'[]'::jsonb) ? $2 FROM nodes WHERE id=$1 FOR SHARE`,
				p.DispatchNodeID, p.RequiredFeature).
				Scan(&nodeStatus, &nodeDraining, &nodeCompatibilityKey, &featurePresent); err != nil {
				return ErrRescueConflict
			}
			if nodeStatus != "HEALTHY" || nodeDraining || !featurePresent ||
				nodeCompatibilityKey != p.TargetCompatibilityKey {
				return ErrRescueConflict
			}
			if p.RequireMemoryCompatible && (snapshotKind != "MEMORY" || snapshotCompatibilityKey == "" ||
				snapshotCompatibilityKey != nodeCompatibilityKey) {
				return ErrRescueConflict
			}
		}
		if p.OldExecutionID == "" || p.NewExecutionID == "" || p.NewExecutionID == p.OldExecutionID || p.OldGeneration < 1 {
			return ErrRescueConflict
		}
		var actualProject string
		err = tx.QueryRow(ctx, `UPDATE machines SET
			current_execution_id=$4, generation=$3+1,
			observed_state='', observed_slot_ip='', observed_readiness='UNKNOWN',
			last_observed_at=NULL, updated_at=now()
			FROM apps a
			WHERE machines.id=$1 AND machines.current_execution_id=$2 AND machines.generation=$3
			AND machines.node_id=$5 AND machines.desired_state <> 'DELETED'
			AND machines.lifecycle_delete_phase='ACTIVE' AND a.id=machines.app_id
			RETURNING a.project_id`, p.MachineID, p.OldExecutionID, p.OldGeneration,
			p.NewExecutionID, p.DispatchNodeID).Scan(&actualProject)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrRescueConflict
		}
		if err != nil {
			return fmt.Errorf("advance rescue execution: %w", err)
		}
		if actualProject != p.ProjectID {
			return ownershipConflict("machine", p.MachineID, "project_id", p.ProjectID, actualProject)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM route_backends WHERE machine_id=$1`,
			p.MachineID); err != nil {
			return fmt.Errorf("detach rescue route: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO snapshot_references(snapshot_id,operation_id,kind)
			VALUES($1,$2,'restore')
			ON CONFLICT (snapshot_id,operation_id) DO UPDATE SET released_at=NULL`, p.SnapshotID, p.OperationID); err != nil {
			return fmt.Errorf("protect rescue snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO operations(id,project_id,machine_id,execution_id,
			generation,kind,idempotency_key,status,request,dispatch_node_id)
			VALUES($1,$2,$3,$4,$5,'rescue',$1,'PENDING',$6::jsonb,$7)`,
			p.OperationID, p.ProjectID, p.MachineID, p.NewExecutionID,
			p.OldGeneration+1, string(p.Request), p.DispatchNodeID); err != nil {
			return fmt.Errorf("enqueue rescue: %w", err)
		}
		created, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
		if err != nil || created == nil {
			if err == nil {
				err = fmt.Errorf("rescue operation disappeared")
			}
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

// ---------------------------------------------------------------------------
// snapshot schedules
// ---------------------------------------------------------------------------

// UpsertSnapshotSchedule 幂等创建/更新调度。
func (s *Store) UpsertSnapshotSchedule(ctx context.Context, sc SnapshotSchedule) error {
	var level any
	if sc.CompressionLevel != nil {
		level = *sc.CompressionLevel
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO snapshot_schedules(id, project_id, app_id, machine_id,
			interval_seconds, jitter_seconds, max_count, max_age_seconds,
			compression, compression_level, enabled, next_run_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now() + make_interval(secs => $5::int))
		ON CONFLICT (id) DO UPDATE SET interval_seconds=EXCLUDED.interval_seconds,
			jitter_seconds=EXCLUDED.jitter_seconds, max_count=EXCLUDED.max_count,
			max_age_seconds=EXCLUDED.max_age_seconds, compression=EXCLUDED.compression,
			compression_level=EXCLUDED.compression_level, enabled=EXCLUDED.enabled,
			updated_at=now()`,
		sc.ID, sc.ProjectID, sc.AppID, sc.MachineID, sc.IntervalSeconds,
		sc.JitterSeconds, sc.MaxCount, sc.MaxAgeSeconds, sc.Compression, level, sc.Enabled)
	if err != nil {
		return fmt.Errorf("upsert snapshot schedule: %w", err)
	}
	return nil
}

// GetSnapshotSchedule 按 ID 查询。
func (s *Store) GetSnapshotSchedule(ctx context.Context, id string) (*SnapshotSchedule, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, project_id, app_id, machine_id, interval_seconds, jitter_seconds,
			max_count, max_age_seconds, compression, compression_level, enabled,
			next_run_at, created_at, updated_at
		FROM snapshot_schedules WHERE id=$1`, id)
	var sc SnapshotSchedule
	if err := row.Scan(&sc.ID, &sc.ProjectID, &sc.AppID, &sc.MachineID,
		&sc.IntervalSeconds, &sc.JitterSeconds, &sc.MaxCount, &sc.MaxAgeSeconds,
		&sc.Compression, &sc.CompressionLevel, &sc.Enabled, &sc.NextRunAt,
		&sc.CreatedAt, &sc.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get snapshot schedule: %w", err)
	}
	return &sc, nil
}

// ListSnapshotSchedules 返回 app 的调度。
func (s *Store) ListSnapshotSchedules(ctx context.Context, appID string) ([]SnapshotSchedule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, project_id, app_id, machine_id, interval_seconds, jitter_seconds,
			max_count, max_age_seconds, compression, compression_level, enabled,
			next_run_at, created_at, updated_at
		FROM snapshot_schedules WHERE app_id=$1 ORDER BY id`, appID)
	if err != nil {
		return nil, fmt.Errorf("list snapshot schedules: %w", err)
	}
	defer rows.Close()
	var out []SnapshotSchedule
	for rows.Next() {
		var sc SnapshotSchedule
		if err := rows.Scan(&sc.ID, &sc.ProjectID, &sc.AppID, &sc.MachineID,
			&sc.IntervalSeconds, &sc.JitterSeconds, &sc.MaxCount, &sc.MaxAgeSeconds,
			&sc.Compression, &sc.CompressionLevel, &sc.Enabled, &sc.NextRunAt,
			&sc.CreatedAt, &sc.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan snapshot schedule: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// ListAllSnapshotSchedules 返回全部调度（retention/调度 reconcile 输入）。
func (s *Store) ListAllSnapshotSchedules(ctx context.Context) ([]SnapshotSchedule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, project_id, app_id, machine_id, interval_seconds, jitter_seconds,
			max_count, max_age_seconds, compression, compression_level, enabled,
			next_run_at, created_at, updated_at
		FROM snapshot_schedules ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list all snapshot schedules: %w", err)
	}
	defer rows.Close()
	var out []SnapshotSchedule
	for rows.Next() {
		var sc SnapshotSchedule
		if err := rows.Scan(&sc.ID, &sc.ProjectID, &sc.AppID, &sc.MachineID,
			&sc.IntervalSeconds, &sc.JitterSeconds, &sc.MaxCount, &sc.MaxAgeSeconds,
			&sc.Compression, &sc.CompressionLevel, &sc.Enabled, &sc.NextRunAt,
			&sc.CreatedAt, &sc.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan snapshot schedule: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// DeleteSnapshotSchedule 删除调度（幂等）。
func (s *Store) DeleteSnapshotSchedule(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM snapshot_schedules WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("delete snapshot schedule: %w", err)
	}
	return nil
}

// ClaimDueSnapshotSchedules atomically claims a bounded, locked set and moves
// each schedule to its next interval plus stable SQL-computed jitter.
func (s *Store) ClaimDueSnapshotSchedules(ctx context.Context, now time.Time, limit int) ([]SnapshotSchedule, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM snapshot_schedules
			WHERE enabled AND next_run_at IS NOT NULL AND next_run_at <= $1
			ORDER BY next_run_at, id
			FOR UPDATE SKIP LOCKED LIMIT $2
		)
		UPDATE snapshot_schedules s SET
			next_run_at = $1 + make_interval(secs => s.interval_seconds +
				CASE WHEN s.jitter_seconds > 0 THEN
					mod(('x'||substr(md5(s.id),1,8))::bit(32)::bigint, s.jitter_seconds + 1)::int
				ELSE 0 END),
			updated_at=$1
		FROM due WHERE s.id=due.id
		RETURNING s.id, s.project_id, s.app_id, s.machine_id, s.interval_seconds, s.jitter_seconds,
			s.max_count, s.max_age_seconds, s.compression, s.compression_level, s.enabled,
			s.next_run_at, s.created_at, s.updated_at`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("claim snapshot schedules: %w", err)
	}
	defer rows.Close()
	var out []SnapshotSchedule
	for rows.Next() {
		var sc SnapshotSchedule
		if err := rows.Scan(&sc.ID, &sc.ProjectID, &sc.AppID, &sc.MachineID,
			&sc.IntervalSeconds, &sc.JitterSeconds, &sc.MaxCount, &sc.MaxAgeSeconds,
			&sc.Compression, &sc.CompressionLevel, &sc.Enabled, &sc.NextRunAt,
			&sc.CreatedAt, &sc.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan claimed schedule: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// AcquireSnapshotReference protects an artifact while a fork/restore operation
// may consume it. A fresh acquire requires READY; re-acquiring a reference the
// same operation already holds is idempotent even if the snapshot later moved
// out of READY (v1.4-A: reference ownership must follow operation lifetime, not
// RPC-window status checks).
func (s *Store) AcquireSnapshotReference(ctx context.Context, snapshotID, operationID, kind string) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO snapshot_references(snapshot_id, operation_id, kind)
		SELECT $1,$2,$3 WHERE
			EXISTS(SELECT 1 FROM snapshots WHERE id=$1 AND status='READY')
			OR EXISTS(SELECT 1 FROM snapshot_references WHERE snapshot_id=$1 AND operation_id=$2 AND released_at IS NULL)
		ON CONFLICT (snapshot_id, operation_id) DO UPDATE SET released_at=NULL`,
		snapshotID, operationID, kind)
	if err != nil {
		return fmt.Errorf("acquire snapshot reference: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSnapshotStatusConflict
	}
	return nil
}

func (s *Store) ReleaseSnapshotReference(ctx context.Context, snapshotID, operationID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE snapshot_references SET released_at=now()
		WHERE snapshot_id=$1 AND operation_id=$2 AND released_at IS NULL`, snapshotID, operationID)
	if err != nil {
		return fmt.Errorf("release snapshot reference: %w", err)
	}
	return nil
}

// ReleaseTerminalOperationReferences releases snapshot references whose
// consuming operation reached a terminal state without an explicit release
// (crash between completion and release, or failed dispatch). References held
// by non-terminal operations stay untouched (v1.4-A).
func (s *Store) ReleaseTerminalOperationReferences(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshot_references r SET released_at=now()
		WHERE r.released_at IS NULL AND EXISTS (
			SELECT 1 FROM operations o WHERE o.id=r.operation_id
				AND o.status IN ('SUCCEEDED','FAILED'))`)
	if err != nil {
		return 0, fmt.Errorf("release terminal operation references: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// SnapshotReferenced reports whether an active fork/restore operation protects
// the snapshot from retention or explicit deletion.
func (s *Store) SnapshotReferenced(ctx context.Context, snapshotID string) (bool, error) {
	var referenced bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM snapshot_references
		WHERE snapshot_id=$1 AND released_at IS NULL
	)`, snapshotID).Scan(&referenced)
	if err != nil {
		return false, fmt.Errorf("check snapshot references: %w", err)
	}
	return referenced, nil
}

// SetSnapshotIntegrity 记录正交的产物完整性观测（v1.4-B）。MISSING 不自动
// 推进 LOST（LOST 只能由节点退役/人工确认/权威 inventory 证据触发）。
func (s *Store) SetSnapshotIntegrity(ctx context.Context, id, integrity string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET integrity=$2, integrity_observed_at=now(), updated_at=now()
		WHERE id=$1 AND status NOT IN ('DELETED','LOST')`, id, integrity)
	if err != nil {
		return fmt.Errorf("set snapshot integrity: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSnapshotStatusConflict
	}
	return nil
}

// ApplySnapshotIntegrity atomically records integrity and its availability
// consequence. A metadata observation never heals CORRUPT; only an explicit
// successful content scrub may do that.
func (s *Store) ApplySnapshotIntegrity(ctx context.Context, id, integrity string) (bool, error) {
	if integrity != "MISSING" && integrity != "CORRUPT" && integrity != "METADATA_VERIFIED" {
		return false, fmt.Errorf("invalid snapshot integrity %q", integrity)
	}
	var changed bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var oldIntegrity, status string
		if err := tx.QueryRow(ctx, `SELECT integrity,status FROM snapshots WHERE id=$1 FOR UPDATE`, id).Scan(&oldIntegrity, &status); err != nil {
			return err
		}
		if oldIntegrity == "CORRUPT" && integrity == "METADATA_VERIFIED" {
			return nil
		}
		newStatus := status
		if (integrity == "MISSING" || integrity == "CORRUPT") && status == "READY" {
			newStatus = "UNAVAILABLE"
		} else if integrity == "METADATA_VERIFIED" && status == "UNAVAILABLE" {
			newStatus = "READY"
		}
		changed = oldIntegrity != integrity || status != newStatus
		_, err := tx.Exec(ctx, `UPDATE snapshots SET integrity=$2,integrity_observed_at=now(),status=$3,updated_at=now()
			WHERE id=$1 AND status NOT IN ('DELETED','LOST')`, id, integrity, newStatus)
		return err
	})
	return changed, err
}

// ApplySnapshotInventoryObservation accepts ordering and applies the complete
// inventory in one transaction. Metadata is compared against PG immutable
// facts; absent non-creating rows become MISSING. Every touched row retains the
// observation ID, epoch, generation and receive time that justified it.
func (s *Store) ApplySnapshotInventoryObservation(ctx context.Context, o InventoryObservation, items map[string]SnapshotInventoryItem) (bool, []IntegrityTransition, error) {
	if o.ResourceType != "snapshot" || o.ItemCount != len(items) {
		return false, nil, nil
	}
	var transitions []IntegrityTransition
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := acceptInventoryObservationTx(ctx, tx, &o); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT id,project_id,source_machine_id,kind,status,size_bytes,checksum,compatibility_key,integrity
			FROM snapshots WHERE node_id=$1 AND status IN ('CREATING','READY','UNAVAILABLE') FOR UPDATE`, o.NodeID)
		if err != nil {
			return err
		}
		type row struct {
			id, project, machine, kind, status, checksum, compatibility, integrity string
			size                                                                   int64
		}
		var all []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.project, &r.machine, &r.kind, &r.status, &r.size, &r.checksum, &r.compatibility, &r.integrity); err != nil {
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
			if r.status == "CREATING" {
				continue
			}
			item, present := items[r.id]
			integrity, reason := "MISSING", "absent from complete inventory"
			if present {
				integrity, reason = "METADATA_VERIFIED", ""
				// Missing immutable fields are not mismatch evidence. They are an
				// insufficient observation and therefore fail closed to UNKNOWN.
				if item.SizeBytes <= 0 || item.Checksum == "" || item.Kind == "" || (r.compatibility != "" && item.CompatibilityKey == "") {
					integrity, reason = "UNKNOWN", "incomplete immutable metadata"
				} else if item.Checksum != r.checksum || item.Kind != r.kind || item.CompatibilityKey != r.compatibility {
					integrity, reason = "CORRUPT", "immutable metadata mismatch"
				}
				// Snapshot size may change when asynchronous compression finishes;
				// checksum/kind/compatibility are the immutable identity. A non-zero
				// observed size is required above, but size drift alone is not corruption.
			}
			// Presence or incomplete metadata is not evidence that corrupt content healed.
			if r.integrity == "CORRUPT" && (integrity == "METADATA_VERIFIED" || integrity == "UNKNOWN") {
				integrity = "CORRUPT"
				reason = "awaiting successful content scrub"
			}
			newStatus := r.status
			if (integrity == "MISSING" || integrity == "CORRUPT") && r.status == "READY" {
				newStatus = "UNAVAILABLE"
			}
			if integrity == "METADATA_VERIFIED" && r.status == "UNAVAILABLE" {
				newStatus = "READY"
			}
			if r.integrity != integrity || r.status != newStatus {
				transitions = append(transitions, IntegrityTransition{ID: r.id, ProjectID: r.project, MachineID: r.machine, From: r.integrity, To: integrity})
			}
			if _, err := tx.Exec(ctx, `UPDATE snapshots SET integrity=$2,integrity_reason=$3,status=$4,
				integrity_observed_at=$5,inventory_epoch=$6,inventory_generation=$7,inventory_received_at=$8,
				inventory_observation_id=$9,updated_at=now() WHERE id=$1`, r.id, integrity, reason, newStatus,
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

// ListSnapshotsOnNode 返回节点上的非终态快照（inventory 对账输入）。
func (s *Store) ListSnapshotsOnNode(ctx context.Context, nodeID string) ([]Snapshot, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+snapshotCols+` FROM snapshots
		WHERE node_id=$1 AND status IN ('CREATING','READY','UNAVAILABLE')`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list snapshots on node: %w", err)
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *snap)
	}
	return out, rows.Err()
}

// SetSnapshotExpiry 更新保留期（retention 计算用）。
func (s *Store) SetSnapshotExpiry(ctx context.Context, id string, at *time.Time) error {
	var arg any
	if at != nil {
		arg = *at
	}
	_, err := s.pool.Exec(ctx, `UPDATE snapshots SET expires_at=$2, updated_at=now() WHERE id=$1`, id, arg)
	if err != nil {
		return fmt.Errorf("set snapshot expiry: %w", err)
	}
	return nil
}

// ForkMachineParams 是 fork 产生 debug machine 行的参数（v1.3-C）。
// fork machine 不参与 app 对账（deployment_id=”、desired_state=CREATED、
// 无 route 投影），生命周期由 TTL 回收。
type ForkMachineParams struct {
	ProjectID               string
	AppID                   string
	MachineID               string
	ExecutionID             string
	NodeID                  string
	ExpiresAt               *time.Time
	RequiredFeature         string
	TargetCompatibilityKey  string
	RequireMemoryCompatible bool
}

// CreateForkMachineAndEnqueue atomically protects the READY snapshot, creates
// the ephemeral machine fact, and writes the fork outbox operation.
func (s *Store) CreateForkMachineAndEnqueue(ctx context.Context, snapshotID string, p ForkMachineParams, operation EnqueueOperationParams) (Operation, error) {
	var op Operation
	if p.MachineID == "" || p.ExecutionID == "" || operation.Kind != "fork" || operation.ProjectID != p.ProjectID ||
		operation.MachineID != p.MachineID || operation.ExecutionID != p.ExecutionID || operation.Generation != 1 ||
		operation.DispatchNodeID != p.NodeID {
		return op, errors.New("fork snapshot: invalid atomic command")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var projectID, nodeID, status, integrity string
		if err := tx.QueryRow(ctx, `SELECT project_id,node_id,status,integrity FROM snapshots WHERE id=$1 FOR UPDATE`, snapshotID).
			Scan(&projectID, &nodeID, &status, &integrity); err != nil {
			return err
		}
		if status != "READY" || integrity == "MISSING" || integrity == "CORRUPT" ||
			projectID != p.ProjectID || nodeID != p.NodeID {
			return ErrSnapshotStatusConflict
		}
		if p.RequiredFeature != "" {
			var nodeStatus string
			var nodeDraining, featurePresent bool
			var nodeCompatibilityKey string
			if err := tx.QueryRow(ctx, `SELECT status,draining,snapshot_compatibility_key,
				coalesce(feature_ids,'[]'::jsonb) ? $2 FROM nodes WHERE id=$1 FOR SHARE`,
				p.NodeID, p.RequiredFeature).
				Scan(&nodeStatus, &nodeDraining, &nodeCompatibilityKey, &featurePresent); err != nil {
				return ErrSnapshotStatusConflict
			}
			if nodeStatus != "HEALTHY" || nodeDraining || !featurePresent ||
				nodeCompatibilityKey != p.TargetCompatibilityKey ||
				(p.RequireMemoryCompatible && nodeCompatibilityKey == "") {
				return ErrSnapshotStatusConflict
			}
		}
		var expires any
		if p.ExpiresAt != nil {
			expires = *p.ExpiresAt
		}
		if _, err := tx.Exec(ctx, `INSERT INTO machines(id,app_id,deployment_id,replica_ordinal,hostname,
			desired_state,generation,current_execution_id,requested_vcpu,requested_mem_mib,requested_disk_mib,
			image_ref,node_id,expires_at,restart_mode,restart_max_attempts,restart_backoff_seconds,restart_stable_window_seconds)
			VALUES($1,$2,'',-1,'','CREATED',1,$3,1,512,10240,'',$4,$5,'NEVER',0,0,0)`,
			p.MachineID, p.AppID, p.ExecutionID, p.NodeID, expires); err != nil {
			return fmt.Errorf("insert fork machine: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO snapshot_references(snapshot_id,operation_id,kind) VALUES($1,$2,'fork')`,
			snapshotID, operation.OperationID); err != nil {
			return fmt.Errorf("protect fork snapshot: %w", err)
		}
		created, err := insertSnapshotOperation(ctx, tx, operation)
		if err != nil {
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

func insertSnapshotOperation(ctx context.Context, tx pgx.Tx, p EnqueueOperationParams) (*Operation, error) {
	existing, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Kind != p.Kind || existing.MachineID != p.MachineID || existing.ExecutionID != p.ExecutionID ||
			existing.Generation != p.Generation || !jsonEqual(existing.Request, p.Request) {
			return nil, ErrRequestConflict
		}
		return existing, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations(id,project_id,machine_id,execution_id,generation,kind,
		idempotency_key,status,request,dispatch_node_id) VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb,NULLIF($8,''))`,
		p.OperationID, p.ProjectID, p.MachineID, p.ExecutionID, p.Generation, p.Kind, string(p.Request), p.DispatchNodeID); err != nil {
		return nil, fmt.Errorf("enqueue %s: %w", p.Kind, err)
	}
	return selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
}

// InsertForkMachine 插入 fork 机器的期望行（幂等）。不产生 create 操作——
// Deprecated: use CreateForkMachineAndEnqueue.
func (s *Store) InsertForkMachine(ctx context.Context, p ForkMachineParams) error {
	var expires any
	if p.ExpiresAt != nil {
		expires = *p.ExpiresAt
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, requested_disk_mib, image_ref, node_id, expires_at,
			restart_mode, restart_max_attempts, restart_backoff_seconds,
			restart_stable_window_seconds)
		VALUES($1,$2,'',-1,'', 'CREATED', 1, $3, 1, 512, 10240, '', $4, $5,
			'NEVER', 0, 0, 0)
		ON CONFLICT (id) DO UPDATE SET
			current_execution_id=EXCLUDED.current_execution_id,
			node_id=EXCLUDED.node_id, expires_at=EXCLUDED.expires_at,
			updated_at=now()
		WHERE machines.desired_state <> 'DELETED'`,
		p.MachineID, p.AppID, p.ExecutionID, p.NodeID, expires)
	if err != nil {
		return fmt.Errorf("insert fork machine: %w", err)
	}
	return nil
}
