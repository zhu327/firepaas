// prewarm.go：v1.4-C（docs/v1.4-plan.md §7）显式镜像预热、coverage 观测与
// 有界 cache pin 的持久层。prewarm operation 复用 operations outbox（kind=
// image_prewarm），逐节点结果落在 image_prewarm_targets；pin 的保护范围是
// project_id + image_digest + target selector，GC 按节点计算 roots。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PrewarmTarget 是 image_prewarm_targets 行。
type PrewarmTarget struct {
	OperationID string
	NodeID      string
	Status      string // PENDING|SUCCEEDED|FAILED
	Error       string
	Attempts    int
	MaxAttempts int
	DeadlineAt  time.Time
	UpdatedAt   time.Time
}

// ImagePin 是 image_pins 行。
type ImagePin struct {
	ID          string
	ProjectID   string
	ImageDigest string
	Selector    string // node_pool:<name> | node:<id>
	Owner       string
	Reason      string
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

var (
	ErrImagePinNotFound   = errors.New("image pin not found")
	ErrImagePinDuplicate  = errors.New("image pin already exists")
	ErrImageSizeUnknown   = errors.New("image size has not been observed")
	ErrPrewarmNotAllowed  = errors.New("prewarm rejected")
	ErrPrewarmNotFound    = errors.New("prewarm operation not found")
	ErrImagePinQuota      = errors.New("image pin quota exceeded")
	ErrImagePinWatermark  = errors.New("image pin target reached disk hard watermark")
	prewarmTerminalStates = []string{"SUCCEEDED", "FAILED"}
)

// FindPrewarmReplay resolves a client idempotency key without consulting any
// mutable admission inputs. The persisted request wraps the canonical client
// intent separately from the resolved dispatch targets.
func (s *Store) FindPrewarmReplay(ctx context.Context, projectID, idempotencyKey string, intent []byte) (*Operation, []PrewarmTarget, error) {
	if idempotencyKey == "" {
		return nil, nil, nil
	}
	var op *Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		existing, err := selectOperationByKey(ctx, tx, projectID, idempotencyKey)
		if err != nil || existing == nil {
			return err
		}
		if existing.Kind != "image_prewarm" || !prewarmIntentEqual(existing.Request, intent) {
			return ErrRequestConflict
		}
		op = existing
		return nil
	})
	if err != nil || op == nil {
		return op, nil, err
	}
	targets, err := s.ListPrewarmTargets(ctx, op.ID)
	return op, targets, err
}

func prewarmIntentEqual(persisted, intent []byte) bool {
	return jsonEqual(prewarmIntent(persisted), intent)
}

func prewarmIntent(request []byte) []byte {
	var envelope struct {
		Intent json.RawMessage `json:"intent"`
	}
	if json.Unmarshal(request, &envelope) == nil && len(envelope.Intent) > 0 {
		return envelope.Intent
	}
	// Compatibility for internal callers that predate the intent envelope.
	return request
}

// CreatePrewarmAndEnqueue atomically persists the outbox operation and its
// per-node targets. Retrying with the same operation ID replays the existing
// rows (idempotent), so leader handover never re-pulls completed nodes.
// maxActive 在同一事务内强制 prewarm 并发上限（避免 check-then-insert 竞态）。
func (s *Store) CreatePrewarmAndEnqueue(ctx context.Context, digest, idempotencyKey string, p EnqueueOperationParams, nodeIDs []string, maxActive int) (Operation, error) {
	var op Operation
	if p.Kind != "image_prewarm" || p.OperationID == "" || p.ProjectID == "" || digest == "" || len(nodeIDs) == 0 {
		return op, errors.New("prewarm: invalid atomic command")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		idemKey := idempotencyKey
		if idemKey == "" {
			idemKey = p.OperationID
		}
		existing, err := selectOperationByKey(ctx, tx, p.ProjectID, idemKey)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.Kind != "image_prewarm" || !prewarmIntentEqual(existing.Request, prewarmIntent(p.Request)) {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}
		// Serialize admission per project. MVCC count alone permits two concurrent
		// transactions to both observe capacity and exceed the limit.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1448142413))`, p.ProjectID); err != nil {
			return fmt.Errorf("lock prewarm project: %w", err)
		}
		// The first lookup can race a concurrent transaction waiting on this
		// project lock. Re-read after acquiring it before quota checks/insertion.
		existing, err = selectOperationByKey(ctx, tx, p.ProjectID, idemKey)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.Kind != "image_prewarm" || !prewarmIntentEqual(existing.Request, prewarmIntent(p.Request)) {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}
		for _, nodeID := range nodeIDs {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, localGCLockKey(nodeID, digest)); err != nil {
				return err
			}
			var deleting bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM local_gc_claims WHERE node_id=$1 AND artifact_key=$2
				AND status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED'))`, nodeID, digest).Scan(&deleting); err != nil {
				return err
			}
			if deleting {
				return ErrLocalGCClaimConflict
			}
		}
		if maxActive > 0 {
			var active int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM operations
				WHERE kind='image_prewarm' AND project_id=$1 AND status IN ('PENDING','CLAIMED')`, p.ProjectID).Scan(&active); err != nil {
				return err
			}
			if active >= maxActive {
				return ErrPrewarmNotAllowed
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO operations(id,project_id,machine_id,execution_id,generation,kind,idempotency_key,status,request,dispatch_node_id)
			VALUES($1,$2,'','',0,'image_prewarm',$3,'PENDING',$4::jsonb,'')`,
			p.OperationID, p.ProjectID, idemKey, string(p.Request)); err != nil {
			return fmt.Errorf("enqueue prewarm: %w", err)
		}
		for _, node := range nodeIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO image_prewarm_targets(operation_id,node_id,status) VALUES($1,$2,'PENDING')`,
				p.OperationID, node); err != nil {
				return fmt.Errorf("insert prewarm target: %w", err)
			}
		}
		created, err := selectOperationByKey(ctx, tx, p.ProjectID, idemKey)
		if err != nil || created == nil {
			if err == nil {
				err = fmt.Errorf("prewarm operation disappeared")
			}
			return err
		}
		op = *created
		return nil
	})
	return op, err
}

// ClaimPendingPrewarmOperations isolates best-effort prewarm from the runtime
// operation queue. A slow registry can therefore not occupy slots used by
// create/delete/rescue operations.
func (s *Store) ClaimPendingPrewarmOperations(ctx context.Context, limit int) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `UPDATE operations SET status='CLAIMED',claimed_at=now(),attempts=attempts+1,updated_at=now()
		WHERE id IN (SELECT id FROM operations WHERE status='PENDING' AND kind='image_prewarm'
		AND (attempts=0 OR updated_at < now()-(interval '2 seconds'*power(2,least(attempts,5))))
		ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id,project_id,machine_id,execution_id,generation,kind,status,coalesce(dispatch_node_id,''),
		coalesce(request::text,'{}'),coalesce(result::text,'{}'),coalesce(error,''),created_at,updated_at`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim prewarm operations: %w", err)
	}
	defer rows.Close()
	var out []Operation
	for rows.Next() {
		var op Operation
		if err := rows.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID, &op.Generation,
			&op.Kind, &op.Status, &op.DispatchNodeID, &op.Request, &op.Result, &op.Error,
			&op.CreatedAt, &op.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

// ListPrewarmTargets returns the per-node dispatch state of a prewarm operation.
func (s *Store) ListPrewarmTargets(ctx context.Context, operationID string) ([]PrewarmTarget, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT operation_id,node_id,status,error,attempts,max_attempts,deadline_at,updated_at
		FROM image_prewarm_targets WHERE operation_id=$1 ORDER BY node_id`, operationID)
	if err != nil {
		return nil, fmt.Errorf("list prewarm targets: %w", err)
	}
	defer rows.Close()
	var out []PrewarmTarget
	for rows.Next() {
		var t PrewarmTarget
		if err := rows.Scan(&t.OperationID, &t.NodeID, &t.Status, &t.Error, &t.Attempts,
			&t.MaxAttempts, &t.DeadlineAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// SetPrewarmTargetStatus records a per-node result. FAILED is terminal within
// its operation (re-run via a new prewarm, which is agent-idempotent anyway).
func (s *Store) SetPrewarmTargetStatus(ctx context.Context, operationID, nodeID, status, errText string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE image_prewarm_targets SET status=$3,error=$4,updated_at=now()
		WHERE operation_id=$1 AND node_id=$2 AND status='PENDING'`,
		operationID, nodeID, status, errText)
	if err != nil {
		return fmt.Errorf("set prewarm target status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("prewarm target %s/%s already terminal", operationID, nodeID)
	}
	return nil
}

// RecordPrewarmTargetAttemptFailure bounds transient/unreachable retries. It
// increments the durable target attempt and converts it to FAILED at the
// attempt budget or deadline. terminal reports whether retrying must stop.
func (s *Store) RecordPrewarmTargetAttemptFailure(ctx context.Context, operationID, nodeID, errText string) (terminal bool, err error) {
	err = s.pool.QueryRow(ctx, `
		UPDATE image_prewarm_targets
		SET attempts=attempts+1,
			status=CASE WHEN attempts+1 >= max_attempts OR now() >= deadline_at THEN 'FAILED' ELSE 'PENDING' END,
			error=$3, updated_at=now()
		WHERE operation_id=$1 AND node_id=$2 AND status='PENDING'
		RETURNING status='FAILED'`, operationID, nodeID, errText).Scan(&terminal)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("record prewarm target attempt: %w", err)
	}
	return terminal, nil
}

// CompletePrewarmWithEvent atomically completes an operation and emits its
// exactly-once completion event. Re-dispatch after completion inserts nothing.
func (s *Store) CompletePrewarmWithEvent(ctx context.Context, opID string, result []byte, ev UserEvent) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var project string
		err := tx.QueryRow(ctx, `UPDATE operations SET status='SUCCEEDED',result=$2::jsonb,error='',completed_at=now(),updated_at=now()
			WHERE id=$1 AND kind='image_prewarm' AND status IN ('PENDING','CLAIMED') RETURNING project_id`, opID, string(result)).Scan(&project)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("complete prewarm: %w", err)
		}
		if ev.ProjectID == "" {
			ev.ProjectID = project
		}
		if len(ev.Details) == 0 {
			ev.Details = []byte(`{}`)
		}
		_, err = tx.Exec(ctx, `INSERT INTO user_events(at,project_id,app_id,machine_id,type,details)
			VALUES(now(),$1,$2,$3,$4,$5::jsonb)`, ev.ProjectID, ev.AppID, ev.MachineID, ev.Type, []byte(ev.Details))
		if err != nil {
			return fmt.Errorf("record prewarm completion event: %w", err)
		}
		return nil
	})
}

// ActivePrewarmCount counts in-flight prewarm operations for a project
// （观测/预检用；强制上限在 CreatePrewarmAndEnqueue 事务内）。
func (s *Store) ActivePrewarmCount(ctx context.Context, projectID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM operations
		WHERE kind='image_prewarm' AND project_id=$1 AND status IN ('PENDING','CLAIMED')`, projectID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count active prewarms: %w", err)
	}
	return n, nil
}

// UpsertImageSize records an observed digest size (PullImage response is the
// observation source; sizes only grow the accounting accuracy).
func (s *Store) UpsertImageSize(ctx context.Context, digest string, sizeMib int64) error {
	if digest == "" || sizeMib <= 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO image_sizes(digest,size_mib,observed_at) VALUES($1,$2,now())
		ON CONFLICT (digest) DO UPDATE SET size_mib=EXCLUDED.size_mib, observed_at=now()`,
		digest, sizeMib)
	if err != nil {
		return fmt.Errorf("upsert image size: %w", err)
	}
	return nil
}

// GetImageSize returns the observed size of a digest (ok=false = unknown).
func (s *Store) GetImageSize(ctx context.Context, digest string) (int64, bool, error) {
	var size int64
	err := s.pool.QueryRow(ctx, `SELECT size_mib FROM image_sizes WHERE digest=$1`, digest).Scan(&size)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get image size: %w", err)
	}
	return size, true, nil
}

// FindImagePinReplay resolves a pin idempotency key before selector expansion
// or other mutable admission checks.
func (s *Store) FindImagePinReplay(ctx context.Context, projectID, idempotencyKey string, request []byte) ([]ImagePin, bool, error) {
	if idempotencyKey == "" {
		return nil, false, nil
	}
	var kind string
	var oldRequest, result []byte
	err := s.pool.QueryRow(ctx, `SELECT kind,request::text,result::text FROM image_pin_idempotency WHERE project_id=$1 AND idempotency_key=$2`, projectID, idempotencyKey).Scan(&kind, &oldRequest, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if kind != "pin" || !jsonEqual(oldRequest, request) {
		return nil, false, ErrRequestConflict
	}
	var pins []ImagePin
	if err := json.Unmarshal(result, &pins); err != nil {
		return nil, false, fmt.Errorf("decode pin idempotency result: %w", err)
	}
	return pins, true, nil
}

type ImagePinLimits struct {
	MaxPins       int
	MaxBytesMib   int64
	MaxTargets    int
	HardWatermark float64
}

// CreateImagePinsAtomic serializes a project's pin admission, rechecks node
// watermark and count/byte/target quotas, and upserts the complete batch in one
// transaction. Selectors must be frozen node:<id> values so later pool growth
// cannot silently expand quota usage.
func (s *Store) CreateImagePinsAtomic(ctx context.Context, pins []ImagePin, ttl time.Duration, idempotencyKey string, request []byte, limits ImagePinLimits) ([]ImagePin, error) {
	if len(pins) == 0 || pins[0].ProjectID == "" || len(pins) > limits.MaxTargets || ttl <= 0 {
		return nil, ErrImagePinQuota
	}
	var out []ImagePin
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		project, digest := pins[0].ProjectID, pins[0].ImageDigest
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1346981442))`, project); err != nil {
			return err
		}
		if idempotencyKey != "" {
			var kind string
			var oldRequest, result []byte
			err := tx.QueryRow(ctx, `SELECT kind,request::text,result::text FROM image_pin_idempotency WHERE project_id=$1 AND idempotency_key=$2`, project, idempotencyKey).Scan(&kind, &oldRequest, &result)
			if err == nil {
				if kind != "pin" || !jsonEqual(oldRequest, request) {
					return ErrRequestConflict
				}
				return json.Unmarshal(result, &out)
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		expiresAt := time.Now().Add(ttl).UTC()
		for i := range pins {
			pins[i].ExpiresAt = expiresAt
		}
		var size int64
		if err := tx.QueryRow(ctx, `SELECT size_mib FROM image_sizes WHERE digest=$1`, digest).Scan(&size); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrImageSizeUnknown
			}
			return err
		}
		for _, pin := range pins {
			if pin.ProjectID != project || pin.ImageDigest != digest || !pin.ExpiresAt.After(time.Now()) || len(pin.Selector) <= 5 || pin.Selector[:5] != "node:" {
				return errors.New("invalid image pin batch")
			}
			nodeID := pin.Selector[5:]
			// Pin admission and GC claim creation share this lock. A successful pin
			// can therefore never race an older delete decision for this node/digest.
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, localGCLockKey(nodeID, digest)); err != nil {
				return err
			}
			var deleting bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM local_gc_claims WHERE node_id=$1 AND artifact_key=$2
				AND status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED'))`, nodeID, digest).Scan(&deleting); err != nil {
				return err
			}
			if deleting {
				return ErrLocalGCClaimConflict
			}
			var total, used int64
			if err := tx.QueryRow(ctx, `SELECT disk_total_mib,disk_used_mib FROM nodes WHERE id=$1 FOR SHARE`, nodeID).Scan(&total, &used); err != nil {
				return fmt.Errorf("pin node %s: %w", nodeID, err)
			}
			if total > 0 && float64(used)/float64(total) >= limits.HardWatermark {
				return ErrImagePinWatermark
			}
		}
		var currentCount int
		var currentBytes int64
		if err := tx.QueryRow(ctx, `SELECT count(*),coalesce(sum(s.size_mib),0)
			FROM image_pins p JOIN image_sizes s ON s.digest=p.image_digest
			WHERE p.project_id=$1 AND p.expires_at>now()`, project).Scan(&currentCount, &currentBytes); err != nil {
			return err
		}
		var additions int
		for _, pin := range pins {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM image_pins WHERE project_id=$1 AND image_digest=$2 AND selector=$3 AND expires_at>now())`, project, digest, pin.Selector).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				additions++
			}
		}
		if currentCount+additions > limits.MaxPins || currentBytes+int64(additions)*size > limits.MaxBytesMib {
			return ErrImagePinQuota
		}
		for _, pin := range pins {
			row := tx.QueryRow(ctx, `INSERT INTO image_pins(id,project_id,image_digest,selector,owner,reason,expires_at)
				VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(project_id,image_digest,selector)
				DO UPDATE SET expires_at=EXCLUDED.expires_at,owner=EXCLUDED.owner,reason=EXCLUDED.reason,updated_at=now()
				RETURNING id,project_id,image_digest,selector,owner,reason,expires_at,created_at`, pin.ID, project, digest, pin.Selector, pin.Owner, pin.Reason, pin.ExpiresAt)
			var created ImagePin
			if err := row.Scan(&created.ID, &created.ProjectID, &created.ImageDigest, &created.Selector, &created.Owner, &created.Reason, &created.ExpiresAt, &created.CreatedAt); err != nil {
				return err
			}
			out = append(out, created)
		}
		if idempotencyKey != "" {
			result, err := json.Marshal(out)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO image_pin_idempotency(project_id,idempotency_key,kind,request,result) VALUES($1,$2,'pin',$3::jsonb,$4::jsonb)`, project, idempotencyKey, string(request), string(result)); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// CreateImagePin is retained for internal single-pin callers/tests. API batch
// admission uses CreateImagePinsAtomic.
func (s *Store) CreateImagePin(ctx context.Context, pin ImagePin) (*ImagePin, error) {
	if !pin.ExpiresAt.After(time.Now()) {
		return nil, fmt.Errorf("create image pin: expires_at must be in the future")
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO image_pins(id,project_id,image_digest,selector,owner,reason,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (project_id,image_digest,selector) DO UPDATE SET expires_at=EXCLUDED.expires_at,
			owner=EXCLUDED.owner, reason=EXCLUDED.reason, updated_at=now()
		RETURNING id,project_id,image_digest,selector,owner,reason,expires_at,created_at`,
		pin.ID, pin.ProjectID, pin.ImageDigest, pin.Selector, pin.Owner, pin.Reason, pin.ExpiresAt)
	var out ImagePin
	if err := row.Scan(&out.ID, &out.ProjectID, &out.ImageDigest, &out.Selector, &out.Owner,
		&out.Reason, &out.ExpiresAt, &out.CreatedAt); err != nil {
		return nil, fmt.Errorf("create image pin: %w", err)
	}
	return &out, nil
}

// ListImagePins returns a project's active pins (projectID 空 = 全部，admin 用)。
func (s *Store) ListImagePins(ctx context.Context, projectID string) ([]ImagePin, error) {
	q := `SELECT id,project_id,image_digest,selector,owner,reason,expires_at,created_at
		FROM image_pins WHERE expires_at > now()`
	args := []any{}
	if projectID != "" {
		q += ` AND project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY created_at`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list image pins: %w", err)
	}
	defer rows.Close()
	var out []ImagePin
	for rows.Next() {
		var p ImagePin
		if err := rows.Scan(&p.ID, &p.ProjectID, &p.ImageDigest, &p.Selector, &p.Owner,
			&p.Reason, &p.ExpiresAt, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeleteImagePin removes a pin and atomically records an optional idempotent
// result. A completed unpin therefore remains replayable after the row is gone.
func (s *Store) DeleteImagePin(ctx context.Context, id, projectID, idempotencyKey string, request []byte) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if idempotencyKey != "" {
			var kind string
			var oldRequest []byte
			err := tx.QueryRow(ctx, `SELECT kind,request::text FROM image_pin_idempotency WHERE project_id=$1 AND idempotency_key=$2`, projectID, idempotencyKey).Scan(&kind, &oldRequest)
			if err == nil {
				if kind != "unpin" || !jsonEqual(oldRequest, request) {
					return ErrRequestConflict
				}
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `DELETE FROM image_pins WHERE id=$1 AND ($2='' OR project_id=$2)`, id, projectID)
		if err != nil {
			return fmt.Errorf("delete image pin: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrImagePinNotFound
		}
		if idempotencyKey != "" {
			result, _ := json.Marshal(map[string]string{"deleted": id})
			if _, err := tx.Exec(ctx, `INSERT INTO image_pin_idempotency(project_id,idempotency_key,kind,request,result) VALUES($1,$2,'unpin',$3::jsonb,$4::jsonb)`, projectID, idempotencyKey, string(request), string(result)); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteExpiredImagePins removes expired pins (housekeeping; protection queries
// already filter on expires_at).
func (s *Store) DeleteExpiredImagePins(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM image_pins WHERE expires_at <= now()`)
	if err != nil {
		return 0, fmt.Errorf("delete expired image pins: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PinnedDigestsForNode computes the node-scoped GC roots from active pins and
// in-flight prewarm operations. A pin only protects nodes its selector matches
// (node_pool:<name> matches the node's pool; node:<id> matches exactly), so one
// pin can never implicitly protect the whole cluster.
func (s *Store) PinnedDigestsForNode(ctx context.Context, nodeID, nodePool string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT image_digest FROM image_pins
		WHERE expires_at > now() AND (selector=$1 OR ($2 <> '' AND selector=$3))`,
		"node:"+nodeID, nodePool, "node_pool:"+nodePool)
	if err != nil {
		return nil, fmt.Errorf("pinned digests for node: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			return nil, err
		}
		out[digest] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// 在途 prewarm 的 digest 也是保护对象：GC 不得与正在拉取的操作互相打架。
	pending, err := s.pool.Query(ctx, `
		SELECT DISTINCT o.request->>'digest' FROM operations o
		JOIN image_prewarm_targets t ON t.operation_id=o.id
		WHERE o.kind='image_prewarm' AND o.status IN ('PENDING','CLAIMED')
			AND t.node_id=$1 AND t.status='PENDING'`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("in-flight prewarm digests for node: %w", err)
	}
	defer pending.Close()
	for pending.Next() {
		var digest string
		if err := pending.Scan(&digest); err != nil {
			return nil, err
		}
		if digest != "" {
			out[digest] = true
		}
	}
	return out, pending.Err()
}

// PinnedBytesForProject sums protected bytes for a project: each active pin
// counts size_mib × current matching node count. Unknown sizes fail closed by
// the API layer (pins require a prewarm observation first).
func (s *Store) PinnedBytesForProject(ctx context.Context, projectID string) (int64, error) {
	var total int64
	err := s.pool.QueryRow(ctx, `
		SELECT coalesce(sum(s.size_mib * CASE
			WHEN p.selector LIKE 'node:%' THEN 1
			ELSE (SELECT count(*) FROM nodes n WHERE n.node_pool = substr(p.selector, 11))
		END),0)
		FROM image_pins p JOIN image_sizes s ON s.digest = p.image_digest
		WHERE p.project_id=$1 AND p.expires_at > now()`, projectID).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("pinned bytes for project: %w", err)
	}
	return total, nil
}

// NodePrewarmStatus summarizes the latest prewarm observation for one digest on
// one node (coverage 查询输入).
type NodePrewarmStatus struct {
	Pending bool
	Failed  bool
	At      time.Time
}

// PrewarmStatusByNode returns only the latest per-node observation for a
// digest. Older failed/pending attempts must not override a newer success.
func (s *Store) PrewarmStatusByNode(ctx context.Context, projectID, digest string) (map[string]NodePrewarmStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (t.node_id) t.node_id,
			t.status='PENDING' AS pending,
			t.status='FAILED' AS failed,
			t.updated_at AS at
		FROM image_prewarm_targets t
		JOIN operations o ON o.id=t.operation_id
		WHERE o.kind='image_prewarm' AND o.project_id=$1 AND o.request->>'digest'=$2
		ORDER BY t.node_id,t.updated_at DESC,t.operation_id DESC`, projectID, digest)
	if err != nil {
		return nil, fmt.Errorf("prewarm status by node: %w", err)
	}
	defer rows.Close()
	out := map[string]NodePrewarmStatus{}
	for rows.Next() {
		var nodeID string
		var st NodePrewarmStatus
		if err := rows.Scan(&nodeID, &st.Pending, &st.Failed, &st.At); err != nil {
			return nil, err
		}
		out[nodeID] = st
	}
	return out, rows.Err()
}
