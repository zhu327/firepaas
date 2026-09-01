// secret_leases.go：v1.2-B（ADR-0024）secret delivery lease 持久层。
//
// 状态机：ISSUED → CLAIMED → DELIVERED → ACKED；RPC 不确定结果为
// UNCERTAIN，过期/失效为 EXPIRED/REVOKED。
// 租约行不含任何明文；所有状态转换走 CAS，二次签发必须换 execution。
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SecretLeaseState 是 lease 状态机的合法状态。
type SecretLeaseState string

const (
	SecretLeaseIssued    SecretLeaseState = "ISSUED"
	SecretLeaseClaimed   SecretLeaseState = "CLAIMED"
	SecretLeaseDelivered SecretLeaseState = "DELIVERED"
	// SecretLeaseUncertain means dispatch crossed the durable pre-send fence but
	// no delivery result was confirmed. The execution may only be deleted.
	SecretLeaseUncertain SecretLeaseState = "UNCERTAIN"
	SecretLeaseAcked     SecretLeaseState = "ACKED"
	SecretLeaseExpired   SecretLeaseState = "EXPIRED"
	SecretLeaseRevoked   SecretLeaseState = "REVOKED"
)

// SecretLease 是一行不含明文的 delivery lease metadata。
type SecretLease struct {
	ID          string           `json:"id"`
	ProjectID   string           `json:"project_id"`
	MachineID   string           `json:"machine_id"`
	ExecutionID string           `json:"execution_id"`
	Generation  int64            `json:"generation"`
	OperationID string           `json:"operation_id"`
	RequestHash string           `json:"request_hash"`
	State       SecretLeaseState `json:"state"`
	ExpiresAt   time.Time        `json:"expires_at"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// DefaultSecretLeaseTTL 覆盖一次 create 派发及其结果确认窗口。
const DefaultSecretLeaseTTL = 10 * time.Minute

var (
	// ErrSecretLeaseConflict：同 machine/execution 已存在任何历史 lease，且
	// 调用并非该 lease 的严格幂等重放。终态 lease 也禁止再次签发。
	ErrSecretLeaseConflict = errors.New("secret lease conflict: lease already exists for execution")
	// ErrSecretLeaseTransition 表示 lease 不存在、绑定不匹配、已过期或状态
	// 不允许；所有 CAS 零行结果均返回该 typed error，调用方必须 fail closed。
	ErrSecretLeaseTransition = errors.New("secret lease transition rejected")
	ErrSecretLeaseNotFound   = errors.New("secret lease not found")
	// ErrSecretLeaseTerminal：lease 已终态/过期，当前 execution 不可再投递。
	// 按 ADR-0024 §6，必须销毁旧 execution 后以新 execution 重签。
	ErrSecretLeaseTerminal = errors.New("secret lease terminal for execution")
)

// EnsureSecretLease 为 create op 幂等签发 lease。只有全部绑定字段一致时
// 才复用现有行；同 machine/execution 的任何其它历史行（包括终态）都拒绝，
// 不确定投递只能销毁旧 execution 后再签发。数据库唯一约束封闭并发窗口。
func (s *Store) EnsureSecretLease(ctx context.Context, projectID, machineID, executionID string,
	generation int64, operationID, requestHash string, ttl time.Duration) (*SecretLease, error) {

	if ttl <= 0 {
		ttl = DefaultSecretLeaseTTL
	}
	if existing, err := s.SecretLeaseForExecution(ctx, machineID, executionID); err == nil {
		if existing.ProjectID == projectID && existing.Generation == generation &&
			existing.OperationID == operationID && existing.RequestHash == requestHash {
			return existing, nil
		}
		return nil, fmt.Errorf("%w: %s/%s", ErrSecretLeaseConflict, machineID, executionID)
	} else if !errors.Is(err, ErrSecretLeaseNotFound) {
		return nil, fmt.Errorf("lookup secret lease: %w", err)
	}

	suffix := executionID
	if len(suffix) > 8 {
		suffix = suffix[len(suffix)-8:]
	}
	id := "sdl-" + uuid.NewString()[:8] + "-" + suffix
	row := s.pool.QueryRow(ctx, `
		INSERT INTO secret_delivery_leases
			(id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'ISSUED',now()+$8::interval)
		RETURNING id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at, created_at, updated_at`,
		id, projectID, machineID, executionID, generation, operationID, requestHash, fmt.Sprintf("%.0f seconds", ttl.Seconds()))

	l, err := scanSecretLease(row)
	if err != nil {
		// 0021 的唯一(machine_id, execution_id)约束封闭并发签发窗口。
		if existing, lookupErr := s.SecretLeaseForExecution(ctx, machineID, executionID); lookupErr == nil {
			if existing.ProjectID == projectID && existing.Generation == generation &&
				existing.OperationID == operationID && existing.RequestHash == requestHash {
				return existing, nil
			}
			return nil, fmt.Errorf("%w: %s/%s", ErrSecretLeaseConflict, machineID, executionID)
		}
		return nil, fmt.Errorf("ensure secret lease: %w", err)
	}
	return l, nil
}

// ClaimSecretLease 在 RPC 前持久化派发 fence（ISSUED → CLAIMED），并严格
// 校验完整绑定和 TTL。CLAIMED 表示请求可能已经发出，因此重入返回
// ErrSecretLeaseTerminal；上层只能终态化并清理 execution，不得重派。
func (s *Store) ClaimSecretLease(ctx context.Context, l *SecretLease) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrSecretLeaseTransition)
	}
	switch l.State {
	case SecretLeaseClaimed, SecretLeaseDelivered, SecretLeaseUncertain, SecretLeaseExpired, SecretLeaseRevoked, SecretLeaseAcked:
		return fmt.Errorf("%w: lease %s is %s", ErrSecretLeaseTerminal, l.ID, l.State)
	}
	if l.ExpiresAt.IsZero() {
		// 零值（未持久化的构造值）：数据不完整，按转换失败处理。
		return fmt.Errorf("%w: lease %s has zero expires_at", ErrSecretLeaseTransition, l.ID)
	}
	if !l.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("%w: lease %s expired at %s", ErrSecretLeaseTerminal, l.ID, l.ExpiresAt.Format(time.RFC3339))
	}
	return s.transitionSecretLease(ctx, l, "CLAIMED", []SecretLeaseState{SecretLeaseIssued})
}

// MarkSecretLeaseDelivered 标记 agent 已完成 guest 写入。
func (s *Store) MarkSecretLeaseDelivered(ctx context.Context, l *SecretLease) error {
	return s.transitionSecretLease(ctx, l, "DELIVERED", []SecretLeaseState{SecretLeaseClaimed, SecretLeaseDelivered})
}

// MarkSecretCreateUncertainAndEnqueueCleanup atomically terminalizes the create,
// records that its secret-bearing RPC may have executed, and persists a fenced
// reap. A leader may repeat this transaction after a crash; it never makes the
// original create dispatchable again.
func (s *Store) MarkSecretCreateUncertainAndEnqueueCleanup(ctx context.Context, op Operation,
	l *SecretLease, cleanupOperationID string, cleanupRequest []byte, cause string) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrSecretLeaseTransition)
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE secret_delivery_leases SET state='UNCERTAIN', updated_at=now()
			WHERE id=$1 AND project_id=$2 AND machine_id=$3 AND execution_id=$4
			AND generation=$5 AND operation_id=$6 AND request_hash=$7
			AND state IN ('CLAIMED','UNCERTAIN')`, l.ID, l.ProjectID, l.MachineID,
			l.ExecutionID, l.Generation, l.OperationID, l.RequestHash)
		if err != nil {
			return fmt.Errorf("mark secret create uncertain: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s to UNCERTAIN", ErrSecretLeaseTransition, l.ID)
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET status='FAILED', error=$2,
			completed_at=now(), updated_at=now() WHERE id=$1 AND status IN ('PENDING','CLAIMED','FAILED')`,
			op.ID, cause); err != nil {
			return fmt.Errorf("terminalize uncertain create: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO operations(id, project_id, machine_id,
			execution_id, generation, kind, idempotency_key, status, request, dispatch_node_id)
			VALUES($1,$2,$3,$4,$5,'reap',$1,'PENDING',$6::jsonb,NULLIF($7,''))
			ON CONFLICT (project_id,idempotency_key) DO NOTHING`, cleanupOperationID,
			op.ProjectID, op.MachineID, op.ExecutionID, op.Generation, string(cleanupRequest),
			op.DispatchNodeID); err != nil {
			return fmt.Errorf("enqueue uncertain secret cleanup: %w", err)
		}
		return nil
	})
}

// MarkSecretLeaseAcked 标记 guest 已消费 secret；允许从任意未过期活跃态
// 直达 ACKED，以补偿 Create 响应丢失。ACKED 重放仍需完整绑定一致。
func (s *Store) MarkSecretLeaseAcked(ctx context.Context, l *SecretLease) error {
	return s.transitionSecretLease(ctx, l, "ACKED", []SecretLeaseState{
		SecretLeaseIssued, SecretLeaseClaimed, SecretLeaseDelivered, SecretLeaseAcked,
	})
}

func (s *Store) transitionSecretLease(ctx context.Context, l *SecretLease, next SecretLeaseState, from []SecretLeaseState) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrSecretLeaseTransition)
	}
	states := make([]string, len(from))
	args := make([]any, 0, 9+len(from))
	args = append(args, next, l.ID, l.ProjectID, l.MachineID, l.ExecutionID, l.Generation, l.OperationID, l.RequestHash, l.ExpiresAt)
	for i, state := range from {
		states[i] = fmt.Sprintf("$%d", 10+i)
		args = append(args, state)
	}
	query := fmt.Sprintf(`UPDATE secret_delivery_leases SET state=$1, updated_at=now()
		WHERE id=$2 AND project_id=$3 AND machine_id=$4 AND execution_id=$5
		AND generation=$6 AND operation_id=$7 AND request_hash=$8 AND expires_at=$9
		AND expires_at > now() AND state IN (%s)`, strings.Join(states, ","))
	tag, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("transition secret lease to %s: %w", next, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s to %s", ErrSecretLeaseTransition, l.ID, next)
	}
	return nil
}

// ReapSecretLeases 处理过期与失效租约：活跃态超时 → EXPIRED；execution
// 已非当前代（或 machine 已终态）→ REVOKED。返回处理数量供观测。
func (s *Store) ReapSecretLeases(ctx context.Context) (expired, revoked int64, err error) {
	rows, rerr := s.pool.Query(ctx, `
		UPDATE secret_delivery_leases l SET state='REVOKED', updated_at=now()
		WHERE l.state IN ('ISSUED','CLAIMED','DELIVERED','UNCERTAIN')
		AND NOT EXISTS (
			SELECT 1 FROM machines m
			WHERE m.id = l.machine_id
			AND m.current_execution_id = l.execution_id
			AND m.desired_state NOT IN ('DELETED')
		) RETURNING id`)
	if rerr != nil {
		return 0, 0, fmt.Errorf("revoke secret leases: %w", rerr)
	}
	revoked, _ = countRows(rows)

	tag, err := s.pool.Exec(ctx, `
		UPDATE secret_delivery_leases SET state='EXPIRED', updated_at=now()
		WHERE state IN ('ISSUED','CLAIMED','DELIVERED') AND expires_at < now()`)
	if err != nil {
		return expired, revoked, fmt.Errorf("expire secret leases: %w", err)
	}
	expired = tag.RowsAffected()
	return expired, revoked, nil
}

func countRows(rows pgx.Rows) (int64, error) {
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

// SecretLeaseForExecution 取该 execution 的最新 lease（含终态）。
func (s *Store) SecretLeaseForExecution(ctx context.Context, machineID, executionID string) (*SecretLease, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at, created_at, updated_at
		FROM secret_delivery_leases
		WHERE machine_id=$1 AND execution_id=$2
		ORDER BY created_at DESC LIMIT 1`, machineID, executionID)
	l, err := scanSecretLease(row)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// SecretLeaseByID 按 id 取 lease。
func (s *Store) SecretLeaseByID(ctx context.Context, id string) (*SecretLease, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at, created_at, updated_at
		FROM secret_delivery_leases WHERE id=$1`, id)
	l, err := scanSecretLease(row)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// MachineHasActiveSecretDelivery 判断 machine 的当前 execution 是否接收过
// secret delivery（任意状态的 lease 行都算——EXPIRED/REVOKED 也可能已写入
// guest，memory snapshot 防护必须覆盖；ADR-0024 §9）。只匹配当前 execution：
// 换代后的旧 lease 不阻塞新代。
func (s *Store) MachineHasActiveSecretDelivery(ctx context.Context, machineID, executionID string) (bool, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM secret_delivery_leases
		WHERE machine_id=$1 AND execution_id=$2`,
		machineID, executionID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("machine has secret delivery: %w", err)
	}
	return n > 0, nil
}

func scanSecretLease(row pgx.Row) (*SecretLease, error) {
	var l SecretLease
	if err := row.Scan(&l.ID, &l.ProjectID, &l.MachineID, &l.ExecutionID, &l.Generation,
		&l.OperationID, &l.RequestHash, &l.State, &l.ExpiresAt, &l.CreatedAt, &l.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSecretLeaseNotFound
		}
		return nil, err
	}
	return &l, nil
}
