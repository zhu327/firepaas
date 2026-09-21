// restart.go 聚合 machine 重启策略的状态机持久化（退避、稳定窗口、
// RESTART_BLOCKED 终态）。原属 store.go，按领域拆分以便导航；同包同 Store，
// 无行为变化。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PrepareRestartBackoff durably binds the delay to the complete failed
// execution before generation changes. Re-observation is idempotent and cannot
// extend the deadline.
func (s *Store) PrepareRestartBackoff(
	ctx context.Context,
	id, failedExecution string,
	generation int64,
	nextAt time.Time,
) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE machines SET restart_failed_execution_id=$2,
		restart_next_attempt_at=$4, updated_at=now()
		WHERE id=$1 AND current_execution_id=$2 AND generation=$3
		AND desired_state <> 'DELETED' AND lifecycle_delete_phase='ACTIVE'
		AND (restart_failed_execution_id IS NULL OR restart_failed_execution_id <> $2)`,
		id, failedExecution, generation, nextAt)
	if err != nil {
		return false, fmt.Errorf("prepare restart backoff: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RestartAttemptNumber returns the ordinal reserved by the next restart CAS.
func (s *Store) RestartAttemptNumber(ctx context.Context, id, failedExecution string, generation int64) (int, error) {
	var attempt int
	err := s.pool.QueryRow(ctx, `SELECT restart_attempts+1 FROM machines
		WHERE id=$1 AND current_execution_id=$2 AND generation=$3
		AND desired_state <> 'DELETED' AND lifecycle_delete_phase='ACTIVE'`,
		id, failedExecution, generation).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrMachineLifecycleClosed
	}
	return attempt, err
}

// EnqueueRestartCAS atomically advances attempt/execution/generation and inserts
// the create outbox row. The failed execution is part of both the CAS and the
// idempotency key supplied by the caller.
func (s *Store) EnqueueRestartCAS(ctx context.Context, projectID, machineID,
	failedExecution, newExecution, operationID string, expectedGeneration int64,
	requestJSON []byte, nextAt time.Time,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var attempt int
		var actualProject string
		err := tx.QueryRow(ctx, `UPDATE machines SET
			current_execution_id=$4, generation=$3+1, desired_state='CREATED',
			observed_state='', observed_slot_ip='', observed_readiness='UNKNOWN',
			last_observed_at=NULL, node_id='', restart_attempts=restart_attempts+1,
			restart_next_attempt_at=$5, restart_stable_since=NULL, updated_at=now()
			FROM apps a
			WHERE machines.id=$1 AND machines.current_execution_id=$2 AND machines.generation=$3
			AND machines.desired_state <> 'DELETED' AND machines.lifecycle_delete_phase='ACTIVE'
			AND machines.restart_failed_execution_id=$2 AND machines.restart_next_attempt_at <= now()
			AND a.id=machines.app_id
			RETURNING machines.restart_attempts, a.project_id`, machineID, failedExecution, expectedGeneration,
			newExecution, nextAt).Scan(&attempt, &actualProject)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMachineLifecycleClosed
		}
		if err != nil {
			return fmt.Errorf("restart machine CAS: %w", err)
		}
		if projectID != "" && projectID != actualProject {
			return ownershipConflict("machine", machineID, "project_id", projectID, actualProject)
		}
		projectID = actualProject
		if _, err := tx.Exec(ctx, `INSERT INTO operations(id, project_id, machine_id,
			execution_id, generation, kind, idempotency_key, status, request)
			VALUES($1,$2,$3,$4,$5,'create',$1,'PENDING',$6::jsonb)
			ON CONFLICT (project_id,idempotency_key) DO NOTHING`, operationID, projectID,
			machineID, newExecution, expectedGeneration+1, string(requestJSON)); err != nil {
			return fmt.Errorf("enqueue restart create: %w", err)
		}
		created, err := selectOperationByKey(ctx, tx, projectID, operationID)
		if err != nil || created == nil {
			if err == nil {
				err = fmt.Errorf("restart operation disappeared")
			}
			return err
		}
		if !jsonEqual(created.Request, requestJSON) {
			return ErrRequestConflict
		}
		op = *created
		return nil
	})
	return op, err
}

// RecordRestartAttempt 记录 restart 尝试（attempts 单调递增；next_attempt_at
// 为固定 backoff 的下一尝试时间）。ADR-0026 §7：stable window 必须从“新
// execution 的 READY”重新起算——旧 execution 的锚点跨 restart 存活会让
// attempts 在新 execution 稳定不足窗口时被提前清零（restart storm），因此
// 每次记账同时清空 restart_stable_since。
func (s *Store) RecordRestartAttempt(ctx context.Context, id string, attempts int, nextAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE machines SET restart_attempts=$2, restart_next_attempt_at=$3,
			restart_stable_since=NULL, updated_at=now()
		WHERE id=$1 AND desired_state <> 'DELETED'`, id, attempts, nextAt)
	if err != nil {
		return fmt.Errorf("record restart attempt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrMachineLifecycleClosed
	}
	return nil
}

// SetRestartStableSince 记录新 execution READY 的稳定窗口起点（NULL 清除）。
func (s *Store) SetRestartStableSince(ctx context.Context, id string, t *time.Time) error {
	var arg any
	if t != nil {
		arg = *t
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET restart_stable_since=$2, updated_at=now() WHERE id=$1`, id, arg)
	if err != nil {
		return fmt.Errorf("set restart stable since: %w", err)
	}
	return nil
}

// ResetRestartAttempts 清零 attempts（成功运行满 stable window 或管理员 reset）。
func (s *Store) ResetRestartAttempts(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE machines SET restart_attempts=0, restart_next_attempt_at=NULL,
			restart_stable_since=NULL, restart_blocked=false, updated_at=now()
		WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("reset restart attempts: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrMachineNotFound
	}
	return nil
}

// BlockRestart 置/清 RESTART_BLOCKED 终态。
func (s *Store) BlockRestart(ctx context.Context, id string, blocked bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET restart_blocked=$2, updated_at=now() WHERE id=$1`, id, blocked)
	if err != nil {
		return fmt.Errorf("block restart: %w", err)
	}
	return nil
}
