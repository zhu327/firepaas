// operations.go：M5.3 操作追踪查询（mvp-plan §9.3 operation trace）。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// OperationTrace 是带全程时间戳的操作记录（复盘/压测关联）。
type OperationTrace struct {
	Operation
	Attempts    int
	ClaimedAt   *time.Time
	CompletedAt *time.Time
}

const operationTraceColumns = `id, project_id, machine_id, execution_id, generation, kind, status,
	coalesce(dispatch_node_id,''), request, result, coalesce(error,''), created_at, updated_at,
	attempts, claimed_at, completed_at`

// ListOperations 按条件查操作（mapper 参数为空即不加条件）。limit<=0 默认 100。
func (s *Store) ListOperations(ctx context.Context, machineID, kind, status string, limit int) ([]OperationTrace, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + operationTraceColumns + ` FROM operations WHERE true`
	args := []any{}
	// 只用白名单列 + 占位参数（无拼接注入面）。
	if machineID != "" {
		args = append(args, machineID)
		q += fmt.Sprintf(" AND machine_id=$%d", len(args))
	}
	if kind != "" {
		args = append(args, kind)
		q += fmt.Sprintf(" AND kind=$%d", len(args))
	}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" AND status=$%d", len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list operations: %w", err)
	}
	defer rows.Close()
	var out []OperationTrace
	for rows.Next() {
		op, err := scanOperationTrace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *op)
	}
	return out, rows.Err()
}

// GetOperation 返回单条操作。
func (s *Store) GetOperation(ctx context.Context, id string) (*OperationTrace, error) {
	var op OperationTrace
	row := s.pool.QueryRow(ctx,
		`SELECT `+operationTraceColumns+` FROM operations WHERE id=$1`, id)
	got, err := scanOperationTraceRow(row)
	if err != nil {
		return nil, err
	}
	op = *got
	return &op, nil
}

// CountOperations 统计某状态操作数（M5.2/5.3 积压 gauge）。status 空=全部。
func (s *Store) CountOperations(ctx context.Context, status string) (int64, error) {
	q := `SELECT count(*) FROM operations`
	args := []any{}
	if status != "" {
		q += ` WHERE status=$1`
		args = append(args, status)
	}
	var n int64
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count operations: %w", err)
	}
	return n, nil
}

// sqlrower：QueryRow/行扫描共用最小接口。
type sqlrower interface{ Scan(dest ...any) error }

func scanOperationTraceRow(row sqlrower) (*OperationTrace, error) {
	var op OperationTrace
	var request, result []byte
	if err := row.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID,
		&op.Generation, &op.Kind, &op.Status, &op.DispatchNodeID,
		&request, &result, &op.Error, &op.CreatedAt, &op.UpdatedAt,
		&op.Attempts, &op.ClaimedAt, &op.CompletedAt); err != nil {
		return nil, fmt.Errorf("scan operation: %w", err)
	}
	op.Request = json.RawMessage(request)
	if len(result) > 0 {
		op.Result = json.RawMessage(result)
	}
	return &op, nil
}

func scanOperationTrace(rows interface{ Scan(...any) error }) (*OperationTrace, error) {
	return scanOperationTraceRow(rows)
}
