// operations.go：M5.3 操作追踪查询（mvp-plan §9.3 operation trace）。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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

// ListOperations 按项目及条件查操作。projectID 是强制租户边界，不能为空。
// limit<=0 默认 100。
func (s *Store) ListOperations(
	ctx context.Context,
	projectID, machineID, kind, status string,
	limit int,
) ([]OperationTrace, error) {
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + operationTraceColumns + ` FROM operations WHERE project_id=$1`
	args := []any{projectID}
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

// GetOperation 返回项目内单条操作。projectID 是强制租户边界，不能为空。
func (s *Store) GetOperation(ctx context.Context, projectID, id string) (*OperationTrace, error) {
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	var op OperationTrace
	row := s.pool.QueryRow(ctx,
		`SELECT `+operationTraceColumns+` FROM operations WHERE project_id=$1 AND id=$2`, projectID, id)
	got, err := scanOperationTraceRow(row)
	if err != nil {
		// 确证 not-found 用哨兵返回，调用方（API 层）据此区分 404 与 5xx。
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	op = *got
	return &op, nil
}

// DeleteTerminalOperationsOlderThan 是终端态 operation 的保留窗清理
// （R2 加固，controller 周期调起）。保留窗内的行继续支持幂等键重放
// （同 key 同请求 → 原行返回，不重复派发）；窗外再携同 key 重试按全新
// 操作处理——这是记录在案的幂等保证时限，调用方（controller 配置与
// 运维文档）须与保留期保持一致。
func (s *Store) DeleteTerminalOperationsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM operations
		WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','SUPERSEDED','TIMED_OUT')
		  AND updated_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("purge terminal operations: %w", err)
	}
	return tag.RowsAffected(), nil
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
