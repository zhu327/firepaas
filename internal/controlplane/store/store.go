// Package store 是控制面的 PG 持久层（M1.5 最小实现）。
// desired/business truth 只在这里落库；Redis 投影见 catalog 包。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRequestConflict 表示同一 (project_id, idempotency_key) 的操作重复提交时
// 携带了不同的请求体（M1 评审 P2-8：控制面同样要拒绝，不能静默返回旧结果）。
var ErrRequestConflict = errors.New("operation idempotency key reused with a different request")

// Store 聚合 PG 访问。
type Store struct {
	pool *pgxpool.Pool
}

// New 构造 Store。
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Machine 是 machines 表行。
type Machine struct {
	ID                 string
	AppID              string
	DeploymentID       string
	ReplicaOrdinal     int
	Hostname           string
	DesiredState       string
	Generation         int64
	CurrentExecutionID string
	RequestedVCPU      int64
	RequestedMemMIB    int64
	IngressPort        int
	ImageRef           string
	Env                map[string]string
	NodeID             string
	ObservedState      string
	ObservedSlotIP     string
	ObservedReadiness  string
	LastObservedAt     *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Operation 是 operations 表行。
type Operation struct {
	ID          string
	ProjectID   string
	MachineID   string
	ExecutionID string
	Generation  int64
	Kind        string
	Status      string
	Request     json.RawMessage
	Result      json.RawMessage
	Error       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// RouteBackendRow 是 route_backends 表行。
type RouteBackendRow struct {
	MachineID         string
	ExecutionID       string
	NodeProxyEndpoint string
	AppPort           int
	Weight            int
	Readiness         string
	Draining          bool
}

// RouteRow 是 controller 计算出的一个活跃 route（hostname+port 及其 backend set）。
// M1 语义：每 hostname 一个活跃端口；M3 发布状态机在此之上引入 generation 迁移。
type RouteRow struct {
	Hostname   string
	Port       int
	AppID      string
	Generation int64
	Backends   []RouteBackendRow
}

// EnsureProject 幂等创建 project（M1 dev 用）。
func (s *Store) EnsureProject(ctx context.Context, id, name string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`, id, name)
	return err
}

// EnsureAppAndEnqueueCreate 在事务中保证 app + machine 期望行存在，并登记 create 操作。
// 相同 (project_id, idempotency_key) 的操作重复提交：
//   - 请求体一致 → 返回已有操作（不产生第二个副本）；
//   - 请求体不同 → 返回 ErrRequestConflict。
func (s *Store) EnsureAppAndEnqueueCreate(
	ctx context.Context,
	projectID, appID, hostname, imageRef string,
	vcpu, memMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64,
	requestJSON []byte,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// 幂等键已存在时先比对请求体，避免无谓 upsert 与冲突静默。
		existing, err := selectOperationByKey(ctx, tx, projectID, operationID)
		if err != nil {
			return err
		}
		if existing != nil {
			if !jsonEqual(existing.Request, requestJSON) {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib)
			VALUES($1,$2,$3,$4,$5,$6)
			ON CONFLICT (id) DO UPDATE SET image_ref=EXCLUDED.image_ref,
				vcpu=EXCLUDED.vcpu, mem_mib=EXCLUDED.mem_mib, updated_at=now()`,
			appID, projectID, hostname, imageRef, vcpu, memMIB); err != nil {
			return fmt.Errorf("upsert app: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
				desired_state, generation, current_execution_id, requested_vcpu,
				requested_mem_mib, image_ref, node_id, ingress_port)
			VALUES($1,$2,$3,0,$4,'CREATED',$5,$6,$7,$8,$9,'',$10)
			ON CONFLICT (id) DO UPDATE SET desired_state='CREATED',
				generation=EXCLUDED.generation,
				current_execution_id=EXCLUDED.current_execution_id,
				ingress_port=EXCLUDED.ingress_port,
				updated_at=now()`,
			machineID, appID, deploymentID, hostname, generation, executionID, vcpu, memMIB, imageRef, ingressPort); err != nil {
			return fmt.Errorf("upsert machine: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request)
			VALUES($1,$2,$3,$4,$5,'create',$1,'PENDING',$6::jsonb)
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			operationID, projectID, machineID, executionID, generation, string(requestJSON)); err != nil {
			return fmt.Errorf("enqueue create: %w", err)
		}

		created, err := selectOperationByKey(ctx, tx, projectID, operationID)
		if err != nil {
			return err
		}
		if created == nil {
			return fmt.Errorf("operation %s disappeared after insert", operationID)
		}
		op = *created
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return op, nil
}

// EnqueueDelete 登记 delete 操作。同一幂等键重复提交的语义与 create 相同。
func (s *Store) EnqueueDelete(ctx context.Context, projectID, machineID, executionID, operationID string, generation int64, requestJSON []byte) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		existing, err := selectOperationByKey(ctx, tx, projectID, operationID)
		if err != nil {
			return err
		}
		if existing != nil {
			if !jsonEqual(existing.Request, requestJSON) {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request)
			VALUES($1,$2,$3,$4,$5,'delete',$1,'PENDING',$6::jsonb)
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			operationID, projectID, machineID, executionID, generation, string(requestJSON)); err != nil {
			return fmt.Errorf("enqueue delete: %w", err)
		}

		created, err := selectOperationByKey(ctx, tx, projectID, operationID)
		if err != nil {
			return err
		}
		if created == nil {
			return fmt.Errorf("operation %s disappeared after insert", operationID)
		}
		op = *created
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return op, nil
}

// ClaimPendingOperations 领取最多 limit 个 PENDING 操作。
// 单条 UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) 原子完成
// 领取与标记（M1 评审 P2-4：旧实现的 SELECT 在事务外，行锁立即释放，
// FOR UPDATE SKIP LOCKED 完全无效，多写者场景会双领）。
func (s *Store) ClaimPendingOperations(ctx context.Context, limit int) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE operations SET status='CLAIMED', claimed_at=now(), updated_at=now()
		WHERE id IN (
			SELECT id FROM operations
			WHERE status='PENDING'
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim ops: %w", err)
	}
	defer rows.Close()

	var ops []Operation
	for rows.Next() {
		var op Operation
		if err := rows.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID, &op.Generation,
			&op.Kind, &op.Status, &op.Request, &op.Result, &op.Error,
			&op.CreatedAt, &op.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan op: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// RequeueOperation 把仍在 CLAIMED 的失败操作放回 PENDING 供下次重试
// （agent 暂时不可达等）。只回退 CLAIMED：已 SUCCEEDED/FAILED 的终态
// 操作不得被复活（M1 评审 P2-3 配套）。
func (s *Store) RequeueOperation(ctx context.Context, opID, opErr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET status='PENDING', error=$1, claimed_at=NULL, updated_at=now()
		WHERE id=$2 AND status='CLAIMED'`, opErr, opID)
	if err != nil {
		return fmt.Errorf("requeue op %s: %w", opID, err)
	}
	return nil
}

// CompleteOperation 记录操作结果。result 为空时写 NULL（result 列可空；
// 不得把空字符串当 jsonb 写入，那会让 UPDATE 静默失败、操作永远滞留在
// CLAIMED/PENDING 循环里）。
func (s *Store) CompleteOperation(ctx context.Context, opID, statusText string, result []byte, opErr string) error {
	if opErr != "" && statusText == "SUCCEEDED" {
		statusText = "FAILED"
	}
	var resultArg any
	if len(result) > 0 {
		resultArg = string(result)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET status=$1, result=$2::jsonb, error=$3,
			completed_at=now(), updated_at=now()
		WHERE id=$4`,
		statusText, resultArg, opErr, opID)
	if err != nil {
		return fmt.Errorf("complete op %s: %w", opID, err)
	}
	return nil
}

// ListMachines 返回所有（或按 project）machine。
func (s *Store) ListMachines(ctx context.Context, projectID string) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at
		FROM machines m
		JOIN apps a ON a.id = m.app_id
		WHERE ($1='' OR a.project_id=$1)
		ORDER BY m.created_at`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list machines: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

// GetMachine 按 id 查询。
func (s *Store) GetMachine(ctx context.Context, id string) (*Machine, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at
		FROM machines m WHERE m.id=$1`, id)
	m, err := scanMachine(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get machine: %w", err)
	}
	return m, nil
}

// UpdateMachineObserved 写入 agent 观测状态。
func (s *Store) UpdateMachineObserved(ctx context.Context, id, executionID, state, slotIP, readiness string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET observed_state=$1, observed_slot_ip=$2, observed_readiness=$3,
			last_observed_at=now(), updated_at=now()
		WHERE id=$4`,
		state, slotIP, readiness, id)
	if err != nil {
		return fmt.Errorf("update observed %s: %w", id, err)
	}
	return nil
}

// MarkMachineDeleted 更新期望状态为 DELETED。
func (s *Store) MarkMachineDeleted(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("mark deleted %s: %w", id, err)
	}
	return nil
}

// ActiveRouteMachines 返回可用于路由投影的 machines（desired=CREATED 且已观测）。
func (s *Store) ActiveRouteMachines(ctx context.Context) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at
		FROM machines m
		WHERE m.desired_state IN ('CREATED','RUNNING') AND m.observed_state='RUNNING'`)
	if err != nil {
		return nil, fmt.Errorf("active route machines: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

// SyncRoutes 把 controller 计算出的活跃 route 集合写入 routes/route_backends
// （ADR-0005：PG 是 route generation/backend 生命周期的权威；Redis 只是投影）。
// 单事务完成：删除不再活跃的 route，替换活跃 route 的 backend set。
// M1 评审 P2-6 之前这两张表是死 schema，无人读写。
func (s *Store) SyncRoutes(ctx context.Context, active []RouteRow) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		activeIDs := make(map[string]bool, len(active))
		for _, r := range active {
			activeIDs[routeRowID(r.Hostname, r.Port)] = true
		}

		// 1) 找出 stale route 并删除（先 backends 再 routes，FK 约束）。
		rows, err := tx.Query(ctx, `SELECT id FROM routes`)
		if err != nil {
			return fmt.Errorf("list routes: %w", err)
		}
		var stale []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("scan route id: %w", err)
			}
			if !activeIDs[id] {
				stale = append(stale, id)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("list routes: %w", err)
		}
		rows.Close()
		if len(stale) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM route_backends WHERE route_id = ANY($1)`, stale); err != nil {
				return fmt.Errorf("delete stale backends: %w", err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM routes WHERE id = ANY($1)`, stale); err != nil {
				return fmt.Errorf("delete stale routes: %w", err)
			}
		}

		// 2) upsert 活跃 route 并整体替换 backend set。
		for _, r := range active {
			id := routeRowID(r.Hostname, r.Port)
			if _, err := tx.Exec(ctx, `
				INSERT INTO routes(id, app_id, hostname, port, active_generation)
				VALUES($1,$2,$3,$4,$5)
				ON CONFLICT (id) DO UPDATE SET app_id=EXCLUDED.app_id,
					hostname=EXCLUDED.hostname, port=EXCLUDED.port,
					active_generation=EXCLUDED.active_generation, updated_at=now()`,
				id, r.AppID, r.Hostname, r.Port, r.Generation); err != nil {
				return fmt.Errorf("upsert route %s: %w", id, err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM route_backends WHERE route_id=$1`, id); err != nil {
				return fmt.Errorf("replace backends for %s: %w", id, err)
			}
			for _, b := range r.Backends {
				if _, err := tx.Exec(ctx, `
					INSERT INTO route_backends(route_id, generation, machine_id, execution_id,
						node_proxy_endpoint, app_port, weight, readiness, draining)
					VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
					id, r.Generation, b.MachineID, b.ExecutionID,
					b.NodeProxyEndpoint, b.AppPort, b.Weight, b.Readiness, b.Draining); err != nil {
					return fmt.Errorf("insert backend for %s: %w", id, err)
				}
			}
		}
		return nil
	})
}

func routeRowID(hostname string, port int) string {
	return fmt.Sprintf("%s:%d", hostname, port)
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// selectOperationByKey 按 (project_id, idempotency_key) 查询操作；不存在返回 nil。
func selectOperationByKey(ctx context.Context, tx pgx.Tx, projectID, operationID string) (*Operation, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at
		FROM operations WHERE project_id=$1 AND idempotency_key=$2`,
		projectID, operationID)
	var op Operation
	err := row.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID, &op.Generation,
		&op.Kind, &op.Status, &op.Request, &op.Result, &op.Error,
		&op.CreatedAt, &op.UpdatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select operation %s: %w", operationID, err)
	}
	return &op, nil
}

// jsonEqual 语义化比较两段 JSON（键序无关；protojson 的 64 位整数序列化为
// 字符串，两侧同为 protojson 文本，类型一致可比较）。
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

type scanner interface{ Scan(dest ...any) error }

func scanMachines(rows pgx.Rows) ([]Machine, error) {
	var out []Machine
	for rows.Next() {
		m, err := scanMachine(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

func scanMachine(row scanner) (*Machine, error) {
	var m Machine
	var envJSON string
	err := row.Scan(&m.ID, &m.AppID, &m.DeploymentID, &m.ReplicaOrdinal, &m.Hostname,
		&m.DesiredState, &m.Generation, &m.CurrentExecutionID,
		&m.RequestedVCPU, &m.RequestedMemMIB, &m.IngressPort, &m.ImageRef,
		&envJSON, &m.NodeID, &m.ObservedState, &m.ObservedSlotIP,
		&m.ObservedReadiness, &m.LastObservedAt, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if envJSON != "" && envJSON != "null" {
		_ = json.Unmarshal([]byte(envJSON), &m.Env)
	}
	return &m, nil
}
