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
	"github.com/jackc/pgx/v5/pgconn"
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
	Placement          json.RawMessage // MachineSpec.placement 序列化（P2-6，重建/重派时还原）
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
	ID             string
	ProjectID      string
	MachineID      string
	ExecutionID    string
	Generation     int64
	Kind           string
	Status         string
	DispatchNodeID string
	Request        json.RawMessage
	Result         json.RawMessage
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
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

// Node 是 nodes 表的行（observed projection，可重建）。
type Node struct {
	ID              string
	NomadNodeID     string
	NodePool        string
	Status          string
	Labels          map[string]string
	VCPUTotal       int64
	MemTotalMib     int64
	DiskTotalMib    int64
	CPUPercent      float64
	MemUsedMib      int64
	MemAllocatedMib int64
	DiskUsedMib     int64
	GRPCAddr        string
	ProxyAddr       string
	LastSeenAt      time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// SchedulerEvent 是 scheduler_events 表的行（调度/对账审计，MVP 最低可观测）。
type SchedulerEvent struct {
	ID          int64
	At          time.Time
	Kind        string // placement|filter_rejection|reconcile|reservation
	MachineID   string
	OperationID string
	NodeID      string
	Reason      string
	Details     json.RawMessage
}

// PendingUsage 是单个节点上在途 create 操作的资源承诺（scheduler pending 记账）。
type PendingUsage struct {
	NodeID string
	VCPU   int64
	MemMib int64
}

// Allocated 是单个节点上已落地 machine 的资源承诺（scheduler allocated 记账）。
type Allocated struct {
	VCPU   int64
	MemMib int64
}

// EnsureProject 幂等创建 project（M1 dev 用）。
func (s *Store) EnsureProject(ctx context.Context, id, name string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`, id, name)
	return err
}

// ProjectQuota 返回项目配额（vcpu / MiB）。
func (s *Store) ProjectQuota(ctx context.Context, projectID string) (vcpu, memMib int64, err error) {
	if err := s.pool.QueryRow(ctx,
		`SELECT vcpu_quota, mem_mib_quota FROM projects WHERE id=$1`, projectID).
		Scan(&vcpu, &memMib); err != nil {
		return 0, 0, fmt.Errorf("project quota %s: %w", projectID, err)
	}
	return vcpu, memMib, nil
}

// EnsureAppAndEnqueueCreate 在事务中保证 app + machine 期望行存在，并登记 create 操作。
// 相同 (project_id, idempotency_key) 的操作重复提交：
//   - 请求体一致 → 返回已有操作（不产生第二个副本）；
//   - 请求体不同 → 返回 ErrRequestConflict。
//
// 并发提交同一幂等键时，先插入者胜出；后到者捕获 23505 后重新读取并比较
// （M2 验收：同一 replica ordinal 的 1000 次并发重试只生成一个 machine/execution）。
func (s *Store) EnsureAppAndEnqueueCreate(
	ctx context.Context,
	projectID, appID, hostname, imageRef string,
	vcpu, memMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64, replicaOrdinal int,
	requestJSON, placementJSON []byte,
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
				requested_mem_mib, image_ref, node_id, ingress_port, placement)
			VALUES($1,$2,$3,$4,$5,'CREATED',$6,$7,$8,$9,$10,'',$11,$12::jsonb)
			ON CONFLICT (id) DO UPDATE SET desired_state='CREATED',
				-- generation 单调不回退（P1-3）：换代重建后用户携旧默认值重试
				-- 不得拉低 fence 水位，否则 agent 会拒绝后续合法 create。
				generation=GREATEST(machines.generation, EXCLUDED.generation),
				current_execution_id=EXCLUDED.current_execution_id,
				replica_ordinal=EXCLUDED.replica_ordinal,
				ingress_port=EXCLUDED.ingress_port,
				placement=EXCLUDED.placement,
				-- 换代（execution 变化）时旧 observed 立即作废：否则 R8 的
				-- “已按本 execution RUNNING 即补账”会在重建循环里永远
				-- 短路成 SUCCEEDED，而 agent 侧根本没有这台 machine
				-- （M2 真机验收发现的无限换代循环）。
				observed_state=CASE WHEN machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN '' ELSE machines.observed_state END,
				observed_slot_ip=CASE WHEN machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN '' ELSE machines.observed_slot_ip END,
				observed_readiness=CASE WHEN machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN 'UNKNOWN' ELSE machines.observed_readiness END,
				last_observed_at=CASE WHEN machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN NULL ELSE machines.last_observed_at END,
				node_id=CASE WHEN machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN '' ELSE machines.node_id END,
				updated_at=now()`,
			machineID, appID, deploymentID, replicaOrdinal, hostname, generation, executionID, vcpu, memMIB, imageRef, ingressPort, placementJSONOrEmpty(placementJSON)); err != nil {
			return fmt.Errorf("upsert machine: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request)
			VALUES($1,$2,$3,$4,$5,'create',$1,'PENDING',$6::jsonb)
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			operationID, projectID, machineID, executionID, generation, string(requestJSON)); err != nil {
			// 并发同幂等键：唯一索引冲突说明另一事务已插入，重读比较。
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				traced, err := selectOperationByKey(ctx, tx, projectID, operationID)
				if err != nil {
					return err
				}
				if traced != nil {
					if !jsonEqual(traced.Request, requestJSON) {
						return ErrRequestConflict
					}
					op = *traced
					return nil
				}
			}
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

// EnqueueDelete 登记 delete 操作。同一幂等键重复提交的语义与 create 相同；
// 区别：终态 FAILED 的 delete 在请求体一致时复活为 PENDING 重试（P1-2）。
// 清理路径（R2/R5/R6 的确定性 opID）必须收敛，否则 agent 侧残留永远无法
// 破除，且每轮 sync 都会空转重入队、刷 scheduler_events。
func (s *Store) EnqueueDelete(ctx context.Context, projectID, machineID, executionID, operationID string, generation int64, requestJSON []byte) (Operation, error) {
	return s.enqueueDeleteKind(ctx, projectID, machineID, executionID, operationID, generation, requestJSON, "delete")
}

// EnqueueReapDelete 登记 reconcile 清理用 delete（kind=reap）：删的是旧代/
// 死亡残留，成功后不得把 machines.desired_state 推进为 DELETED（R2/R5 路径）。
func (s *Store) EnqueueReapDelete(ctx context.Context, projectID, machineID, executionID, operationID string, generation int64, requestJSON []byte) (Operation, error) {
	return s.enqueueDeleteKind(ctx, projectID, machineID, executionID, operationID, generation, requestJSON, "reap")
}

// EnqueueLifecycle 入队 pause/resume 生命周期操作（M4.5 scale-to-zero）。
// 幂等键 = operationID（调用方用 op-lifecycle-{machine}-{exec}-{n} 形态，
// 含时间窗序号防与历史 op 冲突）。kind ∈ {pause, resume}。
func (s *Store) EnqueueLifecycle(ctx context.Context, projectID, machineID, executionID,
	operationID string, generation int64, kind string, requestJSON []byte) (Operation, error) {

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
			VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb)
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			operationID, projectID, machineID, executionID, generation, kind, string(requestJSON)); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				traced, terr := selectOperationByKey(ctx, tx, projectID, operationID)
				if terr != nil {
					return terr
				}
				if traced != nil && !jsonEqual(traced.Request, requestJSON) {
					return ErrRequestConflict
				}
				if traced != nil {
					op = *traced
					return nil
				}
			}
			return fmt.Errorf("enqueue %s: %w", kind, err)
		}
		fresh, ferr := selectOperationByKey(ctx, tx, projectID, operationID)
		if ferr != nil {
			return ferr
		}
		if fresh == nil {
			return fmt.Errorf("enqueue %s: operation vanished", kind)
		}
		op = *fresh
		return nil
	})
	return op, err
}

func (s *Store) enqueueDeleteKind(ctx context.Context, projectID, machineID, executionID, operationID string, generation int64, requestJSON []byte, kind string) (Operation, error) {
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
			if existing.Status == "FAILED" {
				if err := resurrectOperation(ctx, tx, existing.ID); err != nil {
					return err
				}
				existing.Status = "PENDING"
				existing.Error = ""
			}
			op = *existing
			return nil
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request)
			VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb)
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			operationID, projectID, machineID, executionID, generation, kind, string(requestJSON)); err != nil {
			return fmt.Errorf("enqueue %s: %w", kind, err)
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
// 领取与标记（M1 评审 P2-4）。P3-2：重试退避——首试（attempts=0）立即
// 领取；重试按 2s·2^attempts 指数退避（封顶 64s），消灭无健康节点时的
// 每秒热循环与调度事件刷库。
func (s *Store) ClaimPendingOperations(ctx context.Context, limit int) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE operations SET status='CLAIMED', claimed_at=now(), attempts=attempts+1, updated_at=now()
		WHERE id IN (
			SELECT id FROM operations
			WHERE status='PENDING'
			  AND (attempts = 0 OR updated_at < now() - (interval '2 seconds' * power(2, least(attempts, 5))))
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(dispatch_node_id,''),
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
			&op.Kind, &op.Status, &op.DispatchNodeID, &op.Request, &op.Result, &op.Error,
			&op.CreatedAt, &op.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan op: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// RequeueOperation 把仍在 CLAIMED 的失败操作放回 PENDING 供下次重试
// （agent 暂时不可达等）。只回退 CLAIMED：已 SUCCEEDED/FAILED 的终态
// 操作不得被复活（M1 评审 P2-3 配套）。P3-2：保留 claimed_at 作为
// “最近尝试”标记，配合 attempts 驱动指数退避；updated_at=now() 是
// 退避计时起点。
func (s *Store) RequeueOperation(ctx context.Context, opID, opErr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET status='PENDING', error=$1, updated_at=now()
		WHERE id=$2 AND status='CLAIMED'`, opErr, opID)
	if err != nil {
		return fmt.Errorf("requeue op %s: %w", opID, err)
	}
	return nil
}

// RequeueStaleClaimed 回收滞留 CLAIMED 的操作（P1-1）：API 在 claim 之后、
// complete/requeue 之前崩溃（含 leader 切换取消 RPC 后错误路径也失败）会
// 让操作永久卡死，进而把该 machine 的全部 reconcile 决策（R2/R3/R5）挡死。
// 阈值必须大于 AgentRPCTimeout + 余量，避免误伤在途派发。
func (s *Store) RequeueStaleClaimed(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE operations SET status='PENDING', error='claim lease expired (controller crash or leader switch)',
			claimed_at=NULL, updated_at=now()
		WHERE status='CLAIMED' AND updated_at < now() - $1::interval`,
		olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("requeue stale claimed: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// CompleteOperation 记录操作结果。result 为空时写 NULL（result 列可空；
// 不得把空字符串当 jsonb 写入，那会让 UPDATE 静默失败、操作永远滞留在
// CLAIMED/PENDING 循环里）。P3-3：终态保护——只允许从 PENDING/CLAIMED
// 完成，迟到的写入不得覆盖已终态结果；对已终态的重复完成视为幂等成功。
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
		WHERE id=$4 AND status IN ('PENDING','CLAIMED')`,
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
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
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
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
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

// MarkStaleNodes 把 last_seen 超阈值的节点置 UNKNOWN（P3-6c）：节点从
// Nomad 消失后 PG 投影会永远保留最后一次状态，误导 /v1/nodes 审计。
func (s *Store) MarkStaleNodes(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE nodes SET status='UNKNOWN', updated_at=now()
		WHERE status <> 'UNKNOWN' AND last_seen_at < now() - $1::interval`,
		olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("mark stale nodes: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// UpsertNode 写入节点 observed projection（可重建，ADR-0014）。
func (s *Store) UpsertNode(ctx context.Context, n Node) error {
	labelsJSON := "{}"
	if n.Labels != nil {
		b, err := json.Marshal(n.Labels)
		if err != nil {
			return fmt.Errorf("marshal labels: %w", err)
		}
		labelsJSON = string(b)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO nodes(id, nomad_node_id, node_pool, status, labels,
			vcpu_total, mem_total_mib, disk_total_mib, cpu_percent,
			mem_used_mib, mem_allocated_mib, disk_used_mib, grpc_addr, proxy_addr, last_seen_at)
		VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
		ON CONFLICT (nomad_node_id) DO UPDATE SET id=EXCLUDED.id,
			node_pool=EXCLUDED.node_pool, status=EXCLUDED.status, labels=EXCLUDED.labels,
			vcpu_total=EXCLUDED.vcpu_total, mem_total_mib=EXCLUDED.mem_total_mib,
			disk_total_mib=EXCLUDED.disk_total_mib, cpu_percent=EXCLUDED.cpu_percent,
			mem_used_mib=EXCLUDED.mem_used_mib, mem_allocated_mib=EXCLUDED.mem_allocated_mib,
			disk_used_mib=EXCLUDED.disk_used_mib, grpc_addr=EXCLUDED.grpc_addr,
			proxy_addr=EXCLUDED.proxy_addr, last_seen_at=now(), updated_at=now()`,
		n.ID, n.NomadNodeID, n.NodePool, n.Status, labelsJSON,
		n.VCPUTotal, n.MemTotalMib, n.DiskTotalMib, n.CPUPercent,
		n.MemUsedMib, n.MemAllocatedMib, n.DiskUsedMib, n.GRPCAddr, n.ProxyAddr)
	if err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}
	return nil
}

// ListNodes 返回全部节点。
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, nomad_node_id, node_pool, status, labels::text,
			vcpu_total, mem_total_mib, disk_total_mib, cpu_percent,
			mem_used_mib, mem_allocated_mib, disk_used_mib, grpc_addr, proxy_addr,
			last_seen_at, created_at, updated_at
		FROM nodes ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		var labels string
		if err := rows.Scan(&n.ID, &n.NomadNodeID, &n.NodePool, &n.Status, &labels,
			&n.VCPUTotal, &n.MemTotalMib, &n.DiskTotalMib, &n.CPUPercent,
			&n.MemUsedMib, &n.MemAllocatedMib, &n.DiskUsedMib, &n.GRPCAddr, &n.ProxyAddr,
			&n.LastSeenAt, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		if labels != "" && labels != "null" {
			_ = json.Unmarshal([]byte(labels), &n.Labels)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// RecordSchedulerEvent 落一条调度/对账审计事件。
func (s *Store) RecordSchedulerEvent(ctx context.Context, ev SchedulerEvent) error {
	details := ev.Details
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO scheduler_events(kind, machine_id, operation_id, node_id, reason, details)
		VALUES($1,$2,$3,$4,$5,$6::jsonb)`,
		ev.Kind, ev.MachineID, ev.OperationID, ev.NodeID, ev.Reason, string(details))
	if err != nil {
		return fmt.Errorf("record scheduler event: %w", err)
	}
	return nil
}

// ListSchedulerEvents 返回最近 limit 条事件。
func (s *Store) ListSchedulerEvents(ctx context.Context, limit int) ([]SchedulerEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, at, kind, machine_id, operation_id, node_id, reason, details::text
		FROM scheduler_events ORDER BY at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list scheduler events: %w", err)
	}
	defer rows.Close()
	var out []SchedulerEvent
	for rows.Next() {
		var ev SchedulerEvent
		var details string
		if err := rows.Scan(&ev.ID, &ev.At, &ev.Kind, &ev.MachineID, &ev.OperationID,
			&ev.NodeID, &ev.Reason, &details); err != nil {
			return nil, fmt.Errorf("scan scheduler event: %w", err)
		}
		ev.Details = json.RawMessage(details)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// UpdateOperationDispatchNode 记录操作实际派发的节点（optimistic accounting 依据）。
func (s *Store) UpdateOperationDispatchNode(ctx context.Context, opID, nodeID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET dispatch_node_id=$1, updated_at=now()
		WHERE id=$2 AND status IN ('PENDING','CLAIMED')`, nodeID, opID)
	if err != nil {
		return fmt.Errorf("update dispatch node %s: %w", opID, err)
	}
	return nil
}

// ListInFlightOperations 返回未完成的操作（PENDING/CLAIMED）。
func (s *Store) ListInFlightOperations(ctx context.Context) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(dispatch_node_id,''),
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at
		FROM operations WHERE status IN ('PENDING','CLAIMED') ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list in-flight ops: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// HasPendingOperationForMachine 判断该 machine 是否已有未完成操作（防重复下单）。
func (s *Store) HasPendingOperationForMachine(ctx context.Context, machineID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM operations
			WHERE machine_id=$1 AND status IN ('PENDING','CLAIMED'))`,
		machineID).Scan(&exists); err != nil {
		return false, fmt.Errorf("has pending op: %w", err)
	}
	return exists, nil
}

// PendingOperationForMachine 返回该 machine 当前未完成的操作（无则 nil）。
func (s *Store) PendingOperationForMachine(ctx context.Context, machineID string) (*Operation, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(dispatch_node_id,''),
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at
		FROM operations WHERE machine_id=$1 AND status IN ('PENDING','CLAIMED')
		ORDER BY created_at LIMIT 1`, machineID)
	op, err := scanOperation(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pending op for %s: %w", machineID, err)
	}
	return op, nil
}

// GetLatestOperationForMachine 返回该 machine 最近一次操作（任意终态）。
func (s *Store) GetLatestOperationForMachine(ctx context.Context, machineID string) (*Operation, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(dispatch_node_id,''),
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at
		FROM operations WHERE machine_id=$1 ORDER BY created_at DESC, id DESC LIMIT 1`, machineID)
	op, err := scanOperation(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest op for %s: %w", machineID, err)
	}
	return op, nil
}

// FailedCreateAttempts 返回该 machine 自最近一次 SUCCEEDED create 以来
// 连续 FAILED 的 create 数（P1-3 指数退避的输入）。
func (s *Store) FailedCreateAttempts(ctx context.Context, machineID string) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM operations o
		WHERE o.machine_id=$1 AND o.kind='create' AND o.status='FAILED'
			AND o.created_at > COALESCE((
				SELECT max(i.created_at) FROM operations i
				WHERE i.machine_id=$1 AND i.kind='create' AND i.status='SUCCEEDED'
			), '-infinity'::timestamptz)`,
		machineID).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed create attempts %s: %w", machineID, err)
	}
	return n, nil
}

// PendingUsageByNode 汇总每个派发节点上在途 create 操作的资源承诺。
func (s *Store) PendingUsageByNode(ctx context.Context) ([]PendingUsage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.dispatch_node_id, coalesce(sum(m.requested_vcpu),0), coalesce(sum(m.requested_mem_mib),0)
		FROM operations o JOIN machines m ON m.id = o.machine_id
		WHERE o.kind='create' AND o.status IN ('PENDING','CLAIMED') AND o.dispatch_node_id<>''
		GROUP BY o.dispatch_node_id`)
	if err != nil {
		return nil, fmt.Errorf("pending usage: %w", err)
	}
	defer rows.Close()
	var out []PendingUsage
	for rows.Next() {
		var p PendingUsage
		if err := rows.Scan(&p.NodeID, &p.VCPU, &p.MemMib); err != nil {
			return nil, fmt.Errorf("scan pending usage: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MachineNodesByDeployment 返回 deployment → 已占节点集合（反亲和排除集）。
func (s *Store) MachineNodesByDeployment(ctx context.Context) (map[string]map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT deployment_id, node_id FROM machines
		WHERE desired_state IN ('CREATED','RUNNING') AND node_id<>''`)
	if err != nil {
		return nil, fmt.Errorf("machine nodes by deployment: %w", err)
	}
	defer rows.Close()
	out := map[string]map[string]bool{}
	for rows.Next() {
		var dep, node string
		if err := rows.Scan(&dep, &node); err != nil {
			return nil, fmt.Errorf("scan deployment node: %w", err)
		}
		if out[dep] == nil {
			out[dep] = map[string]bool{}
		}
		out[dep][node] = true
	}
	return out, rows.Err()
}

// AllocatedByNode 汇总每个节点上期望存活 machine 的资源承诺。
func (s *Store) AllocatedByNode(ctx context.Context) (map[string]Allocated, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, coalesce(sum(requested_vcpu),0), coalesce(sum(requested_mem_mib),0)
		FROM machines WHERE desired_state IN ('CREATED','RUNNING') AND node_id<>''
		GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("allocated by node: %w", err)
	}
	defer rows.Close()
	out := map[string]Allocated{}
	for rows.Next() {
		var nodeID string
		var a Allocated
		if err := rows.Scan(&nodeID, &a.VCPU, &a.MemMib); err != nil {
			return nil, fmt.Errorf("scan allocated: %w", err)
		}
		out[nodeID] = a
	}
	return out, rows.Err()
}

// ProjectForApp 返回 app 所属 project。
func (s *Store) ProjectForApp(ctx context.Context, appID string) (string, error) {
	var project string
	if err := s.pool.QueryRow(ctx, `SELECT project_id FROM apps WHERE id=$1`, appID).Scan(&project); err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("project for app %s: %w", appID, err)
	}
	return project, nil
}

// ListMachinesOnNode 返回节点上期望存活的 machine。
func (s *Store) ListMachinesOnNode(ctx context.Context, nodeID string) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at
		FROM machines m WHERE m.node_id=$1 AND m.desired_state IN ('CREATED','RUNNING')`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("machines on node: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

// UpdateMachineNodeAndObserved 创建成功后记录节点与观测（optimistic add）。
func (s *Store) UpdateMachineNodeAndObserved(ctx context.Context, id, nodeID, executionID, state, slotIP, readiness string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET node_id=$1, observed_state=$2, observed_slot_ip=$3,
			observed_readiness=$4, last_observed_at=now(), updated_at=now()
		WHERE id=$5`,
		nodeID, state, slotIP, readiness, id)
	if err != nil {
		return fmt.Errorf("update machine node+observed %s: %w", id, err)
	}
	return nil
}

// MarkMachineObservedMissing 标记节点失联/agent 缺失（保守 UNKNOWN，摘路由）。
func (s *Store) MarkMachineObservedMissing(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET observed_state='UNKNOWN', observed_readiness='UNKNOWN',
			last_observed_at=now(), updated_at=now()
		WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("mark machine missing %s: %w", id, err)
	}
	return nil
}

// ResetMachineForRecreate 换代重建：新 execution/generation，清空节点与观测。
func (s *Store) ResetMachineForRecreate(ctx context.Context, id, executionID string, generation int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET current_execution_id=$1, generation=$2, desired_state='CREATED',
			node_id='', observed_state='', observed_slot_ip='', observed_readiness='UNKNOWN',
			last_observed_at=NULL, updated_at=now()
		WHERE id=$3`, executionID, generation, id)
	if err != nil {
		return fmt.Errorf("reset machine %s: %w", id, err)
	}
	return nil
}

// ActiveRouteMachines 返回可用于路由投影的 machines（desired=CREATED 且已观测）。
func (s *Store) ActiveRouteMachines(ctx context.Context) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
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

func placementJSONOrEmpty(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
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

// resurrectOperation 把终态 FAILED 的操作复活为 PENDING（P1-2）。仅在
// 请求体一致的前提下由 EnqueueDelete 调用；SUCCEEDED 不复活（已收敛）。
func resurrectOperation(ctx context.Context, tx pgx.Tx, opID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE operations SET status='PENDING', error='', claimed_at=NULL,
			completed_at=NULL, updated_at=now()
		WHERE id=$1 AND status='FAILED'`, opID)
	if err != nil {
		return fmt.Errorf("resurrect op %s: %w", opID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("resurrect op %s: not in FAILED state", opID)
	}
	return nil
}

// selectOperationByKey 按 (project_id, idempotency_key) 查询操作；不存在返回 nil。
func selectOperationByKey(ctx context.Context, tx pgx.Tx, projectID, operationID string) (*Operation, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, project_id, machine_id, execution_id, generation, kind, status,
			coalesce(dispatch_node_id,''),
			coalesce(request::text,'{}'), coalesce(result::text,'{}'), coalesce(error,''),
			created_at, updated_at
		FROM operations WHERE project_id=$1 AND idempotency_key=$2`,
		projectID, operationID)
	var op Operation
	err := row.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID, &op.Generation,
		&op.Kind, &op.Status, &op.DispatchNodeID, &op.Request, &op.Result, &op.Error,
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

// machineColumns 是所有 machine 扫描路径的公共列序（与 scanMachine 对齐）。
const machineColumns = `id, app_id, deployment_id, replica_ordinal, hostname,
	desired_state, generation, current_execution_id, requested_vcpu,
	requested_mem_mib, ingress_port, image_ref, coalesce(env::text,'{}'),
	coalesce(placement::text,'null'), node_id, observed_state, observed_slot_ip,
	observed_readiness, last_observed_at, created_at, updated_at`

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

func scanOperations(rows pgx.Rows) ([]Operation, error) {
	var out []Operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *op)
	}
	return out, rows.Err()
}

func scanOperation(row scanner) (*Operation, error) {
	var op Operation
	err := row.Scan(&op.ID, &op.ProjectID, &op.MachineID, &op.ExecutionID, &op.Generation,
		&op.Kind, &op.Status, &op.DispatchNodeID, &op.Request, &op.Result, &op.Error,
		&op.CreatedAt, &op.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &op, nil
}

func scanMachine(row scanner) (*Machine, error) {
	var m Machine
	var envJSON, placementJSON string
	err := row.Scan(&m.ID, &m.AppID, &m.DeploymentID, &m.ReplicaOrdinal, &m.Hostname,
		&m.DesiredState, &m.Generation, &m.CurrentExecutionID,
		&m.RequestedVCPU, &m.RequestedMemMIB, &m.IngressPort, &m.ImageRef,
		&envJSON, &placementJSON, &m.NodeID, &m.ObservedState, &m.ObservedSlotIP,
		&m.ObservedReadiness, &m.LastObservedAt, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if envJSON != "" && envJSON != "null" {
		_ = json.Unmarshal([]byte(envJSON), &m.Env)
	}
	if placementJSON != "" && placementJSON != "null" && placementJSON != "{}" {
		m.Placement = json.RawMessage(placementJSON)
	}
	return &m, nil
}
