// Package store 是控制面的 PG 持久层（M1.5 最小实现）。
// desired/business truth 只在这里落库；Redis 投影见 catalog 包。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRequestConflict 表示同一 (project_id, idempotency_key) 的操作重复提交时
// 携带了不同的请求体（M1 评审 P2-8：控制面同样要拒绝，不能静默返回旧结果）。
var ErrRequestConflict = errors.New("operation idempotency key reused with a different request")

// ErrOwnershipConflict 表示调用方试图让已有资源改属另一个 project/app/deployment。
// ownership 是业务身份的一部分，一旦创建不可通过 create upsert 改写。
var ErrOwnershipConflict = errors.New("immutable resource ownership conflict")

// OwnershipConflictError 携带冲突资源的安全标识。它同时匹配
// ErrRequestConflict，以复用现有 create API 的 409 映射；调用方可用
// ErrOwnershipConflict 精确区分所有权冲突。
type OwnershipConflictError struct {
	Resource string
	ID       string
	Field    string
}

func (e *OwnershipConflictError) Error() string {
	return fmt.Sprintf("%s %s has conflicting immutable %s", e.Resource, e.ID, e.Field)
}

func (e *OwnershipConflictError) Is(target error) bool {
	return target == ErrOwnershipConflict || target == ErrRequestConflict
}

func ownershipConflict(resource, id, field, _, _ string) error {
	return &OwnershipConflictError{Resource: resource, ID: id, Field: field}
}

// Store 聚合 PG 访问。
type Store struct {
	pool *pgxpool.Pool
}

// New 构造 Store。
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool 暴露底层连接池（测试与运维工具的直接查询用途；生产路径仍应经由
// Store 方法，避免绕过事务/约束）。
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

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
	RequestedDiskMIB   int64
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
	// v1.2-D（ADR-0026）：lifecycle wait / TTL / restart 持久化。
	ExpiresAt                  *time.Time // 绝对到期时间；NULL = 关闭
	RestartMode                string     // NEVER|ON_FAILURE|ALWAYS
	RestartMaxAttempts         int
	RestartBackoffSeconds      int
	RestartStableWindowSeconds int
	RestartAttempts            int
	RestartNextAttemptAt       *time.Time
	RestartStableSince         *time.Time
	RestartBlocked             bool
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
	Draining        bool // M5.5：手动排水标记（调度不再放置）
	// Evacuate（v1.1，ADR-0021）：drain 时同步驱离存量 machine 的运维意图。
	// 仅与 draining=true 组合有效；ready（draining=false）时一并清零。
	Evacuate bool
	// EvacuationMachineID/StartedAt 是当前驱离步骤的持久化 fence。它使
	// leader 切换后仍只等待同一 replacement，绝不提前推进下一个 machine。
	EvacuationMachineID string
	EvacuationStartedAt *time.Time
	// ImageCache（v1.1，ADR-0018）：节点本地镜像缓存 digest 列表
	//（LRU/创建序，ServiceInfo 20s sync 落库）。
	ImageCache []string
	// v1.2-A（ADR-0023）：runtime capability 投影。
	ProtocolVersion          string
	FeatureIDs               []string
	SnapshotCompatibilityKey string
	LastSeenAt               time.Time
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// SchedulerEvent 是 scheduler_events 表的行（调度/对账审计，MVP 最低可观测）。
type SchedulerEvent struct {
	ID          int64
	At          time.Time
	ProjectID   string
	Kind        string // placement|filter_rejection|reconcile|reservation
	MachineID   string
	OperationID string
	NodeID      string
	Reason      string
	Details     json.RawMessage
}

// PendingUsage 是单个节点上在途 create 操作的资源承诺（scheduler pending 记账）。
type PendingUsage struct {
	NodeID  string
	VCPU    int64
	MemMib  int64
	DiskMib int64 // v1.2-E（ADR-0035）；视图层 0 = 未上报
}

// Allocated 是单个节点上已落地 machine 的资源承诺（scheduler allocated 记账）。
type Allocated struct {
	VCPU    int64
	MemMib  int64
	DiskMib int64 // v1.2-E（ADR-0035）
}

// EnsureProject 幂等创建 project（M1 dev 用）。
func (s *Store) EnsureProject(ctx context.Context, id, name string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`, id, name)
	return err
}

// ProjectQuota 返回项目配额（vcpu / MiB）。
// ProjectQuota（v1.2-E：含磁盘）返回项目配额（PG 权威）。
func (s *Store) ProjectQuota(ctx context.Context, projectID string) (vcpu, memMib, diskMib int64, err error) {
	if err := s.pool.QueryRow(ctx,
		`SELECT vcpu_quota, mem_mib_quota, disk_mib_quota FROM projects WHERE id=$1`, projectID).
		Scan(&vcpu, &memMib, &diskMib); err != nil {
		return 0, 0, 0, fmt.Errorf("project quota %s: %w", projectID, err)
	}
	return vcpu, memMib, diskMib, nil
}

// ProjectUsage 返回项目已分配和在途 create 的资源总量。已分配只计有
// node_id 的期望存活机器；尚未落到节点的 create 由 operations 计入 pending，
// 从而避免将同一新机器重复记为 allocated 和 pending。
// requested_disk_mib=0（历史行/未声明）按默认 10GiB 计（与
// contracts.DefaultDiskMib 对齐；磁盘调度/预约/准入三处同值，ADR-0035）。
func (s *Store) ProjectUsage(ctx context.Context, projectID string) (vcpu, memMib, diskMib int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT
			coalesce((SELECT sum(m.requested_vcpu) FROM machines m
				JOIN apps a ON a.id=m.app_id
				WHERE a.project_id=$1 AND m.desired_state IN ('CREATED','RUNNING') AND m.node_id<>''), 0)
			+ coalesce((SELECT sum(m.requested_vcpu) FROM operations o
				JOIN machines m ON m.id=o.machine_id
				WHERE o.project_id=$1 AND o.kind='create' AND o.status IN ('PENDING','CLAIMED')
					AND m.node_id='' AND o.id=(SELECT min(i.id) FROM operations i
						WHERE i.machine_id=o.machine_id AND i.kind='create' AND i.status IN ('PENDING','CLAIMED'))), 0),
			coalesce((SELECT sum(m.requested_mem_mib) FROM machines m
				JOIN apps a ON a.id=m.app_id
				WHERE a.project_id=$1 AND m.desired_state IN ('CREATED','RUNNING') AND m.node_id<>''), 0)
			+ coalesce((SELECT sum(m.requested_mem_mib) FROM operations o
				JOIN machines m ON m.id=o.machine_id
				WHERE o.project_id=$1 AND o.kind='create' AND o.status IN ('PENDING','CLAIMED')
					AND m.node_id='' AND o.id=(SELECT min(i.id) FROM operations i
						WHERE i.machine_id=o.machine_id AND i.kind='create' AND i.status IN ('PENDING','CLAIMED'))), 0),
			coalesce((SELECT sum(coalesce(nullif(m.requested_disk_mib,0), 10240)) FROM machines m
				JOIN apps a ON a.id=m.app_id
				WHERE a.project_id=$1 AND m.desired_state IN ('CREATED','RUNNING') AND m.node_id<>''), 0)
			+ coalesce((SELECT sum(coalesce(nullif(m.requested_disk_mib,0), 10240)) FROM operations o
				JOIN machines m ON m.id=o.machine_id
				WHERE o.project_id=$1 AND o.kind='create' AND o.status IN ('PENDING','CLAIMED')
					AND m.node_id='' AND o.id=(SELECT min(i.id) FROM operations i
						WHERE i.machine_id=o.machine_id AND i.kind='create' AND i.status IN ('PENDING','CLAIMED'))), 0)
			+ coalesce((SELECT sum((size_bytes+1048575)/1048576) FROM volumes
				WHERE project_id=$1 AND state<>'DELETED'), 0)
			+ coalesce((SELECT sum((a.overlay_size_bytes+1048575)/1048576) FROM volume_attachments a
				JOIN volumes v ON v.id=a.volume_id WHERE v.project_id=$1 AND a.status<>'DETACHED'), 0)`, projectID).Scan(&vcpu, &memMib, &diskMib)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("project usage %s: %w", projectID, err)
	}
	return vcpu, memMib, diskMib, nil
}

// CreateAppAndDeployment 原子创建 app 与初始 ACTIVE deployment。若任一步骤
// 失败事务回滚，避免留下 controller 会对账但没有 deployment 的 app。
func (s *Store) CreateAppAndDeployment(ctx context.Context, projectID string, app App, dep Deployment) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
			VALUES($1,$2,$3,$4,$5,$6,$7,1)`,
			app.ID, projectID, app.Hostname, app.ImageRef, app.VCPU, app.MemMIB, app.DesiredReplicas); err != nil {
			return fmt.Errorf("create app: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port,
				env, placement, health_check, status, secret_refs, auto_standby, services, strategy,
				required_features, egress_policy)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16::jsonb,$17::jsonb)`,
			dep.ID, dep.AppID, dep.Generation, dep.ImageRef, dep.VCPU, dep.MemMIB, dep.Port,
			jsonMap(dep.Env), jsonOrNull(dep.Placement), jsonOrNull(dep.HealthCheck), dep.Status,
			secretRefsJSON(dep.SecretRefs), jsonOrNull(dep.AutoStandby), servicesJSON(dep.Services),
			effectiveStrategyLiteral(dep.Strategy), requiredFeaturesJSON(dep.RequiredFeatures), jsonOrNull(dep.EgressPolicy)); err != nil {
			return fmt.Errorf("create initial deployment: %w", err)
		}
		if len(dep.EgressPolicy) > 0 && string(dep.EgressPolicy) != "null" {
			if err := recordEgressPolicyChangeTx(ctx, tx, projectID, dep); err != nil {
				return err
			}
		}
		return nil
	})
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
	vcpu, memMIB, diskMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64, replicaOrdinal int,
	requestJSON, placementJSON []byte,
) (Operation, error) {
	return s.EnsureAppAndEnqueueCreateWithLifecycle(ctx, projectID, appID, hostname, imageRef,
		vcpu, memMIB, diskMIB, ingressPort, machineID, deploymentID, executionID, operationID,
		generation, replicaOrdinal, requestJSON, placementJSON, nil, "NEVER", 3, 10, 300)
}

// EnsureAppAndEnqueueCreateResurrect 是 app 对账（controller scale up）的显式
// 墓碑复活通道：v1.2-D 守卫拒绝 create upsert 复活 DELETED 行（防快照决策与
// 用户 delete 竞争），但 scale down→up 是 owner 自己的生命周期决策——controller
// 已判定该 ordinal 属于目标代且有意重建。复活时换新 execution、清 observed
// 与 node_id，generation 保持 GREATEST 不回退（墓碑可能已换代，降低会让
// agent 拒绝后续合法 create）。
func (s *Store) EnsureAppAndEnqueueCreateResurrect(
	ctx context.Context,
	projectID, appID, hostname, imageRef string,
	vcpu, memMIB, diskMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64, replicaOrdinal int,
	requestJSON, placementJSON []byte,
) (Operation, error) {
	return s.ensureAppAndEnqueueCreate(ctx, projectID, appID, hostname, imageRef,
		vcpu, memMIB, diskMIB, ingressPort, machineID, deploymentID, executionID, operationID,
		generation, replicaOrdinal, requestJSON, placementJSON, nil, "NEVER", 3, 10, 300, true)
}

// EnsureAppAndEnqueueCreateWithLifecycle atomically persists the initial TTL and
// restart policy with the machine and create outbox row. An idempotent replay
// returns before touching lifecycle fields, so a relative TTL is never extended.
func (s *Store) EnsureAppAndEnqueueCreateWithLifecycle(
	ctx context.Context,
	projectID, appID, hostname, imageRef string,
	vcpu, memMIB, diskMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64, replicaOrdinal int,
	requestJSON, placementJSON []byte, expiresAt *time.Time,
	restartMode string, restartMaxAttempts, restartBackoffSeconds, restartStableWindowSeconds int,
) (Operation, error) {
	return s.ensureAppAndEnqueueCreate(ctx, projectID, appID, hostname, imageRef,
		vcpu, memMIB, diskMIB, ingressPort, machineID, deploymentID, executionID, operationID,
		generation, replicaOrdinal, requestJSON, placementJSON, expiresAt,
		restartMode, restartMaxAttempts, restartBackoffSeconds, restartStableWindowSeconds, false)
}

func (s *Store) ensureAppAndEnqueueCreate(
	ctx context.Context,
	projectID, appID, hostname, imageRef string,
	vcpu, memMIB, diskMIB int64, ingressPort int,
	machineID, deploymentID, executionID, operationID string,
	generation int64, replicaOrdinal int,
	requestJSON, placementJSON []byte, expiresAt *time.Time,
	restartMode string, restartMaxAttempts, restartBackoffSeconds, restartStableWindowSeconds int,
	allowResurrect bool,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// 所有权检查必须先于幂等短路和任何 upsert。否则重复 operation 或
		// machine ID 可被另一个 project/app/deployment 借用，甚至先改写
		// desired state 再因 operation 冲突回滚语义之外返回旧结果。
		var appProject string
		err := tx.QueryRow(ctx, `SELECT project_id FROM apps WHERE id=$1 FOR UPDATE`, appID).Scan(&appProject)
		switch {
		case err == nil && appProject != projectID:
			return ownershipConflict("app", appID, "project_id", projectID, appProject)
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("check app ownership: %w", err)
		}

		var machineApp, machineDeployment, machineProject string
		err = tx.QueryRow(ctx, `
			SELECT m.app_id, m.deployment_id, a.project_id
			FROM machines m JOIN apps a ON a.id=m.app_id
			WHERE m.id=$1 FOR UPDATE OF m`, machineID).
			Scan(&machineApp, &machineDeployment, &machineProject)
		switch {
		case err == nil && machineProject != projectID:
			return ownershipConflict("machine", machineID, "project_id", projectID, machineProject)
		case err == nil && machineApp != appID:
			return ownershipConflict("machine", machineID, "app_id", appID, machineApp)
		case err == nil && machineDeployment != deploymentID:
			return ownershipConflict("machine", machineID, "deployment_id", deploymentID, machineDeployment)
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("check machine ownership: %w", err)
		}

		// deployment 可能由 apps rollout 路径预先创建；legacy create 没有
		// deployment 行，因此只在已存在时验证其 immutable app 归属。
		var deploymentApp string
		err = tx.QueryRow(ctx, `SELECT app_id FROM deployments WHERE id=$1 FOR UPDATE`, deploymentID).
			Scan(&deploymentApp)
		switch {
		case err == nil && deploymentApp != appID:
			return ownershipConflict("deployment", deploymentID, "app_id", appID, deploymentApp)
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("check deployment ownership: %w", err)
		}

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

		var persistedAppProject string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib)
			VALUES($1,$2,$3,$4,$5,$6)
			ON CONFLICT (id) DO UPDATE SET image_ref=EXCLUDED.image_ref,
				vcpu=EXCLUDED.vcpu, mem_mib=EXCLUDED.mem_mib, updated_at=now()
			WHERE apps.project_id=EXCLUDED.project_id
			RETURNING project_id`,
			appID, projectID, hostname, imageRef, vcpu, memMIB).Scan(&persistedAppProject); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if scanErr := tx.QueryRow(ctx, `SELECT project_id FROM apps WHERE id=$1`, appID).Scan(&persistedAppProject); scanErr != nil {
					return fmt.Errorf("resolve app ownership conflict: %w", scanErr)
				}
				return ownershipConflict("app", appID, "project_id", projectID, persistedAppProject)
			}
			return fmt.Errorf("upsert app: %w", err)
		}

		var expiresArg any
		if expiresAt != nil {
			expiresArg = *expiresAt
		}
		var desiredAfter string
		if err := tx.QueryRow(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
				desired_state, generation, current_execution_id, requested_vcpu,
				requested_mem_mib, requested_disk_mib, image_ref, node_id, ingress_port, placement,
				expires_at, restart_mode, restart_max_attempts, restart_backoff_seconds,
				restart_stable_window_seconds)
			VALUES($1,$2,$3,$4,$5,'CREATED',$6,$7,$8,$9,$10,$11,'',$12,$13::jsonb,$14,$15,$16,$17,$18)
			ON CONFLICT (id) DO UPDATE SET
				-- 复活守卫（v1.2-D，ADR-0026 §5）：快照决策与提交之间可能落入
				-- 用户 delete / TTL mark。已删除的 machine 不允许被 create upsert
				-- 复活：整行保持原样（含 desired/generation/execution/observed），
				-- 由事务后置检查报错；restart 与 R3/app 重建共用本函数。
				desired_state = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.desired_state ELSE 'CREATED' END,
				generation = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.generation ELSE GREATEST(machines.generation, EXCLUDED.generation) END,
				current_execution_id = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.current_execution_id ELSE EXCLUDED.current_execution_id END,
				replica_ordinal = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.replica_ordinal ELSE EXCLUDED.replica_ordinal END,
				ingress_port = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.ingress_port ELSE EXCLUDED.ingress_port END,
				placement = CASE WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.placement ELSE EXCLUDED.placement END,
				-- generation 单调不回退（P1-3）：换代重建后用户携旧默认值重试
				-- 不得拉低 fence 水位，否则 agent 会拒绝后续合法 create。
				observed_state = CASE
					WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.observed_state
					WHEN machines.desired_state='DELETED' OR machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN ''
					ELSE machines.observed_state END,
				observed_slot_ip = CASE
					WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.observed_slot_ip
					WHEN machines.desired_state='DELETED' OR machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN ''
					ELSE machines.observed_slot_ip END,
				observed_readiness = CASE
					WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.observed_readiness
					WHEN machines.desired_state='DELETED' OR machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN 'UNKNOWN'
					ELSE machines.observed_readiness END,
				last_observed_at = CASE
					WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.last_observed_at
					WHEN machines.desired_state='DELETED' OR machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN NULL
					ELSE machines.last_observed_at END,
				node_id = CASE
					WHEN machines.desired_state='DELETED' AND NOT $19 THEN machines.node_id
					WHEN machines.desired_state='DELETED' OR machines.current_execution_id IS DISTINCT FROM EXCLUDED.current_execution_id THEN ''
					ELSE machines.node_id END,
				updated_at = now()
			WHERE machines.app_id=EXCLUDED.app_id
			  AND machines.deployment_id=EXCLUDED.deployment_id
			RETURNING desired_state`,
			machineID, appID, deploymentID, replicaOrdinal, hostname, generation, executionID, vcpu, memMIB, diskMIB, imageRef, ingressPort, placementJSONOrEmpty(placementJSON),
			expiresArg, restartMode, restartMaxAttempts, restartBackoffSeconds, restartStableWindowSeconds, allowResurrect).Scan(&desiredAfter); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if scanErr := tx.QueryRow(ctx, `SELECT app_id, deployment_id FROM machines WHERE id=$1`, machineID).
					Scan(&machineApp, &machineDeployment); scanErr != nil {
					return fmt.Errorf("resolve machine ownership conflict: %w", scanErr)
				}
				if machineApp != appID {
					return ownershipConflict("machine", machineID, "app_id", appID, machineApp)
				}
				return ownershipConflict("machine", machineID, "deployment_id", deploymentID, machineDeployment)
			}
			return fmt.Errorf("upsert machine: %w", err)
		}
		// 复活守卫的后置检查：DELETED 行不再登记 create（否则下一轮对账会
		// 派发一个已被删除的 machine）。
		if desiredAfter == "DELETED" {
			return ErrMachineLifecycleClosed
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
func (s *Store) EnqueueDelete(
	ctx context.Context,
	projectID, machineID, executionID, operationID string,
	generation int64,
	requestJSON []byte,
) (Operation, error) {
	return s.enqueueDeleteKind(ctx, projectID, machineID, executionID, operationID, generation, requestJSON, "delete")
}

// EnqueueReapDelete 登记 reconcile 清理用 delete（kind=reap）：删的是旧代/
// 死亡残留，成功后不得把 machines.desired_state 推进为 DELETED（R2/R5 路径）。
func (s *Store) EnqueueReapDelete(
	ctx context.Context,
	projectID, machineID, executionID, operationID string,
	generation int64,
	requestJSON []byte,
) (Operation, error) {
	return s.enqueueDeleteKind(ctx, projectID, machineID, executionID, operationID, generation, requestJSON, "reap")
}

// EnqueueLifecycle 入队 pause/resume 生命周期操作（M4.5 scale-to-zero）。
// 幂等键 = operationID（调用方用 op-lifecycle-{machine}-{exec}-{n} 形态，
// 含时间窗序号防与历史 op 冲突）。kind ∈ {pause, resume}。
func (s *Store) EnqueueLifecycle(ctx context.Context, projectID, machineID, executionID,
	operationID string, generation int64, kind string, requestJSON []byte,
) (Operation, error) {
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

func (s *Store) enqueueDeleteKind(
	ctx context.Context,
	projectID, machineID, executionID, operationID string,
	generation int64,
	requestJSON []byte,
	kind string,
) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		op, err = enqueueOperationIdempotentTx(ctx, tx, projectID, machineID,
			executionID, operationID, generation, requestJSON, kind, true)
		return err
	})
	if err != nil {
		return Operation{}, err
	}
	return op, nil
}

// enqueueOperationIdempotentTx 是幂等 operation 入队的 tx 内联实现（供
// enqueueDeleteKind 与 SoftDeleteAppAndEnqueueDeletes 复用）；resurrectFailed
// 控制 FAILED 终态是否在请求体一致时复活为 PENDING（delete/reap 为 true）。
func enqueueOperationIdempotentTx(
	ctx context.Context, tx pgx.Tx,
	projectID, machineID, executionID, operationID string,
	generation int64, requestJSON []byte, kind string, resurrectFailed bool,
) (Operation, error) {
	existing, err := selectOperationByKey(ctx, tx, projectID, operationID)
	if err != nil {
		return Operation{}, err
	}
	if existing != nil {
		if !jsonEqual(existing.Request, requestJSON) {
			return Operation{}, ErrRequestConflict
		}
		if resurrectFailed && existing.Status == "FAILED" {
			if err := resurrectOperation(ctx, tx, existing.ID); err != nil {
				return Operation{}, err
			}
			existing.Status = "PENDING"
			existing.Error = ""
		}
		return *existing, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request)
		VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb)
		ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
		operationID, projectID, machineID, executionID, generation, kind, string(requestJSON)); err != nil {
		return Operation{}, fmt.Errorf("enqueue %s: %w", kind, err)
	}

	created, err := selectOperationByKey(ctx, tx, projectID, operationID)
	if err != nil {
		return Operation{}, err
	}
	if created == nil {
		return Operation{}, fmt.Errorf("operation %s disappeared after insert", operationID)
	}
	return *created, nil
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
			WHERE status='PENDING' AND kind<>'image_prewarm'
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

// EnqueueOperationParams 是 EnqueueOperation 的参数集（v1.3-B 通用操作入口；
// 幂等键 = operationID，请求体不一致返回 ErrRequestConflict）。
type EnqueueOperationParams struct {
	OperationID    string
	ProjectID      string
	MachineID      string
	ExecutionID    string
	Generation     int64
	Kind           string
	Request        []byte
	DispatchNodeID string
}

// EnqueueOperation 入队一条带 fencing 的通用操作（snapshot_create 等）。
func (s *Store) EnqueueOperation(ctx context.Context, p EnqueueOperationParams) (Operation, error) {
	var op Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		existing, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
		if err != nil {
			return err
		}
		if existing != nil {
			if !jsonEqual(existing.Request, p.Request) {
				return ErrRequestConflict
			}
			op = *existing
			return nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
				kind, idempotency_key, status, request, dispatch_node_id)
			VALUES($1,$2,$3,$4,$5,$6,$1,'PENDING',$7::jsonb,NULLIF($8,''))
			ON CONFLICT (project_id, idempotency_key) DO NOTHING`,
			p.OperationID, p.ProjectID, p.MachineID, p.ExecutionID, p.Generation,
			p.Kind, string(p.Request), p.DispatchNodeID); err != nil {
			return fmt.Errorf("enqueue %s: %w", p.Kind, err)
		}
		created, err := selectOperationByKey(ctx, tx, p.ProjectID, p.OperationID)
		if err != nil {
			return err
		}
		if created == nil {
			return fmt.Errorf("operation %s disappeared after insert", p.OperationID)
		}
		op = *created
		return nil
	})
	return op, err
}

// ListMachines 返回所有（或按 project）machine。
func (s *Store) ListMachines(ctx context.Context, projectID string) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.requested_disk_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at, m.expires_at, m.restart_mode,
			m.restart_max_attempts, m.restart_backoff_seconds,
			m.restart_stable_window_seconds, m.restart_attempts,
			m.restart_next_attempt_at, m.restart_stable_since, m.restart_blocked
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
			m.requested_vcpu, m.requested_mem_mib, m.requested_disk_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at, m.expires_at, m.restart_mode,
			m.restart_max_attempts, m.restart_backoff_seconds,
			m.restart_stable_window_seconds, m.restart_attempts,
			m.restart_next_attempt_at, m.restart_stable_since, m.restart_blocked
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

// UpdateMachineObserved 写入 agent 观测状态，仅当 executionID 仍为当前
// execution 时更新。迟到的旧代观测必须无声丢弃，不能覆盖新代。
func (s *Store) UpdateMachineObserved(ctx context.Context, id, executionID, state, slotIP, readiness string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET observed_state=$1, observed_slot_ip=$2, observed_readiness=$3,
			last_observed_at=now(), updated_at=now()
		WHERE id=$4 AND current_execution_id=$5`,
		state, slotIP, readiness, id, executionID)
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

// ErrMachineNotFound 表示 machine 行不存在（区别于已删除/过期）。
var ErrMachineNotFound = errors.New("machine not found")

// ErrMachineLifecycleClosed 表示 machine 已删除或已过期：禁止通过更新
// TTL/restart policy 复活（v1.2-D，ADR-0026）。
var ErrMachineLifecycleClosed = errors.New("machine deleted or expired; lifecycle update rejected")

// SetMachineExpiry 更新绝对到期时间（NULL = 关闭）。已删除或已过期
// machine 拒绝更新（不能通过延长 TTL 复活）。
func (s *Store) SetMachineExpiry(ctx context.Context, id string, expiresAt *time.Time) error {
	var arg any
	if expiresAt != nil {
		arg = *expiresAt
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE machines SET expires_at=$2, updated_at=now()
		WHERE id=$1 AND desired_state <> 'DELETED'
		  AND (expires_at IS NULL OR expires_at > now())`, id, arg)
	if err != nil {
		return fmt.Errorf("set machine expiry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if s.machineExists(ctx, id) {
			return ErrMachineLifecycleClosed
		}
		return ErrMachineNotFound
	}
	return nil
}

// SetMachineRestartPolicy 持久化 restart policy（控制面唯一权威）。
func (s *Store) SetMachineRestartPolicy(ctx context.Context, id, mode string,
	maxAttempts, backoffSeconds, stableWindowSeconds int,
) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE machines SET restart_mode=$2, restart_max_attempts=$3,
			restart_backoff_seconds=$4, restart_stable_window_seconds=$5,
			updated_at=now()
		WHERE id=$1 AND desired_state <> 'DELETED'`,
		id, mode, maxAttempts, backoffSeconds, stableWindowSeconds)
	if err != nil {
		return fmt.Errorf("set restart policy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if s.machineExists(ctx, id) {
			return ErrMachineLifecycleClosed
		}
		return ErrMachineNotFound
	}
	return nil
}

// machineExists 判断 machine 行是否存在（区分 404 与 409）。
func (s *Store) machineExists(ctx context.Context, id string) bool {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM machines WHERE id=$1)`, id).Scan(&exists); err != nil {
		return false
	}
	return exists
}

// MarkExpiredRouteDetached establishes the durable first TTL phase. Route
// reconciliation excludes these rows before any delete operation can exist.
func (s *Store) MarkExpiredRouteDetached(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE machines SET lifecycle_delete_phase='ROUTE_DETACHED',
		updated_at=now() WHERE id=$1 AND desired_state <> 'DELETED'
		AND lifecycle_delete_phase='ACTIVE' AND expires_at <= now()`, id)
	return err
}

// ExpiredRouteDetached reports the durable phase and verifies that the PG route
// authority no longer contains this execution as a backend.
func (s *Store) ExpiredRouteDetached(ctx context.Context, id, executionID string) (bool, error) {
	var ready bool
	err := s.pool.QueryRow(ctx, `SELECT lifecycle_delete_phase='ROUTE_DETACHED'
		AND NOT EXISTS (SELECT 1 FROM route_backends rb
			WHERE rb.machine_id=$1 AND rb.execution_id=$2)
		FROM machines WHERE id=$1`, id, executionID).Scan(&ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return ready, err
}

// ListExpiredMachines 返回已到期且尚未删除的 machine（TTL 回收输入）。
func (s *Store) ListExpiredMachines(ctx context.Context, now time.Time) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.requested_disk_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at, m.expires_at, m.restart_mode,
			m.restart_max_attempts, m.restart_backoff_seconds,
			m.restart_stable_window_seconds, m.restart_attempts,
			m.restart_next_attempt_at, m.restart_stable_since, m.restart_blocked
		FROM machines m
		WHERE m.expires_at IS NOT NULL AND m.expires_at <= $1
		  AND m.desired_state <> 'DELETED'
		ORDER BY m.expires_at`, now)
	if err != nil {
		return nil, fmt.Errorf("list expired machines: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

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
	imageCache := "[]"
	if len(n.ImageCache) > 0 {
		b, err := json.Marshal(n.ImageCache)
		if err != nil {
			return fmt.Errorf("marshal image cache: %w", err)
		}
		imageCache = string(b)
	}
	featureIDs := "[]"
	if len(n.FeatureIDs) > 0 {
		b, err := json.Marshal(n.FeatureIDs)
		if err != nil {
			return fmt.Errorf("marshal feature ids: %w", err)
		}
		featureIDs = string(b)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO nodes(id, nomad_node_id, node_pool, status, labels,
			vcpu_total, mem_total_mib, disk_total_mib, cpu_percent,
			mem_used_mib, mem_allocated_mib, disk_used_mib, grpc_addr, proxy_addr,
			image_cache, feature_ids, protocol_version, snapshot_compatibility_key,
			last_seen_at)
		VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17,$18,now())
		ON CONFLICT (nomad_node_id) DO UPDATE SET id=EXCLUDED.id,
			node_pool=EXCLUDED.node_pool, status=EXCLUDED.status, labels=EXCLUDED.labels,
			vcpu_total=EXCLUDED.vcpu_total, mem_total_mib=EXCLUDED.mem_total_mib,
			disk_total_mib=EXCLUDED.disk_total_mib, cpu_percent=EXCLUDED.cpu_percent,
			mem_used_mib=EXCLUDED.mem_used_mib, mem_allocated_mib=EXCLUDED.mem_allocated_mib,
			disk_used_mib=EXCLUDED.disk_used_mib, grpc_addr=EXCLUDED.grpc_addr,
			proxy_addr=EXCLUDED.proxy_addr, image_cache=EXCLUDED.image_cache,
			feature_ids=EXCLUDED.feature_ids, protocol_version=EXCLUDED.protocol_version,
			snapshot_compatibility_key=EXCLUDED.snapshot_compatibility_key,
			last_seen_at=now(), updated_at=now()`,
		n.ID, n.NomadNodeID, n.NodePool, n.Status, labelsJSON,
		n.VCPUTotal, n.MemTotalMib, n.DiskTotalMib, n.CPUPercent,
		n.MemUsedMib, n.MemAllocatedMib, n.DiskUsedMib, n.GRPCAddr, n.ProxyAddr,
		imageCache, featureIDs, n.ProtocolVersion, n.SnapshotCompatibilityKey)
	if err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}
	return nil
}

// ErrEvacuationBusy 表示已有另一节点正在 evacuate。数据库部分唯一索引是
// 此不变量的最终仲裁，应用错误用于 API 返回可操作的冲突。
var ErrEvacuationBusy = errors.New("another node evacuation is active")

// SetNodeDraining 置/清节点排水标记（M5.5）。nodeID = /v1/nodes 里的 id。
// evacuate=true 在整个集群最多允许一个节点；ready 同时清除持久步骤。
func (s *Store) SetNodeDraining(ctx context.Context, nodeID string, draining, evacuate bool) error {
	if !draining {
		evacuate = false
	}
	ct, err := s.pool.Exec(ctx, `UPDATE nodes
		SET draining=$2, evacuate=$3,
			evacuation_machine_id=CASE WHEN $3 THEN evacuation_machine_id ELSE NULL END,
			evacuation_started_at=CASE WHEN $3 THEN evacuation_started_at ELSE NULL END,
			updated_at=now() WHERE id=$1`, nodeID, draining, evacuate)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrEvacuationBusy
		}
		return fmt.Errorf("set node draining: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// StartEvacuationStep durably claims the only current machine for node. A
// duplicate claim is harmless and returns false; the marker survives leader
// changes and is cleared only after the replacement is route-ready.
func (s *Store) StartEvacuationStep(ctx context.Context, nodeID, machineID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE nodes SET evacuation_machine_id=$2,
		evacuation_started_at=now(), updated_at=now()
		WHERE id=$1 AND draining=true AND evacuate=true
		AND evacuation_machine_id IS NULL`, nodeID, machineID)
	if err != nil {
		return false, fmt.Errorf("start evacuation step: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClearEvacuationStep releases a completed/abandoned step only if it is still
// the caller's machine, preventing a stale leader from clearing newer work.
func (s *Store) ClearEvacuationStep(ctx context.Context, nodeID, machineID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE nodes SET evacuation_machine_id=NULL,
		evacuation_started_at=NULL, updated_at=now()
		WHERE id=$1 AND evacuation_machine_id=$2`, nodeID, machineID)
	if err != nil {
		return fmt.Errorf("clear evacuation step: %w", err)
	}
	return nil
}

func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, nomad_node_id, node_pool, status, labels::text,
			vcpu_total, mem_total_mib, disk_total_mib, cpu_percent,
			mem_used_mib, mem_allocated_mib, disk_used_mib, grpc_addr, proxy_addr,
			last_seen_at, created_at, updated_at, draining, evacuate,
			coalesce(evacuation_machine_id,''), evacuation_started_at,
			coalesce(image_cache::text,'[]'),
			coalesce(feature_ids::text,'[]'), protocol_version,
			snapshot_compatibility_key
		FROM nodes ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		var labels, imageCache, featureIDs string
		if err := rows.Scan(&n.ID, &n.NomadNodeID, &n.NodePool, &n.Status, &labels,
			&n.VCPUTotal, &n.MemTotalMib, &n.DiskTotalMib, &n.CPUPercent,
			&n.MemUsedMib, &n.MemAllocatedMib, &n.DiskUsedMib, &n.GRPCAddr, &n.ProxyAddr,
			&n.LastSeenAt, &n.CreatedAt, &n.UpdatedAt, &n.Draining, &n.Evacuate,
			&n.EvacuationMachineID, &n.EvacuationStartedAt, &imageCache, &featureIDs,
			&n.ProtocolVersion, &n.SnapshotCompatibilityKey); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		if labels != "" && labels != "null" {
			_ = json.Unmarshal([]byte(labels), &n.Labels)
		}
		if imageCache != "" && imageCache != "null" && imageCache != "[]" {
			_ = json.Unmarshal([]byte(imageCache), &n.ImageCache)
		}
		if featureIDs != "" && featureIDs != "null" && featureIDs != "[]" {
			_ = json.Unmarshal([]byte(featureIDs), &n.FeatureIDs)
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
	projectID := ev.ProjectID
	if projectID == "" && ev.MachineID != "" {
		if err := s.pool.QueryRow(ctx, `SELECT a.project_id FROM machines m JOIN apps a ON a.id=m.app_id WHERE m.id=$1`, ev.MachineID).Scan(&projectID); err != nil &&
			!errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("resolve scheduler event project: %w", err)
		}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO scheduler_events(project_id, kind, machine_id, operation_id, node_id, reason, details)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`,
		projectID, ev.Kind, ev.MachineID, ev.OperationID, ev.NodeID, ev.Reason, string(details))
	if err != nil {
		return fmt.Errorf("record scheduler event: %w", err)
	}
	return nil
}

// ListSchedulerEvents 返回最近 limit 条事件。projectID 非空时严格只返回该项目
// 的事件；无项目归属的系统运维事件仅由 admin（传空 projectID）可读取。
func (s *Store) ListSchedulerEvents(ctx context.Context, projectID string, limit int) ([]SchedulerEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	query := `SELECT id, at, project_id, kind, machine_id, operation_id, node_id, reason, details::text
		FROM scheduler_events`
	args := []any{}
	if projectID != "" {
		query += ` WHERE project_id=$1`
		args = append(args, projectID)
	}
	query += ` ORDER BY at DESC, id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list scheduler events: %w", err)
	}
	defer rows.Close()
	var out []SchedulerEvent
	for rows.Next() {
		var ev SchedulerEvent
		var details string
		if err := rows.Scan(&ev.ID, &ev.At, &ev.ProjectID, &ev.Kind, &ev.MachineID, &ev.OperationID,
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

// SecretCleanupDischarged 判定某 lease 的确定清理（uncertain-cleanup reap）
// 是否已成功——这是 delete 可以在 UNCERTAIN 状态下收敛的充分条件。
func (s *Store) SecretCleanupDischarged(ctx context.Context, createOperationID string) (bool, error) {
	var status string
	err := s.pool.QueryRow(ctx,
		`SELECT status FROM operations WHERE id=$1`, "op-secret-cleanup-"+createOperationID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("secret cleanup lookup: %w", err)
	}
	return status == "SUCCEEDED", nil
}

// CreateDispatchedToAgent 判定某 execution 的 create 是否真实发到过 agent
// （存在携带 dispatch_node 的 create 派发）。不确定 secret 创建只在“可能已落
// agent”时才有不确定语义：若从未派发，任何 agent 都不可能有该 execution 的
// 工件，delete 可以安全按“节点失联”收敛而不是无限等待。
func (s *Store) CreateDispatchedToAgent(ctx context.Context, machineID, executionID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM operations
			WHERE machine_id=$1 AND execution_id=$2 AND kind='create'
			  AND coalesce(dispatch_node_id,'')<>'')`,
		machineID, executionID).Scan(&exists); err != nil {
		return false, fmt.Errorf("create dispatched to agent: %w", err)
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
		SELECT node_id, sum(vcpu), sum(mem), sum(disk) FROM (
			SELECT o.dispatch_node_id AS node_id, m.requested_vcpu AS vcpu, m.requested_mem_mib AS mem,
				coalesce(nullif(m.requested_disk_mib,0),10240) AS disk
			FROM operations o JOIN machines m ON m.id=o.machine_id
			WHERE o.kind='create' AND o.status IN ('PENDING','CLAIMED') AND o.dispatch_node_id<>''
			UNION ALL
			SELECT dispatch_node_id, 0, 0,
				(coalesce((request->>'size_bytes')::bigint,(request->>'max_expanded_bytes')::bigint,0)+1048575)/1048576
			FROM operations WHERE kind IN ('volume_create','dataset_import') AND status IN ('PENDING','CLAIMED') AND dispatch_node_id<>''
			UNION ALL
			SELECT dispatch_node_id, 0, 0, (coalesce((request->>'overlay_size_bytes')::bigint,0)+1048575)/1048576
			FROM operations WHERE kind='volume_attach' AND status IN ('PENDING','CLAIMED')
				AND coalesce((request->>'overlay_size_bytes')::bigint,0)>0 AND dispatch_node_id<>''
		) pending GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("pending usage: %w", err)
	}
	defer rows.Close()
	var out []PendingUsage
	for rows.Next() {
		var p PendingUsage
		if err := rows.Scan(&p.NodeID, &p.VCPU, &p.MemMib, &p.DiskMib); err != nil {
			return nil, fmt.Errorf("scan pending usage: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MachineNodesByDeployment 返回 deployment → 已占节点集合（反亲和排除集）。
// 除已提交 machine 的 node_id，还必须计入在途 create 的 dispatch node：
// 并发（有界 worker）同 tick 初版部署时，第二个放置读取时第一个 machine
// 行尚未写入 node_id，只靠 machines 会让两副本看到同一个“空”集合而落
// 同节点（多节点真机验收复现）。在途 = PENDING/CLAIMED 且已定派发节点；
// 终态 op 与 delete 的 dispatch 不占用（终态 create 已由 machines 覆盖）。
func (s *Store) MachineNodesByDeployment(ctx context.Context) (map[string]map[string]bool, error) {
	out := map[string]map[string]bool{}
	rows, err := s.pool.Query(ctx, `
		SELECT deployment_id, node_id FROM machines
		WHERE desired_state IN ('CREATED','RUNNING') AND node_id<>''`)
	if err != nil {
		return nil, fmt.Errorf("machine nodes by deployment: %w", err)
	}
	defer rows.Close()
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	opRows, err := s.pool.Query(ctx, `
		SELECT m.deployment_id, o.dispatch_node_id
		FROM operations o JOIN machines m ON m.id = o.machine_id
		WHERE o.kind='create' AND o.status IN ('PENDING','CLAIMED')
			AND o.dispatch_node_id IS NOT NULL AND o.dispatch_node_id<>''`)
	if err != nil {
		return nil, fmt.Errorf("inflight create dispatch nodes: %w", err)
	}
	defer opRows.Close()
	for opRows.Next() {
		var dep, node string
		if err := opRows.Scan(&dep, &node); err != nil {
			return nil, fmt.Errorf("scan inflight dispatch node: %w", err)
		}
		if out[dep] == nil {
			out[dep] = map[string]bool{}
		}
		out[dep][node] = true
	}
	return out, opRows.Err()
}

// AllocatedByNode 汇总每个节点上期望存活 machine 的资源承诺。
func (s *Store) AllocatedByNode(ctx context.Context) (map[string]Allocated, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, sum(vcpu), sum(mem), sum(disk) FROM (
			SELECT node_id, requested_vcpu AS vcpu, requested_mem_mib AS mem,
				coalesce(nullif(requested_disk_mib,0),10240) AS disk
			FROM machines WHERE desired_state IN ('CREATED','RUNNING') AND node_id<>''
			UNION ALL
			SELECT node_id, 0, 0, (size_bytes+1048575)/1048576
			FROM volumes WHERE state IN ('READY','UNAVAILABLE','DELETING')
			UNION ALL
			SELECT v.node_id, 0, 0, (a.overlay_size_bytes+1048575)/1048576
			FROM volume_attachments a JOIN volumes v ON v.id=a.volume_id
			WHERE a.status='ATTACHED' AND a.overlay_size_bytes>0
		) allocated GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("allocated by node: %w", err)
	}
	defer rows.Close()
	out := map[string]Allocated{}
	for rows.Next() {
		var nodeID string
		var a Allocated
		if err := rows.Scan(&nodeID, &a.VCPU, &a.MemMib, &a.DiskMib); err != nil {
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
			m.requested_vcpu, m.requested_mem_mib, m.requested_disk_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at, m.expires_at, m.restart_mode,
			m.restart_max_attempts, m.restart_backoff_seconds,
			m.restart_stable_window_seconds, m.restart_attempts,
			m.restart_next_attempt_at, m.restart_stable_since, m.restart_blocked
		FROM machines m WHERE m.node_id=$1 AND m.desired_state IN ('CREATED','RUNNING')`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("machines on node: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

// UpdateMachineNodeAndObserved 创建成功后记录节点与观测（optimistic add）。
// execution CAS 防止旧 create 的迟到成功覆盖已换代的 machine。
func (s *Store) UpdateMachineNodeAndObserved(
	ctx context.Context,
	id, nodeID, executionID, state, slotIP, readiness string,
) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET node_id=$1, observed_state=$2, observed_slot_ip=$3,
			observed_readiness=$4, last_observed_at=now(), updated_at=now()
		WHERE id=$5 AND current_execution_id=$6`,
		nodeID, state, slotIP, readiness, id, executionID)
	if err != nil {
		return fmt.Errorf("update machine node+observed %s: %w", id, err)
	}
	return nil
}

// UpdateMachineObservedWithFenceCAS（R2 评审 P0#3）：以 (current_execution_id,
// generation) 为 CAS 条件写 observed。lifecycle 派发完成后经由本方法落账——
// 在 RPC 与落账之间若机器换代（fence 漂移），更新静默不生效，controller
// 据此不记 SUCCEEDED（绝不把旧代作用的结果量账记到新代头上）。
// 返回 ok=false 说明 fence 已漂移，行未被修改。
func (s *Store) UpdateMachineObservedWithFenceCAS(
	ctx context.Context,
	id, nodeID, executionID string, generation int64,
	state, slotIP, readiness string,
) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE machines SET node_id=$1, observed_state=$2, observed_slot_ip=$3,
			observed_readiness=$4, last_observed_at=now(), updated_at=now()
		WHERE id=$5 AND current_execution_id=$6 AND generation=$7`,
		nodeID, state, slotIP, readiness, id, executionID, generation)
	if err != nil {
		return false, fmt.Errorf("update observed fence-CAS %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkMachineObservedMissing 标记节点失联/agent 缺失（保守 UNKNOWN，摘路由）。
// last_observed_at 是最后一次成功从 agent 获得观测的时间；它绝不能在每轮
// missing 同步时刷新，否则节点丢失重建的超时窗口会被永久重置。
func (s *Store) MarkMachineObservedMissing(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE machines SET observed_state='UNKNOWN', observed_readiness='UNKNOWN',
			updated_at=now()
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
// v1.1（ADR-0017）：PAUSED（standby）是可服务态——readiness 冻结在入睡前值，
// 保留在 route backends，首请求经 proxy autoresume 唤醒（<5s SLO）。
// NOT_READY 入睡的实例同样保留：edge 按 readiness 过滤，不会路由。
func (s *Store) ActiveRouteMachines(ctx context.Context) ([]Machine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.app_id, m.deployment_id, m.replica_ordinal, m.hostname,
			m.desired_state, m.generation, m.current_execution_id,
			m.requested_vcpu, m.requested_mem_mib, m.requested_disk_mib, m.ingress_port, m.image_ref,
			coalesce(m.env::text,'{}'), coalesce(m.placement::text,'{}'),
			coalesce(m.node_id,''),
			coalesce(m.observed_state,''), coalesce(m.observed_slot_ip,''),
			coalesce(m.observed_readiness,''), m.last_observed_at,
			m.created_at, m.updated_at, m.expires_at, m.restart_mode,
			m.restart_max_attempts, m.restart_backoff_seconds,
			m.restart_stable_window_seconds, m.restart_attempts,
			m.restart_next_attempt_at, m.restart_stable_since, m.restart_blocked
		FROM machines m
		WHERE m.desired_state IN ('CREATED','RUNNING')
		  AND m.lifecycle_delete_phase = 'ACTIVE'
		  AND m.observed_state IN ('RUNNING','PAUSED')`)
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
//
// D-2（R2 加固）：同一事务内为每个活跃 hostname 分配单调递增的发布 revision
// （route_publication_revisions 表 insert-on-conflict-increment RETURNING），
// 作为 Redis 投影乱序/重放守卫的高水位来源；leader 换届时新进程从本表继续
// 分配，绝不回退。返回值是 hostname → 本次分配的 revision；每次发布（重建
// 周期）都递增，与 route 内容是否变化无关——revision 只表达"新旧"，不表达
// "是否改了"。已被整体删除的 hostname 不再递增，但表中记录保留：其未来重建
// 必然拿到大于所有历史发布的 revision。
func (s *Store) SyncRoutes(ctx context.Context, active []RouteRow) (map[string]int64, error) {
	revisions := make(map[string]int64)
	err := s.inTx(ctx, func(tx pgx.Tx) error {
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

		// 3) D-2：同一事务分配每个 hostname 的发布 revision（monotone，
		// RETURNING 原子带回）。与 backend 写入同事务保证"权威状态与 revision
		// 同生同灭"：进程崩溃在中间时不存在已分配 revision 却未提交内容（或
		// 反之）的窗口。
		hostnames := make([]string, 0, len(active))
		seen := make(map[string]bool, len(active))
		for _, r := range active {
			if !seen[r.Hostname] {
				seen[r.Hostname] = true
				hostnames = append(hostnames, r.Hostname)
			}
		}
		for _, hostname := range hostnames {
			var rev int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO route_publication_revisions(hostname, revision) VALUES($1, 1)
				ON CONFLICT (hostname) DO UPDATE
				SET revision = route_publication_revisions.revision + 1
				RETURNING revision`, hostname).Scan(&rev); err != nil {
				return fmt.Errorf("allocate publication revision for %s: %w", hostname, err)
			}
			revisions[hostname] = rev
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return revisions, nil
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
	requested_mem_mib, requested_disk_mib, ingress_port, image_ref, coalesce(env::text,'{}'),
	coalesce(placement::text,'null'), node_id, observed_state, observed_slot_ip,
	observed_readiness, last_observed_at, created_at, updated_at, expires_at,
	restart_mode, restart_max_attempts, restart_backoff_seconds,
	restart_stable_window_seconds, restart_attempts, restart_next_attempt_at,
	restart_stable_since, restart_blocked`

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
		&m.RequestedVCPU, &m.RequestedMemMIB, &m.RequestedDiskMIB, &m.IngressPort, &m.ImageRef,
		&envJSON, &placementJSON, &m.NodeID, &m.ObservedState, &m.ObservedSlotIP,
		&m.ObservedReadiness, &m.LastObservedAt, &m.CreatedAt, &m.UpdatedAt,
		&m.ExpiresAt, &m.RestartMode, &m.RestartMaxAttempts, &m.RestartBackoffSeconds,
		&m.RestartStableWindowSeconds, &m.RestartAttempts, &m.RestartNextAttemptAt,
		&m.RestartStableSince, &m.RestartBlocked)
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

// ---------------------------------------------------------------------------
// v1.3-A（ADR-0027）：egress 拒绝摘要与策略变更事实
// ---------------------------------------------------------------------------

// EgressDenySummary 是 egress_deny_summaries 表行（拒绝摘要；明细走 agent
// 日志 sink，Host/SNI 不进本表，避免高基数）。
type EgressDenySummary struct {
	ProjectID          string
	AppID              string
	MachineID          string
	ExecutionID        string
	PolicyGeneration   int64
	AllowedConnections int64
	DeniedConnections  int64
	LimitRejections    int64
	DenyBuckets        json.RawMessage
	UpdatedAt          string
}

// UpsertEgressDenySummary 覆盖式写入 machine/execution/policy_generation 的
// 拒绝摘要（counter 语义，控制面从 agent observed egress_audit 聚合）。
func (s *Store) UpsertEgressDenySummary(ctx context.Context, sum EgressDenySummary) error {
	buckets := sum.DenyBuckets
	if len(buckets) == 0 {
		buckets = json.RawMessage(`[]`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO egress_deny_summaries(project_id, app_id, machine_id, execution_id,
			policy_generation, allowed_connections, denied_connections, limit_rejections,
			deny_buckets, updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,now())
		ON CONFLICT (machine_id, execution_id, policy_generation) DO UPDATE SET
			project_id=EXCLUDED.project_id, app_id=EXCLUDED.app_id,
			allowed_connections=EXCLUDED.allowed_connections,
			denied_connections=EXCLUDED.denied_connections,
			limit_rejections=EXCLUDED.limit_rejections,
			deny_buckets=EXCLUDED.deny_buckets, updated_at=now()`,
		sum.ProjectID, sum.AppID, sum.MachineID, sum.ExecutionID, sum.PolicyGeneration,
		sum.AllowedConnections, sum.DeniedConnections, sum.LimitRejections, string(buckets))
	if err != nil {
		return fmt.Errorf("upsert egress deny summary: %w", err)
	}
	return nil
}

// ListEgressDenySummaries 按 project 返回拒绝摘要（projectID 空 = 全部，
// admin 审计用）。
func (s *Store) ListEgressDenySummaries(ctx context.Context, projectID string) ([]EgressDenySummary, error) {
	q := `SELECT project_id, app_id, machine_id, execution_id, policy_generation,
		allowed_connections, denied_connections, limit_rejections, deny_buckets::text,
		updated_at::text FROM egress_deny_summaries`
	args := []any{}
	if projectID != "" {
		q += ` WHERE project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY updated_at DESC`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list egress deny summaries: %w", err)
	}
	defer rows.Close()
	var out []EgressDenySummary
	for rows.Next() {
		var sum EgressDenySummary
		var buckets string
		if err := rows.Scan(&sum.ProjectID, &sum.AppID, &sum.MachineID, &sum.ExecutionID,
			&sum.PolicyGeneration, &sum.AllowedConnections, &sum.DeniedConnections,
			&sum.LimitRejections, &buckets, &sum.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan egress deny summary: %w", err)
		}
		if buckets != "" && buckets != "null" {
			sum.DenyBuckets = json.RawMessage(buckets)
		}
		out = append(out, sum)
	}
	return out, rows.Err()
}

func recordEgressPolicyChangeTx(ctx context.Context, tx pgx.Tx, projectID string, d Deployment) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO egress_policy_changes(project_id, app_id, deployment_id, generation, policy)
		VALUES($1,$2,$3,$4,$5::jsonb)
		ON CONFLICT (deployment_id) DO NOTHING`,
		projectID, d.AppID, d.ID, d.Generation, string(d.EgressPolicy))
	if err != nil {
		return fmt.Errorf("record egress policy change: %w", err)
	}
	return nil
}

// RecordEgressPolicyChange remains available for repair/backfill callers. Normal
// create/deploy paths record this fact in their deployment transaction.
func (s *Store) RecordEgressPolicyChange(ctx context.Context, projectID, appID, deploymentID string,
	generation int64, policy json.RawMessage,
) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		return recordEgressPolicyChangeTx(ctx, tx, projectID, Deployment{
			ID: deploymentID, AppID: appID, Generation: generation, EgressPolicy: policy,
		})
	})
}

// ListEgressPolicyChanges 返回 app 的策略变更历史（审计）。
func (s *Store) ListEgressPolicyChanges(ctx context.Context, appID string) ([]struct {
	ID           int64
	ProjectID    string
	AppID        string
	DeploymentID string
	Generation   int64
	Policy       json.RawMessage
	CreatedAt    string
}, error,
) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, project_id, app_id, deployment_id, generation, policy::text, created_at::text
		FROM egress_policy_changes WHERE app_id=$1 ORDER BY generation DESC`, appID)
	if err != nil {
		return nil, fmt.Errorf("list egress policy changes: %w", err)
	}
	defer rows.Close()
	var out []struct {
		ID           int64
		ProjectID    string
		AppID        string
		DeploymentID string
		Generation   int64
		Policy       json.RawMessage
		CreatedAt    string
	}
	for rows.Next() {
		var e struct {
			ID           int64
			ProjectID    string
			AppID        string
			DeploymentID string
			Generation   int64
			Policy       json.RawMessage
			CreatedAt    string
		}
		var policy string
		if err := rows.Scan(&e.ID, &e.ProjectID, &e.AppID, &e.DeploymentID, &e.Generation,
			&policy, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan egress policy change: %w", err)
		}
		if policy != "" && policy != "null" {
			e.Policy = json.RawMessage(policy)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
