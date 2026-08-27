package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrRolloutBusy 表示 app 已有活跃 rollout（ADR-0015 单 rollout 互斥）。
var ErrRolloutBusy = errors.New("rollout already in progress")

// ErrAppDeleted 表示 app 已被软删（P0-1：墓碑不接受新的对账动作）。
var ErrAppDeleted = errors.New("app deleted")

// Deployment 是 deployments 表行（不可变发布目标）。
type Deployment struct {
	ID         string
	AppID      string
	Generation int64
	ImageRef   string
	VCPU       int64
	MemMIB     int64
	Port       int
	Env        map[string]string
	// SecretRefs（ADR-0010）：{var: {secret, version}}，随 deployment 固化；
	// controller 在 create 时解析为 secret_env 单向下发。
	SecretRefs  map[string]SecretRef
	Placement   json.RawMessage
	HealthCheck json.RawMessage
	Status      string
	CreatedAt   string
	UpdatedAt   string
}

// Rollout 是 rollouts 表行。
type Rollout struct {
	ID             string
	AppID          string
	FromGeneration int64
	ToGeneration   int64
	Status         string // PREPARING|CUTOVER|ROLLING_BACK|COMPLETE
	Failed         bool
	CutoverAt      *string
	DrainDeadline  *string
	StartedAt      string
	CompletedAt    *string
}

// EnsureApp upsert app 行（apps 表，mvp-plan §5.4 最小模型）。
func (s *Store) EnsureApp(ctx context.Context, projectID, appID, hostname, imageRef string,
	vcpu, memMIB int64, port, replicas int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES($1,$2,$3,$4,$5,$6,$7,1)
		ON CONFLICT (id) DO UPDATE SET hostname=EXCLUDED.hostname,
			image_ref=EXCLUDED.image_ref, vcpu=EXCLUDED.vcpu, mem_mib=EXCLUDED.mem_mib,
			desired_replicas=EXCLUDED.desired_replicas, updated_at=now()`,
		appID, projectID, hostname, imageRef, vcpu, memMIB, replicas)
	if err != nil {
		return fmt.Errorf("upsert app: %w", err)
	}
	return nil
}

// App 是 apps 表行。
type App struct {
	ID              string
	ProjectID       string
	Hostname        string
	ImageRef        string
	VCPU            int64
	MemMIB          int64
	DesiredReplicas int
	Generation      int64
	Deleted         bool // deleted_at 非空（P0-1：app 生命周期终态）
	CreatedAt       string
	UpdatedAt       string
}

// getAppColumns 是 apps 的公共列序（Get/List 共用）。
const appColumns = `id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas,
	generation, (deleted_at IS NOT NULL), created_at::text, updated_at::text`

// scanApp 按列序扫描 app 行。
func scanApp(row scanner) (*App, error) {
	var a App
	if err := row.Scan(&a.ID, &a.ProjectID, &a.Hostname, &a.ImageRef, &a.VCPU, &a.MemMIB,
		&a.DesiredReplicas, &a.Generation, &a.Deleted, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

// GetApp 读 app 行（含软删标记）。
func (s *Store) GetApp(ctx context.Context, appID string) (*App, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+appColumns+` FROM apps WHERE id=$1`, appID)
	a, err := scanApp(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get app: %w", err)
	}
	return a, nil
}

// ListApps 返回全部未删除的 app（P0-1：墓碑不参与对账/展示）。
func (s *Store) ListApps(ctx context.Context) ([]App, error) {
	return s.ListAppsFiltered(ctx, "")
}

// ListAppsFiltered 按 project 过滤（M5.1 受限 key 的列表行过滤；空 = 全部）。
func (s *Store) ListAppsFiltered(ctx context.Context, projectID string) ([]App, error) {
	q := `SELECT ` + appColumns + ` FROM apps WHERE deleted_at IS NULL`
	args := []any{}
	if projectID != "" {
		q += ` AND project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY id`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	defer rows.Close()
	var out []App
	for rows.Next() {
		a, err := scanApp(rows)
		if err != nil {
			return nil, fmt.Errorf("scan app: %w", err)
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// SetAppReplicas 更新 desired_replicas（scale N）。已删除的 app 拒绝
// （P0-1：不允许缩放墓碑）。
func (s *Store) SetAppReplicas(ctx context.Context, appID string, replicas int) error {
	tag, err := s.pool.Exec(ctx, `UPDATE apps SET desired_replicas=$2, updated_at=now()
		WHERE id=$1 AND deleted_at IS NULL`, appID, replicas)
	if err != nil {
		return fmt.Errorf("set replicas: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// 区分不存在与已删除，调用方可回 404/409。
		if a, gerr := s.GetApp(ctx, appID); gerr == nil && a != nil && a.Deleted {
			return ErrAppDeleted
		}
		return ErrNotFound
	}
	return nil
}

// BumpAppGeneration 推进 app.generation（deploy 时）。
func (s *Store) BumpAppGeneration(ctx context.Context, appID string, generation int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE apps SET generation=$2, updated_at=now() WHERE id=$1`,
		appID, generation)
	return err
}

// CreateDeployment 插入 deployment（generation 唯一）。
func (s *Store) CreateDeployment(ctx context.Context, d Deployment) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port,
			env, placement, health_check, status, secret_refs)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12::jsonb)`,
		d.ID, d.AppID, d.Generation, d.ImageRef, d.VCPU, d.MemMIB, d.Port,
		jsonMap(d.Env), jsonOrNull(d.Placement), jsonOrNull(d.HealthCheck), d.Status,
		secretRefsJSON(d.SecretRefs))
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}
	return nil
}

// GetDeployment 读 deployment 行。
// deploymentCols 是 deployments 行的公共投影（secret_refs 在最后）。
const deploymentCols = `id, app_id, generation, image_ref, vcpu, mem_mib, port,
		coalesce(env::text,'{}'), coalesce(placement::text,'null'),
		coalesce(health_check::text,'null'), status, created_at::text, updated_at::text,
		coalesce(secret_refs::text,'{}')`

func scanDeployment(row pgx.Row) (*Deployment, error) {
	var d Deployment
	var envRaw, placeRaw, hcRaw, refsRaw string
	if err := row.Scan(&d.ID, &d.AppID, &d.Generation, &d.ImageRef, &d.VCPU, &d.MemMIB,
		&d.Port, &envRaw, &placeRaw, &hcRaw, &d.Status, &d.CreatedAt, &d.UpdatedAt, &refsRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(envRaw), &d.Env)
	d.Placement = json.RawMessage(placeRaw)
	d.HealthCheck = json.RawMessage(hcRaw)
	if refsRaw != "" && refsRaw != "{}" {
		_ = json.Unmarshal([]byte(refsRaw), &d.SecretRefs)
	}
	return &d, nil
}

func (s *Store) GetDeployment(ctx context.Context, depID string) (*Deployment, error) {
	d, err := scanDeployment(s.pool.QueryRow(ctx, `
		SELECT `+deploymentCols+` FROM deployments WHERE id=$1`, depID))
	if err != nil {
		return nil, fmt.Errorf("get deployment: %w", err)
	}
	return d, nil
}

// ListDeployments 返回 app 的 deployment（新代在前）。
func (s *Store) ListDeployments(ctx context.Context, appID string) ([]Deployment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+deploymentCols+` FROM deployments WHERE app_id=$1 ORDER BY generation DESC`, appID)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	defer rows.Close()
	var out []Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, fmt.Errorf("scan deployment: %w", err)
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

// ActiveDeploymentForApp 返回 app 当前 ACTIVE 的 deployment（无则 nil）。
func (s *Store) ActiveDeploymentForApp(ctx context.Context, appID string) (*Deployment, error) {
	deps, err := s.ListDeployments(ctx, appID)
	if err != nil {
		return nil, err
	}
	for i := range deps {
		if deps[i].Status == "ACTIVE" {
			return &deps[i], nil
		}
	}
	return nil, nil
}

// SetDeploymentStatus 更新 deployment 状态。
func (s *Store) SetDeploymentStatus(ctx context.Context, depID, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE deployments SET status=$2, updated_at=now() WHERE id=$1`,
		depID, status)
	return err
}

// CreateRollout 插入 rollout；app 已有活跃 rollout 时返回 ErrRolloutBusy
// （唯一部分索引的 23505 冲突）。
func (s *Store) CreateRollout(ctx context.Context, r Rollout) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO rollouts(id, app_id, from_generation, to_generation, status)
		VALUES($1,$2,$3,$4,'PREPARING')`,
		r.ID, r.AppID, r.FromGeneration, r.ToGeneration)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrRolloutBusy
		}
		return fmt.Errorf("create rollout: %w", err)
	}
	return nil
}

// ActiveRolloutForApp 返回 app 的活跃 rollout（无则 nil）。
func (s *Store) ActiveRolloutForApp(ctx context.Context, appID string) (*Rollout, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, app_id, from_generation, to_generation, status, failed,
			coalesce(cutover_at::text,''), coalesce(drain_deadline::text,''),
			started_at::text, coalesce(completed_at::text,'')
		FROM rollouts WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')`,
		appID)
	var r Rollout
	var cutover, drain string
	err := row.Scan(&r.ID, &r.AppID, &r.FromGeneration, &r.ToGeneration, &r.Status,
		&r.Failed, &cutover, &drain, &r.StartedAt, &r.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("active rollout: %w", err)
	}
	if cutover != "" {
		r.CutoverAt = &cutover
	}
	if drain != "" {
		r.DrainDeadline = &drain
	}
	return &r, nil
}

// ListActiveRollouts 返回所有活跃 rollout（rollout reconcile 输入）。
func (s *Store) ListActiveRollouts(ctx context.Context) ([]Rollout, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, app_id, from_generation, to_generation, status, failed,
			coalesce(cutover_at::text,''), coalesce(drain_deadline::text,''),
			started_at::text, coalesce(completed_at::text,'')
		FROM rollouts WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK') ORDER BY started_at`)
	if err != nil {
		return nil, fmt.Errorf("list active rollouts: %w", err)
	}
	defer rows.Close()
	var out []Rollout
	for rows.Next() {
		var r Rollout
		var cutover, drain string
		if err := rows.Scan(&r.ID, &r.AppID, &r.FromGeneration, &r.ToGeneration, &r.Status,
			&r.Failed, &cutover, &drain, &r.StartedAt, &r.CompletedAt); err != nil {
			return nil, fmt.Errorf("scan rollout: %w", err)
		}
		if cutover != "" {
			r.CutoverAt = &cutover
		}
		if drain != "" {
			r.DrainDeadline = &drain
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RolloutToCutover 事务推进 PREPARING→CUTOVER 并写 drain_deadline。
func (s *Store) RolloutToCutover(ctx context.Context, appID, deadline string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var cur string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM rollouts WHERE app_id=$1
			 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK') FOR UPDATE`,
			appID).Scan(&cur); err != nil {
			return err
		}
		if cur != "PREPARING" {
			return nil // 已被并发推进
		}
		_, err := tx.Exec(ctx, `UPDATE rollouts SET status='CUTOVER',
			cutover_at=now(), drain_deadline=$2::timestamptz, updated_at=now()
			WHERE app_id=$1 AND status='PREPARING'`, appID, deadline)
		return err
	})
}

// RolloutToRollback 推进 →ROLLING_BACK。
func (s *Store) RolloutToRollback(ctx context.Context, appID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var cur string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM rollouts WHERE app_id=$1
			 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK') FOR UPDATE`,
			appID).Scan(&cur); err != nil {
			return err
		}
		if cur != "PREPARING" && cur != "CUTOVER" {
			return nil
		}
		_, err := tx.Exec(ctx, `UPDATE rollouts SET status='ROLLING_BACK', updated_at=now()
			WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER')`, appID)
		return err
	})
}

// CompleteRollout 结束 rollout（COMPLETE + failed 标记 + completed_at）。
func (s *Store) CompleteRollout(ctx context.Context, appID string, failed bool) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var cur string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM rollouts WHERE app_id=$1
			 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK') FOR UPDATE`,
			appID).Scan(&cur); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE rollouts SET status='COMPLETE', failed=$2,
			completed_at=now(), updated_at=now()
			WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')`,
			appID, failed)
		return err
	})
}

// CompleteRolloutWithStatus 事务完成 rollout 并推进 deployment 状态
// （P2-3：CUTOVER→COMPLETE 与 deployment ACTIVE/SUPERSEDED 同事务，中途
// 崩溃不再留下 ACTIVE 指向旧代的不自愈状态）。
func (s *Store) CompleteRolloutWithStatus(ctx context.Context, appID string, failed bool,
	toDepID string, toStatus string, fromDepID string, fromStatus string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var cur string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM rollouts WHERE app_id=$1
			 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK') FOR UPDATE`,
			appID).Scan(&cur); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // 已被并发推进（幂等）
			}
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE rollouts SET status='COMPLETE', failed=$2,
			completed_at=now(), updated_at=now()
			WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')`,
			appID, failed); err != nil {
			return err
		}
		if toDepID != "" && toStatus != "" {
			if _, err := tx.Exec(ctx, `UPDATE deployments SET status=$2, updated_at=now()
				WHERE id=$1`, toDepID, toStatus); err != nil {
				return err
			}
		}
		if fromDepID != "" && fromStatus != "" {
			if _, err := tx.Exec(ctx, `UPDATE deployments SET status=$2, updated_at=now()
				WHERE id=$1`, fromDepID, fromStatus); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeployApp 事务完成一次发布：建 deployment + 建 rollout + 推进 app.generation
// （P2-3：三步同事务；中途失败不留孤儿 rollout 卡住 S3 超时与 409 互斥）。
// 已有活跃 rollout 时返回 ErrRolloutBusy。互斥由「锁定 app 行 + 部分唯一
// 索引」双重保证：同 app 的并发 deploy 会在 app 行锁上串行化，后到者
// 查到先到者的活跃 rollout；并发窗口极端情况下由 rollouts 的唯一部分
// 索引兑底（插入冲突 → ErrRolloutBusy）。
func (s *Store) DeployApp(ctx context.Context, d Deployment, r Rollout, appGeneration int64) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		// 锁 app 行：同 app 的并发 deploy 在此串行化（不能对 count(*) FOR UPDATE）。
		var curGen int64
		if err := tx.QueryRow(ctx, `SELECT generation FROM apps
			WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, r.AppID).Scan(&curGen); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM rollouts
			WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')`, r.AppID).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return ErrRolloutBusy
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port,
				env, placement, health_check, status, secret_refs)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12::jsonb)`,
			d.ID, d.AppID, d.Generation, d.ImageRef, d.VCPU, d.MemMIB, d.Port,
			jsonMap(d.Env), jsonOrNull(d.Placement), jsonOrNull(d.HealthCheck), d.Status,
			secretRefsJSON(d.SecretRefs)); err != nil {
			return fmt.Errorf("create deployment: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO rollouts(id, app_id, from_generation, to_generation, status)
			VALUES($1,$2,$3,$4,'PREPARING')`,
			r.ID, r.AppID, r.FromGeneration, r.ToGeneration); err != nil {
			if isUniqueViolation(err) {
				return ErrRolloutBusy
			}
			return fmt.Errorf("create rollout: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE apps SET generation=$2, updated_at=now()
			WHERE id=$1`, r.AppID, appGeneration); err != nil {
			return fmt.Errorf("bump app generation: %w", err)
		}
		return nil
	})
}

// SoftDeleteApp 事务标记 app 删除（P0-1）：deleted_at 置位 + 终结活跃
// rollout（failed=true）+ ACTIVE deployment 置 SUPERSEDED。副本的机器删除
// 由调用方经 operation outbox 下发（FK 级联会绕过 outbox）。
func (s *Store) SoftDeleteApp(ctx context.Context, appID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE apps SET deleted_at=now(), desired_replicas=0,
			updated_at=now() WHERE id=$1 AND deleted_at IS NULL`, appID)
		if err != nil {
			return fmt.Errorf("soft delete app: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // 幂等：已删除
		}
		if _, err := tx.Exec(ctx, `UPDATE rollouts SET status='COMPLETE', failed=true,
			completed_at=now(), updated_at=now()
			WHERE app_id=$1 AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')`, appID); err != nil {
			return fmt.Errorf("complete active rollouts: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE deployments SET status='SUPERSEDED', updated_at=now()
			WHERE app_id=$1 AND status='ACTIVE'`, appID); err != nil {
			return fmt.Errorf("supersede deployments: %w", err)
		}
		return nil
	})
}

// LatestCreateAttempt 返回 machine 最近一次 create 操作的 claim 次数与状态
// （S2 重试耗尽判定的输入；attempts 跨幂等复活累计）。
func (s *Store) LatestCreateAttempt(ctx context.Context, machineID string) (attempts int, status string, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT attempts, status FROM operations
		WHERE machine_id=$1 AND kind='create'
		ORDER BY created_at DESC LIMIT 1`, machineID).Scan(&attempts, &status)
	if err != nil {
		return 0, "", err
	}
	return attempts, status, nil
}

// ListMachinesForApp 返回 app 的机器（desired != DELETED 默认过滤）。
func (s *Store) ListMachinesForApp(ctx context.Context, appID string, includeDeleted bool) ([]Machine, error) {
	q := `SELECT ` + machineColumns + ` FROM machines WHERE app_id=$1`
	if !includeDeleted {
		q += ` AND desired_state != 'DELETED'`
	}
	q += ` ORDER BY replica_ordinal, generation`
	rows, err := s.pool.Query(ctx, q, appID)
	if err != nil {
		return nil, fmt.Errorf("list machines for app: %w", err)
	}
	defer rows.Close()
	return scanMachines(rows)
}

// CountMachinesForDeployment 统计 deployment 的机器（desired != DELETED）。
func (s *Store) CountMachinesForDeployment(ctx context.Context, depID string) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM machines WHERE deployment_id=$1 AND desired_state != 'DELETED'`,
		depID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count deployment machines: %w", err)
	}
	return n, nil
}

// ErrNotFound 是 store 层的未找到哨兵。
var ErrNotFound = errors.New("not found")

// UserDeleteOpID 是用户语义 delete（kind=delete）的幂等键约定：
// machineID + execution 尾部 8 字符。必须嵌 execution（P0-2）：墓碑行复活
// 会换新 execution，裸 machineID 会在 scale down→up→down 的第二次缩容时
// 撞「同幂等键不同请求体」的 ErrRequestConflict。API 与 controller 共用
// 本函数保证两处不发散。
func UserDeleteOpID(machineID, executionID string) string {
	suffix := ""
	if len(executionID) >= 8 {
		suffix = executionID[len(executionID)-8:]
	} else if executionID != "" {
		suffix = executionID
	}
	if suffix == "" {
		// 无 execution（异常防御）：uuid 防撞键。
		suffix = uuid.NewString()[:8]
	}
	return "op-del-" + machineID + "-" + suffix
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func jsonMap(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	raw, _ := json.Marshal(m)
	return string(raw)
}

func jsonOrNull(b []byte) string {
	if len(b) == 0 {
		return "null"
	}
	return string(b)
}
