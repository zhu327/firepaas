// projects.go：v1.2-E（ADR-0035）项目配额与限流配置的持久层。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrQuotaRevisionConflict：CAS 更新配额时 revision 不匹配（并发管理操作）。
var ErrQuotaRevisionConflict = errors.New("project quota revision conflict")

// ErrProjectExists：创建已存在的 project（调用方应复用或换名）。
var ErrProjectExists = errors.New("project already exists")

// ErrProjectNotEmpty：删除仍有业务资源（apps/volumes/snapshots/pins）的 project。
var ErrProjectNotEmpty = errors.New("project not empty")

// Project 是项目 CRUD 视图（配额/限流走既有 governance 端点）。
type Project struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateProject 创建项目（非幂等：已存在返回 ErrProjectExists；配额列走 DB 默认）。
func (s *Store) CreateProject(ctx context.Context, id, name string) (*Project, error) {
	var p Project
	err := s.pool.QueryRow(ctx, `
		INSERT INTO projects(id, name) VALUES($1,$2)
		RETURNING id, name, created_at`, id, name,
	).Scan(&p.ID, &p.Name, &p.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrProjectExists
		}
		return nil, fmt.Errorf("create project %s: %w", id, err)
	}
	return &p, nil
}

// GetProject 取单个项目；不存在返回 ErrNotFound。
func (s *Store) GetProject(ctx context.Context, id string) (*Project, error) {
	var p Project
	err := s.pool.QueryRow(ctx, `SELECT id, name, created_at FROM projects WHERE id=$1`, id).
		Scan(&p.ID, &p.Name, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get project %s: %w", id, err)
	}
	return &p, nil
}

// ListProjects 列出全部项目（按 id 排序；租户侧在 API 层过滤）。
func (s *Store) ListProjects(ctx context.Context) ([]Project, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, created_at FROM projects ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	var out []Project
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.ID, &p.Name, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeleteProjectIfEmpty 仅当项目无业务资源时删除，否则 ErrProjectNotEmpty。
// 业务资源：apps、volumes、未过期 pins、snapshots（含已删快照的审计行？以
// snapshots 表为准）、api_keys（key 随项目级联撤销，删除前需先 revoke/随删）。
func (s *Store) DeleteProjectIfEmpty(ctx context.Context, id string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var n int
		checks := []string{
			`SELECT count(*) FROM apps WHERE project_id=$1`,
			`SELECT count(*) FROM volumes WHERE project_id=$1 AND state<>'DELETED'`,
			`SELECT count(*) FROM snapshots WHERE project_id=$1`,
			`SELECT count(*) FROM image_pins WHERE project_id=$1 AND expires_at>now()`,
		}
		for _, q := range checks {
			if err := tx.QueryRow(ctx, q, id).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				return ErrProjectNotEmpty
			}
		}
		tag, err := tx.Exec(ctx, `DELETE FROM projects WHERE id=$1`, id)
		if err != nil {
			return fmt.Errorf("delete project %s: %w", id, err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ProjectQuotaDetail 是项目的完整配额视图（含乐观锁 revision）。
type ProjectQuotaDetail struct {
	VCPU                      int64 `json:"vcpu_quota"`
	MemMib                    int64 `json:"mem_mib_quota"`
	DiskMib                   int64 `json:"disk_mib_quota"`
	MachineConcurrency        int64 `json:"machine_concurrency"`
	RuntimeSessionConcurrency int64 `json:"runtime_session_concurrency"`
	Revision                  int64 `json:"revision"`
}

// GetProjectQuotaDetail 返回项目配额 + revision（供 ETag/If-Match）。
func (s *Store) GetProjectQuotaDetail(ctx context.Context, projectID string) (*ProjectQuotaDetail, error) {
	var d ProjectQuotaDetail
	err := s.pool.QueryRow(ctx, `
		SELECT vcpu_quota, mem_mib_quota, disk_mib_quota,
			machine_concurrency, runtime_session_concurrency, quota_revision
		FROM projects WHERE id=$1`, projectID).
		Scan(&d.VCPU, &d.MemMib, &d.DiskMib, &d.MachineConcurrency,
			&d.RuntimeSessionConcurrency, &d.Revision)
	if err != nil {
		// 确证 not-found 用哨兵返回，API 层据此区分 404 与 5xx。
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("project quota detail %s: %w", projectID, err)
	}
	return &d, nil
}

// UpdateProjectQuota CAS 更新配额：revision 匹配才生效并 +1；不匹配返回
// ErrQuotaRevisionConflict。配额降低不驱逐已有 machine/会话（ADR-0035 §2）。
func (s *Store) UpdateProjectQuota(ctx context.Context, projectID string, rev int64,
	d ProjectQuotaDetail,
) (*ProjectQuotaDetail, error) {
	var out ProjectQuotaDetail
	err := s.pool.QueryRow(ctx, `
		UPDATE projects SET
			vcpu_quota=$2, mem_mib_quota=$3, disk_mib_quota=$4,
			machine_concurrency=$5, runtime_session_concurrency=$6,
			quota_revision=quota_revision+1
		WHERE id=$1 AND quota_revision=$7
		RETURNING vcpu_quota, mem_mib_quota, disk_mib_quota,
			machine_concurrency, runtime_session_concurrency, quota_revision`,
		projectID, d.VCPU, d.MemMib, d.DiskMib, d.MachineConcurrency,
		d.RuntimeSessionConcurrency, rev).
		Scan(&out.VCPU, &out.MemMib, &out.DiskMib, &out.MachineConcurrency,
			&out.RuntimeSessionConcurrency, &out.Revision)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrQuotaRevisionConflict
		}
		return nil, fmt.Errorf("update project quota %s: %w", projectID, err)
	}
	return &out, nil
}

// ProjectMachineUsage 返回项目活跃 machine 数（allocated + 在途 create，
// 与 ProjectUsage 同一形状）。machine_concurrency 的检查输入。
func (s *Store) ProjectMachineUsage(ctx context.Context, projectID string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM machines m
				JOIN apps a ON a.id=m.app_id
				WHERE a.project_id=$1 AND m.desired_state IN ('CREATED','RUNNING') AND m.node_id<>'')
			+ (SELECT count(DISTINCT m.id) FROM operations o
				JOIN machines m ON m.id=o.machine_id
				WHERE o.project_id=$1 AND o.kind='create' AND o.status IN ('PENDING','CLAIMED')
					AND m.node_id=''
					AND NOT EXISTS (SELECT 1 FROM machines live
						JOIN apps la ON la.id=live.app_id
						WHERE live.id=m.id AND la.project_id=$1 AND live.desired_state IN ('CREATED','RUNNING')
							AND live.node_id<>''))`, projectID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("project machine usage %s: %w", projectID, err)
	}
	return n, nil
}

// RateLimitConfig 是一个 project 的三类令牌桶参数。
type RateLimitConfig struct {
	ReadRate      float64   `json:"read_rate"`
	ReadBurst     float64   `json:"read_burst"`
	MutationRate  float64   `json:"mutation_rate"`
	MutationBurst float64   `json:"mutation_burst"`
	StreamRate    float64   `json:"stream_rate"`
	StreamBurst   float64   `json:"stream_burst"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// DefaultRateLimitConfig 与 migration 0019 的 DEFAULT 对齐。
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		ReadRate: 100, ReadBurst: 200,
		MutationRate: 20, MutationBurst: 40,
		StreamRate: 5, StreamBurst: 10,
	}
}

// GetRateLimitConfig 返回 project 限流配置；无行 = 默认值（found=false）。
func (s *Store) GetRateLimitConfig(ctx context.Context, projectID string) (RateLimitConfig, bool, error) {
	var c RateLimitConfig
	err := s.pool.QueryRow(ctx, `
		SELECT read_rate, read_burst, mutation_rate, mutation_burst,
			stream_rate, stream_burst, updated_at
		FROM project_rate_limits WHERE project_id=$1`, projectID).
		Scan(&c.ReadRate, &c.ReadBurst, &c.MutationRate, &c.MutationBurst,
			&c.StreamRate, &c.StreamBurst, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DefaultRateLimitConfig(), false, nil
		}
		return c, false, fmt.Errorf("rate limit config %s: %w", projectID, err)
	}
	return c, true, nil
}

// UpsertRateLimitConfig 写入（admin API 入口；全字段更新）。
func (s *Store) UpsertRateLimitConfig(ctx context.Context, projectID string, c RateLimitConfig) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO project_rate_limits
			(project_id, read_rate, read_burst, mutation_rate, mutation_burst, stream_rate, stream_burst, updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,now())
		ON CONFLICT (project_id) DO UPDATE SET
			read_rate=EXCLUDED.read_rate, read_burst=EXCLUDED.read_burst,
			mutation_rate=EXCLUDED.mutation_rate, mutation_burst=EXCLUDED.mutation_burst,
			stream_rate=EXCLUDED.stream_rate, stream_burst=EXCLUDED.stream_burst,
			updated_at=now()`,
		projectID, c.ReadRate, c.ReadBurst, c.MutationRate, c.MutationBurst, c.StreamRate, c.StreamBurst)
	if err != nil {
		return fmt.Errorf("upsert rate limit config %s: %w", projectID, err)
	}
	return nil
}
