package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

// v1.2-E（ADR-0035）：配额 CAS 更新与限流配置。
func TestProjectQuotaRevisionCAS(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-quota-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "quota-test"); err != nil {
		t.Fatal(err)
	}
	d, err := s.GetProjectQuotaDetail(ctx, project)
	if err != nil || d.Revision != 1 {
		t.Fatalf("initial quota detail: %+v err=%v", d, err)
	}
	// 正确 revision 更新成功并 +1。
	d.VCPU = 8
	out, err := s.UpdateProjectQuota(ctx, project, d.Revision, *d)
	if err != nil {
		t.Fatal(err)
	}
	if out.Revision != 2 || out.VCPU != 8 {
		t.Fatalf("updated quota: %+v", out)
	}
	// 旧 revision 再写 → 409 冲突。
	if _, err := s.UpdateProjectQuota(ctx, project, d.Revision, *d); !errors.Is(err, ErrQuotaRevisionConflict) {
		t.Fatalf("stale revision must conflict, got %v", err)
	}
}

// v1.5（最小可用项目面）：项目 CRUD——创建/重复 409/列表/详情/非空保护/删除。
func TestProjectCRUD(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-crud-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	p, err := s.CreateProject(ctx, project, "crud-test")
	if err != nil || p.ID != project || p.Name != "crud-test" {
		t.Fatalf("create project: %+v err=%v", p, err)
	}
	// 重复创建 → ErrProjectExists（调用方映射 409）。
	if _, err := s.CreateProject(ctx, project, "dup"); !errors.Is(err, ErrProjectExists) {
		t.Fatalf("duplicate create must fail with ErrProjectExists, got %v", err)
	}
	// 详情与列表可见。
	got, err := s.GetProject(ctx, project)
	if err != nil || got.ID != project {
		t.Fatalf("get project: %+v err=%v", got, err)
	}
	all, err := s.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, q := range all {
		if q.ID == project {
			found = true
		}
	}
	if !found {
		t.Fatal("list must contain the created project")
	}
	// 非空保护：建一个 app 后删除必须拒绝。
	err = s.CreateAppAndDeployment(
		ctx,
		project,
		App{
			ID:              project + "-app",
			Hostname:        project + ".local",
			ImageRef:        "img:v1",
			VCPU:            1,
			MemMIB:          512,
			DesiredReplicas: 1,
		},
		Deployment{
			ID:         project + "-dep",
			AppID:      project + "-app",
			Generation: 1,
			ImageRef:   "img:v1",
			VCPU:       1,
			MemMIB:     512,
			Port:       80,
			Status:     "ACTIVE",
		},
	)
	if err != nil {
		t.Fatalf("seed app: %v", err)
	}
	if err := s.DeleteProjectIfEmpty(ctx, project); !errors.Is(err, ErrProjectNotEmpty) {
		t.Fatalf("non-empty delete must fail with ErrProjectNotEmpty, got %v", err)
	}
	// 不存在 → ErrNotFound。
	if err := s.DeleteProjectIfEmpty(ctx, project+"-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing delete must fail with ErrNotFound, got %v", err)
	}
}

func TestRateLimitConfigDefaultAndUpsert(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-rl-%d", os.Getpid())
	// 无行 → 默认值。
	cfg, found, err := s.GetRateLimitConfig(ctx, project)
	if err != nil || found {
		t.Fatalf("missing row must yield defaults: %+v found=%v err=%v", cfg, found, err)
	}
	if cfg != DefaultRateLimitConfig() {
		t.Fatalf("defaults mismatch: %+v", cfg)
	}
	// 写入后读回。
	cfg.StreamRate = 2
	cfg.StreamBurst = 4
	if err := s.UpsertRateLimitConfig(ctx, project, cfg); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetRateLimitConfig(ctx, project)
	if err != nil || !found || got.StreamRate != 2 || got.StreamBurst != 4 {
		t.Fatalf("upserted config: %+v found=%v err=%v", got, found, err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM project_rate_limits WHERE project_id=$1`, project)
	})
}
