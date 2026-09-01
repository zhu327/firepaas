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
