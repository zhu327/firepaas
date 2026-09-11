// retention_index_test.go：review 2026-09-10 的保留期与索引回归。
//
// 用独立 schema（search_path）隔离：保留期删除是全局的（DELETE WHERE at <
// cutoff），共享 lab 库会误删历史事件，测试必须有自己的表空间。
package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
)

func testIsolatedStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run store tests")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("store_ret_test_%d", os.Getpid())

	boot, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boot.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
		boot.Close()
		t.Fatal(err)
	}
	if _, err := boot.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		boot.Close()
		t.Fatal(err)
	}
	boot.Close()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, cerr := pgxpool.New(context.Background(), dsn)
		if cerr != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	return New(pool)
}

func TestDeleteSchedulerEventsOlderThan(t *testing.T) {
	s := testIsolatedStore(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	fresh := time.Now().UTC()
	for _, row := range []struct {
		at     time.Time
		reason string
	}{
		{old, "old"},
		{fresh, "fresh"},
	} {
		if _, err := s.Pool().Exec(ctx, `
			INSERT INTO scheduler_events(at, project_id, kind, machine_id, reason)
			VALUES($1,'p-ret','placement','m-ret',$2)`, row.at, row.reason); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.DeleteSchedulerEventsOlderThan(ctx, time.Now().Add(-1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged = %d, want 1", n)
	}
	var remaining int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM scheduler_events`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1 (fresh event must survive)", remaining)
	}
}

// TestHotPathIndexesExist：0038 的索引是上线的性能前提，锁进回归。
func TestHotPathIndexesExist(t *testing.T) {
	s := testIsolatedStore(t)
	ctx := context.Background()
	want := []string{
		"user_events_at",
		"machines_node_id",
		"volumes_node_id",
		"snapshots_node_id",
		"apps_project_id",
	}
	for _, name := range want {
		var exists bool
		if err := s.Pool().QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname=$1)`,
			name).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("index %s missing (migration 0038 not applied?)", name)
		}
	}
}
