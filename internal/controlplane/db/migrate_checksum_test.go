// migrate_checksum_test.go：review 2026-09-10 的迁移器回归。
//
//   - schema_migrations.checksum 在首次升级时按当前文件内容回填；已应用行
//     的内容被改写必须 fail closed（AGENTS.md：migration 视为已发布历史）；
//   - 迁移失败（含 checksum 不匹配）后会话级 advisory lock 必须释放，不能
//     把锁带回连接池阻塞后续迁移。
//
// 使用独立 schema + search_path，避免与并行运行的其它 package 的迁移调用
// 互相污染（schema_migrations 是全局表）。
package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testMigrationPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run migration tests")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("mig_test_%d", os.Getpid())

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
	t.Cleanup(func() {
		pool.Close()
		cleanup, cerr := pgxpool.New(context.Background(), dsn)
		if cerr != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	return pool, schema
}

func migrationChecksum(t *testing.T, version string) string {
	t.Helper()
	raw, err := migrationsFS.ReadFile("migrations/" + version)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestMigrateChecksumBackfillAndFailClosed(t *testing.T) {
	pool, _ := testMigrationPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const version = "0037_v15_fork_machine_uniqueness.sql"
	want := migrationChecksum(t, version)

	// 首次迁移后应已写入 checksum（新行直接插入，历史行回填）。
	var got string
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(checksum,'') FROM schema_migrations WHERE version=$1`, version).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("checksum = %q, want %q", got, want)
	}

	// 模拟升级前的 NULL 行：再次 Migrate 必须回填而不是报错。
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum=NULL WHERE version=$1`, version); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("NULL checksum must be backfilled: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(checksum,'') FROM schema_migrations WHERE version=$1`, version).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("backfilled checksum = %q, want %q", got, want)
	}

	// 已应用 migration 内容被改写：必须 fail closed。
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum='tampered' WHERE version=$1`, version); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("tampered checksum: err = %v, want checksum mismatch", err)
	}
	// 恢复后照常可迁移（同时验证失败路径没有卡死迁移器）。
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, version, want); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate after restore: %v", err)
	}
}

// TestMigrateReleasesAdvisoryLockOnFailure：checksum 失败路径在返回前必须释放
// 会话级 advisory lock。用第二个连接池（独立会话）try_lock 验证。
func TestMigrateReleasesAdvisoryLockOnFailure(t *testing.T) {
	pool, _ := testMigrationPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const version = "0037_v15_fork_machine_uniqueness.sql"
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum='tampered' WHERE version=$1`, version); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err == nil {
		t.Fatal("tampered checksum must fail")
	}

	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	other, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	// 用带超时的阻塞式 pg_advisory_lock 代替轮询 try_lock：其它 package 的
	// 全量迁移可能持锁超过 1s，轮询窗口会产生假阳性；真泄漏会在超时后暴露。
	lockCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := other.Exec(lockCtx, `SELECT pg_advisory_lock(hashtext('firepaas-schema-migrations'))`); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("advisory lock leaked after failed migration (subsequent migrators would block)")
		}
		t.Fatal(err)
	}
	if _, err := other.Exec(ctx,
		`SELECT pg_advisory_unlock(hashtext('firepaas-schema-migrations'))`); err != nil {
		t.Fatal(err)
	}
}
