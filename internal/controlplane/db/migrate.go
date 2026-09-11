// Package db 提供 PG 连接与最小迁移器（M1）。
package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Options 是业务连接池的显式治理参数（生产就绪 P1：不再裸 pgxpool.New，
// 无连接上限/生命周期管理会把慢查询或连接泄漏放大成全池耗尽）。
// 未设置（零值）的字段回退到 DefaultOptions 的对应默认值。
type Options struct {
	MaxConns          int32         // 默认 16
	MinConns          int32         // 默认 2
	MaxConnLifetime   time.Duration // 默认 30m
	MaxConnIdleTime   time.Duration // 默认 5m
	HealthCheckPeriod time.Duration // 默认 30s
	// StatementTimeout 经 RuntimeParams 下发到每个会话（默认 30s）。
	// 迁移事务内以 SET LOCAL 归零，不受此限制。0 = 不设。
	StatementTimeout time.Duration
}

// DefaultOptions 返回带默认治理值的 Options。
func DefaultOptions() Options {
	return Options{
		MaxConns:          16,
		MinConns:          2,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		StatementTimeout:  30 * time.Second,
	}
}

// Open 按显式治理参数创建 PG 连接池。
func Open(ctx context.Context, dsn string, opts Options) (*pgxpool.Pool, error) {
	def := DefaultOptions()
	if opts.MaxConns <= 0 {
		opts.MaxConns = def.MaxConns
	}
	if opts.MinConns < 0 {
		opts.MinConns = def.MinConns
	}
	if opts.MaxConnLifetime <= 0 {
		opts.MaxConnLifetime = def.MaxConnLifetime
	}
	if opts.MaxConnIdleTime <= 0 {
		opts.MaxConnIdleTime = def.MaxConnIdleTime
	}
	if opts.HealthCheckPeriod <= 0 {
		opts.HealthCheckPeriod = def.HealthCheckPeriod
	}
	if opts.StatementTimeout < 0 {
		opts.StatementTimeout = def.StatementTimeout
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = opts.MinConns
	cfg.MaxConnLifetime = opts.MaxConnLifetime
	cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	cfg.HealthCheckPeriod = opts.HealthCheckPeriod
	if opts.StatementTimeout > 0 {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", opts.StatementTimeout.Milliseconds())
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return pool, nil
}

// Migrate 按文件名顺序执行未应用的迁移，每个迁移一个事务。
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	// Test packages and HA processes may start together. Serialize the complete
	// read/apply/record sequence on one session so two migrators cannot both
	// observe a version as absent and race its DDL/ledger insert.
	// 等待锁本身不受业务池下发的 statement_timeout 限制（两个迁移器并发启动
	// 时后到者可能等待较久）；在事务内 SET LOCAL 归零，会话级锁在提交后仍持有。
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration lock: %w", err)
	}
	// 会话级 advisory lock 不随事务回滚释放，且可能在 `pg_advisory_lock`
	// 已生效、读结果阶段才报错（如 context 取消）。释放必须无条件注册在
	// 锁语句之前；未持锁时 pg_advisory_unlock 是 no-op（返回 false）。
	// review 2026-09-10：旧实现把 defer 注册在 lockTx.Commit 之后，commit
	// 失败时连接带着锁回池，后续所有迁移永久阻塞。
	defer func() {
		_, _ = conn.Exec(
			context.WithoutCancel(ctx),
			`SELECT pg_advisory_unlock(hashtext('firepaas-schema-migrations'))`,
		)
	}()
	if _, err := lockTx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
		_ = lockTx.Rollback(ctx)
		return fmt.Errorf("prepare migration lock: %w", err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT pg_advisory_lock(hashtext('firepaas-schema-migrations'))`); err != nil {
		_ = lockTx.Rollback(ctx)
		return fmt.Errorf("lock migrations: %w", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration lock: %w", err)
	}

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	// checksum（review 2026-09-10）：本次升级前的已应用行 checksum 为 NULL，
	// 首次运行时按当前文件内容回填；之后内容不一致即 fail closed。
	// 已发布 migration 视为不可变历史（AGENTS.md），此处是机器强制。
	if _, err := conn.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text`); err != nil {
		return fmt.Errorf("add schema_migrations.checksum: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	versions := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			versions = append(versions, e.Name())
		}
	}
	sort.Strings(versions)

	for _, version := range versions {
		sqlBytes, err := migrationsFS.ReadFile("migrations/" + version)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", version, err)
		}
		sum := sha256.Sum256(sqlBytes)
		checksum := hex.EncodeToString(sum[:])

		var stored string
		err = conn.QueryRow(ctx,
			`SELECT coalesce(checksum,'') FROM schema_migrations WHERE version=$1`, version).Scan(&stored)
		switch {
		case err == nil:
			if stored != "" && stored != checksum {
				return fmt.Errorf(
					"migration %s checksum mismatch (applied=%s current=%s): 已应用 migration 不可重写",
					version, stored, checksum)
			}
			if stored == "" {
				// 升级前的历史行：首次回填，无法追溯此前是否被改动。
				if _, err := conn.Exec(ctx,
					`UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, version, checksum); err != nil {
					return fmt.Errorf("backfill migration checksum %s: %w", version, err)
				}
			}
			continue
		case errors.Is(err, pgx.ErrNoRows):
			// 未应用，下面执行。
		default:
			return fmt.Errorf("check migration %s: %w", version, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", version, err)
		}
		// 迁移可能跑长 DDL/回填，不受业务池下发的 statement_timeout 限制。
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("disable statement timeout for migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations(version, checksum) VALUES($1,$2)`, version, checksum); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
	}
	return nil
}
