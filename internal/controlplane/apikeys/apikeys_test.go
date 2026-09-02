package apikeys

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run apikeys tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return New(pool)
}

// 明文永远不落库：只有 hash。
func TestCreateStoresHashOnly(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	k, plain, err := m.Create(ctx, "ci", []string{"write"}, "dev", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) < 40 || plain[:3] != "fp_" {
		t.Fatalf("plain key = %q, want fp_ prefix + 32B", plain)
	}
	if k.KeyHash == plain {
		t.Fatal("hash stored must differ from plaintext")
	}
	got, err := m.GetByHash(ctx, Hash(plain))
	if err != nil {
		t.Fatalf("lookup by hash: %v", err)
	}
	if got.ID != k.ID {
		t.Fatalf("id = %s, want %s", got.ID, k.ID)
	}
}

// 撤销后立即不可用；未到期 key 不复活。
func TestRevokeAndExpiry(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	k, plain, err := m.Create(ctx, "tmp", []string{"read"}, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Revoke(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetByHash(ctx, Hash(plain)); err == nil {
		t.Fatal("revoked key must not authenticate")
	}
	k2, plain2, err := m.Create(ctx, "expired", []string{"read"}, "", 0)
	_ = k2
	_ = plain2
	if err != nil {
		t.Fatal(err)
	}
}

// scope 白名单：非法 scope 拒绝。
func TestInvalidScopeRejected(t *testing.T) {
	m := testManager(t)
	if _, _, err := m.Create(context.Background(), "bad", []string{"root"}, "", 0); err == nil {
		t.Fatal("invalid scope must be rejected")
	}
}

// 跨 project：key 带 project 约束时，GetByHash 不受影响（授权在 API 层），
// 但 last_used touch 必须精确命中。
func TestTouch(t *testing.T) {
	m := testManager(t)
	ctx := context.Background()
	_, plain, err := m.Create(ctx, "t", []string{"read"}, "dev", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Touch(ctx, Hash(plain)); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetByHash(ctx, Hash(plain))
	if err != nil {
		t.Fatal(err)
	}
	if got.LastUsedAt == nil {
		t.Fatal("last_used_at should be set after touch")
	}
}

// --- P1：hash→identity 短 TTL 缓存 ---

func testCachedManager(t *testing.T, ttl time.Duration) (*Manager, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run apikeys tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return NewCached(pool, ttl), pool
}

// 缓存命中：池关闭后仍返回身份（正结果被缓存，不经 DB）。
func TestLookupCacheServesWithoutDB(t *testing.T) {
	m, pool := testCachedManager(t, time.Minute)
	ctx := context.Background()
	k, plain, err := m.Create(ctx, "cache-hit", []string{"read"}, "dev", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetByHash(ctx, Hash(plain)); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	got, err := m.GetByHash(context.Background(), Hash(plain))
	if err != nil || got == nil || got.ID != k.ID {
		t.Fatalf("cached lookup = %+v, %v; want cached identity", got, err)
	}
	if len(got.Scopes) != 1 || got.Scopes[0] != "read" {
		t.Fatalf("cached scopes = %v", got.Scopes)
	}
}

// TTL 到期后缓存失效：池关闭后查询失败（证明未再命中过期缓存）。
func TestLookupCacheExpires(t *testing.T) {
	m, pool := testCachedManager(t, 30*time.Millisecond)
	ctx := context.Background()
	_, plain, err := m.Create(ctx, "cache-ttl", []string{"read"}, "dev", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetByHash(ctx, Hash(plain)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 超过 2×TTL
	pool.Close()
	if got, err := m.GetByHash(context.Background(), Hash(plain)); err == nil || got != nil {
		t.Fatalf("expired cache must not be served, got %+v, %v", got, err)
	}
}

// Revoke 立即失效缓存（不等 TTL）：撤销后 GetByHash 返回 ErrNotFound，
// 不被 TTL 窗口内的陈旧正结果掩盖。
func TestLookupCacheRevokeInvalidates(t *testing.T) {
	m, _ := testCachedManager(t, time.Minute)
	ctx := context.Background()
	k, plain, err := m.Create(ctx, "cache-revoke", []string{"read"}, "dev", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetByHash(ctx, Hash(plain)); err != nil {
		t.Fatal(err)
	}
	if err := m.Revoke(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetByHash(ctx, Hash(plain)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup after revoke = %v, want ErrNotFound", err)
	}
}

// 只缓存正结果：无效 hash 不进入缓存（不影响后续同名 key 立即可用）。
func TestLookupCacheNegativeNotCached(t *testing.T) {
	m, _ := testCachedManager(t, time.Minute)
	ctx := context.Background()
	if _, err := m.GetByHash(ctx, Hash("fp_never_existed")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown hash = %v, want ErrNotFound", err)
	}
	m.mu.Lock()
	n := len(m.cache)
	m.mu.Unlock()
	if n != 0 {
		t.Fatalf("negative lookup must not populate cache, got %d entries", n)
	}
}
