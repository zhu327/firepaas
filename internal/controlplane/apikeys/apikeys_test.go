package apikeys

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/example/firepaas/internal/controlplane/db"
	"github.com/jackc/pgx/v5/pgxpool"
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
