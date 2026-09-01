// Package apikeys：M5.1（mvp-plan §9.1）API key 模型。
//
//   - 库中只存 SHA-256(key)（hex）；明文只在 CreateAPIKey 返回时出现一次。
//   - scope 三档：read < write < admin；project_id NULL = 全部项目。
//   - 常量时间比较由调用方（API auth）完成——这里不接触明文。
package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Key 是一条 API key 的落库形态。
type Key struct {
	ID         string
	Name       string
	KeyHash    string
	Scopes     []string
	ProjectID  string // 空 = 全部项目
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
}

// Manager 负责 api_keys 表的读写。
type Manager struct{ pool *pgxpool.Pool }

// New 构造 Manager。
func New(pool *pgxpool.Pool) *Manager { return &Manager{pool: pool} }

// ValidScopes 是合法 scope 全集。Beyond 三档的形状 v1 不接（计划风险表）。
var ValidScopes = []string{"admin", "write", "read"}

// Create 生成新 key：'fp_'+64 hex（32B）。返回（记录, 明文）；明文只有这一次。
func (m *Manager) Create(
	ctx context.Context,
	name string,
	scopes []string,
	projectID string,
	ttl time.Duration,
) (*Key, string, error) {
	if name == "" {
		return nil, "", fmt.Errorf("key name is required")
	}
	if ttl < 0 {
		// P3-19：负 TTL 语义不是“已过期”，是输入错误，拒绝。
		return nil, "", fmt.Errorf("ttl must be >= 0")
	}
	if len(scopes) == 0 {
		scopes = []string{"read"}
	}
	for _, s := range scopes {
		if !slices.Contains(ValidScopes, s) {
			return nil, "", fmt.Errorf("invalid scope %q", s)
		}
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, "", fmt.Errorf("key material: %w", err)
	}
	plain := "fp_" + hex.EncodeToString(raw)
	h := sha256.Sum256([]byte(plain))
	keyHash := hex.EncodeToString(h[:])

	var k Key
	var expiresAt, lastUsedAt, revokedAt *time.Time
	err := m.pool.QueryRow(ctx, `
		INSERT INTO api_keys(id, name, key_hash, scopes, project_id, expires_at)
		VALUES($1,$2,$3,$4::text[],NULLIF($5,''),
		       CASE WHEN $6::bigint > 0 THEN now() + ($6::bigint || ' seconds')::interval END)
		RETURNING id, name, key_hash, scopes, coalesce(project_id,''), created_at, expires_at, last_used_at, revoked_at`,
		NewID(), name, keyHash, scopes, projectID, int64(ttl/time.Second),
	).Scan(&k.ID, &k.Name, &k.KeyHash, &k.Scopes, &k.ProjectID,
		&k.CreatedAt, &expiresAt, &lastUsedAt, &revokedAt)
	if err != nil {
		return nil, "", fmt.Errorf("create api key: %w", err)
	}
	k.ExpiresAt, k.LastUsedAt, k.RevokedAt = expiresAt, lastUsedAt, revokedAt
	return &k, plain, nil
}

// GetByHash 按 key_hash 查有效 key（未撤销、未过期）。
func (m *Manager) GetByHash(ctx context.Context, keyHash string) (*Key, error) {
	var k Key
	var expiresAt, lastUsedAt, revokedAt *time.Time
	err := m.pool.QueryRow(ctx, `
		SELECT id, name, key_hash, scopes, coalesce(project_id,''), created_at, expires_at, last_used_at, revoked_at
		FROM api_keys
		WHERE key_hash=$1 AND revoked_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())`,
		keyHash,
	).Scan(&k.ID, &k.Name, &k.KeyHash, &k.Scopes, &k.ProjectID,
		&k.CreatedAt, &expiresAt, &lastUsedAt, &revokedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get api key: %w", err)
	}
	k.ExpiresAt, k.LastUsedAt, k.RevokedAt = expiresAt, lastUsedAt, revokedAt
	return &k, nil
}

// ErrNotFound：hash 不存在/已撤销/已过期。
var ErrNotFound = fmt.Errorf("api key not found or revoked")

// Touch 更新 last_used_at（错误仅告警，不阻断请求路径）。
func (m *Manager) Touch(ctx context.Context, keyHash string) error {
	_, err := m.pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at=now() WHERE key_hash=$1`, keyHash)
	return err
}

// List 列出全部 key（admin 端点用）。
func (m *Manager) List(ctx context.Context) ([]Key, error) {
	rows, err := m.pool.Query(ctx, `
		SELECT id, name, key_hash, scopes, coalesce(project_id,''), created_at, expires_at, last_used_at, revoked_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		var expiresAt, lastUsedAt, revokedAt *time.Time
		if err := rows.Scan(&k.ID, &k.Name, &k.KeyHash, &k.Scopes, &k.ProjectID,
			&k.CreatedAt, &expiresAt, &lastUsedAt, &revokedAt); err != nil {
			return nil, err
		}
		k.ExpiresAt, k.LastUsedAt, k.RevokedAt = expiresAt, lastUsedAt, revokedAt
		out = append(out, k)
	}
	return out, rows.Err()
}

// Revoke 撤销（软删）。不存在也幂等成功。
func (m *Manager) Revoke(ctx context.Context, id string) error {
	if _, err := m.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at=now() WHERE id=$1 AND revoked_at IS NULL`, id); err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	return nil
}

// Hash 计算 key 的 SHA-256 十六进制（认证路径调用）。
func Hash(plain string) string {
	h := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(h[:])
}

// NewID 生成 key id。
func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "apik_" + hex.EncodeToString(b)
}
