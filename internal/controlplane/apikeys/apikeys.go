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
	"sync"
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
type Manager struct {
	pool *pgxpool.Pool

	// cacheTTL > 0 时启用 hash→identity 短 TTL 缓存（认证热路径的读放大保护，
	// 生产就绪 P1）：每请求一次 PG 查询会在高 QPS 下放大 DB 压力。
	// 安全窗口：本进程 Revoke 立即失效（Revoke 清缓存）；跨实例撤销的生效
	// 时延 ≤ cacheTTL（副本各自缓存），这是记录在案的 TTL 上界。
	mu       sync.Mutex
	cacheTTL time.Duration
	cache    map[string]cacheEntry
}

type cacheEntry struct {
	key       Key
	expiresAt time.Time
}

// New 构造 Manager（不启用认证缓存）。
func New(pool *pgxpool.Pool) *Manager { return &Manager{pool: pool} }

// NewCached 构造启用 hash→identity 短 TTL 缓存的 Manager；ttl <= 0 等价 New。
func NewCached(pool *pgxpool.Pool, ttl time.Duration) *Manager {
	m := &Manager{pool: pool}
	if ttl > 0 {
		m.cacheTTL = ttl
		m.cache = make(map[string]cacheEntry)
	}
	return m
}

// ValidScopes 是合法 scope 全集。write/debug 为历史兼容别名：write 授予
// deploy+exec 等全部非 admin 能力，debug 授予 exec；新 key 推荐按最小权限
// 直接申请 deploy / exec。admin 授予一切。
var ValidScopes = []string{"admin", "write", "read", "debug", "deploy", "exec"}

// ProjectRoles 是 project 级 RBAC 角色到 scope 束的映射（文档与 CLI --role
// 展开用；落库仍是 scopes，不新增成员表）：
//
//   - viewer: 只读
//   - operator: 可 exec（logs/exec/cp）不可 deploy
//   - deployer: 可 deploy 不可 exec
//   - maintainer: deploy+exec（write 等价，不含 admin）
//   - owner: 项目内 admin（含自助发 key、配额读、预热自助）
var ProjectRoles = map[string][]string{
	"viewer":     {"read"},
	"operator":   {"read", "exec"},
	"deployer":   {"read", "deploy"},
	"maintainer": {"read", "deploy", "exec"},
	"owner":      {"admin"},
}

// ExpandRole 将角色名展开为 scopes；ok=false 表示未知角色。
func ExpandRole(role string) ([]string, bool) {
	s, ok := ProjectRoles[role]
	if !ok {
		return nil, false
	}
	return slices.Clone(s), true
}

// Create 生成新 key：'fp_'+64 hex（32B）。返回（记录, 明文）；明文只有这一次。
func (m *Manager) Create(
	ctx context.Context,
	name string,
	scopes []string,
	projectID string,
	ttl time.Duration,
) (*Key, string, error) {
	if name == "" {
		return nil, "", fmt.Errorf("%w: key name is required", ErrInvalidInput)
	}
	if ttl < 0 {
		// P3-19：负 TTL 语义不是“已过期”，是输入错误，拒绝。
		return nil, "", fmt.Errorf("%w: ttl must be >= 0", ErrInvalidInput)
	}
	if len(scopes) == 0 {
		scopes = []string{"read"}
	}
	for _, s := range scopes {
		if !slices.Contains(ValidScopes, s) {
			return nil, "", fmt.Errorf("%w: invalid scope %q", ErrInvalidInput, s)
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
// 启用缓存时优先命中缓存；只缓存正结果（无效 key 仍然穿透到 DB，
// 保证新建 key 立即可用、且不因负缓存放大暴力探测的 DB 成本语义）。
func (m *Manager) GetByHash(ctx context.Context, keyHash string) (*Key, error) {
	if k, ok := m.cached(keyHash); ok {
		return k, nil
	}
	k, err := m.lookupByHash(ctx, keyHash)
	if err != nil {
		return nil, err
	}
	m.remember(keyHash, k)
	return k, nil
}

// cached 读取未过期缓存条目（返回副本，调用方不可跨请求共享可变切片）。
func (m *Manager) cached(keyHash string) (*Key, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil {
		return nil, false
	}
	e, ok := m.cache[keyHash]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	k := e.key
	k.Scopes = slices.Clone(k.Scopes)
	return &k, true
}

// remember 写入正结果缓存。
func (m *Manager) remember(keyHash string, k *Key) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil {
		return
	}
	entry := *k
	entry.Scopes = slices.Clone(k.Scopes)
	m.cache[keyHash] = cacheEntry{key: entry, expiresAt: time.Now().Add(m.cacheTTL)}
}

// invalidate 清空缓存（Revoke 后调用：按 id 撤销无法反查 hash，全清最安全）。
func (m *Manager) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.cache)
}

// lookupByHash 按 key_hash 查有效 key（未撤销、未过期）。
func (m *Manager) lookupByHash(ctx context.Context, keyHash string) (*Key, error) {
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

// ErrInvalidInput：创建入参非法（API 层映射 400；DB 错误不应落入 400）。
var ErrInvalidInput = fmt.Errorf("invalid api key input")

// Touch 更新 last_used_at（错误仅告警，不阻断请求路径）。
func (m *Manager) Touch(ctx context.Context, keyHash string) error {
	_, err := m.pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at=now() WHERE key_hash=$1`, keyHash)
	return err
}

// GetByID 按 id 取 key（含已撤销/过期，用于自助 revoke 的归属校验）。
func (m *Manager) GetByID(ctx context.Context, id string) (*Key, error) {
	var k Key
	var expiresAt, lastUsedAt, revokedAt *time.Time
	err := m.pool.QueryRow(ctx, `
		SELECT id, name, key_hash, scopes, coalesce(project_id,''), created_at, expires_at, last_used_at, revoked_at
		FROM api_keys WHERE id=$1`, id,
	).Scan(&k.ID, &k.Name, &k.KeyHash, &k.Scopes, &k.ProjectID,
		&k.CreatedAt, &expiresAt, &lastUsedAt, &revokedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get api key by id: %w", err)
	}
	k.ExpiresAt, k.LastUsedAt, k.RevokedAt = expiresAt, lastUsedAt, revokedAt
	return &k, nil
}

// ListByProject 列出某项目 key（projectID 空 = 全部，global 调用方用）。
func (m *Manager) ListByProject(ctx context.Context, projectID string) ([]Key, error) {
	q := `SELECT id, name, key_hash, scopes, coalesce(project_id,''), created_at, expires_at, last_used_at, revoked_at
		FROM api_keys`
	args := []any{}
	if projectID != "" {
		q += ` WHERE project_id=$1`
		args = append(args, projectID)
	}
	q += ` ORDER BY created_at DESC`
	rows, err := m.pool.Query(ctx, q, args...)
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
// 先清缓存再写库：写库失败时宁可穿透查询（fail-closed 方向），
// 不放过已撤销的 hash。
func (m *Manager) Revoke(ctx context.Context, id string) error {
	m.invalidate()
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
