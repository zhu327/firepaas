// secrets.go：M4 secrets v1 持久层（ADR-0010）。
//
// 红线：任何函数的返回值/错误信息/日志都不得携带明文值；明文只在
// Resolve 函数的内存 map 中短暂存在，经 gRPC secret_env 下发后即丢弃。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/example/firepaas/internal/controlplane/secrets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SecretMeta secrets 元数据（无值）。审计只记 ID/name/version。
type SecretMeta struct {
	ID         string    `json:"id"`
	ProjectID  string    `json:"project_id"`
	Name       string    `json:"name"`
	Version    int64     `json:"version"`
	KeyVersion int       `json:"key_version"`
	CreatedAt  time.Time `json:"created_at"`
}

// SealedRow 一行密文（不经解密的透传载体）。
type SealedRow struct {
	Meta       SecretMeta
	Ciphertext []byte
	WrappedDEK []byte
}

// SecretRef 引用：按 project 内 name 解析，version 缺省 = 最新。
type SecretRef struct {
	Secret  string `json:"secret"`
	Version *int64 `json:"version,omitempty"`
}

// ParseRefRef 解析 "NAME" / "NAME@3" 形态的引用字符串。
func ParseSecretRef(s string) (SecretRef, error) {
	name, ver := s, ""
	if i := strings.IndexByte(s, '@'); i >= 0 {
		name, ver = s[:i], s[i+1:]
	}
	ref := SecretRef{Secret: name}
	if ver != "" {
		v, err := strconv.ParseInt(ver, 10, 64)
		if err != nil || v < 1 {
			return ref, fmt.Errorf("bad secret version %q", ver)
		}
		ref.Version = &v
	}
	if name == "" {
		return ref, errors.New("empty secret name")
	}
	return ref, nil
}

const secretCols = `id, project_id, name, version, key_version, created_at`

func scanSecret(row pgx.Row) (*SecretMeta, error) {
	var m SecretMeta
	if err := row.Scan(&m.ID, &m.ProjectID, &m.Name, &m.Version, &m.KeyVersion, &m.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// NextSecretVersion 返回该 secret 的下一个版本号（写入前的取号；AAD 需要
// version 先行）。并发同 name 写入靠 UNIQUE(project,name,version) 冲突失败。
func (s *Store) NextSecretVersion(ctx context.Context, projectID, name string) (int64, error) {
	var maxV int64
	if err := s.pool.QueryRow(ctx, `SELECT coalesce(max(version),0) FROM secrets
		WHERE project_id=$1 AND name=$2`, projectID, name).Scan(&maxV); err != nil {
		return 0, fmt.Errorf("next secret version: %w", err)
	}
	return maxV + 1, nil
}

// PutSecretVersion 以显式版本号插入新版本（配合 NextSecretVersion 取号）。
// 并发同 name 取到相同版本号时，UNIQUE(project,name,version) 冲突返回
// ErrSecretVersionConflict（调用方映射 409，客户端重试即取新号）。
func (s *Store) PutSecretVersion(ctx context.Context, projectID, name string, version int64,
	ciphertext, wrappedDEK []byte, createdBy string) (SecretMeta, error) {

	var m SecretMeta
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		id := "sec-" + uuid.NewString()[:16]
		if _, err := tx.Exec(ctx, `INSERT INTO secrets(id, project_id, name, version,
				value_ciphertext, dek_wrapped, key_version, created_by)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			id, projectID, name, version, ciphertext, wrappedDEK,
			secrets.KeyVersion, createdBy); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrSecretVersionConflict
			}
			return err
		}
		m = SecretMeta{ID: id, ProjectID: projectID, Name: name,
			Version: version, KeyVersion: secrets.KeyVersion, CreatedAt: time.Now()}
		return nil
	})
	return m, err
}

// ListSecrets 返回每个 name 的最新版本元数据。
func (s *Store) ListSecrets(ctx context.Context, projectID string) ([]SecretMeta, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON (name) `+secretCols+`
		FROM secrets WHERE project_id=$1 ORDER BY name, version DESC`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	defer rows.Close()
	var out []SecretMeta
	for rows.Next() {
		var m SecretMeta
		if err := rows.Scan(&m.ID, &m.ProjectID, &m.Name, &m.Version, &m.KeyVersion, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteSecret 删除全部版本，返回删除行数。已注入运行中 VM 的值不受影响
// （observed state 不含秘密，重建需重新可用——调用方应把该事实返回给用户）。
func (s *Store) DeleteSecret(ctx context.Context, projectID, name string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM secrets WHERE project_id=$1 AND name=$2`,
		projectID, name)
	if err != nil {
		return 0, fmt.Errorf("delete secret: %w", err)
	}
	return tag.RowsAffected(), nil
}

var errSecretNotFound = errors.New("secret not found")

// ErrSecretVersionConflict：并发写入同一 secret 时 UNIQUE(project,name,
// version) 冲突。幂等性由客户端重试保证（重取版本号再 Seal）。
var ErrSecretVersionConflict = errors.New("secret version conflict (concurrent write); retry")

// GetSecretMeta 只取元数据（不含任何密文字节）。
func (s *Store) GetSecretMeta(ctx context.Context, projectID, name string, version *int64) (*SecretMeta, error) {
	row, err := s.GetSealedSecret(ctx, projectID, name, version)
	if err != nil {
		return nil, err
	}
	meta := row.Meta
	return &meta, nil
}

// GetSealedSecret 取指定（或最新）版本的密文行。version 缺省 = 最新。
func (s *Store) GetSealedSecret(ctx context.Context, projectID, name string, version *int64) (*SealedRow, error) {
	q := `SELECT ` + secretCols + `, value_ciphertext, dek_wrapped FROM secrets
		WHERE project_id=$1 AND name=$2`
	args := []any{projectID, name}
	if version != nil {
		q += ` AND version=$3`
		args = append(args, *version)
	} else {
		q += ` ORDER BY version DESC LIMIT 1`
	}
	row := s.pool.QueryRow(ctx, q, args...)
	var r SealedRow
	var ct, wrapped []byte
	if err := row.Scan(&r.Meta.ID, &r.Meta.ProjectID, &r.Meta.Name, &r.Meta.Version,
		&r.Meta.KeyVersion, &r.Meta.CreatedAt, &ct, &wrapped); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errSecretNotFound
		}
		return nil, fmt.Errorf("get secret: %w", err)
	}
	r.Ciphertext, r.WrappedDEK = ct, wrapped
	return &r, nil
}

// ResolveSecretValue 取出并解密单个版本（latest = *nil）。
func ResolveSecretValue(ctx context.Context, s *Store, cm *secrets.Manager,
	projectID, name string, version *int64) (string, *SecretMeta, error) {

	row, err := s.GetSealedSecret(ctx, projectID, name, version)
	if err != nil {
		return "", nil, err
	}
	pt, err := cm.Open(secrets.SealedValue{
		Ciphertext: row.Ciphertext, WrappedDEK: row.WrappedDEK,
	}, projectID, row.Meta.Name, row.Meta.Version)
	if err != nil {
		return "", nil, err
	}
	return string(pt), &row.Meta, nil
}

// ResolveDeploymentSecretRefs 把 deployment.secret_refs 解析为明文 env。
// 任一引用缺失即整体失败（create 应终态化，不做部分注入）。
func ResolveDeploymentSecretRefs(ctx context.Context, s *Store, cm *secrets.Manager,
	projectID, deploymentID string) (map[string]string, error) {

	refs, err := DeploymentSecretRefs(ctx, s, deploymentID)
	if err != nil || len(refs) == 0 {
		return nil, err
	}
	out := make(map[string]string, len(refs))
	for varName, ref := range refs {
		pt, _, err := ResolveSecretValue(ctx, s, cm, projectID, ref.Secret, ref.Version)
		if err != nil {
			if errors.Is(err, errSecretNotFound) {
				return nil, fmt.Errorf("secret %q (ref %q): %w", ref.Secret, varName, errSecretNotFound)
			}
			return nil, err
		}
		out[varName] = pt
	}
	return out, nil
}

// DeploymentSecretRefs 读 deployment 的 secret_refs 绑定。
func DeploymentSecretRefs(ctx context.Context, s *Store, deploymentID string) (map[string]SecretRef, error) {
	var raw string
	err := s.pool.QueryRow(ctx, `SELECT coalesce(secret_refs::text,'{}')
		FROM deployments WHERE id=$1`, deploymentID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("deployment secret refs: %w", err)
	}
	if raw == "" || raw == "{}" {
		return nil, nil
	}
	out := map[string]SecretRef{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("parse secret_refs: %w", err)
	}
	return out, nil
}

// secretRefsJSON 序列化引用 map（nil → '{}'）。
func secretRefsJSON(refs map[string]SecretRef) string {
	if len(refs) == 0 {
		return "{}"
	}
	b, err := json.Marshal(refs)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// AnySecretProject 返回该 name 的 secret 归属 project（M5.1 跨 project 防线）。
// name 在多个 project 下同名时取任意一个：防线只用于拒绝"明显不属于本
// project"的访问，放行路径由 handler 按实际 project 精确查询兜底。
func (s *Store) AnySecretProject(ctx context.Context, name string) (string, bool) {
	var p string
	err := s.pool.QueryRow(ctx,
		`SELECT project_id FROM secrets WHERE name=$1 ORDER BY version DESC LIMIT 1`, name).Scan(&p)
	if err != nil {
		return "", false
	}
	return p, true
}
