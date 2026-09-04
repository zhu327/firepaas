// apikeys.go：M5.1（mvp-plan §9.1）API key 管理端点 + v1.5 自助轮换。
//
// v1.5 行为：
//
//   - 全局身份（root / 无 project 绑定 admin key）：全部操作（任意 project、任意合法 scopes）。
//
//   - 受限 project admin（本项目 admin scope）：仅本项目内自助 create/list/revoke/rotate，
//     且申请 scopes ⊆ 自身 scopes（防提权）；project_id 为空自动归一到自身，跨项目 403。
//
//   - 非 admin 或匿名：中间件 scope 门槛已拦（admin），handler 再复核 admin 能力。
//
//     POST   /v1/apikeys              创建（响应仅此一次携带明文 key；支持 role 展开）
//     GET    /v1/apikeys              列表（全局=全部；受限=本项目；永不回显 key/hash）
//     DELETE /v1/apikeys/{id}         撤销（软删，幂等；受限仅本项目）
//     POST   /v1/apikeys/{id}/rotate  轮换：同名同 scopes 同项目发新 key 并撤销旧 key
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
)

type createAPIKeyBody struct {
	Name      string   `json:"name"`
	Scopes    []string `json:"scopes"`     // 缺省 ["read"]；与 role 二选一
	Role      string   `json:"role"`       // viewer|operator|deployer|maintainer|owner（展开为 scopes）
	ProjectID string   `json:"project_id"` // 空 = 全部项目（仅全局身份）或归一到自身（受限身份）
	TTLHours  int      `json:"ttl_hours"`  // 0 = 不过期
}

// requireKeyAdmin 复核调用方具备 admin 能力（中间件已做 scope 门槛，这里防漏注册）。
func (a *API) requireKeyAdmin(w http.ResponseWriter, r *http.Request) (identity, bool) {
	id := identFrom(r)
	if id.Kind != "root" && !scopeAllows(id.Scopes, "admin") {
		writeErr(w, 403, "api key management requires admin scope")
		return id, false
	}
	if id.Kind != "root" && id.Kind != "key" {
		writeErr(w, 403, "api key management requires a global identity (root token or an unscoped admin key)")
		return id, false
	}
	return id, true
}

// scopesSubset 判定 requested 的能力 ⊆ have 的能力（防提权：自助发 key
// 不能签发调用方没有的能力；admin 覆盖一切，write 覆盖 deploy/exec/read）。
// 用能力语义而非字符串包含，否则 admin 身份反而签发不出 deploy/exec key。
func scopesSubset(requested, have []string) bool {
	for _, s := range requested {
		if !scopeAllows(have, s) {
			return false
		}
	}
	return true
}

// resolveCreateTarget 解析创建目标：返回 (projectID, scopes, ok)。受限身份自动
// 归一 project 并强制 scope 子集；全局身份自由指定。
func resolveCreateTarget(id identity, bodyProject string, bodyScopes []string) (string, []string, bool) {
	if identityIsGlobal(id) {
		return bodyProject, bodyScopes, true
	}
	project := bodyProject
	if project == "" {
		project = id.ProjectID
	}
	if project != id.ProjectID {
		return "", nil, false
	}
	if !scopesSubset(bodyScopes, id.Scopes) {
		return "", nil, false
	}
	return project, bodyScopes, true
}

func (a *API) createAPIKey(w http.ResponseWriter, r *http.Request) {
	if a.apiKeys == nil {
		writeErr(w, 503, "api keys disabled (no pool)")
		return
	}
	id, ok := a.requireKeyAdmin(w, r)
	if !ok {
		return
	}
	var b createAPIKeyBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if b.Name == "" {
		writeErr(w, 400, "name is required")
		return
	}
	scopes := b.Scopes
	if b.Role != "" {
		expanded, ok := apikeys.ExpandRole(b.Role)
		if !ok {
			writeErr(w, 400, "unknown role "+b.Role+" (want viewer|operator|deployer|maintainer|owner)")
			return
		}
		if len(scopes) > 0 {
			writeErr(w, 400, "scopes and role are mutually exclusive")
			return
		}
		scopes = expanded
	}
	if len(scopes) == 0 {
		scopes = []string{"read"}
	}
	project, scopes, ok := resolveCreateTarget(id, b.ProjectID, scopes)
	if !ok {
		writeErr(
			w,
			403,
			"cross-project access denied (scoped keys may only issue keys for their own project with a scope subset)",
		)
		return
	}
	// 受限身份创建前确认目标项目存在（外键错误转 404 而非 500）。
	if project != "" {
		if _, err := a.store.GetProject(r.Context(), project); err != nil {
			writeErr(w, 404, "project not found: "+project)
			return
		}
	}
	k, plain, err := a.apiKeys.Create(r.Context(), b.Name, scopes, project,
		time.Duration(b.TTLHours)*time.Hour)
	if err != nil {
		// 仅入参非法映射 400（安全文案）；DB 错误不再伪装成客户端错误。
		if errors.Is(err, apikeys.ErrInvalidInput) {
			writeErr(w, 400, err.Error())
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]any{
		"id":         k.ID,
		"name":       k.Name,
		"scopes":     k.Scopes,
		"project_id": k.ProjectID,
		"expires_at": k.ExpiresAt,
		// 明文只有这一次；客户端丢失即重新创建。
		"key": plain,
	})
}

func (a *API) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	if a.apiKeys == nil {
		writeErr(w, 503, "api keys disabled")
		return
	}
	id, ok := a.requireKeyAdmin(w, r)
	if !ok {
		return
	}
	// ?project_id 显式过滤：受限身份只能查自己（跨项目 403）；全局身份可过滤任意。
	filter := r.URL.Query().Get("project_id")
	if !identityIsGlobal(id) {
		if filter != "" && filter != id.ProjectID {
			writeErr(w, 403, "cross-project access denied")
			return
		}
		filter = id.ProjectID
	}
	keys, err := a.apiKeys.ListByProject(r.Context(), filter)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		revoked := k.RevokedAt != nil
		out = append(out, map[string]any{
			"id":           k.ID,
			"name":         k.Name,
			"scopes":       k.Scopes,
			"project_id":   k.ProjectID,
			"created_at":   k.CreatedAt,
			"expires_at":   k.ExpiresAt,
			"last_used_at": k.LastUsedAt,
			"revoked_at":   k.RevokedAt,
			"revoked":      revoked,
		})
	}
	writeJSON(w, 200, map[string]any{"keys": out})
}

func (a *API) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	if a.apiKeys == nil {
		writeErr(w, 503, "api keys disabled")
		return
	}
	id, ok := a.requireKeyAdmin(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	if !identityIsGlobal(id) {
		target, err := a.apiKeys.GetByID(r.Context(), keyID)
		if err != nil {
			// 不存在也幂等成功（与全局语义一致），但跨项目不存在不得 oracle：
			// 统一返回 revoked，避免枚举他项目 key id。
			if errors.Is(err, apikeys.ErrNotFound) {
				writeJSON(w, 200, map[string]string{"id": keyID, "status": "revoked"})
				return
			}
			writeInternalErr(w, r, err)
			return
		}
		if target.ProjectID != id.ProjectID {
			writeErr(w, 403, "cross-project access denied")
			return
		}
	}
	if err := a.apiKeys.Revoke(r.Context(), keyID); err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]string{"id": keyID, "status": "revoked"})
}

// rotateAPIKeyBody：轮换可选覆盖 TTL；name/scopes/project 继承旧 key。
type rotateAPIKeyBody struct {
	TTLHours *int `json:"ttl_hours"`
}

// rotateAPIKey 自助轮换：同名同 scopes 同项目发新 key，成功后撤销旧 key。
// 全局身份可轮换任意 key；受限身份仅本项目。旧 key 不存在→404（全局）或
// 统一 revoked（受限防 oracle 见 revoke）。新 key 明文仅此一次返回。
func (a *API) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	if a.apiKeys == nil {
		writeErr(w, 503, "api keys disabled")
		return
	}
	id, ok := a.requireKeyAdmin(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	var body rotateAPIKeyBody
	if raw := r.Body; raw != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
	}
	target, err := a.apiKeys.GetByID(r.Context(), keyID)
	if err != nil {
		if errors.Is(err, apikeys.ErrNotFound) {
			if identityIsGlobal(id) {
				writeErr(w, 404, "api key not found")
			} else {
				writeJSON(w, 200, map[string]string{"id": keyID, "status": "revoked"})
			}
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	if target.RevokedAt != nil {
		writeErr(w, 409, "api key already revoked; create a new key instead")
		return
	}
	if !identityIsGlobal(id) {
		if target.ProjectID != id.ProjectID {
			writeErr(w, 403, "cross-project access denied")
			return
		}
		if !scopesSubset(target.Scopes, id.Scopes) {
			writeErr(w, 403, "cannot rotate a key with scopes beyond your own")
			return
		}
	}
	var ttl time.Duration
	if body.TTLHours != nil {
		if *body.TTLHours < 0 {
			writeErr(w, 400, "ttl_hours must be >= 0")
			return
		}
		ttl = time.Duration(*body.TTLHours) * time.Hour
	} else if target.ExpiresAt != nil {
		if d := time.Until(*target.ExpiresAt); d > 0 {
			ttl = d
		}
	}
	nk, plain, err := a.apiKeys.Create(r.Context(), target.Name, target.Scopes, target.ProjectID, ttl)
	if err != nil {
		if errors.Is(err, apikeys.ErrInvalidInput) {
			writeErr(w, 400, err.Error())
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	if err := a.apiKeys.Revoke(r.Context(), keyID); err != nil {
		// 新 key 已签发，旧 key 撤销失败不得回滚新 key（否则明文已发出但库中无记录
		// 更危险）；返回新 key + 明确告警，调用方重试 revoke。
		writeJSON(w, 201, map[string]any{
			"id": nk.ID, "name": nk.Name, "scopes": nk.Scopes,
			"project_id": nk.ProjectID, "expires_at": nk.ExpiresAt, "key": plain,
			"rotated_from": keyID, "rotate_warning": "new key issued but old key revocation failed; retry DELETE /v1/apikeys/" + keyID,
		})
		return
	}
	writeJSON(w, 201, map[string]any{
		"id": nk.ID, "name": nk.Name, "scopes": nk.Scopes,
		"project_id": nk.ProjectID, "expires_at": nk.ExpiresAt, "key": plain,
		"rotated_from": keyID,
	})
}
