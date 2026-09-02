// apikeys.go：M5.1（mvp-plan §9.1）API key 管理端点（admin scope，routeScope 表已收口）。
// P0（R2 评审）：本组端点是身份签发面——handler 层重新复核调用方为全局
// 身份（root / 无 project 绑定 key），不依赖中间件单点；受限身份 403。
//
//	POST   /v1/apikeys       创建（响应仅此一次携带明文 key）
//	GET    /v1/apikeys       列表（永不回显 key 与 hash）
//	DELETE /v1/apikeys/{id}  撤销（软删，幂等）
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
	Scopes    []string `json:"scopes"`     // 缺省 ["read"]；subset of {read,write,admin}
	ProjectID string   `json:"project_id"` // 空 = 全部项目
	TTLHours  int      `json:"ttl_hours"`  // 0 = 不过期
}

// requireGlobalIdentity 是 apikey 管理端点的第二道防线（P0，R2）：
// 即使中间件被绕过/漏注册，handler 自身也拒绝所有非全局身份。
func (a *API) requireGlobalIdentity(w http.ResponseWriter, r *http.Request) bool {
	if identityIsGlobal(identFrom(r)) {
		return true
	}
	writeErr(w, 403, "api key management requires a global identity (root token or an unscoped admin key)")
	return false
}

func (a *API) createAPIKey(w http.ResponseWriter, r *http.Request) {
	if a.apiKeys == nil {
		writeErr(w, 503, "api keys disabled (no pool)")
		return
	}
	if !a.requireGlobalIdentity(w, r) {
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
	if len(b.Scopes) == 0 {
		b.Scopes = []string{"read"}
	}
	k, plain, err := a.apiKeys.Create(r.Context(), b.Name, b.Scopes, b.ProjectID,
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
	if !a.requireGlobalIdentity(w, r) {
		return
	}
	keys, err := a.apiKeys.List(r.Context())
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
	if !a.requireGlobalIdentity(w, r) {
		return
	}
	if err := a.apiKeys.Revoke(r.Context(), r.PathValue("id")); err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]string{"id": r.PathValue("id"), "status": "revoked"})
}
