// authm5.go：M5.1（mvp-plan §9.1）认证与授权收口。
//
//   - 认证：Bearer → 常量时间命中原样 FIREPAAS_API_TOKEN（root，全 scope）
//     或 api_keys 表 SHA-256 hash 命中（未撤销/未过期）。
//   - 授权：静态路由表 routeScope[r.Pattern] = 最小 scope（read<write<admin）；
//     表外路由只需已认证。
//   - 跨 project：projectGated 清单内的 by-id 路由，受限 key（project_id≠""）
//     只能访问本项目资源（403）。
package main

import (
	"context"
	"crypto/subtle"
	"net/http"
	"slices"
	"strings"

	"github.com/example/firepaas/internal/controlplane/apikeys"
)

// identity 是请求携带的调用方身份（写入 request context）。
type identity struct {
	Kind      string // "root" | "key" | "anon"
	KeyID     string
	KeyName   string
	ProjectID string // 空 = 不受限
	Scopes    []string
}

type identCtxKey struct{}

// identFrom 读回调用方身份；未认证为 anon。
func identFrom(r *http.Request) identity {
	if v, ok := r.Context().Value(identCtxKey{}).(identity); ok {
		return v
	}
	return identity{Kind: "anon", Scopes: nil}
}

var scopeRank = map[string]int{"read": 1, "write": 2, "admin": 3}

func maxRank(scopes []string) int {
	r := 0
	for _, s := range scopes {
		if v, ok := scopeRank[s]; ok && v > r {
			r = v
		}
	}
	return r
}

// routeScope：r.Pattern → 最小 scope。缺席 = only-authenticated。
// 新端点必须在这里登记（M5.1 起 auth wrapper 统一走授权表）。
var routeScope = map[string]string{
	"POST /v1/machines":              "write",
	"DELETE /v1/machines/{id}":       "write",
	"POST /v1/machines/{id}/pause":   "write",
	"POST /v1/machines/{id}/resume":  "write",
	"POST /v1/secrets":               "write",
	"DELETE /v1/secrets/{name}":      "write",
	"PUT /v1/apps/{id}/secret-refs":  "write",
	"POST /v1/apps":                  "write",
	"POST /v1/apps/{id}/deployments": "write",
	"POST /v1/apps/{id}/scale":       "write",
	"POST /v1/apps/{id}/rollback":    "write",
	"DELETE /v1/apps/{id}":           "write",
	"POST /v1/apikeys":               "admin",
	"GET /v1/apikeys":                "admin",
	"DELETE /v1/apikeys/{id}":        "admin",
	// M5.4/M5.5 预留（本里程碑后续切片注册路由时生效）。
	"POST /v1/system/reprojections": "admin",
	"POST /v1/nodes/{id}/drain":     "admin",
	"POST /v1/nodes/{id}/ready":     "admin",
}

// projectGated：受限 key 的跨 project 防线覆盖所有 by-id 资源路径。
var projectGated = map[string]bool{
	"GET /v1/machines/{id}":               true,
	"DELETE /v1/machines/{id}":            true,
	"POST /v1/machines/{id}/pause":        true,
	"POST /v1/machines/{id}/resume":       true,
	"GET /v1/machines/{id}/traffic-token": true,
	"GET /v1/apps/{id}":                   true,
	"POST /v1/apps/{id}/deployments":      true,
	"POST /v1/apps/{id}/scale":            true,
	"POST /v1/apps/{id}/rollback":         true,
	"DELETE /v1/apps/{id}":                true,
	"PUT /v1/apps/{id}/secret-refs":       true,
	"GET /v1/secrets/{name}":              true,
	"DELETE /v1/secrets/{name}":           true,
}

// effectiveProjectID：受限 key 的 project 优先；否则由 handler 自行解析
// query 参数/默认值。
func effectiveProjectID(r *http.Request, fallback string) string {
	if id := identFrom(r); id.ProjectID != "" {
		return id.ProjectID
	}
	if v := r.URL.Query().Get("project_id"); v != "" {
		return v
	}
	return fallback
}

// resourceProject 求 by-id 资源的归属 project（gate 路径专用）。
func (a *API) resourceProject(ctx context.Context, r *http.Request) (string, bool) {
	p := r.Pattern
	switch {
	case strings.Contains(p, "/machines/"):
		m, err := a.store.GetMachine(ctx, r.PathValue("id"))
		if err != nil || m == nil {
			return "", false
		}
		app, err := a.store.GetApp(ctx, m.AppID)
		if err != nil || app == nil {
			return "", false
		}
		return app.ProjectID, true
	case strings.Contains(p, "/apps/"):
		app, err := a.store.GetApp(ctx, r.PathValue("id"))
		if err != nil || app == nil {
			return "", false
		}
		return app.ProjectID, true
	case strings.Contains(p, "/secrets/"):
		return a.store.AnySecretProject(ctx, r.PathValue("name"))
	default:
		return "", false
	}
}

// auth：认证（root token / api key）→ 授权（routeScope）→ 跨 project 防线。
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	if a.authDisabled {
		return func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(context.WithValue(r.Context(), identCtxKey{},
				identity{Kind: "root", Scopes: []string{"admin"}})))
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if got == "" {
			writeErr(w, 401, "unauthorized")
			return
		}
		var id identity
		if a.apiToken != "" && subtle.ConstantTimeCompare([]byte(got), []byte(a.apiToken)) == 1 {
			id = identity{Kind: "root", Scopes: []string{"admin"}}
		} else if a.apiKeys != nil {
			k, err := a.apiKeys.GetByHash(r.Context(), apikeys.Hash(got))
			if err == nil {
				id = identity{Kind: "key", KeyID: k.ID, KeyName: k.Name,
					ProjectID: k.ProjectID, Scopes: k.Scopes}
				_ = a.apiKeys.Touch(r.Context(), apikeys.Hash(got))
			}
		}
		if id.Kind == "" {
			writeErr(w, 401, "unauthorized")
			return
		}
		// 授权：routeScope 登记的最小 scope。
		if need := routeScope[r.Pattern]; need != "" && maxRank(id.Scopes) < scopeRank[need] {
			writeErr(w, 403, "insufficient scope: require "+need)
			return
		}
		// 跨 project：gate 清单内的路由，受限 key 只放行本项目资源。
		if id.ProjectID != "" && projectGated[r.Pattern] {
			rp, ok := a.resourceProject(r.Context(), r)
			if !ok || rp != id.ProjectID {
				writeErr(w, 403, "cross-project access denied")
				return
			}
		}
		next(w, r.WithContext(context.WithValue(r.Context(), identCtxKey{}, id)))
	}
}

// keyNameForAudit：审计日志里带上调用方（root/key 名），不落凭证明文。
func keyNameForAudit(r *http.Request) string {
	id := identFrom(r)
	switch id.Kind {
	case "root":
		return "root"
	case "key":
		if id.KeyName == "" {
			return id.KeyID
		}
		return id.KeyName
	default:
		return ""
	}
}

// hasScope 供 handler 内精细校验（如 traffic-token 仍要求 write）。
func hasScope(id identity, want string) bool {
	if id.Kind == "root" {
		return true
	}
	return slices.Contains(id.Scopes, want)
}
