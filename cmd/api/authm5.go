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
	"strings"
	"time"

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

// withIdentity 注入身份（auth wrapper 与测试用；请求路径唯一写点是 auth）。
func withIdentity(ctx context.Context, id identity) context.Context {
	return context.WithValue(ctx, identCtxKey{}, id)
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

// routeScope：r.Pattern → 最小 scope。auth wrapper 对未登记路由默认拒绝，
// 因而每个经 auth 包装的端点必须在这里显式登记。
var routeScope = map[string]string{
	"POST /v1/machines":     "write",
	"GET /v1/machines":      "read",
	"GET /v1/machines/{id}": "read",
	// 节点拓扑及无法可靠归属 project 的系统事件属于平台运维信息；
	// 项目级 read key 不可枚举。
	"GET /v1/nodes":                  "admin",
	"GET /v1/events":                 "read",
	"GET /v1/secrets":                "read",
	"GET /v1/secrets/{name}":         "read",
	"GET /v1/apps":                   "read",
	"GET /v1/apps/{id}":              "read",
	"GET /v1/operations":             "read",
	"GET /v1/operations/{id}":        "read",
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
	"POST /v1/system/reprojections":  "admin",
	"POST /v1/nodes/{id}/drain":      "admin",
	"POST /v1/nodes/{id}/ready":      "admin",
	// P1-4（M5 评审）：traffic-token 铸造数据面凭证，按 write 收口
	// （此前不在表内，read key 也能拿 token 直连 workload）。
	"GET /v1/machines/{id}/traffic-token": "write",
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
	"GET /v1/operations/{id}":             true,
}

// effectiveProjectID：project 解析的唯一入口（P1-2/P1-3，M5 评审）。
// 优先级：受限 key 的 identity project > query project_id >
// X-Firepaas-Project 头 > fallback。受限 key 永远解析到自己的 project——
// query/body/header 都无法越权指定。
func effectiveProjectID(r *http.Request, fallback string) string {
	if id := identFrom(r); id.ProjectID != "" {
		return id.ProjectID
	}
	if v := r.URL.Query().Get("project_id"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Firepaas-Project"); v != "" {
		return v
	}
	return fallback
}

// clampBodyProject 校验请求体里的 project_id（P1-2）：受限 key 显式指定
// 他 project → 拒绝；留空 → 归一到 identity project。ok=false 表示 403。
func clampBodyProject(r *http.Request, requested string) (string, bool) {
	id := identFrom(r)
	if id.ProjectID == "" {
		return requested, true // 不受限调用方：显式指定优先
	}
	if requested == "" {
		return id.ProjectID, true
	}
	return requested, requested == id.ProjectID
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
		// P1-3：gate 与 handler 同源——都按 effectiveProjectID（受限 key =
		// 自己的 project）查存在性；不再用同名任取的启发式。
		project := effectiveProjectID(r, "dev")
		if _, err := a.store.GetSecretMeta(ctx, project, r.PathValue("name"), nil); err != nil {
			return "", false
		}
		return project, true
	case strings.Contains(p, "/operations/"):
		op, err := a.store.GetOperation(ctx, effectiveProjectID(r, "dev"), r.PathValue("id"))
		if err != nil || op == nil {
			return "", false
		}
		return op.ProjectID, true
	default:
		return "", false
	}
}

// auth：认证（root token / api key）→ 授权（routeScope）→ 跨 project 防线。
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	if a.authDisabled {
		return func(w http.ResponseWriter, r *http.Request) {
			// P1-1：调用方标识经响应头递给外层审计中间件（context 不可变，
			// 外层读不到内层 WithValue 的结果）。
			w.Header().Set("X-Firepaas-Caller", "root")
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
				// P3-16：last_used_at 节流写（≥1min 一次），避免每请求一行 UPDATE。
				if k.LastUsedAt == nil || time.Since(*k.LastUsedAt) > time.Minute {
					_ = a.apiKeys.Touch(r.Context(), apikeys.Hash(got))
				}
			}
		}
		if id.Kind == "" {
			writeErr(w, 401, "unauthorized")
			return
		}
		// 以下 project gate 必须看到刚认证出的 identity，而不是原始请求。
		r = r.WithContext(withIdentity(r.Context(), id))
		// 默认拒绝：未知（或漏登记）路由不因“已认证”而自动获得权限。
		need, registered := routeScope[r.Pattern]
		if !registered {
			writeErr(w, 403, "route is not authorized")
			return
		}
		if maxRank(id.Scopes) < scopeRank[need] {
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
		// P1-1：审计 caller 经响应头传出（见 auditMiddleware；header 在
		// handler 写 body 前设置，外层中间件在 next 返回后可读）。
		if name := callerName(id); name != "" {
			w.Header().Set("X-Firepaas-Caller", name)
		}
		next(w, r)
	}
}

// callerName：审计里展示的调用方标识（root/key 名），无凭证明文。
func callerName(id identity) string {
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
