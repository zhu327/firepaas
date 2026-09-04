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
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
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

var scopeRank = map[string]int{"read": 1, "debug": 2, "write": 2, "admin": 3}

func maxRank(scopes []string) int {
	r := 0
	for _, s := range scopes {
		if v, ok := scopeRank[s]; ok && v > r {
			r = v
		}
	}
	return r
}

// scopeGrants：scope → 授予的能力集合（v1.5 RBAC 拆分：deploy 与 exec 互斥可分）。
//
//   - read：只读
//   - exec / debug（历史别名）：logs/exec/cp（read + exec）
//   - deploy：app 创建/部署/扩缩/回滚/删除（read + deploy）
//   - write（历史兼容）：全部非 admin mutation（read + exec + deploy + write）
//   - admin：一切（含项目配额写、节点运维、自助 key 的越权边界）
//
// 路由表 routeScope 的 need 取值：read / exec / deploy / write / admin /
// debug（debug 仅历史路由残留，新路由用 exec）。
var scopeGrants = map[string]map[string]bool{
	"read":   {"read": true},
	"exec":   {"read": true, "exec": true, "debug": true},
	"debug":  {"read": true, "exec": true, "debug": true},
	"deploy": {"read": true, "deploy": true},
	"write": {
		"read": true, "exec": true, "debug": true,
		"deploy": true, "write": true,
	},
	"admin": {
		"read": true, "exec": true, "debug": true,
		"deploy": true, "write": true, "admin": true,
	},
}

// scopeAllows 判定 scopes 是否授予 need 能力。未知 scope 永不授予。
func scopeAllows(scopes []string, need string) bool {
	for _, s := range scopes {
		if g, ok := scopeGrants[s]; ok && g[need] {
			return true
		}
	}
	return false
}

// routeScope：r.Pattern → 最小 scope。auth wrapper 对未登记路由默认拒绝，
// 因而每个经 auth 包装的端点必须在这里显式登记。
var routeScope = map[string]string{
	"POST /v1/machines": "write",
	"GET /v1/machines":  "read",
	// v1.2-E（ADR-0035）：配额读 = read（项目自身可见）；写与限流配置 = admin。
	"GET /v1/projects/{id}/quota":       "read",
	"PUT /v1/projects/{id}/quota":       "admin",
	"GET /v1/projects/{id}/rate-limits": "admin",
	"PUT /v1/projects/{id}/rate-limits": "admin",
	"GET /v1/machines/{id}":             "read",
	// 节点拓扑及无法可靠归属 project 的系统事件属于平台运维信息；
	// 项目级 read key 不可枚举。
	"GET /v1/nodes":                   "admin",
	"GET /v1/events":                  "read",
	"GET /v1/system/scheduler-events": "admin",
	"GET /v1/secrets":                 "read",
	"GET /v1/secrets/{name}":          "read",
	"GET /v1/apps":                    "read",
	"GET /v1/apps/{id}":               "read",
	"GET /v1/operations":              "read",
	"GET /v1/operations/{id}":         "read",
	"DELETE /v1/machines/{id}":        "write",
	"POST /v1/machines/{id}/pause":    "write",
	"POST /v1/machines/{id}/resume":   "write",
	"POST /v1/secrets":                "write",
	"DELETE /v1/secrets/{name}":       "write",
	"PUT /v1/apps/{id}/secret-refs":   "write",
	"POST /v1/apps":                   "deploy",
	"POST /v1/apps/{id}/deployments":  "deploy",
	"POST /v1/apps/{id}/scale":        "deploy",
	"POST /v1/apps/{id}/rollback":     "deploy",
	"DELETE /v1/apps/{id}":            "deploy",
	"POST /v1/apikeys":                "admin",
	"GET /v1/apikeys":                 "admin",
	"DELETE /v1/apikeys/{id}":         "admin",
	"POST /v1/apikeys/{id}/rotate":    "admin",
	"POST /v1/system/reprojections":   "admin",
	"POST /v1/nodes/{id}/drain":       "admin",
	"POST /v1/nodes/{id}/ready":       "admin",
	// P1-4（M5 评审）：traffic-token 铸造数据面凭证，按 write 收口
	// （此前不在表内，read key 也能拿 token 直连 workload）。
	"GET /v1/machines/{id}/traffic-token": "write",
	// v1.2-C（ADR-0025）：实时 logs/exec/cp 使用独立 debug scope（等价 write）；
	// read key 只能读历史元数据。
	"GET /v1/machines/{id}/logs":  "exec",
	"POST /v1/machines/{id}/exec": "exec",
	"PUT /v1/machines/{id}/files": "exec",
	"GET /v1/machines/{id}/files": "exec",
	"GET /v1/capabilities":        "read",
	// v1.2-D：wait/TTL/restart 治理（TTL/restart 变更按 write 收口）。
	"GET /v1/machines/{id}/wait":           "read",
	"GET /v1/operations/{id}/wait":         "read",
	"GET /v1/rollouts/{id}/wait":           "read",
	"PUT /v1/machines/{id}/ttl":            "write",
	"POST /v1/machines/{id}/restart-reset": "admin",
	// v1.3：egress audit、snapshot/checkpoint/fork/rescue 与 LOCAL_RW volume。
	"GET /v1/apps/{id}/egress-audit":                         "read",
	"POST /v1/machines/{id}/snapshots":                       "write",
	"GET /v1/snapshots":                                      "read",
	"GET /v1/snapshots/{id}":                                 "read",
	"DELETE /v1/snapshots/{id}":                              "write",
	"POST /v1/machines/{id}/snapshot-schedules":              "write",
	"GET /v1/machines/{id}/snapshot-schedules":               "read",
	"DELETE /v1/machines/{id}/snapshot-schedules/{schedule}": "write",
	"POST /v1/snapshots/{id}/fork":                           "write",
	"POST /v1/snapshots/{id}/preflight":                      "read",
	"POST /v1/machines/{id}/rescue":                          "write",
	"POST /v1/volumes":                                       "write",
	"GET /v1/volumes":                                        "read",
	"GET /v1/volumes/{id}":                                   "read",
	"DELETE /v1/volumes/{id}":                                "write",
	"POST /v1/machines/{id}/volume-attach":                   "write",
	"POST /v1/machines/{id}/volume-detach":                   "write",
	// v1.5（租户自助预热）：prewarm/pin 写 = write（项目配额内自助，handler 层
	// 对受限身份禁 node_ids、只许 node_pool）；coverage 读 = read（受限身份只见
	// 脱敏汇总，不暴露 node 拓扑）。全局 admin 保留全量视图。
	"POST /v1/images/prewarm":     "write",
	"GET /v1/images/coverage":     "read",
	"POST /v1/images/pins":        "write",
	"GET /v1/images/pins":         "read",
	"DELETE /v1/images/pins/{id}": "write",
	// v1.5（项目 CRUD，最小可用）：创建/删除 = admin 且全局身份；读 = read
	//（受限身份仅见本项目，handler 层过滤）。
	"POST /v1/projects":        "admin",
	"GET /v1/projects":         "read",
	"GET /v1/projects/{id}":    "read",
	"DELETE /v1/projects/{id}": "admin",
}

// v1.5 自助 key 说明（中间件不再一刀切拦截受限身份）：受限 project admin
// 可在 handler 约束下管理本项目 key（目标 project == 自身 + 申请 scopes 的
// 能力 ⊆ 自身）；全局 key 管理仍需全局身份（handler 内 identityIsGlobal
// 判定）。中间件只做 admin scope 门槛，越权边界由 handler 强制。

// identityIsGlobal 判定身份是否为全局身份（root token 或无 project 绑定的 key）。
func identityIsGlobal(id identity) bool {
	return id.Kind == "root" || (id.Kind == "key" && id.ProjectID == "")
}

// projectGated：受限 key 的跨 project 防线覆盖所有 by-id 资源路径。
var projectGated = map[string]bool{
	"GET /v1/projects/{id}":                                  true,
	"DELETE /v1/projects/{id}":                               true,
	"GET /v1/projects/{id}/quota":                            true,
	"PUT /v1/projects/{id}/quota":                            true,
	"GET /v1/projects/{id}/rate-limits":                      true,
	"PUT /v1/projects/{id}/rate-limits":                      true,
	"GET /v1/machines/{id}":                                  true,
	"GET /v1/machines/{id}/logs":                             true,
	"POST /v1/machines/{id}/exec":                            true,
	"PUT /v1/machines/{id}/files":                            true,
	"GET /v1/machines/{id}/files":                            true,
	"GET /v1/machines/{id}/wait":                             true,
	"PUT /v1/machines/{id}/ttl":                              true,
	"POST /v1/machines/{id}/restart-reset":                   true,
	"GET /v1/operations/{id}":                                true,
	"GET /v1/operations/{id}/wait":                           true,
	"GET /v1/rollouts/{id}/wait":                             true,
	"DELETE /v1/machines/{id}":                               true,
	"POST /v1/machines/{id}/pause":                           true,
	"POST /v1/machines/{id}/resume":                          true,
	"GET /v1/machines/{id}/traffic-token":                    true,
	"GET /v1/apps/{id}":                                      true,
	"POST /v1/apps/{id}/deployments":                         true,
	"POST /v1/apps/{id}/scale":                               true,
	"POST /v1/apps/{id}/rollback":                            true,
	"DELETE /v1/apps/{id}":                                   true,
	"PUT /v1/apps/{id}/secret-refs":                          true,
	"GET /v1/secrets/{name}":                                 true,
	"DELETE /v1/secrets/{name}":                              true,
	"GET /v1/apps/{id}/egress-audit":                         true,
	"POST /v1/machines/{id}/snapshots":                       true,
	"GET /v1/snapshots/{id}":                                 true,
	"DELETE /v1/snapshots/{id}":                              true,
	"POST /v1/snapshots/{id}/fork":                           true,
	"POST /v1/snapshots/{id}/preflight":                      true,
	"POST /v1/machines/{id}/snapshot-schedules":              true,
	"GET /v1/machines/{id}/snapshot-schedules":               true,
	"DELETE /v1/machines/{id}/snapshot-schedules/{schedule}": true,
	"POST /v1/machines/{id}/rescue":                          true,
	"GET /v1/volumes/{id}":                                   true,
	"DELETE /v1/volumes/{id}":                                true,
	"POST /v1/machines/{id}/volume-attach":                   true,
	"POST /v1/machines/{id}/volume-detach":                   true,
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
	machineProject := func() (string, bool) {
		m, err := a.store.GetMachine(ctx, r.PathValue("id"))
		if err != nil || m == nil {
			return "", false
		}
		app, err := a.store.GetApp(ctx, m.AppID)
		if err != nil || app == nil {
			return "", false
		}
		return app.ProjectID, true
	}
	snapshotProject := func(id string) (string, bool) {
		snap, err := a.store.GetSnapshot(ctx, id)
		if err != nil || snap == nil {
			return "", false
		}
		return snap.ProjectID, true
	}
	volumeProject := func(id string) (string, bool) {
		volume, err := a.store.GetVolume(ctx, id)
		if err != nil || volume == nil {
			return "", false
		}
		return volume.ProjectID, true
	}
	sameProject := func(left string, leftOK bool, right string, rightOK bool) (string, bool) {
		return left, leftOK && rightOK && left == right
	}

	switch {
	case strings.Contains(p, "/projects/"):
		// Project-scoped governance resources use the project path parameter as
		// their ownership key. The handler still decides whether it exists.
		return r.PathValue("id"), r.PathValue("id") != ""
	case p == "DELETE /v1/machines/{id}/snapshot-schedules/{schedule}":
		mp, mok := machineProject()
		schedule, err := a.store.GetSnapshotSchedule(ctx, r.PathValue("schedule"))
		sp, sok := "", err == nil && schedule != nil && schedule.MachineID == r.PathValue("id")
		if sok {
			sp = schedule.ProjectID
		}
		return sameProject(mp, mok, sp, sok)
	case p == "POST /v1/machines/{id}/rescue":
		// The documented snapshot_id lives in the JSON body, which middleware
		// must not consume. Gate the path-owned machine here; the handler resolves
		// the body snapshot and enforces same-project ownership before mutation.
		return machineProject()
	case p == "POST /v1/machines/{id}/volume-attach" || p == "POST /v1/machines/{id}/volume-detach":
		mp, mok := machineProject()
		vp, vok := volumeProject(r.URL.Query().Get("volume_id"))
		return sameProject(mp, mok, vp, vok)
	case strings.Contains(p, "/snapshots/"):
		return snapshotProject(r.PathValue("id"))
	case strings.Contains(p, "/volumes/"):
		return volumeProject(r.PathValue("id"))
	case strings.Contains(p, "/machines/"):
		return machineProject()
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
	case strings.Contains(p, "/rollouts/"):
		// v1.2-D wait：rollout → app → project（gate 与 handler 同源）。
		rl, err := a.store.GetRollout(ctx, r.PathValue("id"))
		if err != nil || rl == nil {
			return "", false
		}
		app, err := a.store.GetApp(ctx, rl.AppID)
		if err != nil || app == nil {
			return "", false
		}
		return app.ProjectID, true
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

func (a *API) allowsProjectResource(ctx context.Context, r *http.Request, projectID string) bool {
	rp, ok := a.resourceProject(ctx, r)
	return ok && rp == projectID
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
		} else {
			// R2 评审 P1（401 限流）：暴力探测的桶空时直接 429，只做 hash+
			// 桶判定，不触碰 PG（无效 key 后端不可用时不再被用来放大 DB 查询）。
			ip := clientIP(r)
			if a.authThrottle != nil && !a.authThrottle.hasCapacity(ip) {
				if a.metrics != nil {
					a.metrics.Inc("firepaas_auth_failure_throttled_total", map[string]string{}, 1)
				}
				writeErr(w, 429, "too many failed authentication attempts; retry later")
				return
			}
			if a.apiKeys != nil {
				hash := apikeys.Hash(got)
				k, err := a.apiKeys.GetByHash(r.Context(), hash)
				switch {
				case err == nil:
					id = identity{
						Kind: "key", KeyID: k.ID, KeyName: k.Name,
						ProjectID: k.ProjectID, Scopes: k.Scopes,
					}
					// P3-16：last_used_at 节流写（≥1min 一次），避免每请求一行 UPDATE。
					if k.LastUsedAt == nil || time.Since(*k.LastUsedAt) > time.Minute {
						_ = a.apiKeys.Touch(r.Context(), hash)
					}
				case errors.Is(err, apikeys.ErrNotFound):
					// 无效/已撤销/已过期 key：确认失败后消耗一枚试错令牌。
					if a.authThrottle != nil {
						a.authThrottle.record(ip)
					}
				default:
					// P1：PG 依赖失败不能降级成 401（错误信号 + 引发客户端无谓轮换）；
					// 返回 503（可重试）+ 指标/日志，缓存命中不受 PG 抖动影响。
					slog.Error("api key lookup failed", "route", r.Pattern, "error", err)
					if a.metrics != nil {
						a.metrics.Inc("firepaas_apikey_lookup_errors_total", map[string]string{}, 1)
					}
					writeErr(w, 503, "authentication backend temporarily unavailable")
					return
				}
			} else if a.authThrottle != nil {
				// 未装配 apikey 后端：任何非 root token 都必是失败尝试，计入桶。
				a.authThrottle.record(ip)
			}
		}
		if id.Kind == "" {
			writeErr(w, 401, "unauthorized")
			return
		}
		// 以下 project gate 必须看到刚认证出的 identity，而不是原始请求。
		r = r.WithContext(withIdentity(r.Context(), id))
		// v1.2-E（ADR-0035）：project × route_class 限流（auth 后、gate 前）。
		if !a.applyRateLimit(w, r, id.ProjectID) {
			return
		}
		// 默认拒绝：未知（或漏登记）路由不因“已认证”而自动获得权限。
		need, registered := routeScope[r.Pattern]
		if !registered {
			writeErr(w, 403, "route is not authorized")
			return
		}
		if _, ok := scopeGrants[need]; !ok {
			writeErr(w, 403, "route is not authorized")
			return
		}
		if !scopeAllows(id.Scopes, need) {
			writeErr(w, 403, "insufficient scope: require "+need)
			return
		}
		// v1.5：apikey 管理不再在中间件一刀切拦截受限身份；越权边界
		// 由 handler 强制（全局操作需全局身份，项目自助需本项目 + 能力子集）。
		// 跨 project：gate 清单内的路由，受限 key 只放行本项目资源。
		if id.ProjectID != "" && projectGated[r.Pattern] &&
			!a.allowsProjectResource(r.Context(), r, id.ProjectID) {
			writeErr(w, 403, "cross-project access denied")
			return
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
