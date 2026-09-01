// governance.go：v1.2-E（ADR-0035）API 治理。
//
//   - 限流：project × route_class（read/mutation/runtime-stream），
//     Redis 原子令牌桶；read fail-open，mutation/stream fail-closed。
//   - 项目配额：GET/PUT /v1/projects/{id}/quota（revision CAS + ETag）。
//   - 限流配置：GET/PUT /v1/projects/{id}/rate-limits（admin）。
//   - runtime 会话并发：logs/exec/cp 按 project 计数，超限 429。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/ratelimit"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// rootProject 是 root 调用方（无 project 归属）的限流键。
const rootProject = "__root__"

// streamRoutes 是 runtime-stream 类路由（长连接/流式/长轮询）。
var streamRoutes = map[string]bool{
	"/v1/machines/{id}/logs":   true,
	"/v1/machines/{id}/exec":   true,
	"/v1/machines/{id}/files":  true,
	"/v1/machines/{id}/wait":   true,
	"/v1/operations/{id}/wait": true,
	"/v1/rollouts/{id}/wait":   true,
}

// routeClassOf 归类路由：流式端点 → runtime-stream；其余 GET/HEAD →
// read；其余 → mutation。r.Pattern 由 net/http 提供，含方法前缀
// （如 "GET /v1/machines/{id}/wait"）——先归一去掉前缀再比对。
func routeClassOf(pattern, method string) ratelimit.Class {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	if streamRoutes[pattern] {
		return ratelimit.Stream
	}
	if method == http.MethodGet || method == http.MethodHead {
		return ratelimit.Read
	}
	return ratelimit.Mutation
}

// applyRateLimit 在 auth（身份已解析）之后执行。ok=false 时已写响应。
// Redis 故障按风险分级：read fail-open（记 degraded 指标），mutation/
// stream fail-closed（503，明确 rate_limiter_unavailable）。
func (a *API) applyRateLimit(w http.ResponseWriter, r *http.Request, callerProject string) bool {
	if a.limiter == nil {
		return true // 未装配（限流关闭）——仅测试/开发模式
	}
	project := callerProject
	if project == "" {
		project = rootProject
	}
	class := routeClassOf(r.Pattern, r.Method)
	ok, retryAfter, err := a.limiter.Allow(r.Context(), project, class)
	if err != nil {
		if a.metrics != nil {
			a.metrics.Inc("firepaas_api_ratelimit_degraded_total", map[string]string{"class": string(class)}, 1)
		}
		if class == ratelimit.Read {
			return true // fail-open：只读路径不因限流器故障而拒绝
		}
		writeErr(w, 503, "rate_limiter_unavailable")
		return false
	}
	if ok {
		return true
	}
	secs := int(retryAfter/time.Second) + 1
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	if a.metrics != nil {
		a.metrics.Inc("firepaas_api_ratelimited_total", map[string]string{"class": string(class)}, 1)
	}
	// v1.2-F：限流拒绝事件（project 归属；无请求路径等无界字段）。
	a.enqueueUserEvent(project, "", "", "ratelimit.rejected",
		map[string]any{"class": string(class), "retry_after_seconds": secs})
	writeErr(w, 429, "rate limited: retry after "+strconv.Itoa(secs)+"s")
	return false
}

// ---------------------------------------------------------------------------
// 项目配额端点
// ---------------------------------------------------------------------------

func (a *API) getProjectQuota(w http.ResponseWriter, r *http.Request) {
	d, err := a.store.GetProjectQuotaDetail(r.Context(), r.PathValue("id"))
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			writeErr(w, 404, "project not found")
		} else {
			writeErr(w, 500, err.Error())
		}
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"rev-%d"`, d.Revision))
	writeJSON(w, 200, d)
}

func (a *API) putProjectQuota(w http.ResponseWriter, r *http.Request) {
	var body store.ProjectQuotaDetail
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, 400, "invalid quota body: "+err.Error())
		return
	}
	// revision 可经 body 或 If-Match 头；头优先。
	if v := r.Header.Get("If-Match"); v != "" {
		var rev int64
		if _, err := fmt.Sscanf(v, `"rev-%d"`, &rev); err == nil {
			body.Revision = rev
		}
	}
	if body.VCPU <= 0 || body.MemMib <= 0 || body.DiskMib <= 0 ||
		body.MachineConcurrency <= 0 || body.RuntimeSessionConcurrency <= 0 {
		writeErr(w, 400, "quota values must be > 0")
		return
	}
	out, err := a.store.UpdateProjectQuota(r.Context(), r.PathValue("id"), body.Revision, body)
	if err != nil {
		writeErr(w, 409, "quota revision conflict: "+err.Error())
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"rev-%d"`, out.Revision))
	writeJSON(w, 200, out)
}

// ---------------------------------------------------------------------------
// 限流配置端点（admin）
// ---------------------------------------------------------------------------

func (a *API) getRateLimits(w http.ResponseWriter, r *http.Request) {
	c, found, err := a.store.GetRateLimitConfig(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"project_id": r.PathValue("id"), "found": found, "config": c})
}

func (a *API) putRateLimits(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ReadRate      float64 `json:"read_rate"`
		ReadBurst     float64 `json:"read_burst"`
		MutationRate  float64 `json:"mutation_rate"`
		MutationBurst float64 `json:"mutation_burst"`
		StreamRate    float64 `json:"stream_rate"`
		StreamBurst   float64 `json:"stream_burst"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, 400, "invalid rate limit body: "+err.Error())
		return
	}
	for _, v := range []float64{body.ReadRate, body.ReadBurst, body.MutationRate, body.MutationBurst, body.StreamRate, body.StreamBurst} {
		if v < 0 {
			writeErr(w, 400, "rate limit values must be >= 0 (0 = unlimited)")
			return
		}
	}
	cfg := store.RateLimitConfig{
		ReadRate: body.ReadRate, ReadBurst: body.ReadBurst,
		MutationRate: body.MutationRate, MutationBurst: body.MutationBurst,
		StreamRate: body.StreamRate, StreamBurst: body.StreamBurst,
	}
	if err := a.store.UpsertRateLimitConfig(r.Context(), r.PathValue("id"), cfg); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if a.limiter != nil {
		a.limiter.SetConfig(r.PathValue("id"), ratelimit.Config{
			Read:     ratelimit.Limits{Rate: cfg.ReadRate, Burst: cfg.ReadBurst},
			Mutation: ratelimit.Limits{Rate: cfg.MutationRate, Burst: cfg.MutationBurst},
			Stream:   ratelimit.Limits{Rate: cfg.StreamRate, Burst: cfg.StreamBurst},
		})
	}
	writeJSON(w, 200, map[string]string{"status": "updated"})
}

// ---------------------------------------------------------------------------
// runtime 会话并发（runtime_session_concurrency）
// ---------------------------------------------------------------------------

// sessionCounter remains as an assembly marker; the authoritative counter is
// the Redis lease semaphore shared by every API replica.
type sessionCounter struct{}

func newSessionCounter() *sessionCounter { return &sessionCounter{} }

// acquireRuntimeSession 按项目获取 runtime 会话名额（logs/exec/cp）。
// ok=false 时已写 429。release 必须在会话结束时调用（含错误路径）。
func (a *API) acquireRuntimeSession(w http.ResponseWriter, r *http.Request, machineID string) (release func(), streamCtx context.Context, ok bool) {
	if a.sessions == nil || a.store == nil || a.limiter == nil {
		writeErr(w, 503, "runtime_session_limiter_unavailable")
		return nil, nil, false
	}
	project, err := a.machineProject(r.Context(), machineID)
	if err != nil {
		writeErr(w, 500, "resolve machine project: "+err.Error())
		return nil, nil, false
	}
	d, err := a.store.GetProjectQuotaDetail(r.Context(), project)
	if err != nil {
		writeErr(w, 503, "runtime_session_quota_unavailable")
		return nil, nil, false
	}
	limit := d.RuntimeSessionConcurrency
	if limit <= 0 {
		writeErr(w, 503, "runtime_session_quota_unavailable")
		return nil, nil, false
	}
	streamCtx, cancelStream := context.WithCancel(r.Context())
	// 注意：本函数的具名返回值是 release；闭包不得捕获它——return 会把闭包
	// 自身赋给该变量，调用即无限递归（栈溢出）。用独立局部变量捕获。
	rel, active, err := a.limiter.AcquireSession(streamCtx, project, limit, 2*time.Minute, cancelStream)
	if err != nil {
		cancelStream()
		writeErr(w, 503, "runtime_session_limiter_unavailable")
		return nil, nil, false
	}
	if a.metrics != nil {
		a.metrics.Set("firepaas_runtime_sessions_active", map[string]string{"project_id": project}, uint64(active))
	}
	if rel == nil {
		cancelStream()
		if a.metrics != nil {
			a.metrics.Inc("firepaas_runtime_rejections_total", map[string]string{"reason": "session_concurrency"}, 1)
		}
		a.enqueueUserEvent(project, "", machineID, "session.rejected", map[string]any{"limit": limit})
		writeErr(w, 429, fmt.Sprintf("runtime session concurrency limit %d reached for project %s", limit, project))
		return nil, nil, false
	}
	return func() { rel(); cancelStream() }, streamCtx, true
}

// eventWorkers bounds best-effort event goroutines. A saturated queue drops the
// audit event rather than allowing rejected requests to create unbounded work.
var eventWorkers = make(chan struct{}, 32)

func (a *API) enqueueUserEvent(project, app, machine, typ string, details map[string]any) {
	select {
	case eventWorkers <- struct{}{}:
		go func() {
			defer func() { <-eventWorkers }()
			a.recordUserEventBestEffort(project, app, machine, typ, details)
		}()
	default:
	}
}

// recordUserEventBestEffort（v1.2-F）：fire-and-forget 事件写入（限流路径
// 不得因事件失败而阻塞/改变响应）。
func (a *API) recordUserEventBestEffort(project, app, machine, typ string, details map[string]any) {
	if a.store == nil || project == "" || project == rootProject {
		return
	}
	raw, _ := json.Marshal(details)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = a.store.RecordUserEvent(ctx, store.UserEvent{
		ProjectID: project, AppID: app, MachineID: machine, Type: typ, Details: raw,
	})
}

// machineProject 解析 machine → project（app 归属）。
func (a *API) machineProject(ctx context.Context, machineID string) (string, error) {
	m, err := a.store.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		return "", fmt.Errorf("machine %s not found", machineID)
	}
	project, err := a.store.ProjectForApp(ctx, m.AppID)
	if err != nil || project == "" {
		return "", fmt.Errorf("project for machine %s not found", machineID)
	}
	return project, nil
}
