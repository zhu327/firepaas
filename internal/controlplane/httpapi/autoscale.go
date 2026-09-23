// autoscale.go：ADR-0041 并发自动弹性——策略读写与手动接管。
//
// 端点：
//
//	PUT /v1/apps/{id}/autoscale  全量替换策略（deploy scope；不改 desired，下一拍收敛）
//	GET /v1/apps/{id}/autoscale  读策略（read scope）
//
// 手动 POST /scale 在策略启用中隐含接管：单条 UPDATE 写 desired 并关
// enabled（store.TakeoverScale），+ autoscale.takeover 事件。
package httpapi

import (
	"errors"
	"net/http"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// autoscaleBody 是策略的全量替换体（PUT 语义；字段缺省 = 零值，
// 由 ValidateAutoscalePolicy 按 §1 约束拒绝，不做归一猜测）。
// Wave4 新维度缺省 = 关闭（0/空 = off，不参与 max-of-wants）：PUT 忘带
// 字段会关闭该维度，行为与既有关闭语义一致，见 API 文档。
type autoscaleBody struct {
	Enabled           bool    `json:"enabled"`
	MinReplicas       int     `json:"min_replicas"`
	MaxReplicas       int     `json:"max_replicas"`
	TargetConcurrency int     `json:"target_concurrency"`
	ScaleDownDelaySec int     `json:"scale_down_delay_sec"`
	PanicThreshold    float64 `json:"panic_threshold"`
	TargetRPS         int     `json:"target_rps"`
	TargetCPURatio    float64 `json:"target_cpu_ratio"`
	CustomPromQuery   string  `json:"custom_prom_query"`
	CustomTarget      float64 `json:"custom_target"`
	CustomMode        string  `json:"custom_mode"`
}

func autoscalePolicyFromBody(b autoscaleBody) store.AutoscalePolicy {
	return store.AutoscalePolicy{
		Enabled:           b.Enabled,
		MinReplicas:       b.MinReplicas,
		MaxReplicas:       b.MaxReplicas,
		TargetConcurrency: b.TargetConcurrency,
		ScaleDownDelaySec: b.ScaleDownDelaySec,
		PanicThreshold:    b.PanicThreshold,
		TargetRPS:         b.TargetRPS,
		TargetCPURatio:    b.TargetCPURatio,
		CustomPromQuery:   b.CustomPromQuery,
		CustomTarget:      b.CustomTarget,
		CustomMode:        b.CustomMode,
	}
}

func autoscaleBodyFromPolicy(p store.AutoscalePolicy) autoscaleBody {
	return autoscaleBody{
		Enabled:           p.Enabled,
		MinReplicas:       p.MinReplicas,
		MaxReplicas:       p.MaxReplicas,
		TargetConcurrency: p.TargetConcurrency,
		ScaleDownDelaySec: p.ScaleDownDelaySec,
		PanicThreshold:    p.PanicThreshold,
		TargetRPS:         p.TargetRPS,
		TargetCPURatio:    p.TargetCPURatio,
		CustomPromQuery:   p.CustomPromQuery,
		CustomTarget:      p.CustomTarget,
		CustomMode:        p.CustomMode,
	}
}

func (a *API) getAutoscale(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if app == nil || app.Deleted {
		writeErr(w, 404, "app not found")
		return
	}
	writeJSON(w, 200, map[string]any{
		"app_id":           appID,
		"autoscale":        autoscaleBodyFromPolicy(app.Autoscale()),
		"desired_replicas": app.DesiredReplicas,
	})
}

func (a *API) putAutoscale(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	var body autoscaleBody
	if err := decodeJSONBody(w, r, &body, 1<<20, false); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	// custom_prom_query 是任意 PromQL（可读中心 Prometheus 全量指标，跨租户
	// 可见）：设置/保持需要 admin scope；deploy scope 只允许关闭（清空）。
	if (body.CustomPromQuery != "" || body.CustomTarget != 0) &&
		!scopeAllows(identFrom(r).Scopes, "admin") {
		writeErr(w, 403, "custom_prom_query requires admin scope (arbitrary PromQL can read cross-tenant metrics)")
		return
	}
	policy := autoscalePolicyFromBody(body)
	if err := store.ValidateAutoscalePolicy(policy); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if app == nil || app.Deleted {
		writeErr(w, 404, "app not found")
		return
	}
	prev := app.Autoscale()
	// 入库前归一（"" 与 per_replica 同语义统一写法，库内单写法）。
	policy = store.NormalizeAutoscalePolicy(policy)
	if err := a.store.SetAutoscalePolicy(r.Context(), appID, policy); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrAppDeleted) {
			writeErr(w, 404, "app not found")
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	// 策略变更审计（enable/disable/参数变更各一条，type=autoscale.policy）。
	action := "update"
	if policy.Enabled != prev.Enabled {
		if policy.Enabled {
			action = "enable"
		} else {
			action = "disable"
		}
	}
	// 回显归一后策略（custom_mode:"" → per_replica），与 GET 一致；审计
	// 事件保留原始请求体。
	normalized := autoscaleBodyFromPolicy(policy)
	a.enqueueUserEvent(app.ProjectID, appID, "", store.UserEventAutoscalePolicy, map[string]any{
		"action": action,
		"prev":   autoscaleBodyFromPolicy(prev),
		"next":   body,
	})
	writeJSON(w, 200, map[string]any{"app_id": appID, "autoscale": normalized})
}
