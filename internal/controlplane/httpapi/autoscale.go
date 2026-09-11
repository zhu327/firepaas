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
	"encoding/json"
	"errors"
	"net/http"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// autoscaleBody 是策略的全量替换体（PUT 语义；字段缺省 = 零值，
// 由 ValidateAutoscalePolicy 按 §1 约束拒绝，不做归一猜测）。
type autoscaleBody struct {
	Enabled           bool    `json:"enabled"`
	MinReplicas       int     `json:"min_replicas"`
	MaxReplicas       int     `json:"max_replicas"`
	TargetConcurrency int     `json:"target_concurrency"`
	ScaleDownDelaySec int     `json:"scale_down_delay_sec"`
	PanicThreshold    float64 `json:"panic_threshold"`
}

func autoscalePolicyFromBody(b autoscaleBody) store.AutoscalePolicy {
	return store.AutoscalePolicy{
		Enabled:           b.Enabled,
		MinReplicas:       b.MinReplicas,
		MaxReplicas:       b.MaxReplicas,
		TargetConcurrency: b.TargetConcurrency,
		ScaleDownDelaySec: b.ScaleDownDelaySec,
		PanicThreshold:    b.PanicThreshold,
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
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
	a.enqueueUserEvent(app.ProjectID, appID, "", store.UserEventAutoscalePolicy, map[string]any{
		"action": action,
		"prev":   autoscaleBodyFromPolicy(prev),
		"next":   body,
	})
	writeJSON(w, 200, map[string]any{"app_id": appID, "autoscale": body})
}
