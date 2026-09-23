package httpapi

import (
	"net/http"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// rollout_approve.go：Wave3 Stage B——人工审批放行（promotion 卡点）。
//
//	POST /v1/rollouts/{id}/approve  放行 PAUSED_FOR_APPROVAL → CUTOVER（deploy scope）
//
// 语义：只有 PAUSED 态可放行（其它状态 409：已放行/并发推进/无等待项）；
// 放行即写 cutover/drain deadline，后续走既有 CUTOVER 路径（观察窗评估、
// 到期回收旧代）。审批人记 approved_by（key 名或 root），审计不断档。

// approveDrainGrace 返回放行写入的 drain 期限：API 装配期与 controller 共用
// 同一 env 源（FIREPAAS_ROLLOUT_DRAIN，缺省 30s；CUTOVER 分支以行内 deadline
// 为准），单源注入保证不漂移。
func (a *API) approveDrainGrace() time.Duration {
	if a != nil && a.drainGrace > 0 {
		return a.drainGrace
	}
	return 30 * time.Second
}

func (a *API) approveRollout(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rl, err := a.store.GetRollout(r.Context(), id)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if rl == nil {
		writeErr(w, 404, "rollout not found")
		return
	}
	if rl.Status != "PAUSED_FOR_APPROVAL" {
		writeErr(w, 409, "rollout is not awaiting approval (status "+rl.Status+")")
		return
	}
	by := callerName(identFrom(r))
	if by == "" {
		by = "unknown"
	}
	approved, err := a.store.ApproveRollout(r.Context(), rl.AppID, by,
		time.Now().Add(a.approveDrainGrace()))
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if !approved {
		writeErr(w, 409, "rollout is not awaiting approval (concurrent transition)")
		return
	}
	app, err := a.store.GetApp(r.Context(), rl.AppID)
	if err == nil && app != nil {
		a.enqueueUserEvent(app.ProjectID, app.ID, "", store.UserEventRolloutUpdated, map[string]any{
			"status": "approved", "from_generation": rl.FromGeneration,
			"to_generation": rl.ToGeneration, "by": by,
		})
	}
	writeJSON(w, 202, map[string]any{
		"rollout_id": id, "app_id": rl.AppID, "status": "CUTOVER",
	})
}
