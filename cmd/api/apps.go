// apps.go：M3 app/deployment API（mvp-plan §7.4）。
//
// 端点：
//
//	POST   /v1/apps                     创建 app（+ 初始 deployment ACTIVE）
//	GET    /v1/apps                     列表
//	GET    /v1/apps/{id}                详情（deployments/machines/rollout）
//	POST   /v1/apps/{id}/deployments    发布新 generation（ADR-0015 互斥）
//	POST   /v1/apps/{id}/scale          scale N
//	POST   /v1/apps/{id}/rollback       手动回滚
//	DELETE /v1/apps/{id}                删除全部副本（MVP：app 行保留为墓碑）
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/example/firepaas/internal/controlplane/store"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/example/firepaas/shared/pkg/id"
	"google.golang.org/protobuf/encoding/protojson"
)

type createAppBody struct {
	AppID        string            `json:"app_id"`
	ProjectID    string            `json:"project_id"`
	Hostname     string            `json:"hostname"`
	Image        string            `json:"image"`
	VCPU         int64             `json:"vcpu"`
	MemMIB       int64             `json:"mem_mib"`
	Port         int               `json:"port"`
	Replicas     int               `json:"replicas"`
	Env          map[string]string `json:"env"`
	NodePool     string            `json:"node_pool"`
	Labels       map[string]string `json:"labels"`
	AntiAffinity string            `json:"anti_affinity"`
	HealthCheck  *healthCheckBody  `json:"health_check"`
}

func (a *API) createApp(w http.ResponseWriter, r *http.Request) {
	var body createAppBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.Hostname == "" || body.Image == "" {
		writeErr(w, 400, "hostname and image are required")
		return
	}
	if body.ProjectID == "" {
		body.ProjectID = "dev"
	}
	if body.AppID == "" {
		body.AppID = "app-" + id.New()
	}
	if body.VCPU == 0 {
		body.VCPU = 1
	}
	if body.MemMIB == 0 {
		body.MemMIB = 512
	}
	if body.Port == 0 {
		body.Port = 8080
	}
	if body.Replicas == 0 {
		body.Replicas = 1
	}
	if body.Replicas < 1 || body.Replicas > 100 {
		writeErr(w, 400, "replicas must be in [1,100]")
		return
	}

	hcJSON, err := marshalHealthCheck(body.HealthCheck)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	placeJSON, err := marshalPlacement(body.NodePool, body.Labels, body.AntiAffinity)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	if err := a.store.EnsureApp(r.Context(), body.ProjectID, body.AppID, body.Hostname,
		body.Image, body.VCPU, body.MemMIB, body.Port, body.Replicas); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	depID := "dep-" + body.AppID + "-1"
	if err := a.store.CreateDeployment(r.Context(), store.Deployment{
		ID: depID, AppID: body.AppID, Generation: 1,
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: body.Port,
		Env: body.Env, Placement: placeJSON, HealthCheck: hcJSON, Status: "ACTIVE",
	}); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{
		"app_id":     body.AppID,
		"hostname":   body.Hostname,
		"deployment": depID,
		"generation": 1,
		"replicas":   body.Replicas,
	})
}

func (a *API) listApps(w http.ResponseWriter, r *http.Request) {
	apps, err := a.store.ListApps(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"apps": apps})
}

func (a *API) getApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	deps, err := a.store.ListDeployments(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	machines, err := a.store.ListMachinesForApp(r.Context(), appID, false)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	rollout, err := a.store.ActiveRolloutForApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"app": app, "deployments": deps, "machines": machines, "active_rollout": rollout,
	})
}

type deployBody struct {
	Image        string            `json:"image"`
	VCPU         int64             `json:"vcpu"`
	MemMIB       int64             `json:"mem_mib"`
	Port         int               `json:"port"`
	Env          map[string]string `json:"env"`
	NodePool     string            `json:"node_pool"`
	Labels       map[string]string `json:"labels"`
	AntiAffinity string            `json:"anti_affinity"`
	HealthCheck  *healthCheckBody  `json:"health_check"`
}

func (a *API) deployApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	var body deployBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	// 继承当前 ACTIVE deployment 的未覆盖字段（部署体不可变性的一部分）。
	active, err := a.store.ActiveDeploymentForApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if active == nil {
		writeErr(w, 409, "no active deployment")
		return
	}
	if body.Image == "" {
		body.Image = active.ImageRef
	}
	if body.VCPU == 0 {
		body.VCPU = active.VCPU
	}
	if body.MemMIB == 0 {
		body.MemMIB = active.MemMIB
	}
	if body.Port == 0 {
		body.Port = active.Port
	}
	if body.Env == nil {
		body.Env = active.Env
	}

	hcJSON, err := marshalHealthCheck(body.HealthCheck)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	placeJSON, err := marshalPlacement(body.NodePool, body.Labels, body.AntiAffinity)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	newGen := app.Generation + 1
	depID := fmt.Sprintf("dep-%s-%d", appID, newGen)
	rolloutID := "rollout-" + id.New()

	// 单 rollout 互斥（ADR-0015 S7）：活跃 rollout 存在 → 409。
	if err := a.store.CreateRollout(r.Context(), store.Rollout{
		ID: rolloutID, AppID: appID, FromGeneration: active.Generation, ToGeneration: newGen,
	}); err != nil {
		if errors.Is(err, store.ErrRolloutBusy) {
			writeErr(w, 409, "rollout already in progress for this app")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	if err := a.store.BumpAppGeneration(r.Context(), appID, newGen); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if err := a.store.CreateDeployment(r.Context(), store.Deployment{
		ID: depID, AppID: appID, Generation: newGen,
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: body.Port,
		Env: body.Env, Placement: placeJSON, HealthCheck: hcJSON, Status: "PREPARING",
	}); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{
		"app_id": appID, "deployment": depID, "generation": newGen,
		"rollout_id": rolloutID, "status": "PREPARING",
	})
}

func (a *API) scaleApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	var body struct {
		Replicas int `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.Replicas < 1 || body.Replicas > 100 {
		writeErr(w, 400, "replicas must be in [1,100]")
		return
	}
	if err := a.store.SetAppReplicas(r.Context(), appID, body.Replicas); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, 404, "app not found")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"app_id": appID, "desired_replicas": body.Replicas})
}

func (a *API) rollbackApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	if err := a.store.RolloutToRollback(r.Context(), appID); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"app_id": appID, "status": "ROLLING_BACK"})
}

func (a *API) deleteApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	machines, err := a.store.ListMachinesForApp(r.Context(), appID, false)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	// 逐副本下发 delete（kind=delete 语义：成功即 desired=DELETED）。
	// MVP 保留 app 行为墓碑（FK 级联会绕过 operation outbox 直接删机器行）。
	for _, m := range machines {
		req := &pb.DeleteMachineRequest{
			MachineId:   m.ID,
			ExecutionId: m.CurrentExecutionID,
			Generation:  uint64(m.Generation),
			OperationId: "op-del-" + m.ID,
		}
		raw, err := protojson.Marshal(req)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if _, err := a.store.EnqueueDelete(r.Context(), app.ProjectID, m.ID,
			m.CurrentExecutionID, req.OperationId, m.Generation, raw); err != nil {
			if errors.Is(err, store.ErrRequestConflict) {
				writeErr(w, 409, err.Error())
				return
			}
			writeErr(w, 500, err.Error())
			return
		}
	}
	// 级联结束活跃 rollout（S9）。
	if rl, err := a.store.ActiveRolloutForApp(r.Context(), appID); err == nil && rl != nil {
		_ = a.store.CompleteRollout(r.Context(), appID, true)
	}
	// 墓碑化：replicas=0 + 全部 deployment SUPERSEDED，确保 AppController
	// 不再为已删除的 app 补建 ordinal（否则 scale 对账会无限重建）。
	_ = a.store.SetAppReplicas(r.Context(), appID, 0)
	if deps, err := a.store.ListDeployments(r.Context(), appID); err == nil {
		for _, d := range deps {
			if d.Status == "ACTIVE" || d.Status == "PREPARING" {
				_ = a.store.SetDeploymentStatus(r.Context(), d.ID, "SUPERSEDED")
			}
		}
	}
	writeJSON(w, 202, map[string]any{"app_id": appID, "machines_to_delete": len(machines)})
}

// marshalHealthCheck 把 API body 的探针声明编码为 proto JSON（入库用）。
func marshalHealthCheck(h *healthCheckBody) ([]byte, error) {
	if h == nil {
		return nil, nil
	}
	var typ pb.HealthCheckSpec_Type
	switch strconvUpper(h.Type) {
	case "HTTP":
		typ = pb.HealthCheckSpec_HTTP
	case "TCP":
		typ = pb.HealthCheckSpec_TCP
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("health_check.type must be http or tcp")
	}
	return protojson.Marshal(&pb.HealthCheckSpec{
		Type:               typ,
		Target:             h.Target,
		IntervalSeconds:    h.IntervalSeconds,
		TimeoutSeconds:     h.TimeoutSeconds,
		UnhealthyThreshold: h.UnhealthyThreshold,
	})
}

// marshalPlacement 把放置约束编码为 proto JSON（入库用）。
func marshalPlacement(nodePool string, labels map[string]string, antiAffinity string) ([]byte, error) {
	aa := pb.PlacementConstraints_NONE
	if antiAffinity == "DEPLOYMENT" {
		aa = pb.PlacementConstraints_DEPLOYMENT
	}
	return protojson.Marshal(&pb.PlacementConstraints{
		NodePool: nodePool, Labels: labels, AntiAffinity: aa,
	})
}

func strconvUpper(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out = append(out, c)
	}
	return string(out)
}

var _ = strconv.Itoa
