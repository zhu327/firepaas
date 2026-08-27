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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/example/firepaas/internal/controlplane/store"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/example/firepaas/shared/pkg/id"
	"google.golang.org/protobuf/encoding/protojson"
)

type createAppBody struct {
	AppID        string                     `json:"app_id"`
	ProjectID    string                     `json:"project_id"`
	Hostname     string                     `json:"hostname"`
	Image        string                     `json:"image"`
	VCPU         int64                      `json:"vcpu"`
	MemMIB       int64                      `json:"mem_mib"`
	Port         int                        `json:"port"`
	Replicas     int                        `json:"replicas"`
	Env          map[string]string          `json:"env"`
	NodePool     string                     `json:"node_pool"`
	Labels       map[string]string          `json:"labels"`
	AntiAffinity string                     `json:"anti_affinity"`
	HealthCheck  *healthCheckBody           `json:"health_check"`
	SecretRefs   map[string]store.SecretRef `json:"secret_refs"`
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
	// P1-2：镜像引用策略（digest 形态 + registry allowlist）。
	normalizedImage, err := a.images.Validate(body.Image)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	body.Image = normalizedImage
	if body.ProjectID == "" {
		body.ProjectID = "dev"
	}
	if body.AppID == "" {
		body.AppID = "app-" + id.New()
	}
	// P3：已存在的 app（含软删墓碑）拒绝重建，避免 EnsureApp upsert 静默
	// 改写 desired_replicas / dep-{app}-1 唯一键 500。
	if existing, err := a.store.GetApp(r.Context(), body.AppID); err != nil {
		writeErr(w, 500, err.Error())
		return
	} else if existing != nil {
		if existing.Deleted {
			writeErr(w, 409, "app id already used (deleted); choose a new app_id")
		} else {
			writeErr(w, 409, "app already exists")
		}
		return
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
	// M4：create 时的 secret 引用校验（值不经 API 出现）。
	if len(body.SecretRefs) > 0 {
		if a.secrets == nil {
			writeErr(w, 503, "secrets disabled: FIREPAAS_SECRETS_MASTER_KEY not configured")
			return
		}
		// P3-13：过滤移除绑定（null）条目并校验存在性。
		refs, err := a.validateSecretRefs(r, body.SecretRefs)
		if err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		body.SecretRefs = refs
	}
	depID := "dep-" + body.AppID + "-1"
	if err := a.store.CreateDeployment(r.Context(), store.Deployment{
		ID: depID, AppID: body.AppID, Generation: 1,
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: body.Port,
		Env: body.Env, SecretRefs: body.SecretRefs,
		Placement: placeJSON, HealthCheck: hcJSON, Status: "ACTIVE",
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
	project := effectiveProjectID(r, "")
	apps, err := a.store.ListAppsFiltered(r.Context(), project)
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
	Image        string                     `json:"image"`
	VCPU         int64                      `json:"vcpu"`
	MemMIB       int64                      `json:"mem_mib"`
	Port         int                        `json:"port"`
	Env          map[string]string          `json:"env"`
	NodePool     string                     `json:"node_pool"`
	Labels       map[string]string          `json:"labels"`
	AntiAffinity string                     `json:"anti_affinity"`
	HealthCheck  *healthCheckBody           `json:"health_check"`
	SecretRefs   map[string]store.SecretRef `json:"secret_refs"` // M4（ADR-0010）
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
	} else {
		// P1-2：镜像引用策略（digest 形态 + registry allowlist）。
		normalizedImage, err := a.images.Validate(body.Image)
		if err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		body.Image = normalizedImage
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
	// M4：secret_refs 未提供时继承当前 ACTIVE deployment 的绑定。
	if body.SecretRefs == nil {
		body.SecretRefs = active.SecretRefs
	}
	// P3-13：显式传 {"VAR": null} 移除绑定；nil 继承；其余校验后固化。
	refs, err := a.validateSecretRefs(r, body.SecretRefs)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	body.SecretRefs = refs

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

	// P2-3：deployment + rollout + app.generation 同事务；中途失败不再
	// 留下「有活跃 rollout、无 to-gen deployment」的孤儿（卡 S3 超时 + 409）。
	// 单 rollout 互斥（ADR-0015 S7）由事务内 FOR UPDATE 检查 + 部分唯一索引
	// 双重保证。
	if err := a.store.DeployApp(r.Context(), store.Deployment{
		ID: depID, AppID: appID, Generation: newGen,
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: body.Port,
		Env: body.Env, SecretRefs: body.SecretRefs,
		Placement: placeJSON, HealthCheck: hcJSON, Status: "PREPARING",
	}, store.Rollout{
		ID: rolloutID, AppID: appID, FromGeneration: active.Generation, ToGeneration: newGen,
	}, newGen); err != nil {
		if errors.Is(err, store.ErrRolloutBusy) {
			writeErr(w, 409, "rollout already in progress for this app")
			return
		}
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
	// P3：无活跃 rollout 时明确 404（此前会因查不到行报 500）。
	if rl, err := a.store.ActiveRolloutForApp(r.Context(), appID); err != nil {
		writeErr(w, 500, err.Error())
		return
	} else if rl == nil {
		writeErr(w, 404, "no active rollout to roll back")
		return
	} else if rl.Status == "ROLLING_BACK" {
		writeErr(w, 409, "rollout already rolling back")
		return
	}
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
	if app.Deleted {
		// 幂等：重复删除返回当前收敛视图。
		machines, _ := a.store.ListMachinesForApp(r.Context(), appID, false)
		writeJSON(w, 202, map[string]any{"app_id": appID, "machines_to_delete": len(machines), "already_deleted": true})
		return
	}
	machines, err := a.store.ListMachinesForApp(r.Context(), appID, false)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	// P0-1：先事务墓碑化（deleted_at + 终结 rollout + deployment SUPERSEDED），
	// 再下发副本 delete。若先 delete 后墓碑，中间崩溃会让 controller 在窗口内
	// 补建副本（复活）。墓碑后 reconcileApp 不再补建，未收敛的机器由
	// processPGMachine 的 R5 路径继续收敛。
	if err := a.store.SoftDeleteApp(r.Context(), appID); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	// 逐副本下发 delete（kind=delete 语义：成功即 desired=DELETED）。
	// MVP 保留 app 行为墓碑（FK 级联会绕过 operation outbox 直接删机器行）。
	// opID 嵌 execution 后缀（P0-2）：否则墓碑行若被复活会撞同幂等键不同
	// 请求体的 409，永久无法再删。
	for _, m := range machines {
		req := &pb.DeleteMachineRequest{
			MachineId:   m.ID,
			ExecutionId: m.CurrentExecutionID,
			Generation:  uint64(m.Generation),
			OperationId: store.UserDeleteOpID(m.ID, m.CurrentExecutionID),
		}
		raw, err := protojson.Marshal(req)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if _, err := a.store.EnqueueDelete(r.Context(), app.ProjectID, m.ID,
			m.CurrentExecutionID, req.OperationId, m.Generation, raw); err != nil {
			if errors.Is(err, store.ErrRequestConflict) {
				// 同 execution 的重复 delete 请求体必一致；冲突只来自历史脏数据
				//（旧版裸 op-del-{id}）。记日志继续，不阻塞删除收敛。
				slog.Warn("delete app: idempotency conflict (legacy opID?)", "machine_id", m.ID, "error", err.Error())
				continue
			}
			writeErr(w, 500, err.Error())
			return
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
	switch strings.ToUpper(h.Type) {
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

// validateSecretRefs 校验引用格式与 secret 存在性（只查元数据，不取值）。
// 值为 nil 的条目是"移除绑定"语义（CLI --secret VAR= 生成）：直接从 map
// 剔除，不出错（P3-13；secret_refs 为 nil 才是继承语义，二者区分开）。
func (a *API) validateSecretRefs(r *http.Request, refs map[string]store.SecretRef) (map[string]store.SecretRef, error) {
	out := make(map[string]store.SecretRef, len(refs))
	for varName, ref := range refs {
		if varName == "" {
			return nil, fmt.Errorf("secret ref %q: var name is required", varName)
		}
		if ref.Secret == "" && ref.Version == nil {
			// 移除绑定（JSON null / 空对象）：不进入新 deployment。
			continue
		}
		if ref.Secret == "" {
			return nil, fmt.Errorf("secret ref %q: secret name is required", varName)
		}
		ver := ref.Version
		if _, err := a.store.GetSecretMeta(r.Context(), projectOr(r, "dev"), ref.Secret, ver); err != nil {
			return nil, fmt.Errorf("secret %q (ref %q) not found", ref.Secret, varName)
		}
		out[varName] = ref
	}
	return out, nil
}

// setAppSecretRefs 更新 app 的 secret 绑定：构造一次"仅换绑定"的发布
// （其余字段继承 ACTIVE deployment），走现有 rollout 状态机（ADR-0010 §2：
// 更新 secret 触发新 deployment version）。
func (a *API) setAppSecretRefs(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	var body struct {
		SecretRefs map[string]store.SecretRef `json:"secret_refs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	active, err := a.store.ActiveDeploymentForApp(r.Context(), appID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if active == nil {
		writeErr(w, 409, "no active deployment")
		return
	}
	hc := healthCheckBody{}
	if active.HealthCheck != nil {
		_ = json.Unmarshal(active.HealthCheck, &hc)
	}
	place := struct {
		NodePool     string            `json:"node_pool"`
		Labels       map[string]string `json:"labels"`
		AntiAffinity string            `json:"anti_affinity"`
	}{}
	if active.Placement != nil && string(active.Placement) != "null" {
		_ = json.Unmarshal(active.Placement, &place)
	}
	db := deployBody{
		Image: active.ImageRef, VCPU: active.VCPU, MemMIB: active.MemMIB,
		Port: active.Port, Env: active.Env,
		NodePool: place.NodePool, Labels: place.Labels, AntiAffinity: place.AntiAffinity,
		HealthCheck: &hc, SecretRefs: body.SecretRefs,
	}
	raw, err := json.Marshal(db)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	a.deployApp(w, r)
}
