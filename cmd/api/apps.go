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
	Services     []serviceBody              `json:"services"` // v1.1（ADR-0022）：多端口声明；nil = 单端口（port）
	Replicas     int                        `json:"replicas"`
	Env          map[string]string          `json:"env"`
	NodePool     string                     `json:"node_pool"`
	Labels       map[string]string          `json:"labels"`
	AntiAffinity string                     `json:"anti_affinity"`
	HealthCheck  *healthCheckBody           `json:"health_check"`
	SecretRefs   map[string]store.SecretRef `json:"secret_refs"`
	AutoStandby  *autoStandbyBody           `json:"auto_standby"` // v1.1（ADR-0017）
}

// serviceBody 是 services 声明的单条（v1.1，ADR-0022）。
type serviceBody struct {
	Name         string `json:"name"`
	InternalPort int    `json:"internal_port"`
}

// autoStandbyBody 是 auto_standby 策略声明（v1.1，ADR-0017）。
// ignore_destination_ports 透传 app 声明（如监控拨测端口不计入活跃）；
// ignore_source_cidrs 是平台保留字段，不对外暴露（错配会破坏唤醒语义）。
type autoStandbyBody struct {
	Enabled                bool     `json:"enabled"`
	IdleTimeoutSeconds     uint32   `json:"idle_timeout_seconds"`
	IgnoreDestinationPorts []uint32 `json:"ignore_destination_ports"`
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
	// P1-2（M5 评审）：受限 key 只能创建自己 project 的 app；body.project_id
	// 显式越权 → 403，留空 → 归一到 identity project。
	project, ok := clampBodyProject(r, body.ProjectID)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	body.ProjectID = project
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
	// 仅纯单端口请求默认 8080。services 请求的主端口由第一项决定，
	// 不能先注入 8080 再与合法的 services[0] 冲突。
	if len(body.Services) == 0 && body.Port == 0 {
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
	// v1.1（ADR-0022）：services 声明校验与归一（与单端口 port 互斥）。
	services, port, err := resolveServices(body.Services, body.Port)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	// v1.1（ADR-0017）：auto_standby 策略校验与 protojson 固化。
	autoStandbyJSON, err := marshalAutoStandby(body.AutoStandby)
	if err != nil {
		writeErr(w, 400, err.Error())
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
	// app 与其初始 deployment 必须作为一个业务单元提交；否则请求在两次
	// 写之间失败会留下没有 ACTIVE deployment 的 app。
	if err := a.store.CreateAppAndDeployment(r.Context(), body.ProjectID, store.App{
		ID: body.AppID, Hostname: body.Hostname, ImageRef: body.Image,
		VCPU: body.VCPU, MemMIB: body.MemMIB, DesiredReplicas: body.Replicas,
	}, store.Deployment{
		ID: depID, AppID: body.AppID, Generation: 1,
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: port,
		Services: services, AutoStandby: autoStandbyJSON,
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
	Services     []serviceBody              `json:"services"` // v1.1（ADR-0022）；nil = 继承/单端口
	Strategy     string                     `json:"strategy"` // v1.1-F：bluegreen（默认）| rolling
	Env          map[string]string          `json:"env"`
	NodePool     string                     `json:"node_pool"`
	Labels       map[string]string          `json:"labels"`
	AntiAffinity string                     `json:"anti_affinity"`
	HealthCheck  *healthCheckBody           `json:"health_check"`
	SecretRefs   map[string]store.SecretRef `json:"secret_refs"`  // M4（ADR-0010）
	AutoStandby  *autoStandbyBody           `json:"auto_standby"` // v1.1（ADR-0017）；nil = 继承
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
	// v1.1（ADR-0022）：services/port 继承语义——services 与 port 均未提供时
	// 继承 ACTIVE deployment（多 service 声明原样继承）；只提供 port = 回到
	// 单端口声明（清空 services）；只提供 services = 多端口声明。
	if body.Services == nil && body.Port == 0 {
		if len(active.Services) > 0 {
			body.Services = make([]serviceBody, 0, len(active.Services))
			for _, svc := range active.Services {
				body.Services = append(body.Services, serviceBody{Name: svc.Name, InternalPort: svc.InternalPort})
			}
		} else {
			body.Port = active.Port
		}
	}
	// v1.1（ADR-0017）：auto_standby 未提供时继承（含“显式关闭”的继承）。
	if body.AutoStandby == nil && len(active.AutoStandby) > 0 && string(active.AutoStandby) != "null" {
		var inherited autoStandbyBody
		if err := json.Unmarshal(active.AutoStandby, &inherited); err == nil {
			body.AutoStandby = &inherited
		}
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
	// v1.1：探针声明随 deployment 继承（与 vcpu/mem/port/env 同语义）。
	// 此前 deploy 未携带 health_check 时新代静默丢掉探针（UNCONFIGURED
	// 即入路由），rolling 逐批切流会在 guest 服务未起时打出 502。
	if body.HealthCheck == nil && len(active.HealthCheck) > 0 && string(active.HealthCheck) != "null" {
		hcJSON = active.HealthCheck
	}
	placeJSON, err := marshalPlacement(body.NodePool, body.Labels, body.AntiAffinity)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	// v1.1（ADR-0022/0017/F）：services / auto_standby / strategy 解析。
	services, port, err := resolveServices(body.Services, body.Port)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	autoStandbyJSON, err := marshalAutoStandby(body.AutoStandby)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	strategy, err := resolveStrategy(body.Strategy)
	if err != nil {
		writeErr(w, 400, err.Error())
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
		ImageRef: body.Image, VCPU: body.VCPU, MemMIB: body.MemMIB, Port: port,
		Services: services, AutoStandby: autoStandbyJSON, Strategy: strategy,
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
		if _, err := a.store.GetSecretMeta(r.Context(), effectiveProjectID(r, "dev"), ref.Secret, ver); err != nil {
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
		Port: active.Port, Env: active.Env, Strategy: active.EffectiveStrategy(),
		NodePool: place.NodePool, Labels: place.Labels, AntiAffinity: place.AntiAffinity,
		HealthCheck: &hc, SecretRefs: body.SecretRefs,
	}
	if len(active.Services) > 0 {
		db.Services = make([]serviceBody, 0, len(active.Services))
		for _, svc := range active.Services {
			db.Services = append(db.Services, serviceBody{Name: svc.Name, InternalPort: svc.InternalPort})
		}
	}
	if len(active.AutoStandby) > 0 && string(active.AutoStandby) != "null" {
		var policy autoStandbyBody
		if err := json.Unmarshal(active.AutoStandby, &policy); err != nil {
			writeErr(w, 500, "decode active auto_standby: "+err.Error())
			return
		}
		db.AutoStandby = &policy
	}
	raw, err := json.Marshal(db)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	a.deployApp(w, r)
}

// ---------------------------------------------------------------------------
// v1.1：services / auto_standby / strategy 解析与校验
// ---------------------------------------------------------------------------

// resolveServices 归一 services 声明：返回 (store services, 主端口)。
//   - services 与 port 同时声明：port 必须等于 services[0].internal_port；
//   - 只声明 services（1..8 条）：主 service = 第一条（继承单端口语义）；
//   - 只声明 port（或均缺省，port 已由调用方归一）：单端口，services = nil。
func resolveServices(services []serviceBody, port int) ([]store.ServiceSpec, int, error) {
	if len(services) == 0 {
		if port != 0 && (port < 1 || port > 65535) {
			return nil, 0, fmt.Errorf("port must be in [1,65535]")
		}
		return nil, port, nil
	}
	if len(services) > 8 {
		return nil, 0, fmt.Errorf("services supports at most 8 entries in v1.1")
	}
	if port != 0 && port != services[0].InternalPort {
		return nil, 0, fmt.Errorf("port conflicts with services[0].internal_port (%d)", services[0].InternalPort)
	}
	out := make([]store.ServiceSpec, 0, len(services))
	seenPort := map[int]bool{}
	seenName := map[string]bool{}
	for i, svc := range services {
		if svc.InternalPort < 1 || svc.InternalPort > 65535 {
			return nil, 0, fmt.Errorf("services[%d].internal_port must be in [1,65535]", i)
		}
		if seenPort[svc.InternalPort] {
			return nil, 0, fmt.Errorf("services[%d].internal_port %d duplicated", i, svc.InternalPort)
		}
		name := svc.Name
		if name == "" {
			name = fmt.Sprintf("svc-%d", svc.InternalPort)
		}
		if seenName[name] {
			return nil, 0, fmt.Errorf("services[%d].name %q duplicated", i, name)
		}
		seenPort[svc.InternalPort] = true
		seenName[name] = true
		out = append(out, store.ServiceSpec{Name: name, InternalPort: svc.InternalPort})
	}
	return out, out[0].InternalPort, nil
}

// marshalAutoStandby 校验并序列化 auto_standby 策略（protojson）。
// nil / 未启用 → nil（未声明，行为与 M5 完全一致）。
func marshalAutoStandby(b *autoStandbyBody) (json.RawMessage, error) {
	if b == nil || !b.Enabled {
		return nil, nil
	}
	if b.IdleTimeoutSeconds < 5 {
		return nil, fmt.Errorf("auto_standby.idle_timeout_seconds must be >= 5 when enabled")
	}
	for _, p := range b.IgnoreDestinationPorts {
		if p == 0 || p > 65535 {
			return nil, fmt.Errorf("auto_standby.ignore_destination_ports entry %d out of range", p)
		}
	}
	raw, err := protojson.Marshal(&pb.AutoStandbyPolicy{
		Enabled:                true,
		IdleTimeoutSeconds:     b.IdleTimeoutSeconds,
		IgnoreDestinationPorts: b.IgnoreDestinationPorts,
	})
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

// resolveStrategy 校验发布策略（v1.1-F）：空/bluegreen/rolling。
func resolveStrategy(strategy string) (string, error) {
	switch strategy {
	case "", "bluegreen":
		return "bluegreen", nil
	case "rolling":
		return "rolling", nil
	default:
		return "", fmt.Errorf("strategy must be bluegreen or rolling")
	}
}
