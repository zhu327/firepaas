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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/zhu327/firepaas/internal/capabilities"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/appcommand"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/id"
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
	Egress       *egressPolicyBody          `json:"egress"`       // v1.3-A（ADR-0027）
}

// egressPolicyBody 是 egress policy 的 API 声明（v1.3-A，ADR-0027）。
// policy_generation 由平台按 deployment generation 写入，客户端不得声明。
type egressPolicyBody struct {
	Mode              string   `json:"mode"` // unrestricted | deny_all | allowlist
	AllowedCIDRs      []string `json:"allowed_cidrs"`
	DeniedCIDRs       []string `json:"denied_cidrs"`
	AllowedDomains    []string `json:"allowed_domains"`
	MaxTCPConnections uint32   `json:"max_tcp_connections"`
	AuditAll          bool     `json:"audit_all"`
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.Hostname == "" || body.Image == "" {
		writeErr(w, 400, "hostname and image are required")
		return
	}
	// R2 评审：显式拒绝负 vcpu/mem（0 走默认值）与超出面数的 port。
	if body.VCPU < 0 || body.MemMIB < 0 {
		writeErr(w, 400, "vcpu and mem_mib must be >= 0")
		return
	}
	if body.Port < 0 || body.Port > 65535 {
		writeErr(w, 400, "port must be in [0,65535]")
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
		writeInternalErr(w, r, err)
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
		writeInternalErr(w, r, err)
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
	// v1.3-A（ADR-0027）：egress policy 校验与 protojson 固化（generation 与
	// deployment 一致，初始 deployment = 1）。
	egressJSON, err := marshalEgressPolicy(body.Egress, 1)
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
		refs, err := a.validateSecretRefs(r.Context(), effectiveProjectID(r, "dev"), body.SecretRefs)
		if err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		body.SecretRefs = refs
	}
	// v1.2-B（ADR-0024 §9）：secret_refs 与启用的 auto_standby 互斥。
	if err := rejectSecretStandbyCombo(body.SecretRefs, body.AutoStandby); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	depID := "dep-" + body.AppID + "-1"
	// v1.2-A（ADR-0023）：启动必需能力由平台从 deployment 语义推导。
	requiredFeatures := requiredFeaturesForSecretRefs(body.SecretRefs)
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
		RequiredFeatures: requiredFeatures,
		EgressPolicy:     egressJSON,
	}); err != nil {
		writeInternalErr(w, r, err)
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
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"apps": apps})
}

func (a *API) getApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	deps, err := a.store.ListDeployments(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	machines, err := a.store.ListMachinesForApp(r.Context(), appID, false)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	rollout, err := a.store.ActiveRolloutForApp(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
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
	Egress       *egressPolicyBody          `json:"egress"`       // v1.3-A（ADR-0027）；nil = 继承
}

func (a *API) deployApp(w http.ResponseWriter, r *http.Request) {
	var body deployBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	intent, err := deploymentIntent(r.PathValue("id"), effectiveProjectID(r, "dev"), body, false)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	result, err := a.appCommands.Execute(r.Context(), intent)
	if err != nil {
		writeDeploymentCommandError(w, r, err)
		return
	}
	writeDeploymentResult(w, result)
}

type deploymentResult struct {
	AppID      string `json:"app_id"`
	Deployment string `json:"deployment"`
	Generation int64  `json:"generation"`
	RolloutID  string `json:"rollout_id"`
	Status     string `json:"status"`
}

func deploymentIntent(appID, projectID string, body deployBody, inheritAll bool) (appcommand.Intent, error) {
	var services []appcommand.Service
	if body.Services != nil {
		services = make([]appcommand.Service, len(body.Services))
		for i, svc := range body.Services {
			services[i] = appcommand.Service{Name: svc.Name, InternalPort: svc.InternalPort}
		}
	}
	var healthCheck *appcommand.HealthCheck
	if body.HealthCheck != nil {
		healthCheck = &appcommand.HealthCheck{
			Type: body.HealthCheck.Type, Target: body.HealthCheck.Target,
			IntervalSeconds: body.HealthCheck.IntervalSeconds, TimeoutSeconds: body.HealthCheck.TimeoutSeconds,
			UnhealthyThreshold: body.HealthCheck.UnhealthyThreshold,
		}
	}
	var standby *appcommand.AutoStandby
	if body.AutoStandby != nil {
		standby = &appcommand.AutoStandby{
			Enabled:                body.AutoStandby.Enabled,
			IdleTimeoutSeconds:     body.AutoStandby.IdleTimeoutSeconds,
			IgnoreDestinationPorts: append([]uint32(nil), body.AutoStandby.IgnoreDestinationPorts...),
		}
	}
	var egress *appcommand.EgressPolicy
	if body.Egress != nil {
		egress = &appcommand.EgressPolicy{
			Mode: body.Egress.Mode, AllowedCIDRs: body.Egress.AllowedCIDRs,
			DeniedCIDRs: body.Egress.DeniedCIDRs, AllowedDomains: body.Egress.AllowedDomains,
			MaxTCPConnections: body.Egress.MaxTCPConnections, AuditAll: body.Egress.AuditAll,
		}
	}
	return appcommand.Intent{
		AppID: appID, ProjectID: projectID, Image: body.Image, VCPU: body.VCPU,
		MemMIB: body.MemMIB, Port: body.Port, Services: services, Strategy: body.Strategy, Env: body.Env,
		NodePool: body.NodePool, Labels: body.Labels, AntiAffinity: body.AntiAffinity, HealthCheck: healthCheck,
		SecretRefs: body.SecretRefs, AutoStandby: standby, Egress: egress, InheritAll: inheritAll,
		ReadActiveFirst: inheritAll,
	}, nil
}

func writeDeploymentCommandError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, appcommand.ErrAppNotFound):
		writeErr(w, 404, err.Error())
	case errors.Is(err, appcommand.ErrNoActiveDeployment), errors.Is(err, appcommand.ErrRolloutBusy):
		writeErr(w, 409, err.Error())
	case errors.Is(err, appcommand.ErrInvalidIntent):
		writeErr(w, 400, strings.TrimPrefix(err.Error(), appcommand.ErrInvalidIntent.Error()+": "))
	default:
		writeInternalErr(w, r, err)
	}
}

func writeDeploymentResult(w http.ResponseWriter, result appcommand.Result) {
	writeJSON(w, 202, deploymentResult{
		AppID: result.AppID, Deployment: result.DeploymentID,
		Generation: result.Generation, RolloutID: result.RolloutID, Status: result.Status,
	})
}

func (a *API) scaleApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	var body struct {
		Replicas int `json:"replicas"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
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
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 202, map[string]any{"app_id": appID, "desired_replicas": body.Replicas})
}

func (a *API) rollbackApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	// P3：无活跃 rollout 时明确 404（此前会因查不到行报 500）。
	if rl, err := a.store.ActiveRolloutForApp(r.Context(), appID); err != nil {
		writeInternalErr(w, r, err)
		return
	} else if rl == nil {
		writeErr(w, 404, "no active rollout to roll back")
		return
	} else if rl.Status == "ROLLING_BACK" {
		writeErr(w, 409, "rollout already rolling back")
		return
	}
	if err := a.store.RolloutToRollback(r.Context(), appID); err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 202, map[string]any{"app_id": appID, "status": "ROLLING_BACK"})
}

func (a *API) deleteApp(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	// R2 评审（app 删除原子化）：墓碑（deleted_at + 终结 rollout + deployment
	// SUPERSEDED）与全部副本 delete 入队在同一个 PG 事务提交（P0-1 的
	// “墓碑先于 delete”原序保留，同时消除两者之间的崩溃窗口）。
	// already_deleted 的幂等重试同样走本方法：它会为尚未收敛的机器补发
	// delete（同幂等键，store.UserDeleteOpID），以对消“入队途中崩溃只发了
	// 部分删除”的残留。
	result, err := a.store.SoftDeleteAppAndEnqueueDeletes(r.Context(), appID,
		func(m store.Machine) store.AppDeleteOp {
			// opID 嵌 execution 后缀（P0-2）：否则墓碑行若被复活会撞同幂等键
			// 不同请求体的 409，永久无法再删。
			opID := store.UserDeleteOpID(m.ID, m.CurrentExecutionID)
			raw, _ := protojson.Marshal(&pb.DeleteMachineRequest{
				MachineId: m.ID, ExecutionId: m.CurrentExecutionID,
				Generation: uint64(m.Generation), OperationId: opID,
			})
			return store.AppDeleteOp{
				MachineID: m.ID, ExecutionID: m.CurrentExecutionID,
				Generation: m.Generation, OperationID: opID, Request: raw,
			}
		})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, 404, "app not found")
			return
		}
		if errors.Is(err, store.ErrRequestConflict) {
			// 评审回流：同幂等键、不同请求体的历史脏数据会让删除整体回滚，
			// 客户端需要一个可识别、可动作的信号而非不区分的 500。
			slog.Warn("app delete blocked by idempotency conflict (legacy dirty row?)", "app_id", appID)
			writeErr(w, 409, "delete blocked by conflicting in-flight operation; contact operator to reconcile")
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 202, map[string]any{
		"app_id": appID, "machines_to_delete": result.Pending,
		"already_deleted": result.AlreadyDeleted,
	})
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
func (a *API) validateSecretRefs(
	ctx context.Context,
	projectID string,
	refs map[string]store.SecretRef,
) (map[string]store.SecretRef, error) {
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
		if _, err := a.store.GetSecretMeta(ctx, projectID, ref.Secret, ver); err != nil {
			return nil, fmt.Errorf("secret %q (ref %q) not found", ref.Secret, varName)
		}
		out[varName] = ref
	}
	return out, nil
}

// setAppSecretRefs updates bindings by invoking the same transport-independent
// deployment command as deployApp. Secret changes still create a new immutable
// deployment and rollout (ADR-0010 §2).
func (a *API) setAppSecretRefs(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	var body struct {
		SecretRefs map[string]store.SecretRef `json:"secret_refs"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	intent, err := deploymentIntent(appID, effectiveProjectID(r, "dev"), deployBody{SecretRefs: body.SecretRefs}, true)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	result, err := a.appCommands.Execute(r.Context(), intent)
	if err != nil {
		writeDeploymentCommandError(w, r, err)
		return
	}
	writeDeploymentResult(w, result)
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

// autoStandbyEnabled 判断 auto_standby 声明是否处于启用状态。
func autoStandbyEnabled(b *autoStandbyBody) bool {
	return b != nil && b.Enabled
}

// rejectSecretStandbyCombo（v1.2-B，ADR-0024 §9）：接收 secret 的
// execution 禁止 memory snapshot，因此 secret_refs 与启用的 auto_standby
// 互斥——在 deployment 固化前拒绝，避免到 agent 侧 create 才失败。
func rejectSecretStandbyCombo(refs map[string]store.SecretRef, standby *autoStandbyBody) error {
	if len(refs) > 0 && autoStandbyEnabled(standby) {
		return fmt.Errorf("secret_refs cannot be combined with enabled auto_standby: " +
			"secret executions forbid memory snapshots (ADR-0024)")
	}
	return nil
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

// marshalEgressPolicy 校验并序列化 egress policy（v1.3-A，ADR-0027）。
// nil → nil（未声明）。generation 由平台按 deployment generation 注入。
// 域名条目经 NormalizeEgressDomain 归一（小写/去尾点）；CIDR 由契约校验。
func marshalEgressPolicy(b *egressPolicyBody, generation int64) (json.RawMessage, error) {
	if b == nil {
		return nil, nil
	}
	var mode pb.EgressPolicySpec_Mode
	switch strings.ToLower(b.Mode) {
	case "", "unrestricted":
		mode = pb.EgressPolicySpec_UNRESTRICTED
	case "deny_all":
		mode = pb.EgressPolicySpec_DENY_ALL
	case "allowlist":
		mode = pb.EgressPolicySpec_ALLOWLIST
	default:
		return nil, fmt.Errorf("egress.mode must be unrestricted, deny_all or allowlist")
	}
	if b.MaxTCPConnections > 65535 {
		return nil, fmt.Errorf("egress.max_tcp_connections must be <= 65535")
	}
	domains := make([]string, 0, len(b.AllowedDomains))
	for _, d := range b.AllowedDomains {
		normalized, err := agentv1.NormalizeEgressDomain(d)
		if err != nil {
			return nil, err
		}
		domains = append(domains, normalized)
	}
	raw, err := protojson.Marshal(&pb.EgressPolicySpec{
		Mode:              mode,
		AllowedCidrs:      b.AllowedCIDRs,
		DeniedCidrs:       b.DeniedCIDRs,
		AllowedDomains:    domains,
		MaxTcpConnections: b.MaxTCPConnections,
		PolicyGeneration:  uint64(generation),
		AuditAll:          b.AuditAll,
	})
	if err != nil {
		return nil, err
	}
	if err := agentv1.ValidateEgressPolicy(&pb.EgressPolicySpec{
		Mode:              mode,
		AllowedCidrs:      b.AllowedCIDRs,
		DeniedCidrs:       b.DeniedCIDRs,
		AllowedDomains:    domains,
		MaxTcpConnections: b.MaxTCPConnections,
		PolicyGeneration:  uint64(generation),
		AuditAll:          b.AuditAll,
	}); err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

// getAppEgressAudit（v1.3-A，ADR-0027）：app 的 egress 拒绝摘要与策略变更
// 历史（project 隔离；只含计数与 strategy 事实，无 Host/SNI 明细）。
func (a *API) getAppEgressAudit(w http.ResponseWriter, r *http.Request) {
	appID := r.PathValue("id")
	app, err := a.store.GetApp(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	// project 隔离：非 admin 身份只能读自己 project 的摘要。
	project := effectiveProjectID(r, "")
	if project != "" && project != app.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	sums, err := a.store.ListEgressDenySummaries(r.Context(), app.ProjectID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	// 只回显本 app 的行。
	appSums := make([]store.EgressDenySummary, 0, len(sums))
	for _, s := range sums {
		if s.AppID == appID {
			appSums = append(appSums, s)
		}
	}
	changes, err := a.store.ListEgressPolicyChanges(r.Context(), appID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"app_id": appID, "project_id": app.ProjectID,
		"deny_summaries": appSums, "policy_changes": changes,
	})
}

// requiredFeaturesForSecretRefs 返回平台推导的启动必需能力（v1.2-A，
// ADR-0023）。绑定 secret 的 deployment 要求 secret.oneshot.v1（one-shot
// 安全通道）；客户端不能直接声明内部 feature。
func requiredFeaturesForSecretRefs(refs map[string]store.SecretRef) []string {
	if len(refs) == 0 {
		return nil
	}
	return []string{capabilities.SecretOneShotV1}
}
