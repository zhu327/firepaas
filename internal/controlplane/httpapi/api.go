package httpapi

// api.go 收拢 HTTP 层装配、路由表与进程级中间件。包内 handler 按领域分文件
// （apps/snapshots/...）；cmd/api 只负责依赖装配与进程生命周期。
//
// 路由表是控制面公开契约的唯一登记点：新增端点必须同时在本文件 Register、
// authm5.go 的 routeScope/projectGated 表登记，未登记默认拒绝。

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
	"github.com/zhu327/firepaas/internal/controlplane/appcommand"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/imagepolicy"
	"github.com/zhu327/firepaas/internal/controlplane/ratelimit"
	"github.com/zhu327/firepaas/internal/controlplane/secrets"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type requestIDContextKey struct{}

// requestIDFrom 返回 requestIDMiddleware 注入 context 的请求 ID（无则空串）。
func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}

// requestIDMiddleware 为每个请求生成/透传 X-Request-Id：入站 header 缺失
// 或非法（超长/非安全字符集）时用 crypto/rand 生成 16 位 hex，始终回显到
// 响应头并注入 context 供下游读取。不向 gRPC 传播（数据面契约不变）。
//
// review L6：入站 ID 不可无条件信任（日志/响应头膨胀）；限长 128 且只允许
// [A-Za-z0-9._-]，不合法则重新生成。
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if !validRequestID(id) {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, id)))
	})
}

// validRequestID 限制长度与字符集（不引入无界/可注入的调用方内容）。
func validRequestID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

// API 是 M2 最小 HTTP 服务。
type API struct {
	store        *store.Store
	apiToken     string
	authDisabled bool
	images       *imagepolicy.Policy // 镜像引用策略（P1-2：digest/allowlist）
	appCommands  *appcommand.Command // transport-independent deploy use case
	secrets      *secrets.Manager    // M4：信封加密（nil = /v1/secrets 返回 503）
	traffic      *traffic.Signer     // M4：execution-bound credential 现算
	apiKeys      *apikeys.Manager    // M5.1：API key 哈希库（nil = 只认 root token）
	cat          *catalog.Catalog    // M5.4：显式重投影（Redis 投影句柄）
	metrics      *metrics.Registry   // M5.4：重投影计数等系统指标
	kicker       *RouteKicker        // P3-13：leader controller 的即时重建句柄
	rgw          *RuntimeGateway     // v1.2-C：每副本只读 agent 客户端网关
	// v1.2-E（ADR-0035）
	limiter  *ratelimit.Limiter // API 限流（nil = 未装配，仅开发模式）
	sessions *sessionCounter    // runtime 会话并发计数
	// R2 加固：/readyz 的真实依赖探测句柄；401 失败尝试限流桶（nil = 关闭）。
	pool         *pgxpool.Pool
	rdb          *redis.Client
	authThrottle *AuthFailureThrottle
	metricsToken string
	version      string
	// limits 是 v1.4-C 准入上限（cmd/api 装配期解析 env 后传入）。
	limits PrewarmLimits
}

// routeKicker 把 leader 实例 controller 的 KickRouteRebuild 递给 API 层
// （controller 在 leader 回调内构造，无法重排到 API 之前）。
// RouteKicker 把 leader 实例 controller 的 KickRouteRebuild 递给 HTTP 层
// （controller 在 leader 回调内构造，无法重排到 API 之前）。
type RouteKicker struct {
	mu sync.Mutex
	fn func() (time.Duration, error)
}

func (k *RouteKicker) Set(fn func() (time.Duration, error)) {
	k.mu.Lock()
	k.fn = fn
	k.mu.Unlock()
}

// Clear 在 leader 回调退出时撤销旧 controller 的函数指针，避免失锁实例
// 继续从 API goroutine 写投影。
func (k *RouteKicker) Clear() {
	k.mu.Lock()
	k.fn = nil
	k.mu.Unlock()
}

// Kick 立即重建路由投影；leader 未就绪时返回 false（端点降级为等 ticker）。
func (k *RouteKicker) Kick() (time.Duration, error, bool) {
	k.mu.Lock()
	fn := k.fn
	k.mu.Unlock()
	if fn == nil {
		return 0, nil, false
	}
	d, err := fn()
	return d, err, true
}

// runtimeGW 把本 API 副本的只读 agent 客户端解析递给 HTTP 层。
// resolver 生命周期独立于 leader 任期，handover 时不会出现 follower 503 窗口。
// RuntimeGateway 把本 API 副本的只读 agent 客户端解析递给 HTTP 层。
// resolver 生命周期独立于 leader 任期，handover 时不会出现 follower 503 窗口。
type RuntimeGateway struct {
	mu sync.Mutex
	fn func(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error)
}

func (g *RuntimeGateway) Set(
	fn func(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error),
) {
	g.mu.Lock()
	g.fn = fn
	g.mu.Unlock()
}

func (g *RuntimeGateway) Clear() {
	g.mu.Lock()
	g.fn = nil
	g.mu.Unlock()
}

// Get 解析 machine 的 agent 客户端与节点能力；resolver 未就绪返回 503 语义错误。
func (g *RuntimeGateway) Get(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error) {
	g.mu.Lock()
	fn := g.fn
	g.mu.Unlock()
	if fn == nil {
		return nil, nil, fmt.Errorf("runtime resolver not ready")
	}
	return fn(ctx, machineID)
}

type createMachineBody struct {
	MachineID      string            `json:"machine_id"`
	Hostname       string            `json:"hostname"`
	Image          string            `json:"image"`
	VCPU           int64             `json:"vcpu"`
	MemMIB         int64             `json:"mem_mib"`
	Port           int               `json:"port"`
	ProjectID      string            `json:"project_id"`
	AppID          string            `json:"app_id"`
	DeploymentID   string            `json:"deployment_id"`
	ReplicaOrdinal uint32            `json:"replica_ordinal"`
	ExecutionID    string            `json:"execution_id"`
	Generation     int64             `json:"generation"`
	OperationID    string            `json:"operation_id"`
	Env            map[string]string `json:"env"`
	NodePool       string            `json:"node_pool"`
	Labels         map[string]string `json:"labels"`
	AntiAffinity   string            `json:"anti_affinity"`
	HealthCheck    *healthCheckBody  `json:"health_check"`
	// v1.2-D（ADR-0026）：TTL（秒，0=关闭）与 restart policy。
	TTLSeconds    int64              `json:"ttl_seconds"`
	RestartPolicy *restartPolicyBody `json:"restart_policy"`
}

// restartPolicyBody 是 createMachine 的 restart policy 声明（v1.2-D）。
type restartPolicyBody struct {
	Mode                string `json:"mode"` // NEVER|ON_FAILURE|ALWAYS
	MaxAttempts         int    `json:"max_attempts"`
	BackoffSeconds      int    `json:"backoff_seconds"`
	StableWindowSeconds int    `json:"stable_window_seconds"`
}

type healthCheckBody struct {
	Type               string `json:"type"` // http | tcp
	Target             string `json:"target"`
	IntervalSeconds    uint32 `json:"interval_seconds"`
	TimeoutSeconds     uint32 `json:"timeout_seconds"`
	UnhealthyThreshold uint32 `json:"unhealthy_threshold"`
}

func (a *API) createMachine(w http.ResponseWriter, r *http.Request) {
	var body createMachineBody
	if err := decodeJSONBody(w, r, &body, 1<<20, false); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	// P0：fence 字段必须由服务端生成。此前直接把客户端提交的
	// machine_id/execution_id/generation 透传进 CreateMachineRequest，任何
	// write-scope 调用方都能操纵 fencing 高水位（如 pinning 一个超大
	// generation 阻断该 machine 的后续 create）。客户端仅保留 operation_id
	// 作为幂等键。
	if body.MachineID != "" || body.ExecutionID != "" || body.Generation != 0 {
		writeErr(
			w,
			400,
			"machine_id, execution_id and generation are server-generated; clients must not send them (operation_id remains the client idempotency key)",
		)
		return
	}
	if body.Hostname == "" || body.Image == "" || body.OperationID == "" {
		writeErr(w, 400, "hostname, image and operation_id are required")
		return
	}
	// R2 评审：负值必须显式 400——负数进 uint64() 转换会绕成天量资源，
	// 不能靠“0 → 默认值”反推合法。
	if body.VCPU < 0 || body.MemMIB < 0 {
		writeErr(w, 400, "vcpu and mem_mib must be >= 0")
		return
	}
	if body.Port < 0 || body.Port > 65535 {
		writeErr(w, 400, "port must be in [0,65535] (0 = default 8080)")
		return
	}
	if body.TTLSeconds < 0 {
		writeErr(w, 400, "ttl_seconds must be >= 0")
		return
	}
	// P1-2（M5 评审）：受限 key 只能建自己 project 的 machine（同 createApp）。
	project, ok := clampBodyProject(r, body.ProjectID)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	body.ProjectID = project
	if body.ProjectID == "" {
		body.ProjectID = "dev"
	}
	if body.AppID == "" {
		body.AppID = "app-" + body.Hostname
	}
	if body.DeploymentID == "" {
		body.DeploymentID = "dep-" + body.Hostname
	}
	// 与 app/deployment 路径一致执行镜像策略，不能留下绕过 image validation
	// 的 public machine-create 端点。
	normalizedImage, err := a.images.Validate(body.Image)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	body.Image = normalizedImage
	// M2 验收：同一 replica ordinal 的并发重试必须落同一 machine_id。
	// machine_id 由服务端按 (app_id, replica_ordinal) 稳定推导；execution_id /
	// generation 同样服务端正生成（客户端提交已在入口拒绝）。
	body.MachineID = fmt.Sprintf("%s-r%d", body.AppID, body.ReplicaOrdinal)
	body.ExecutionID = "exec-1"
	body.Generation = 1
	if body.VCPU == 0 {
		body.VCPU = 1
	}
	if body.MemMIB == 0 {
		body.MemMIB = 512
	}
	if body.Port == 0 {
		body.Port = 8080
	}
	antiAffinity := pb.PlacementConstraints_NONE
	if body.AntiAffinity == "DEPLOYMENT" {
		antiAffinity = pb.PlacementConstraints_DEPLOYMENT
	}
	var healthCheck *pb.HealthCheckSpec
	if body.HealthCheck != nil {
		hc := body.HealthCheck
		hcType := pb.HealthCheckSpec_TYPE_UNSPECIFIED
		switch strings.ToUpper(hc.Type) {
		case "HTTP":
			hcType = pb.HealthCheckSpec_HTTP
		case "TCP":
			hcType = pb.HealthCheckSpec_TCP
		}
		if hcType != pb.HealthCheckSpec_TYPE_UNSPECIFIED {
			healthCheck = &pb.HealthCheckSpec{
				Type:               hcType,
				Target:             hc.Target,
				IntervalSeconds:    hc.IntervalSeconds,
				TimeoutSeconds:     hc.TimeoutSeconds,
				UnhealthyThreshold: hc.UnhealthyThreshold,
			}
		}
	}
	// v1.2-D（ADR-0026）：restart policy 解析（默认 NEVER；控制面唯一权威）。
	restartPolicy, err := marshalRestartPolicy(body.RestartPolicy)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	req := &pb.CreateMachineRequest{
		MachineId:   body.MachineID,
		Generation:  uint64(body.Generation),
		OperationId: body.OperationID,
		Spec: &pb.MachineSpec{
			ProjectId:      body.ProjectID,
			AppId:          body.AppID,
			DeploymentId:   body.DeploymentID,
			ReplicaOrdinal: body.ReplicaOrdinal,
			ExecutionId:    body.ExecutionID,
			Hostname:       body.Hostname,
			ImageRef:       body.Image,
			Vcpu:           uint64(body.VCPU),
			MemMib:         uint64(body.MemMIB),
			Env:            body.Env,
			Network:        &pb.NetworkSpec{IngressPort: uint64(body.Port)},
			HealthCheck:    healthCheck,
			RestartPolicy:  restartPolicy,
			Placement: &pb.PlacementConstraints{
				NodePool:     body.NodePool,
				Labels:       body.Labels,
				AntiAffinity: antiAffinity,
			},
		},
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		writeInternalErr(w, r, fmt.Errorf("marshal request: %w", err))
		return
	}

	var expiresAt *time.Time
	if body.TTLSeconds > 0 {
		t := time.Now().Add(time.Duration(body.TTLSeconds) * time.Second)
		expiresAt = &t
	}
	mode, maxAttempts, backoff, stable := "NEVER", 3, 10, 300
	if body.RestartPolicy != nil && body.RestartPolicy.Mode != "" {
		mode = strings.ToUpper(body.RestartPolicy.Mode)
		if body.RestartPolicy.MaxAttempts > 0 {
			maxAttempts = body.RestartPolicy.MaxAttempts
		}
		if body.RestartPolicy.BackoffSeconds > 0 {
			backoff = body.RestartPolicy.BackoffSeconds
		}
		if body.RestartPolicy.StableWindowSeconds > 0 {
			stable = body.RestartPolicy.StableWindowSeconds
		}
	}
	op, err := a.store.EnsureAppAndEnqueueCreateWithLifecycle(r.Context(),
		body.ProjectID, body.AppID, body.Hostname, body.Image, body.VCPU, body.MemMIB,
		int64(agentv1.EffectiveDiskMib(req.Spec.GetDiskMib())),
		body.Port, body.MachineID, body.DeploymentID, body.ExecutionID, body.OperationID,
		body.Generation, int(body.ReplicaOrdinal), raw, placementJSON(req.Spec.Placement),
		expiresAt, mode, maxAttempts, backoff, stable)
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, err.Error())
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 202, map[string]any{
		"operation_id": op.ID,
		"status":       op.Status,
		"machine_id":   op.MachineID,
	})
}

func (a *API) listMachines(w http.ResponseWriter, r *http.Request) {
	project := effectiveProjectID(r, "")
	machines, err := a.store.ListMachines(r.Context(), project)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"machines": machines})
}

func (a *API) getMachine(w http.ResponseWriter, r *http.Request) {
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	writeJSON(w, 200, m)
}

func (a *API) deleteMachine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), id)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	executionID := r.URL.Query().Get("execution_id")
	if executionID == "" {
		executionID = m.CurrentExecutionID
	} else if executionID != m.CurrentExecutionID {
		// R2 评审：客户端显式指定了非当前 execution——说明调用方基于陈旧
		// 状态发删除（或者 fenced 攻击面）；用机当前执行删除会违背调用方
		// 意图，用旧执行删除被 agent fence 拒绝。返回 409 让客户端重读后重试。
		writeErr(w, 409, "execution_id does not match the machine's current execution; re-read and retry")
		return
	}
	operationID := r.URL.Query().Get("operation_id")
	if operationID == "" {
		writeErr(w, 400, "operation_id query parameter is required")
		return
	}
	req := &pb.DeleteMachineRequest{
		MachineId:   id,
		ExecutionId: executionID,
		Generation:  uint64(m.Generation),
		OperationId: operationID,
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		writeInternalErr(w, r, fmt.Errorf("marshal delete: %w", err))
		return
	}
	project, err := a.store.ProjectForApp(r.Context(), m.AppID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if project == "" {
		writeErr(w, 409, "machine app has no project")
		return
	}
	op, err := a.store.EnqueueDelete(r.Context(), project, id, executionID, operationID, m.Generation, raw)
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, err.Error())
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 202, map[string]any{"operation_id": op.ID, "status": op.Status})
}

// listNodes 输出节点 observed projection（调度器输入，审计用）。
func (a *API) listNodes(w http.ResponseWriter, r *http.Request) {
	// review 2026-09-10：节点拓扑是平台运维信息（routeScope 注释），
	// 项目 owner 的 admin capability 不足以枚举节点。
	if !a.requireGlobalIdentity(w, r) {
		return
	}
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"nodes": nodes})
}

// listEvents（v1.2-F）：租户事件流（user_events，append-only）。
// 过滤：project/app/machine/type/since/before（keyset 游标）；受限 key 只能
// 看自己的 project（effectiveProjectID 收口），root 必须显式带 project_id。
func (a *API) listEvents(w http.ResponseWriter, r *http.Request) {
	project := effectiveProjectID(r, "")
	if project == "" {
		writeErr(w, 400, "project_id query parameter is required")
		return
	}
	f := store.UserEventFilter{
		ProjectID: project,
		AppID:     r.URL.Query().Get("app_id"), MachineID: r.URL.Query().Get("machine_id"),
		Type: r.URL.Query().Get("type"),
	}
	if v := r.URL.Query().Get("before"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			f.Before = n
		}
	}
	if v := r.URL.Query().Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.Since = t
		} else {
			writeErr(w, 400, "since must be RFC3339")
			return
		}
	}
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	f.Limit = limit
	events, err := a.store.ListUserEvents(r.Context(), f)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	var next int64
	if len(events) == limit {
		next = events[len(events)-1].ID
	}
	writeJSON(w, 200, map[string]any{"events": events, "next_before": next})
}

// listSchedulerEvents（v1.2-F）：内部调度/对账事件（admin；与租户事件分离）。
func (a *API) listSchedulerEvents(w http.ResponseWriter, r *http.Request) {
	// review 2026-09-10：调度事件携带 node_id 等平台拓扑；受限身份不得枚举。
	if !a.requireGlobalIdentity(w, r) {
		return
	}
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	events, err := a.store.ListSchedulerEvents(r.Context(), effectiveProjectID(r, ""), limit)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": events})
}

// IsTruthy 解析宽松的布尔环境变量（1/true/yes/on，大小写不敏感）。
func IsTruthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}

// marshalRestartPolicy 解析并归一 restart policy（v1.2-D，ADR-0026）。
func marshalRestartPolicy(b *restartPolicyBody) (*pb.RestartPolicy, error) {
	if b == nil {
		return nil, nil
	}
	var mode pb.RestartPolicy_Mode
	switch strings.ToUpper(b.Mode) {
	case "", "NEVER":
		mode = pb.RestartPolicy_NEVER
	case "ON_FAILURE":
		mode = pb.RestartPolicy_ON_FAILURE
	case "ALWAYS":
		mode = pb.RestartPolicy_ALWAYS
	default:
		return nil, fmt.Errorf("restart_policy.mode must be NEVER, ON_FAILURE or ALWAYS")
	}
	if b.MaxAttempts < 0 || b.BackoffSeconds < 0 || b.StableWindowSeconds < 0 {
		return nil, fmt.Errorf("restart_policy counts must be >= 0")
	}
	if b.MaxAttempts > 100 {
		return nil, fmt.Errorf("restart_policy.max_attempts must be <= 100")
	}
	return &pb.RestartPolicy{
		Mode:           mode,
		MaxAttempts:    uint32(b.MaxAttempts),
		BackoffSeconds: uint32(b.BackoffSeconds),
	}, nil
}

// placementJSON 序列化放置约束（nil 返回空字节，存 NULL/默认）。
func placementJSON(p *pb.PlacementConstraints) []byte {
	if p == nil {
		return nil
	}
	raw, err := protojson.Marshal(p)
	if err != nil {
		return nil
	}
	return raw
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// decodeJSONBody 收敛 JSON 请求体样板：MaxBytesReader 限流 + Decode。
// maxBytes 按调用方传入（默认 1<<20；governance 1<<16；runtime exec/cp 用
// runtimeMaxBody）；allowEmpty=true 时忽略全部解码错误（空 body/可选 body
// 合法，调用方直接用零值继续，与历史 `_ = Decode` 语义一致）。
// 非空分支返回 error，调用方保留各自的 writeErr 文案（governance 的
// "invalid quota/rate limit body" 等不变）。
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64, allowEmpty bool) error {
	if r == nil || r.Body == nil {
		if allowEmpty {
			return nil
		}
		return errors.New("EOF")
	}
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes)).Decode(dst)
	if allowEmpty {
		return nil
	}
	return err
}

// Config 是 HTTP 层的完整依赖装配。字段只在构造时读取；handler 不自行读
// 进程环境（可变的准入参数在 cmd/api 装配期解析后传入）。
type Config struct {
	Store         *store.Store
	APIToken      string
	AuthDisabled  bool
	Images        *imagepolicy.Policy
	AppCommands   *appcommand.Command
	Secrets       *secrets.Manager
	Traffic       *traffic.Signer
	APIKeys       *apikeys.Manager
	Catalog       *catalog.Catalog
	Metrics       *metrics.Registry
	Kicker        *RouteKicker
	Runtime       *RuntimeGateway
	Limiter       *ratelimit.Limiter
	Pool          *pgxpool.Pool
	Redis         *redis.Client
	AuthThrottle  *AuthFailureThrottle
	MetricsToken  string
	Version       string
	PrewarmLimits PrewarmLimits
}

// New 构造 API。可能为 nil 的依赖由各 handler 按既有语义 fail-closed。
func New(cfg Config) *API {
	return &API{
		store:        cfg.Store,
		apiToken:     cfg.APIToken,
		authDisabled: cfg.AuthDisabled,
		images:       cfg.Images,
		appCommands:  cfg.AppCommands,
		secrets:      cfg.Secrets,
		traffic:      cfg.Traffic,
		apiKeys:      cfg.APIKeys,
		cat:          cfg.Catalog,
		metrics:      cfg.Metrics,
		kicker:       cfg.Kicker,
		rgw:          cfg.Runtime,
		limiter:      cfg.Limiter,
		sessions:     newSessionCounter(),
		pool:         cfg.Pool,
		rdb:          cfg.Redis,
		authThrottle: cfg.AuthThrottle,
		metricsToken: cfg.MetricsToken,
		version:      cfg.Version,
		limits:       cfg.PrewarmLimits,
	}
}

// Wrap 组合进程级中间件：panic recover 最外层，其次 request ID，最后审计。
func Wrap(next http.Handler, reg *metrics.Registry) http.Handler {
	return recoverMiddleware(requestIDMiddleware(auditMiddlewareWithMetrics(reg, next)))
}

// Register 装载全部 HTTP 路由。
func Register(mux *http.ServeMux, api *API) {
	mux.HandleFunc("GET /v1/health", api.health)
	// /readyz（R2 评审）：真实依赖探活（PG SELECT 1 + Redis PING，各 ≤1s
	// 超时）；/v1/health 保留静态轻探针（nomad/consul check 用 readyz）。
	mux.HandleFunc("GET /readyz", api.readyz)
	mux.HandleFunc("POST /v1/machines", api.auth(api.createMachine))
	mux.HandleFunc("GET /v1/machines", api.auth(api.listMachines))
	mux.HandleFunc("GET /v1/machines/{id}", api.auth(api.getMachine))
	mux.HandleFunc("DELETE /v1/machines/{id}", api.auth(api.deleteMachine))
	// M4.5 scale-to-zero（mvp-plan §8.4）：显式 pause/resume；proxy 侧
	// autoresume 负责 standby → Running 的首流量唤醑。
	mux.HandleFunc("POST /v1/machines/{id}/pause", api.auth(api.pauseMachine))
	mux.HandleFunc("POST /v1/machines/{id}/resume", api.auth(api.resumeMachine))
	mux.HandleFunc("GET /v1/nodes", api.auth(api.listNodes))
	mux.HandleFunc("GET /v1/capabilities", api.auth(api.listCapabilities))
	mux.HandleFunc("GET /v1/events", api.auth(api.listEvents))
	mux.HandleFunc("GET /v1/system/scheduler-events", api.auth(api.listSchedulerEvents))
	// M4：secrets v1（ADR-0010，值只进不出——无 reveal 端点）。
	mux.HandleFunc("POST /v1/secrets", api.auth(api.putSecret))
	mux.HandleFunc("GET /v1/secrets", api.auth(api.listSecrets))
	mux.HandleFunc("GET /v1/secrets/{name}", api.auth(api.getSecretMeta))
	mux.HandleFunc("DELETE /v1/secrets/{name}", api.auth(api.deleteSecret))
	// ADR-0040 G2a（§15）：EastWestPolicy 租户自助 CRUD（dst 侧归属授权）。
	mux.HandleFunc("PUT /v1/eastwest-policies", api.auth(api.putEastWestPolicy))
	mux.HandleFunc("GET /v1/eastwest-policies", api.auth(api.listEastWestPolicies))
	mux.HandleFunc("DELETE /v1/eastwest-policies", api.auth(api.deleteEastWestPolicy))
	// M4（ADR-0006）：execution-bound proxy credential 按需现算给 edge。
	mux.HandleFunc("GET /v1/machines/{id}/traffic-token", api.auth(api.trafficToken))
	mux.HandleFunc("PUT /v1/apps/{id}/secret-refs", api.auth(api.setAppSecretRefs))
	// M3：app/deployment/rollout（mvp-plan §7.4、ADR-0015）。
	mux.HandleFunc("POST /v1/apps", api.auth(api.createApp))
	mux.HandleFunc("GET /v1/apps", api.auth(api.listApps))
	mux.HandleFunc("GET /v1/apps/{id}", api.auth(api.getApp))
	mux.HandleFunc("POST /v1/apps/{id}/deployments", api.auth(api.deployApp))
	mux.HandleFunc("POST /v1/apps/{id}/scale", api.auth(api.scaleApp))
	// ADR-0041：并发自动弹性策略（PUT 全量替换/deploy；GET 读/read）。
	mux.HandleFunc("PUT /v1/apps/{id}/autoscale", api.auth(api.putAutoscale))
	mux.HandleFunc("GET /v1/apps/{id}/autoscale", api.auth(api.getAutoscale))
	mux.HandleFunc("POST /v1/apps/{id}/rollback", api.auth(api.rollbackApp))
	mux.HandleFunc("DELETE /v1/apps/{id}", api.auth(api.deleteApp))
	// v1.3-A（ADR-0027）：egress 拒绝摘要与策略变更审计（project 隔离）。
	mux.HandleFunc("GET /v1/apps/{id}/egress-audit", api.auth(api.getAppEgressAudit))
	// v1.3-B（ADR-0028）：node-local snapshot 资源 + checkpoint + schedule。
	mux.HandleFunc("POST /v1/machines/{id}/snapshots", api.auth(api.createSnapshot))
	mux.HandleFunc("GET /v1/snapshots", api.auth(api.listSnapshots))
	mux.HandleFunc("GET /v1/snapshots/{id}", api.auth(api.getSnapshot))
	mux.HandleFunc("DELETE /v1/snapshots/{id}", api.auth(api.deleteSnapshot))
	mux.HandleFunc("POST /v1/machines/{id}/snapshot-schedules", api.auth(api.upsertSnapshotSchedule))
	mux.HandleFunc("GET /v1/machines/{id}/snapshot-schedules", api.auth(api.listSnapshotSchedules))
	mux.HandleFunc("DELETE /v1/machines/{id}/snapshot-schedules/{schedule}", api.auth(api.deleteSnapshotSchedule))
	// v1.3-C（ADR-0028）：受限 fork + filesystem rescue。
	mux.HandleFunc("POST /v1/snapshots/{id}/fork", api.auth(api.forkSnapshot))
	mux.HandleFunc("POST /v1/machines/{id}/rescue", api.auth(api.rescueMachine))
	// v1.4-D：只读 restore preflight（不改变 restore/fork 状态机）。
	mux.HandleFunc("POST /v1/snapshots/{id}/preflight", api.auth(api.snapshotPreflight))
	// v1.3-D（ADR-0029）：LOCAL_RW volume。
	mux.HandleFunc("POST /v1/volumes", api.auth(api.createVolume))
	mux.HandleFunc("GET /v1/volumes", api.auth(api.listVolumes))
	mux.HandleFunc("GET /v1/volumes/{id}", api.auth(api.getVolume))
	mux.HandleFunc("DELETE /v1/volumes/{id}", api.auth(api.deleteVolume))
	mux.HandleFunc("POST /v1/machines/{id}/volume-attach", api.auth(api.attachVolume))
	mux.HandleFunc("POST /v1/machines/{id}/volume-detach", api.auth(api.detachVolume))
	// v1.4-C（docs/v1.4-plan.md §7）：镜像预热/覆盖率/cache pin。
	mux.HandleFunc("POST /v1/images/prewarm", api.auth(api.prewarmImage))
	mux.HandleFunc("GET /v1/images/coverage", api.auth(api.imageCoverage))
	mux.HandleFunc("POST /v1/images/pins", api.auth(api.createImagePin))
	mux.HandleFunc("GET /v1/images/pins", api.auth(api.listImagePins))
	mux.HandleFunc("DELETE /v1/images/pins/{id}", api.auth(api.deleteImagePin))
	// v1.2-C（ADR-0025）：受控运行时通道（logs/exec/cp；debug scope）。
	mux.HandleFunc("GET /v1/machines/{id}/logs", api.auth(api.machineLogs))
	mux.HandleFunc("POST /v1/machines/{id}/exec", api.auth(api.machineExec))
	mux.HandleFunc("PUT /v1/machines/{id}/files", api.auth(api.machineFilesPut))
	mux.HandleFunc("GET /v1/machines/{id}/files", api.auth(api.machineFilesGet))
	// v1.2-D（ADR-0026）：wait / TTL / restart 治理。
	mux.HandleFunc("GET /v1/machines/{id}/wait", api.auth(api.waitMachine))
	mux.HandleFunc("GET /v1/operations/{id}/wait", api.auth(api.waitOperation))
	mux.HandleFunc("GET /v1/rollouts/{id}/wait", api.auth(api.waitRollout))
	mux.HandleFunc("PUT /v1/machines/{id}/ttl", api.auth(api.updateMachineTTL))
	mux.HandleFunc("POST /v1/machines/{id}/restart-reset", api.auth(api.resetRestart))
	// v1.2-E（ADR-0035）：项目配额与限流配置（配额写 = admin；读 = read）。
	mux.HandleFunc("GET /v1/projects/{id}/quota", api.auth(api.getProjectQuota))
	mux.HandleFunc("PUT /v1/projects/{id}/quota", api.auth(api.putProjectQuota))
	mux.HandleFunc("GET /v1/projects/{id}/rate-limits", api.auth(api.getRateLimits))
	mux.HandleFunc("PUT /v1/projects/{id}/rate-limits", api.auth(api.putRateLimits))
	// v1.5（最小可用项目面）：projects CRUD（配额/限流仍走 governance 端点）。
	mux.HandleFunc("POST /v1/projects", api.auth(api.createProject))
	mux.HandleFunc("GET /v1/projects", api.auth(api.listProjects))
	mux.HandleFunc("GET /v1/projects/{id}", api.auth(api.getProject))
	mux.HandleFunc("DELETE /v1/projects/{id}", api.auth(api.deleteProject))
	// M5.1（mvp-plan §9.1）+ v1.5 自助轮换：admin scope，越权边界由 handler 强制。
	mux.HandleFunc("POST /v1/apikeys", api.auth(api.createAPIKey))
	mux.HandleFunc("GET /v1/apikeys", api.auth(api.listAPIKeys))
	mux.HandleFunc("DELETE /v1/apikeys/{id}", api.auth(api.revokeAPIKey))
	mux.HandleFunc("POST /v1/apikeys/{id}/rotate", api.auth(api.rotateAPIKey))
	// M5.3：操作追踪（请求/结果字段已脱敏）。
	mux.HandleFunc("GET /v1/operations", api.auth(api.listOperations))
	mux.HandleFunc("GET /v1/operations/{id}", api.auth(api.getOperation))
	// M5.4：显式投影重建（admin scope）。
	mux.HandleFunc("POST /v1/system/reprojections", api.auth(api.reproject))
	// M5.5：节点排水/复原（admin scope，drain/rebuild 升级承诺）。
	mux.HandleFunc("POST /v1/nodes/{id}/drain", api.auth(api.drainNode))
	mux.HandleFunc("POST /v1/nodes/{id}/ready", api.auth(api.readyNode))
	// P3-15（M5 评审）：/metrics 默认开放（Prometheus 内网抓取）；生产可设
	// FIREPAAS_METRICS_TOKEN 收口（Bearer 匹配，与 API token 相互独立）。
	mux.HandleFunc("GET /metrics", api.metricsHandler)
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok", "version": a.version})
}

func (a *API) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if a.metricsToken != "" {
		got := bearerToken(r)
		if subtle.ConstantTimeCompare([]byte(got), []byte(a.metricsToken)) != 1 {
			writeErr(w, 401, "unauthorized")
			return
		}
	}
	a.metrics.Handler().ServeHTTP(w, r)
}
