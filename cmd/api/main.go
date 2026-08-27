// Command api 是 firepaas 控制面入口（M2 单实例 vertical slice + M2a leader）。
//
// 目标形态（mvp-plan §5.4/§6、ADR-0007/0014）：
//   - REST：machines 最小 CRUD + nodes/events 观测端点 + /metrics
//   - PG desired/operations 权威；controller 只在 leader 上运行
//   - Nomad discovery → 节点 gRPC 池 → 调度（过滤+Best-of-K）→ Redis 预约
//   - Redis route 投影可重建
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/controlplane/apikeys"
	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/example/firepaas/internal/controlplane/controller"
	"github.com/example/firepaas/internal/controlplane/db"
	"github.com/example/firepaas/internal/controlplane/imagepolicy"
	"github.com/example/firepaas/internal/controlplane/leader"
	"github.com/example/firepaas/internal/controlplane/nodemanager"
	"github.com/example/firepaas/internal/controlplane/reservations"
	"github.com/example/firepaas/internal/controlplane/secrets"
	"github.com/example/firepaas/internal/controlplane/store"
	"github.com/example/firepaas/internal/controlplane/traffic"
	"github.com/example/firepaas/internal/observability/metrics"
	"github.com/example/firepaas/internal/scheduler"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpPort := envOr("FIREPAAS_HTTP_PORT", "8080")
	pgURL := envOr("FIREPAAS_POSTGRES_URL", "postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable")
	redisAddr := envOr("FIREPAAS_REDIS_ADDR", "127.0.0.1:6379")
	nomadAddr := envOr("FIREPAAS_NOMAD_ADDR", "http://127.0.0.1:4646")
	legacyProxyAddr := envOr("FIREPAAS_AGENT_PROXY_ADDR", "127.0.0.1:5107")

	pool, err := db.Open(ctx, pgURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	st := store.New(pool)
	if err := st.EnsureProject(ctx, "dev", "development"); err != nil {
		return err
	}
	// M5.1（mvp-plan §9.1）：api_keys 哈希存储 + 最小 scope。
	apiKeyMgr := apikeys.New(pool)

	// 认证默认开启（评审 P1-1）：未显式设置 FIREPAAS_AUTH_DISABLED 时，
	// 缺少 FIREPAAS_API_TOKEN 直接拒绝启动，而不是静默无认证。
	apiToken := os.Getenv("FIREPAAS_API_TOKEN")
	authDisabled := isTruthy(os.Getenv("FIREPAAS_AUTH_DISABLED"))
	if apiToken == "" && !authDisabled {
		return errors.New("FIREPAAS_API_TOKEN is required (or set FIREPAAS_AUTH_DISABLED=true for local dev only)")
	}
	if authDisabled {
		slog.Warn("API authentication DISABLED (dev only; never in lab/production)")
	}

	// M4：secrets 信封加密主密钥 + proxy credential HMAC 密钥（部署注入）。
	// 都可选：未配置 secrets 时 /v1/secrets 全部 503；未配置 traffic key 时
	// create 不下发凭证（需 agent 侧同步关校验，仅过渡期）。
	var secretsMgr *secrets.Manager
	if mk := os.Getenv("FIREPAAS_SECRETS_MASTER_KEY"); mk != "" {
		m, err := secrets.NewManager(mk)
		if err != nil {
			return fmt.Errorf("FIREPAAS_SECRETS_MASTER_KEY: %w", err)
		}
		secretsMgr = m
		slog.Info("secrets envelope encryption enabled", "key_version", secrets.KeyVersion)
	}
	var trafficSigner *traffic.Signer
	if tk := os.Getenv("FIREPAAS_TRAFFIC_TOKEN_KEY"); tk != "" {
		raw, err := base64.StdEncoding.DecodeString(tk)
		if err != nil || len(raw) < 32 {
			return errors.New("FIREPAAS_TRAFFIC_TOKEN_KEY must be base64 of >=32 bytes")
		}
		trafficSigner, err = traffic.NewSigner(raw)
		if err != nil {
			return err
		}
		slog.Info("proxy credential signer enabled")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	cat := catalog.New(rdb)
	resv := reservations.New(rdb, 120*time.Second)
	reg := metrics.New()
	// M5.2：单机宿主资源 gauge 采样（只读 /proc，15s 周期）。
	go hostSampler(ctx, reg)
	placer := scheduler.New(scheduler.DefaultBestOfKConfig(), scheduler.Options{})

	// M2a leader：controller（reconcile+放置）只在持锁实例运行；备实例只读待命。
	go func() {
		err := leader.Elect(ctx, pool, leader.Key, func(lctx context.Context) error {
			nm, err := nodemanager.New(nodemanager.Config{
				NomadAddr:     nomadAddr,
				JobName:       "firepaas-agentd",
				DiscoverEvery: 10 * time.Second,
				InfoEvery:     20 * time.Second,
				Store:         st,
			})
			if err != nil {
				slog.Error("nodemanager", "error", err)
				return err
			}
			defer nm.Close()
			go func() {
				if err := nm.Run(lctx); err != nil && lctx.Err() == nil {
					slog.Error("nodemanager exited", "error", err)
				}
			}()

			ctrl := controller.New(st, cat, nm, resv, placer, reg, controller.Config{
				DefaultAppPort:       8080,
				LegacyAgentProxyAddr: legacyProxyAddr,
				OpPollInterval:       time.Second,
				SyncInterval:         5 * time.Second,
				RebuildInterval:      30 * time.Second,
				ReconcileGrace:       30 * time.Second,
				MaxPlacementAttempts: 3,
				RolloutTimeout:       envDur("FIREPAAS_ROLLOUT_TIMEOUT", 300*time.Second),
				RolloutDrainGrace:    envDur("FIREPAAS_ROLLOUT_DRAIN", 30*time.Second),
				Secrets:              secretsMgr,
				Traffic:              trafficSigner,
			})
			slog.Info("running control loop as leader")
			return ctrl.Run(lctx)
		})
		if err != nil && ctx.Err() == nil {
			slog.Error("leader loop exited", "error", err)
		}
	}()

	api := &API{store: st, apiToken: apiToken, authDisabled: authDisabled,
		images: imagepolicy.NewWithOptions(envOr("FIREPAAS_REGISTRY_ALLOWLIST", ""),
			isTruthy(envOr("FIREPAAS_IMAGE_REQUIRE_DIGEST", "false"))),
		secrets: secretsMgr, traffic: trafficSigner, apiKeys: apiKeyMgr,
		cat: cat, metrics: reg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/machines", api.auth(api.createMachine))
	mux.HandleFunc("GET /v1/machines", api.auth(api.listMachines))
	mux.HandleFunc("GET /v1/machines/{id}", api.auth(api.getMachine))
	mux.HandleFunc("DELETE /v1/machines/{id}", api.auth(api.deleteMachine))
	// M4.5 scale-to-zero（mvp-plan §8.4）：显式 pause/resume；proxy 侧
	// autoresume 负责 standby → Running 的首流量唤醑。
	mux.HandleFunc("POST /v1/machines/{id}/pause", api.auth(api.pauseMachine))
	mux.HandleFunc("POST /v1/machines/{id}/resume", api.auth(api.resumeMachine))
	mux.HandleFunc("GET /v1/nodes", api.auth(api.listNodes))
	mux.HandleFunc("GET /v1/events", api.auth(api.listEvents))
	// M4：secrets v1（ADR-0010，值只进不出——无 reveal 端点）。
	mux.HandleFunc("POST /v1/secrets", api.auth(api.putSecret))
	mux.HandleFunc("GET /v1/secrets", api.auth(api.listSecrets))
	mux.HandleFunc("GET /v1/secrets/{name}", api.auth(api.getSecretMeta))
	mux.HandleFunc("DELETE /v1/secrets/{name}", api.auth(api.deleteSecret))
	// M4（ADR-0006）：execution-bound proxy credential 按需现算给 edge。
	mux.HandleFunc("GET /v1/machines/{id}/traffic-token", api.auth(api.trafficToken))
	mux.HandleFunc("PUT /v1/apps/{id}/secret-refs", api.auth(api.setAppSecretRefs))
	// M3：app/deployment/rollout（mvp-plan §7.4、ADR-0015）。
	mux.HandleFunc("POST /v1/apps", api.auth(api.createApp))
	mux.HandleFunc("GET /v1/apps", api.auth(api.listApps))
	mux.HandleFunc("GET /v1/apps/{id}", api.auth(api.getApp))
	mux.HandleFunc("POST /v1/apps/{id}/deployments", api.auth(api.deployApp))
	mux.HandleFunc("POST /v1/apps/{id}/scale", api.auth(api.scaleApp))
	mux.HandleFunc("POST /v1/apps/{id}/rollback", api.auth(api.rollbackApp))
	mux.HandleFunc("DELETE /v1/apps/{id}", api.auth(api.deleteApp))
	// M5.1（mvp-plan §9.1）：API key 管理（admin scope，routeScope 表收口）。
	mux.HandleFunc("POST /v1/apikeys", api.auth(api.createAPIKey))
	mux.HandleFunc("GET /v1/apikeys", api.auth(api.listAPIKeys))
	mux.HandleFunc("DELETE /v1/apikeys/{id}", api.auth(api.revokeAPIKey))
	// M5.3：操作追踪（请求/结果字段已脱敏）。
	mux.HandleFunc("GET /v1/operations", api.auth(api.listOperations))
	mux.HandleFunc("GET /v1/operations/{id}", api.auth(api.getOperation))
	// M5.4：显式投影重建（admin scope）。
	mux.HandleFunc("POST /v1/system/reprojections", api.auth(api.reproject))
	// M5.5：节点排水/复原（admin scope，drain/rebuild 升级承诺）。
	mux.HandleFunc("POST /v1/nodes/{id}/drain", api.auth(api.drainNode))
	mux.HandleFunc("POST /v1/nodes/{id}/ready", api.auth(api.readyNode))
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { reg.Handler().ServeHTTP(w, nil) })

	srv := &http.Server{Addr: ":" + httpPort, Handler: auditMiddleware(mux)}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("control-plane API listening", "port", httpPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// API 是 M2 最小 HTTP 服务。
type API struct {
	store        *store.Store
	apiToken     string
	authDisabled bool
	images       *imagepolicy.Policy // 镜像引用策略（P1-2：digest/allowlist）
	secrets      *secrets.Manager    // M4：信封加密（nil = /v1/secrets 返回 503）
	traffic      *traffic.Signer     // M4：execution-bound credential 现算
	apiKeys      *apikeys.Manager    // M5.1：API key 哈希库（nil = 只认 root token）
	cat          *catalog.Catalog    // M5.4：显式重投影（Redis 投影句柄）
	metrics      *metrics.Registry   // M5.4：重投影计数等系统指标
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
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.Hostname == "" || body.Image == "" || body.OperationID == "" {
		writeErr(w, 400, "hostname, image and operation_id are required")
		return
	}
	if body.ProjectID == "" {
		body.ProjectID = "dev"
	}
	if body.AppID == "" {
		body.AppID = "app-" + body.Hostname
	}
	if body.DeploymentID == "" {
		body.DeploymentID = "dep-" + body.Hostname
	}
	// M2 验收：同一 replica ordinal 的并发重试必须落同一 machine_id。
	// machine_id 缺省按 (app_id, replica_ordinal) 稳定推导；显式提供时原样使用。
	if body.MachineID == "" {
		body.MachineID = fmt.Sprintf("%s-r%d", body.AppID, body.ReplicaOrdinal)
	}
	if body.ExecutionID == "" {
		body.ExecutionID = "exec-1"
	}
	if body.Generation == 0 {
		body.Generation = 1
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
			Placement: &pb.PlacementConstraints{
				NodePool:     body.NodePool,
				Labels:       body.Labels,
				AntiAffinity: antiAffinity,
			},
		},
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		writeErr(w, 500, "marshal request: "+err.Error())
		return
	}

	op, err := a.store.EnsureAppAndEnqueueCreate(r.Context(),
		body.ProjectID, body.AppID, body.Hostname, body.Image, body.VCPU, body.MemMIB,
		body.Port, body.MachineID, body.DeploymentID, body.ExecutionID, body.OperationID,
		body.Generation, int(body.ReplicaOrdinal), raw, placementJSON(req.Spec.Placement))
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, err.Error())
			return
		}
		writeErr(w, 500, err.Error())
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
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"machines": machines})
}

func (a *API) getMachine(w http.ResponseWriter, r *http.Request) {
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err.Error())
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
		writeErr(w, 500, err.Error())
		return
	}
	if m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	executionID := r.URL.Query().Get("execution_id")
	if executionID == "" {
		executionID = m.CurrentExecutionID
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
		writeErr(w, 500, "marshal delete: "+err.Error())
		return
	}
	op, err := a.store.EnqueueDelete(r.Context(), "dev", id, executionID, operationID, m.Generation, raw)
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, err.Error())
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"operation_id": op.ID, "status": op.Status})
}

// listNodes 输出节点 observed projection（调度器输入，审计用）。
func (a *API) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"nodes": nodes})
}

// listEvents 输出最近调度/对账事件。
func (a *API) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	events, err := a.store.ListSchedulerEvents(r.Context(), limit)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"events": events})
}

func isTruthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDur 解析时长环境变量（非法值回退默认）。
func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
