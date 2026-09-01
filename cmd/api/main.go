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
	"crypto/subtle"
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
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
	"github.com/zhu327/firepaas/internal/controlplane/appcommand"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/controller"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/imagepolicy"
	"github.com/zhu327/firepaas/internal/controlplane/leader"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/ratelimit"
	"github.com/zhu327/firepaas/internal/controlplane/reservations"
	"github.com/zhu327/firepaas/internal/controlplane/secrets"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
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
	defer func() { _ = rdb.Close() }()
	cat := catalog.New(rdb)
	resv := reservations.New(rdb, 120*time.Second)
	reg := metrics.New()
	// M5.2：单机宿主资源 gauge 采样（只读 /proc，15s 周期）。
	go hostSampler(ctx, reg)
	// v1.1（ADR-0018）：镜像亲和权重（默认 0.5；0 = 关闭）——与 R/K/α 同一
	// 配置面（Best-of-K 打分参数的可运维热调入口，v1.1 以 env 暴露）。
	placerCfg := scheduler.DefaultBestOfKConfig()
	if raw := os.Getenv("FIREPAAS_SCHED_WEIGHT_IMAGE"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v >= 0 {
			placerCfg.WeightImage = v
		}
	}
	placer := scheduler.New(placerCfg, scheduler.Options{})

	// 每个 API 副本都维护只读 Nomad discovery + agent 连接池，使 follower 可
	// 直接服务 logs/exec/cp。ServiceInfo→PG 同步和 mutation controller 仍严格
	// 只在 leader 任期内运行，避免多个 nodemanager 并发写 observed projection。
	nm, err := nodemanager.New(nodemanager.Config{
		NomadAddr:     nomadAddr,
		JobName:       "firepaas-agentd",
		DiscoverEvery: 10 * time.Second,
		InfoEvery:     20 * time.Second,
		Store:         st,
	})
	if err != nil {
		return fmt.Errorf("nodemanager: %w", err)
	}
	defer nm.Close()
	go func() {
		if err := nm.RunDiscovery(ctx); err != nil && ctx.Err() == nil {
			slog.Error("node discovery exited", "error", err)
		}
	}()

	// M2a leader：controller（reconcile+放置）只在持锁实例运行；备实例只读待命。
	// P3-13：routeKicker 把 leader 内 controller 的即时重建能力暴露给重投影。
	kicker := &routeKicker{}
	rgw := &runtimeGW{}
	rgw.Set(nm.AgentRuntimeForMachine)
	go func() {
		err := leader.Elect(ctx, pool, leader.Key, func(lctx context.Context) error {
			go func() {
				if err := nm.RunServiceInfo(lctx); err != nil && lctx.Err() == nil {
					slog.Error("node service info sync exited", "error", err)
				}
			}()

			ctrl := controller.New(st, cat, nm, resv, placer, reg, controller.Config{
				DefaultAppPort:        8080,
				LegacyAgentProxyAddr:  legacyProxyAddr,
				OpPollInterval:        time.Second,
				SyncInterval:          5 * time.Second,
				RebuildInterval:       30 * time.Second,
				ReconcileGrace:        30 * time.Second,
				NodeLossRecreateAfter: envDur("FIREPAAS_NODE_LOSS_RECREATE_AFTER", time.Minute),
				MaxPlacementAttempts:  3,
				RolloutTimeout:        envDur("FIREPAAS_ROLLOUT_TIMEOUT", 300*time.Second),
				RolloutDrainGrace:     envDur("FIREPAAS_ROLLOUT_DRAIN", 30*time.Second),
				Secrets:               secretsMgr,
				Traffic:               trafficSigner,
				// v1.1（ADR-0018/0021）：部署预取 top-K 与 evacuate 步超时。
				PrefetchTopK:        envInt("FIREPAAS_PREFETCH_TOPK", 3),
				EvacuateStepTimeout: envDur("FIREPAAS_EVACUATE_STEP_TIMEOUT", 5*time.Minute),
				// v1.4（ADR-0036）：本地 GC 默认 off。delete 仅在 agent
				// 广告 lock-aware quarantine capability 后执行。
				UserEventsRetention: envDur("FIREPAAS_USER_EVENTS_RETENTION", 168*time.Hour),
				GC: controller.GCConfig{
					Mode:      envOr("FIREPAAS_LOCAL_GC_MODE", "off"),
					MinAge:    envDur("FIREPAAS_GC_MIN_AGE", time.Hour),
					HighWater: envFloat("FIREPAAS_GC_HIGH_WATERMARK", 0.85),
					LowWater:  envFloat("FIREPAAS_GC_LOW_WATERMARK", 0.70),
					Interval:  envDur("FIREPAAS_GC_INTERVAL", 5*time.Minute),
					Grace:     envDur("FIREPAAS_LOCAL_GC_GRACE", 10*time.Minute),
				},
				Scrub: controller.ScrubConfig{
					Enabled:  envBool("FIREPAAS_SCRUB_ENABLED", false),
					Interval: envDur("FIREPAAS_SCRUB_INTERVAL", time.Hour),
					Budget:   envInt("FIREPAAS_SCRUB_BUDGET", 1),
				},
			})
			slog.Info("running control loop as leader")
			kicker.Set(ctrl.KickRouteRebuild)
			defer kicker.Clear()
			return ctrl.Run(lctx)
		})
		if err != nil && ctx.Err() == nil {
			slog.Error("leader loop exited", "error", err)
		}
	}()

	// v1.2-E（ADR-0035）：API 限流（Redis 令牌桶；配置 PG + 10s 缓存）。
	var apiLimiter *ratelimit.Limiter
	if rdb != nil {
		apiLimiter = ratelimit.New(rdb, func(ctx context.Context, project string) (ratelimit.Config, error) {
			c, _, err := st.GetRateLimitConfig(ctx, project)
			if err != nil {
				return ratelimit.Config{}, err
			}
			return ratelimit.Config{
				Read:     ratelimit.Limits{Rate: c.ReadRate, Burst: c.ReadBurst},
				Mutation: ratelimit.Limits{Rate: c.MutationRate, Burst: c.MutationBurst},
				Stream:   ratelimit.Limits{Rate: c.StreamRate, Burst: c.StreamBurst},
			}, nil
		}, 10*time.Second)
	}
	images := imagepolicy.NewWithOptions(envOr("FIREPAAS_REGISTRY_ALLOWLIST", ""),
		isTruthy(envOr("FIREPAAS_IMAGE_REQUIRE_DIGEST", "false")))
	api := &API{
		store: st, apiToken: apiToken, authDisabled: authDisabled,
		images: images, appCommands: appcommand.New(st, images),
		secrets: secretsMgr, traffic: trafficSigner, apiKeys: apiKeyMgr,
		cat: cat, metrics: reg, kicker: kicker, rgw: rgw,
		limiter: apiLimiter, sessions: newSessionCounter(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /v1/health",
		func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) },
	)
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
	// P3-15（M5 评审）：/metrics 默认开放（Prometheus 内网抓取）；生产可设
	// FIREPAAS_METRICS_TOKEN 收口（Bearer 匹配，与 API token 相互独立）。
	metricsToken := envOr("FIREPAAS_METRICS_TOKEN", "")
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if metricsToken != "" {
			got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			if subtle.ConstantTimeCompare([]byte(got), []byte(metricsToken)) != 1 {
				writeErr(w, 401, "unauthorized")
				return
			}
		}
		reg.Handler().ServeHTTP(w, r)
	})

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
	appCommands  *appcommand.Command // transport-independent deploy use case
	secrets      *secrets.Manager    // M4：信封加密（nil = /v1/secrets 返回 503）
	traffic      *traffic.Signer     // M4：execution-bound credential 现算
	apiKeys      *apikeys.Manager    // M5.1：API key 哈希库（nil = 只认 root token）
	cat          *catalog.Catalog    // M5.4：显式重投影（Redis 投影句柄）
	metrics      *metrics.Registry   // M5.4：重投影计数等系统指标
	kicker       *routeKicker        // P3-13：leader controller 的即时重建句柄
	rgw          *runtimeGW          // v1.2-C：每副本只读 agent 客户端网关
	// v1.2-E（ADR-0035）
	limiter  *ratelimit.Limiter // API 限流（nil = 未装配，仅开发模式）
	sessions *sessionCounter    // runtime 会话并发计数
}

// routeKicker 把 leader 实例 controller 的 KickRouteRebuild 递给 API 层
// （controller 在 leader 回调内构造，无法重排到 API 之前）。
type routeKicker struct {
	mu sync.Mutex
	fn func() (time.Duration, error)
}

func (k *routeKicker) Set(fn func() (time.Duration, error)) {
	k.mu.Lock()
	k.fn = fn
	k.mu.Unlock()
}

// Clear 在 leader 回调退出时撤销旧 controller 的函数指针，避免失锁实例
// 继续从 API goroutine 写投影。
func (k *routeKicker) Clear() {
	k.mu.Lock()
	k.fn = nil
	k.mu.Unlock()
}

// Kick 立即重建路由投影；leader 未就绪时返回 false（端点降级为等 ticker）。
func (k *routeKicker) Kick() (time.Duration, error, bool) {
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
type runtimeGW struct {
	mu sync.Mutex
	fn func(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error)
}

func (g *runtimeGW) Set(fn func(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error)) {
	g.mu.Lock()
	g.fn = fn
	g.mu.Unlock()
}

func (g *runtimeGW) Clear() {
	g.mu.Lock()
	g.fn = nil
	g.mu.Unlock()
}

// Get 解析 machine 的 agent 客户端与节点能力；resolver 未就绪返回 503 语义错误。
func (g *runtimeGW) Get(ctx context.Context, machineID string) (*agentclient.Client, map[string]bool, error) {
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.Hostname == "" || body.Image == "" || body.OperationID == "" {
		writeErr(w, 400, "hostname, image and operation_id are required")
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
		writeErr(w, 500, "marshal request: "+err.Error())
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
	project, err := a.store.ProjectForApp(r.Context(), m.AppID)
	if err != nil {
		writeErr(w, 500, err.Error())
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
		writeErr(w, 500, err.Error())
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
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	events, err := a.store.ListSchedulerEvents(r.Context(), effectiveProjectID(r, ""), limit)
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDur 解析时长环境变量（非法值回退默认）。
func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return parsed
}

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

// envInt 解析整数环境变量（非法值回退默认）。
func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil && v > 0 && v < 1 {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
