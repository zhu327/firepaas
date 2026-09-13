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

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
	"github.com/zhu327/firepaas/internal/controlplane/appcommand"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/controller"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/fabric"
	"github.com/zhu327/firepaas/internal/controlplane/httpapi"
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
	"github.com/zhu327/firepaas/shared/pkg/env"
	"github.com/zhu327/firepaas/shared/pkg/logging"
)

// buildVersion 由 -ldflags "-X main.buildVersion=..." 注入发布版本。
var buildVersion = "dev"

func main() {
	logging.Setup()
	if err := run(); err != nil {
		slog.Error("api terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpPort := env.Get("FIREPAAS_HTTP_PORT", "8080")
	pgURL := env.Get("FIREPAAS_POSTGRES_URL", "postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable")
	redisAddr := env.Get("FIREPAAS_REDIS_ADDR", "127.0.0.1:6379")
	nomadAddr := env.Get("FIREPAAS_NOMAD_ADDR", "http://127.0.0.1:4646")
	legacyProxyAddr := env.Get("FIREPAAS_AGENT_PROXY_ADDR", "127.0.0.1:5107")

	// P1：业务池显式治理（上限/生命周期/健康检查/statement 超时，env 可调）。
	pool, err := db.Open(ctx, pgURL, db.Options{
		MaxConns:          int32(env.Int("FIREPAAS_PG_MAX_CONNS", 16)),
		MinConns:          int32(env.Int("FIREPAAS_PG_MIN_CONNS", 2)),
		MaxConnLifetime:   env.Dur("FIREPAAS_PG_MAX_CONN_LIFETIME", 30*time.Minute),
		MaxConnIdleTime:   env.Dur("FIREPAAS_PG_MAX_CONN_IDLE_TIME", 5*time.Minute),
		HealthCheckPeriod: env.Dur("FIREPAAS_PG_HEALTH_CHECK_PERIOD", 30*time.Second),
		StatementTimeout:  env.Dur("FIREPAAS_PG_STATEMENT_TIMEOUT", 30*time.Second),
	})
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
	// P1：认证热路径 hash→identity 短 TTL 缓存（默认 60s，本副本内 Revoke 立
	// 即失效；跨副本撤销生效时延 ≤ TTL，为记录在案的安全窗口）。
	// hash→identity 缓存 TTL（默认 60s；显式 0 = 关闭缓存，逐请求查库；
	// envDur 会把非正值回退默认，所以这里自行解析以保留“禁用”语义）。
	apiKeyCacheTTL := 60 * time.Second
	if raw := os.Getenv("FIREPAAS_API_KEY_CACHE_TTL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			apiKeyCacheTTL = d
		} else {
			slog.Warn("invalid FIREPAAS_API_KEY_CACHE_TTL, keeping default", "value", raw)
		}
	}
	apiKeyMgr := apikeys.NewCached(pool, apiKeyCacheTTL)

	// 认证默认开启（评审 P1-1）：未显式设置 FIREPAAS_AUTH_DISABLED 时，
	// 缺少 FIREPAAS_API_TOKEN 直接拒绝启动，而不是静默无认证。
	apiToken := os.Getenv("FIREPAAS_API_TOKEN")
	authDisabled := httpapi.IsTruthy(os.Getenv("FIREPAAS_AUTH_DISABLED"))
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
	} else {
		// 启动即有声响（R2 评审 P0）：secrets 已按设计关闭——/v1/secrets 端点
		// 503；更重要的是 controller 现在对 secret-bearing deployment 的
		// create 派发 fail-closed（op FAILED），不会静默创建“缺 secret”的 VM。
		slog.Warn("FIREPAAS_SECRETS_MASTER_KEY not configured: secrets disabled " +
			"(/v1/secrets 503; secret-bearing deployments will fail dispatch fail-closed)")
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
	// review 2026-09-10：控制面 RED 指标的策展 HELP（auto-type 已可用，
	// HELP 文本只能显式声明）。
	reg.DescribeCounter("firepaas_api_requests_total",
		"API requests by route, method and status code.")
	reg.DescribeHistogram("firepaas_api_request_duration_seconds",
		"API request latency in seconds by route and method.")
	// P0#5（契约 C-1）：agent 客户端证书热重载的到期观测——每次加载/重载
	// 成功时记录 NotAfter，告警规则见 iac/observability/prometheus-alerts.yml。
	agentclient.NotAfterHook = func(expiry time.Time) {
		file := os.Getenv("FIREPAAS_AGENT_TLS_CERT")
		if file == "" {
			file = "agent-client-cert"
		}
		reg.Set("firepaas_tls_cert_not_after_seconds", map[string]string{"file": file}, uint64(expiry.Unix()))
	}
	// M5.2：单机宿主资源 gauge 采样（只读 /proc，15s 周期）。
	go func() {
		defer func() {
			if p := recover(); p != nil {
				slog.Error("host sampler panic", "panic", p)
			}
		}()
		hostSampler(ctx, reg)
	}()
	// v1.1（ADR-0018）：镜像亲和权重（默认 0.5；0 = 关闭）——与 R/K/α 同一
	// 配置面（Best-of-K 打分参数的可运维热调入口，v1.1 以 env 暴露）。
	// 注意：scheduler.New 把 WeightImage=0 视为“未配置”并归一化为默认值，
	// 所以“关闭”必须经 Options.ImageAffinityDisabled 显式传递。
	placerCfg := scheduler.DefaultBestOfKConfig()
	placerOpts := scheduler.Options{}
	if raw := os.Getenv("FIREPAAS_SCHED_WEIGHT_IMAGE"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v >= 0 {
			if v == 0 {
				placerOpts.ImageAffinityDisabled = true
			} else {
				placerCfg.WeightImage = v
			}
		} else {
			slog.Warn("invalid FIREPAAS_SCHED_WEIGHT_IMAGE, keeping default", "value", raw)
		}
	}
	placer := scheduler.New(placerCfg, placerOpts)

	// 每个 API 副本都维护只读 Nomad discovery + agent 连接池，使 follower 可
	// 直接服务 logs/exec/cp。ServiceInfo→PG 同步和 mutation controller 仍严格
	// 只在 leader 任期内运行，避免多个 nodemanager 并发写 observed projection。
	nodeInfoEvery := 20 * time.Second
	nm, err := nodemanager.New(nodemanager.Config{
		NomadAddr:     nomadAddr,
		JobName:       env.Get("FIREPAAS_AGENT_JOB_NAME", "firepaas-agentd"),
		DiscoverEvery: 10 * time.Second,
		InfoEvery:     nodeInfoEvery,
		Store:         st,
	})
	if err != nil {
		return fmt.Errorf("nodemanager: %w", err)
	}
	defer nm.Close()
	go func() {
		defer func() {
			if p := recover(); p != nil {
				slog.Error("node discovery goroutine panic", "panic", p)
			}
		}()
		if err := nm.RunDiscovery(ctx); err != nil && ctx.Err() == nil {
			slog.Error("node discovery exited", "error", err)
		}
	}()

	// M2a leader：controller（reconcile+放置）只在持锁实例运行；备实例只读待命。
	// P3-13：routeKicker 把 leader 内 controller 的即时重建能力暴露给重投影。
	// P1：选主走独立专用连接（不经业务池），池耗尽不卡抢锁/心跳/解锁。
	kicker := &httpapi.RouteKicker{}
	rgw := &httpapi.RuntimeGateway{}
	rgw.Set(nm.AgentRuntimeForMachine)
	go func() {
		// goroutine 入口 recover：进程仍持 HTTP 服务，panic 否则会造成无人
		// 感知的静默吃栈退出；与错误路径同语义（log 后退出）。
		defer func() {
			if p := recover(); p != nil {
				slog.Error("leader goroutine panic", "panic", p)
			}
		}()
		err := leader.Elect(ctx, pgURL, leader.Key, func(lctx context.Context) error {
			// ADR-0040 §24（W0-2）：单次解析后复用，避免 reconciler 与 controller 双读漂移。
			meshEnabled, err := parseMeshMode(env.Get("FIREPAAS_MESH", "disabled"))
			if err != nil {
				return err
			}
			go func() {
				if err := nm.RunServiceInfo(lctx); err != nil && lctx.Err() == nil {
					slog.Error("node service info sync exited", "error", err)
				}
			}()

			// ADR-0040 §18（T4b）：fabric 下发协调器（leader 写者纪律）。
			// 仅 FIREPAAS_MESH=eastwest 装配；agent 侧同开关联动（未启用的
			// 节点不上报公钥，不会被注册进 mesh）。
			if meshEnabled {
				edgeHub := fabric.EdgeHubConfig{
					NodeID:   env.Get("FIREPAAS_MESH_EDGE_ID", "edge-hub"),
					Pubkey:   strings.TrimSpace(os.Getenv("FIREPAAS_MESH_EDGE_PUBKEY")),
					Endpoint: os.Getenv("FIREPAAS_MESH_EDGE_ENDPOINT"),
				}
				fr, err := fabric.New(fabric.Config{
					Store:      st,
					NodeSource: nm,
					CellPrefix: env.Get("FIREPAAS_MESH_CELL_PREFIX", "fd7a:9a55::/40"),
					WgPort:     uint16(env.Int("FIREPAAS_MESH_WG_PORT", 51820)),
					// G2c：.internal 记录源（publisher 写 dns:internal:*，快照下发）。
					DNS: cat,
					// G2d（§14）：edge hub WG 注册（公钥/endpoint 由运维配置）+
					// mesh 寻址投影（edge 消费；nil = 不写，edge 回落 legacy）。
					EdgeHub:        edgeHub,
					IngressPort:    env.Int("FIREPAAS_AGENT_FABRIC_INGRESS_PORT", 5109),
					MeshProjection: cat,
				})
				if err != nil {
					return fmt.Errorf("fabric reconciler: %w", err)
				}
				go func() {
					if err := fr.Run(lctx); err != nil && lctx.Err() == nil {
						slog.Error("fabric reconciler exited", "error", err)
					}
				}()
			}

			ctrl := controller.New(st, cat, nm, resv, placer, reg, controller.Config{
				// R2 加固：派发有界并发（默认 4）与 operations 保留窗（默认 7d）。
				DispatchWorkers: env.Int("FIREPAAS_OP_DISPATCH_WORKERS", 4),
				OperationRetention: time.Duration(env.Int(
					"FIREPAAS_OPERATION_RETENTION_DAYS", 7)) * 24 * time.Hour,
				DefaultAppPort:       8080,
				LegacyAgentProxyAddr: legacyProxyAddr,
				// P1（独立评审）：dns:internal 投影 TTL 与 edge
				// FIREPAAS_EDGE_STALE_WINDOW 同源（默认 120s）。
				DNSStaleWindow:        env.Dur("FIREPAAS_DNS_STALE_WINDOW", 120*time.Second),
				OpPollInterval:        time.Second,
				SyncInterval:          5 * time.Second,
				RebuildInterval:       30 * time.Second,
				NodeStaleAfter:        3 * nodeInfoEvery, // 覆盖 nodemanager InfoEvery 三轮心跳
				ReconcileGrace:        30 * time.Second,
				NodeLossRecreateAfter: env.Dur("FIREPAAS_NODE_LOSS_RECREATE_AFTER", time.Minute),
				MaxPlacementAttempts:  3,
				RolloutTimeout:        env.Dur("FIREPAAS_ROLLOUT_TIMEOUT", 300*time.Second),
				RolloutDrainGrace:     env.Dur("FIREPAAS_ROLLOUT_DRAIN", 30*time.Second),
				Secrets:               secretsMgr,
				Traffic:               trafficSigner,
				// ADR-0040 T4c：与 fabric reconciler 同开关/同 cell 前缀；
				// mesh 未启用时派发跳过身份/ULA 分配（legacy 零回归）。
				FabricMesh: controller.FabricMeshConfig{
					Enabled:    meshEnabled,
					CellPrefix: env.Get("FIREPAAS_MESH_CELL_PREFIX", "fd7a:9a55::/40"),
				},
				// v1.1（ADR-0018/0021）：部署预取 top-K 与 evacuate 步超时。
				PrefetchTopK:        env.Int("FIREPAAS_PREFETCH_TOPK", 3),
				EvacuateStepTimeout: env.Dur("FIREPAAS_EVACUATE_STEP_TIMEOUT", 5*time.Minute),
				// ADR-0041：并发弹性节拍（默认 10s）与配额冻结（默认 100s）。
				AutoscaleInterval:    env.Dur("FIREPAAS_AUTOSCALE_INTERVAL", 10*time.Second),
				AutoscaleQuotaFreeze: env.Dur("FIREPAAS_AUTOSCALE_QUOTA_FREEZE", 100*time.Second),
				// v1.4（ADR-0036）：本地 GC 默认 off。delete 仅在 agent
				// 广告 lock-aware quarantine capability 后执行。
				UserEventsRetention: env.Dur("FIREPAAS_USER_EVENTS_RETENTION", 168*time.Hour),
				// review 2026-09-10：调度/对账事件保留期（此前无保留期）。
				SchedulerEventsRetention: env.Dur("FIREPAAS_SCHEDULER_EVENTS_RETENTION", 168*time.Hour),
				GC: controller.GCConfig{
					Mode:      env.Get("FIREPAAS_LOCAL_GC_MODE", "off"),
					MinAge:    env.Dur("FIREPAAS_GC_MIN_AGE", time.Hour),
					HighWater: envFraction("FIREPAAS_GC_HIGH_WATERMARK", 0.85),
					LowWater:  envFraction("FIREPAAS_GC_LOW_WATERMARK", 0.70),
					Interval:  env.Dur("FIREPAAS_GC_INTERVAL", 5*time.Minute),
					Grace:     env.Dur("FIREPAAS_LOCAL_GC_GRACE", 10*time.Minute),
				},
				Scrub: controller.ScrubConfig{
					Enabled:  env.Bool("FIREPAAS_SCRUB_ENABLED", false),
					Interval: env.Dur("FIREPAAS_SCRUB_INTERVAL", time.Hour),
					Budget:   env.Int("FIREPAAS_SCRUB_BUDGET", 1),
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
	images := imagepolicy.NewWithOptions(env.Get("FIREPAAS_REGISTRY_ALLOWLIST", ""),
		httpapi.IsTruthy(env.Get("FIREPAAS_IMAGE_REQUIRE_DIGEST", "false")))
	// R2 评审 P1（401 限流）：无效/撤销 key 尝试按来源 IP 令牌桶限流
	//（默认 20/min；0 = 关闭）。记录在案：桶空后的 429 不查 PG（hash only）。
	authThrottle := httpapi.NewAuthFailureThrottle(httpapi.ParseThrottleRate(
		os.Getenv("FIREPAAS_AUTH_FAILURE_RATE_PER_MINUTE"), 20))
	api := httpapi.New(httpapi.Config{
		Store: st, APIToken: apiToken, AuthDisabled: authDisabled,
		Images: images, AppCommands: appcommand.New(st, images),
		Secrets: secretsMgr, Traffic: trafficSigner, APIKeys: apiKeyMgr,
		Catalog: cat, Metrics: reg, Kicker: kicker, Runtime: rgw,
		Limiter: apiLimiter, Pool: pool, Redis: rdb, AuthThrottle: authThrottle,
		MetricsToken:  env.Get("FIREPAAS_METRICS_TOKEN", ""),
		Version:       buildVersion,
		PrewarmLimits: prewarmLimitsFromEnv(),
	})
	mux := http.NewServeMux()
	httpapi.Register(mux, api)

	// P1：HTTP server 显式超时与 panic recover。ReadTimeout/WriteTimeout 不设
	// （logs/exec/cp 是流式或 hijack 响应，wait 系列是长轮询，整体写超时会误杀）；
	// 仅以 ReadHeaderTimeout 防 Slowloris，IdleTimeout 回收空闲 keep-alive。
	srv := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           httpapi.Wrap(mux, reg),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
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

// prewarmLimitsFromEnv 在装配期解析 v1.4-C 准入上限（env 只收紧默认值）。
func prewarmLimitsFromEnv() httpapi.PrewarmLimits {
	l := httpapi.DefaultPrewarmLimits()
	if v := env.Int("FIREPAAS_PREWARM_MAX_TARGET_NODES", 0); v > 0 {
		l.MaxTargetNodes = v
	}
	if v := env.Int("FIREPAAS_PIN_MAX_TTL_SECONDS", 0); v > 0 {
		l.MaxPinTTL = time.Duration(v) * time.Second
	}
	if v := env.Int("FIREPAAS_PIN_MAX_BYTES_MIB", 0); v > 0 {
		l.MaxPinnedBytesMib = int64(v)
	}
	if v := env.Int("FIREPAAS_PREWARM_MAX_ACTIVE", 0); v > 0 {
		l.MaxActivePrewarms = v
	}
	if v := env.Float("FIREPAAS_PIN_HARD_WATERMARK", 0); v > 0 && v < 1 {
		l.HardWatermarkFrac = v
	}
	if v := env.Int("FIREPAAS_PIN_MAX_PER_PROJECT", 0); v > 0 {
		l.MaxPinsPerProject = v
	}
	return l
}

// parseMeshMode 解析 FIREPAAS_MESH（ADR-0040 §24，W0-2）：G1 只接受
// disabled/eastwest；未知值（含 full 与拼写错误）fail-closed 报错，
// 与 agentd 同行为，避免控制面静默 legacy 而 agent 崩溃循环的脑裂半启用。
func parseMeshMode(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "disabled":
		return false, nil
	case "eastwest":
		return true, nil
	default:
		return false, fmt.Errorf("FIREPAAS_MESH=%q unsupported (accepts: disabled, eastwest)", v)
	}
}

// envFraction 解析 (0,1) 区间的浮点环境变量（非法/越界回退默认并告警）。
// 解析复用 shared/pkg/env.Float，区间检查保留本地（与旧 envFloat 同语义）。
func envFraction(key string, def float64) float64 {
	f := env.Float(key, def)
	if f <= 0 || f >= 1 {
		if raw := os.Getenv(key); raw != "" {
			slog.Warn("invalid env value; using default", "key", key, "value", raw, "default", def)
		}
		return def
	}
	return f
}
