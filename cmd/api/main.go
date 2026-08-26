// Command api 是 firepaas 控制面入口（M1.5 单实例 vertical slice）。
//
// 目标形态（mvp-plan §5.4）：
//   - REST：machines 最小 CRUD（apps/deployments 完整模型在 M3）
//   - PG desired/operations 权威，controller 调 agent，Redis 路由投影
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/example/firepaas/internal/controlplane/controller"
	"github.com/example/firepaas/internal/controlplane/db"
	"github.com/example/firepaas/internal/controlplane/store"
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
	agentAddr := envOr("FIREPAAS_AGENT_ADDR", "127.0.0.1:5108")
	agentProxyAddr := envOr("FIREPAAS_AGENT_PROXY_ADDR", "127.0.0.1:5107")

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

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	cat := catalog.New(rdb)

	ctrl, err := controller.New(ctx, st, cat, controller.Config{
		AgentAddr:      agentAddr,
		AgentProxyAddr: agentProxyAddr,
		DefaultAppPort: 8080,
		OpPollInterval: time.Second,
		SyncInterval:   5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer ctrl.Close()
	go func() {
		if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("controller exited", "error", err)
		}
	}()

	api := &API{store: st, apiToken: apiToken, authDisabled: authDisabled}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/machines", api.auth(api.createMachine))
	mux.HandleFunc("GET /v1/machines", api.auth(api.listMachines))
	mux.HandleFunc("GET /v1/machines/{id}", api.auth(api.getMachine))
	mux.HandleFunc("DELETE /v1/machines/{id}", api.auth(api.deleteMachine))

	srv := &http.Server{Addr: ":" + httpPort, Handler: mux}
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

// API 是 M1.5 最小 HTTP 服务。
type API struct {
	store        *store.Store
	apiToken     string
	authDisabled bool
}

type createMachineBody struct {
	MachineID    string            `json:"machine_id"`
	Hostname     string            `json:"hostname"`
	Image        string            `json:"image"`
	VCPU         int64             `json:"vcpu"`
	MemMIB       int64             `json:"mem_mib"`
	Port         int               `json:"port"`
	ProjectID    string            `json:"project_id"`
	AppID        string            `json:"app_id"`
	DeploymentID string            `json:"deployment_id"`
	ExecutionID  string            `json:"execution_id"`
	Generation   int64             `json:"generation"`
	OperationID  string            `json:"operation_id"`
	Env          map[string]string `json:"env"`
}

func (a *API) createMachine(w http.ResponseWriter, r *http.Request) {
	var body createMachineBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.MachineID == "" || body.Hostname == "" || body.Image == "" || body.OperationID == "" {
		writeErr(w, 400, "machine_id, hostname, image and operation_id are required")
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

	req := &pb.CreateMachineRequest{
		MachineId:   body.MachineID,
		Generation:  uint64(body.Generation),
		OperationId: body.OperationID,
		Spec: &pb.MachineSpec{
			ProjectId:    body.ProjectID,
			AppId:        body.AppID,
			DeploymentId: body.DeploymentID,
			ExecutionId:  body.ExecutionID,
			Hostname:     body.Hostname,
			ImageRef:     body.Image,
			Vcpu:         uint64(body.VCPU),
			MemMib:       uint64(body.MemMIB),
			Env:          body.Env,
			Network:      &pb.NetworkSpec{IngressPort: uint64(body.Port)},
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
		body.Generation, raw)
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
	project := r.URL.Query().Get("project_id")
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

// auth 校验 Bearer token（常数时间比较）。认证默认开启；仅显式设置
// FIREPAAS_AUTH_DISABLED=true（本地开发）时跳过（评审 P1-1）。
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	if a.authDisabled || a.apiToken == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(a.apiToken)) != 1 {
			writeErr(w, 401, "unauthorized")
			return
		}
		next(w, r)
	}
}

func isTruthy(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
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
