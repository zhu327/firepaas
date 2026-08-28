// secrets.go：M4 API —— secrets v1（ADR-0010）与 execution-bound credential。
//
// 红线：
//   - 任何端点不返回 secret 明文（无 reveal）；值仅经 create 链路进 VM；
//   - 审计行只记 secret id/name/version；traffic-token 响应体绝不落日志
//     （auditMiddleware 本就不记 body/	query，字段黑名单兜底）。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/example/firepaas/internal/controlplane/store"
	"github.com/example/firepaas/internal/security/redact"
)

// auditMiddleware 为每个请求输出一行结构化审计：时间/方法/路径（无 query/
// 无 body）/状态/耗时。
//
// 防御深度（P3-17）：审计字段经 redact.RedactMap 过滤后再输出——当前
// 字段全部安全，但未来住新增字段（如 header 快照、request 摘要）命中
// 黑名单（secret_env/proxy_credential/authorization/…）会自动打码，
// 而不是靠评审记住约定。
func auditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &auditRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		fields := redact.RedactMap(map[string]any{
			"method":      r.Method,
			"path":        r.URL.Path,
			"status":      rec.status,
			"duration_ms": time.Since(start).Milliseconds(),
			// P1-1：调用方标识由 auth wrapper 经响应头传出（context 不可变，
			// 外层中间件读不到内层注入的 identity）。
			"caller": rec.Header().Get("X-Firepaas-Caller"),
			// 显式约定：query/body/authorization 永不入审计。
		})
		if v, ok := fields["caller"]; ok && v == "" {
			delete(fields, "caller")
		}
		args := make([]any, 0, len(fields)*2)
		for k, v := range fields {
			args = append(args, k, v)
		}
		slog.Info("http audit", args...)
	})
}

type auditRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (a *auditRecorder) WriteHeader(code int) {
	if !a.wroteHeader {
		a.status = code
		a.wroteHeader = true
	}
	a.ResponseWriter.WriteHeader(code)
}

// ---- secrets v1 ----

type putSecretBody struct {
	ProjectID string `json:"project_id"`
	Name      string `json:"name"`
	Value     string `json:"value"` // 只在请求内存在，立即加密，绝不回显
	CreatedBy string `json:"created_by"`
}

func (a *API) secretsEnabled(w http.ResponseWriter) bool {
	if a.secrets == nil {
		writeErr(w, 503, "secrets disabled: FIREPAAS_SECRETS_MASTER_KEY not configured")
		return false
	}
	return true
}

func parseVersionQuery(r *http.Request) (*int64, error) {
	v := r.URL.Query().Get("version")
	if v == "" {
		return nil, nil
	}
	var n int64
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 1 {
		return nil, fmt.Errorf("bad version %q", v)
	}
	return &n, nil
}

func (a *API) putSecret(w http.ResponseWriter, r *http.Request) {
	if !a.secretsEnabled(w) {
		return
	}
	var body putSecretBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	// P1-2：受限 key 只能写自己 project（body.project_id 不可越权指定）。
	project, ok := clampBodyProject(r, body.ProjectID)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	body.ProjectID = project
	if body.ProjectID == "" || body.Name == "" || body.Value == "" {
		writeErr(w, 400, "project_id, name and value are required")
		return
	}
	ctx := r.Context()
	// 先取版本号再加密：AAD 把密文绑定到 (project, name, version)，防止
	// 行间密文互换。并发同 name 写入靠 UNIQUE 冲突失败，客户端重试即可。
	version, err := a.store.NextSecretVersion(ctx, body.ProjectID, body.Name)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	sealed, err := a.secrets.Seal([]byte(body.Value), body.ProjectID, body.Name, version)
	if err != nil {
		writeErr(w, 500, "seal secret: "+err.Error())
		return
	}
	meta, err := a.store.PutSecretVersion(ctx, body.ProjectID, body.Name, version,
		sealed.Ciphertext, sealed.WrappedDEK, body.CreatedBy)
	if err != nil {
		if errors.Is(err, store.ErrSecretVersionConflict) {
			// P3-15：并发写入同 name 撞版本号 → 409 + Retry-After（客户端重试
			// 会取到新版本号，不是永久错误）。
			w.Header().Set("Retry-After", "1")
			writeErr(w, 409, "concurrent write to the same secret; retry with a new version")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	body.Value = "" // 尽快丢弃明文引用
	slog.Info("secret version created", "secret_id", meta.ID,
		"name", meta.Name, "version", meta.Version)
	writeJSON(w, 201, meta)
}

func (a *API) listSecrets(w http.ResponseWriter, r *http.Request) {
	if !a.secretsEnabled(w) {
		return
	}
	metas, err := a.store.ListSecrets(r.Context(), effectiveProjectID(r, "dev"))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if metas == nil {
		metas = []store.SecretMeta{}
	}
	writeJSON(w, 200, metas)
}

func (a *API) getSecretMeta(w http.ResponseWriter, r *http.Request) {
	if !a.secretsEnabled(w) {
		return
	}
	ver, err := parseVersionQuery(r)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	meta, err := a.store.GetSecretMeta(r.Context(), effectiveProjectID(r, "dev"), r.PathValue("name"), ver)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, meta)
}

func (a *API) deleteSecret(w http.ResponseWriter, r *http.Request) {
	if !a.secretsEnabled(w) {
		return
	}
	name := r.PathValue("name")
	n, err := a.store.DeleteSecret(r.Context(), effectiveProjectID(r, "dev"), name)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	slog.Info("secret deleted", "name", name, "versions_removed", n)
	writeJSON(w, 200, map[string]any{"name": name, "versions_removed": n,
		"note": "values already injected into running machines are unaffected; later recreations fail until refs updated"})
}

// ---- execution-bound proxy credential（ADR-0006） ----

// trafficToken 按需现算 machine 当前 execution 的 credential 给 edge。
// 凭证不在任何地方持久化；edge 收到后只在内存保存、逐请求携带。
func (a *API) trafficToken(w http.ResponseWriter, r *http.Request) {
	if a.traffic == nil {
		writeErr(w, 503, "traffic token signer disabled: FIREPAAS_TRAFFIC_TOKEN_KEY not configured")
		return
	}
	machineID := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	if m.DesiredState == "DELETED" || m.CurrentExecutionID == "" {
		writeErr(w, 410, "machine has no active execution")
		return
	}
	writeJSON(w, 200, map[string]string{
		"machine_id":   machineID,
		"execution_id": m.CurrentExecutionID,
		"token":        a.traffic.Token(machineID, m.CurrentExecutionID),
	})
}
