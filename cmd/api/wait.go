// wait.go：v1.2-D（ADR-0026）有界 wait API。
//
//	GET /v1/machines/{id}/wait?execution_id=X&timeout_ms=N
//	GET /v1/operations/{id}/wait?timeout_ms=N
//	GET /v1/rollouts/{id}/wait?generation=Y&timeout_ms=N
//
// PG 状态是结果权威；进程内固定轮询兑底（v1.2 不引入 LISTEN/NOTIFY）。
// 最大等待 5 分钟；客户端断开不取消底层 operation。
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/security/redact"
)

const (
	waitMaxTimeout = 5 * time.Minute
	waitPollEvery  = 250 * time.Millisecond
)

func waitTimeout(r *http.Request) time.Duration {
	ms := 30_000
	if v := r.URL.Query().Get("timeout_ms"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ms = n
		}
	}
	if ms <= 0 {
		ms = 30_000
	}
	d := time.Duration(ms) * time.Millisecond
	if d > waitMaxTimeout {
		d = waitMaxTimeout
	}
	return d
}

// waitMachine 等待 machine 的指定 execution READY。generation/execution 被
// 替换时返回 superseded，不能把旧代到达目标误判为成功。
func (a *API) waitMachine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wantExec := r.URL.Query().Get("execution_id")
	if wantExec == "" {
		writeErr(w, 400, "execution_id query parameter is required")
		return
	}
	deadline := time.Now().Add(waitTimeout(r))
	ticker := time.NewTicker(waitPollEvery)
	defer ticker.Stop()
	for {
		m, err := a.store.GetMachine(r.Context(), id)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if m == nil {
			writeErr(w, 404, "machine not found")
			return
		}
		if m.CurrentExecutionID != wantExec {
			writeJSON(w, 200, map[string]any{
				"outcome": "superseded", "status": "superseded", "execution_id": m.CurrentExecutionID,
				"generation": m.Generation, "observed_state": m.ObservedState,
			})
			return
		}
		switch {
		case m.DesiredState == "DELETED":
			writeJSON(w, 200, map[string]any{"outcome": "terminal", "status": "terminal",
				"terminal_reason": "deleted", "execution_id": m.CurrentExecutionID,
				"generation": m.Generation})
			return
		case m.RestartBlocked:
			writeJSON(w, 200, map[string]any{"outcome": "terminal", "status": "terminal",
				"terminal_reason": "restart_blocked", "execution_id": m.CurrentExecutionID,
				"generation": m.Generation})
			return
		case m.ObservedState == "STOPPED" && m.RestartMode == "NEVER":
			writeJSON(w, 200, map[string]any{"outcome": "terminal", "status": "terminal",
				"terminal_reason": "stopped", "execution_id": m.CurrentExecutionID,
				"generation": m.Generation})
			return
		case machineReady(m):
			// 目标语义是“execution X ready”（ADR-0026 §10）：RUNNING 但未
			// READY 的 machine 不能提前返回 reached；UNCONFIGURED 等价
			// READY（与 route 发布判定对齐，ADR-0008）。
			writeJSON(w, 200, map[string]any{"outcome": "reached", "status": "reached",
				"execution_id": m.CurrentExecutionID, "generation": m.Generation,
				"observed_state": m.ObservedState, "readiness": m.ObservedReadiness})
			return
		}
		if time.Now().After(deadline) {
			writeJSON(w, 200, map[string]any{"outcome": "timed_out", "status": "timed_out",
				"execution_id": m.CurrentExecutionID, "generation": m.Generation,
				"observed_state": m.ObservedState})
			return
		}
		select {
		case <-r.Context().Done():
			return // 客户端断开不取消底层 operation
		case <-ticker.C:
		}
	}
}

// machineReady 判定 machine 是否达到“ready”目标（v1.2-D，ADR-0008/0026）。
func machineReady(m *store.Machine) bool {
	if m.ObservedState != "RUNNING" && m.ObservedState != "PAUSED" {
		return false
	}
	switch m.ObservedReadiness {
	case "READY", "UNCONFIGURED":
		return true
	default: // UNKNOWN / NOT_READY / 空
		return false
	}
}

// waitOperation 等待 operation 进入终态（SUCCEEDED|FAILED）。
func (a *API) waitOperation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	project := effectiveProjectID(r, "dev")
	deadline := time.Now().Add(waitTimeout(r))
	ticker := time.NewTicker(waitPollEvery)
	defer ticker.Stop()
	for {
		op, err := a.store.GetOperation(r.Context(), project, id)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if op == nil {
			writeErr(w, 404, "operation not found")
			return
		}
		if operationTerminal(op.Status) {
			writeJSON(w, 200, map[string]any{"outcome": "terminal", "status": "terminal",
				"operation_status": op.Status, "error": redact.RedactText(op.Error),
				"machine_id": op.MachineID, "execution_id": op.ExecutionID})
			return
		}
		if time.Now().After(deadline) {
			writeJSON(w, 200, map[string]any{"outcome": "timed_out", "status": "timed_out",
				"operation_status": op.Status, "machine_id": op.MachineID})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func operationTerminal(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED", "SUPERSEDED", "TIMED_OUT":
		return true
	default:
		return false
	}
}

// waitRollout 等待 rollout generation Y 到达终态。
func (a *API) waitRollout(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v := r.URL.Query().Get("generation")
	if v == "" {
		writeErr(w, 400, "generation query parameter is required")
		return
	}
	wantGen, err := strconv.ParseInt(v, 10, 64)
	if err != nil || wantGen <= 0 {
		writeErr(w, 400, "generation query parameter must be positive")
		return
	}
	deadline := time.Now().Add(waitTimeout(r))
	ticker := time.NewTicker(waitPollEvery)
	defer ticker.Stop()
	for {
		rl, err := a.store.GetRollout(r.Context(), id)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if rl == nil {
			writeErr(w, 404, "rollout not found")
			return
		}
		if rl.ToGeneration != wantGen {
			writeJSON(w, 200, map[string]any{"outcome": "superseded", "status": "superseded",
				"to_generation": rl.ToGeneration, "rollout_status": rl.Status})
			return
		}
		if rolloutTerminal(rl.Status) {
			writeJSON(w, 200, map[string]any{"outcome": "terminal", "status": "terminal",
				"rollout_status": rl.Status, "failed": rl.Failed,
				"to_generation": rl.ToGeneration})
			return
		}
		if time.Now().After(deadline) {
			writeJSON(w, 200, map[string]any{"outcome": "timed_out", "status": "timed_out",
				"rollout_status": rl.Status, "to_generation": rl.ToGeneration})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func rolloutTerminal(status string) bool {
	switch status {
	case "COMPLETE", "FAILED", "CANCELLED", "SUPERSEDED":
		return true
	default:
		return false
	}
}

// machineTTLBody 是 TTL 更新请求体。
type machineTTLBody struct {
	TTLSeconds int64 `json:"ttl_seconds"` // 0 = 关闭 TTL
}

// updateMachineTTL 更新到期时间；已删除/已过期 machine 拒绝复活。
func (a *API) updateMachineTTL(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body machineTTLBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.TTLSeconds < 0 {
		writeErr(w, 400, "ttl_seconds must be >= 0")
		return
	}
	var expires *time.Time
	if body.TTLSeconds > 0 {
		t := time.Now().Add(time.Duration(body.TTLSeconds) * time.Second)
		expires = &t
	}
	if err := a.store.SetMachineExpiry(r.Context(), id, expires); err != nil {
		switch {
		case errors.Is(err, store.ErrMachineLifecycleClosed):
			writeErr(w, 409, "machine deleted or expired; TTL update rejected")
		case errors.Is(err, store.ErrMachineNotFound):
			writeErr(w, 404, "machine not found: "+id)
		default:
			writeErr(w, 500, "set machine expiry: "+err.Error())
		}
		return
	}
	writeJSON(w, 200, map[string]any{"machine_id": id, "expires_at": expires, "ttl_seconds": body.TTLSeconds})
}

// resetRestart 管理员清零 restart attempts（RESTART_BLOCKED → 可重启）。
func (a *API) resetRestart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.store.ResetRestartAttempts(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrMachineNotFound) {
			writeErr(w, 404, "machine not found: "+id)
		} else {
			writeErr(w, 500, err.Error())
		}
		return
	}
	writeJSON(w, 200, map[string]any{"machine_id": id, "restart_attempts": 0, "restart_blocked": false})
}
