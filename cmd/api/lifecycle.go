// lifecycle.go：M4.5 scale-to-zero 的 API 面（mvp-plan §8.4）。
//
//	POST /v1/machines/{id}/pause   入队 pause 操作（standby 快照）
//	POST /v1/machines/{id}/resume  入队 resume 操作（snapshot 恢复；快照损坏
//	                               时 controller 自动转 cold-start 重建）
//
// 说明：自动 idle 检测（CPU 阈值 × 时长）需要每机 usage 管道（agent List
// 暴露 VM metrics → PG 投影），v1 以显式 API + proxy autoresume 组合交付；
// 自动化判定记入 mvp-plan §8 执行记录的遗留项。
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func (a *API) enqueueLifecycle(w http.ResponseWriter, r *http.Request, kind string) {
	machineID := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	if m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	if m.DesiredState == "DELETED" {
		writeErr(w, 410, "machine is deleted")
		return
	}
	if m.CurrentExecutionID == "" {
		writeErr(w, 409, "machine has no active execution")
		return
	}
	// v1.2-B（ADR-0024 §9）：接收过 secret 的 execution 禁止 memory snapshot
	//（standby/pause 会快照 guest RAM，含 secret tmpfs）。agent 侧同样拒绝
	//（双保险，防 PG 滞后）。
	if kind == "pause" {
		hasSecret, err := a.store.MachineHasActiveSecretDelivery(r.Context(),
			m.ID, m.CurrentExecutionID)
		if err != nil {
			// secret 判定失败必须 fail closed——误判“无 secret”意味着对含敏感
			// tmpfs 的 VM 做 memory snapshot。
			writeInternalErr(w, r, err)
			return
		}
		if hasSecret {
			writeErr(w, 409, "machine received one-shot secrets; pause/standby forbidden for this execution (ADR-0024)")
			return
		}
	}
	project, err := a.store.ProjectForApp(r.Context(), m.AppID)
	if err != nil || project == "" {
		project = "dev"
	}
	// 幂等键含秒级序号：同秒重试幂等，跨秒可再次下发（如 resume 失败后重试）。
	opID := fmt.Sprintf("op-%s-%s-%s-%d", kind, machineID, uuid.NewString()[:8], time.Now().Unix())
	req := fmt.Sprintf(`{"machine_id":%q,"execution_id":%q}`, machineID, m.CurrentExecutionID)
	op, err := a.store.EnqueueLifecycle(r.Context(), project, machineID,
		m.CurrentExecutionID, opID, m.Generation, kind, []byte(req))
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	slog.Info("lifecycle op enqueued", "kind", kind, "machine_id", machineID,
		"operation_id", op.ID)
	writeJSON(w, 202, map[string]string{
		"machine_id": machineID, "operation_id": op.ID,
		"kind": op.Kind, "status": op.Status,
	})
}

func (a *API) pauseMachine(w http.ResponseWriter, r *http.Request) {
	a.enqueueLifecycle(w, r, "pause")
}

func (a *API) resumeMachine(w http.ResponseWriter, r *http.Request) {
	a.enqueueLifecycle(w, r, "resume")
}
