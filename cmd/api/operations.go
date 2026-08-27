// operations.go：M5.3 操作追踪端点（mvp-plan §9.3 operation trace）。
//
//	GET /v1/operations?machine_id=&kind=&status=&limit=    列表（read scope）
//	GET /v1/operations/{id}                                详情
//
// 安全：request/result 经 redact.RedactJSONBytes 全树脱敏后才出（M4 单向下发
// 字段绝不回显）；error 也可能带回显信息（当前为错误文案，无明文凭证）。
package main

import (
	"net/http"
	"strconv"

	"github.com/example/firepaas/internal/controlplane/store"
	"github.com/example/firepaas/internal/security/redact"
)

func (a *API) listOperations(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	ops, err := a.store.ListOperations(r.Context(), q.Get("machine_id"), q.Get("kind"), q.Get("status"), limit)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(ops))
	for _, op := range ops {
		out = append(out, opJSON(op))
	}
	writeJSON(w, 200, map[string]any{"operations": out})
}

func (a *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := a.store.GetOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 404, "operation not found")
		return
	}
	writeJSON(w, 200, opJSON(*op))
}

// opJSON 输出 trace 字段；request/result/error 全树脱敏。
func opJSON(op store.OperationTrace) map[string]any {
	fields := map[string]any{
		"id":            op.ID,
		"project_id":    op.ProjectID,
		"machine_id":    op.MachineID,
		"execution_id":  op.ExecutionID,
		"generation":    op.Generation,
		"kind":          op.Kind,
		"status":        op.Status,
		"dispatch_node": op.DispatchNodeID,
		"attempts":      op.Attempts,
		"created_at":    op.CreatedAt,
		"updated_at":    op.UpdatedAt,
		"claimed_at":    op.ClaimedAt,
		"completed_at":  op.CompletedAt,
		"request":       string(redact.RedactJSONBytes(op.Request)),
		"result":        string(redact.RedactJSONBytes(op.Result)),
		"error":         op.Error,
	}
	return fields
}
