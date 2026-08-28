package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/example/firepaas/internal/controlplane/store"
)

// drainNode / readyNode：M5.5（mvp-plan §9.5）节点排水开关。
// draining=true → 调度停止放置新车，存量流量不受影响；ready 复原。
//
// v1.1（ADR-0021）：POST /v1/nodes/{id}/drain 请求体增加 {"evacuate": bool}
// （默认 false = M5.5 兼容语义）。evacuate=true 时 controller 逐实例驱离存量
// machine（换代重建到其它节点），节点 machine 归零后可安全维护/升级。
type drainBody struct {
	Evacuate bool `json:"evacuate"`
}

func (a *API) setNodeDraining(w http.ResponseWriter, r *http.Request, draining bool, evacuate bool) {
	id := r.PathValue("id")
	if err := a.store.SetNodeDraining(r.Context(), id, draining, evacuate); err != nil {
		if errors.Is(err, store.ErrEvacuationBusy) {
			// 集群级单 evacuate 互斥（ADR-0021：不做并发多节点驱离）。
			writeErr(w, 409, "another node evacuation is already in progress")
			return
		}
		writeErr(w, 404, "node not found: "+id)
		return
	}
	state := "ready"
	if draining {
		if evacuate {
			state = "draining+evacuate"
		} else {
			state = "draining"
		}
	}
	slog.Info("node drain state changed", "node_id", id, "state", state)
	writeJSON(w, 200, map[string]string{"id": id, "status": state})
}

func (a *API) drainNode(w http.ResponseWriter, r *http.Request) {
	var body drainBody
	// 空 body / 非 JSON body = {"evacuate": false}（M5.5 兼容）。
	if raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			writeErr(w, 400, "bad request: "+err.Error())
			return
		}
	}
	a.setNodeDraining(w, r, true, body.Evacuate)
}

func (a *API) readyNode(w http.ResponseWriter, r *http.Request) {
	a.setNodeDraining(w, r, false, false)
}
