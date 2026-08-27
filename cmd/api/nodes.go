package main

import (
	"log/slog"
	"net/http"
)

// drainNode / readyNode：M5.5（mvp-plan §9.5）节点排水开关。
// draining=true → 调度停止放置新车，存量流量不受影响；ready 复原。
func (a *API) setNodeDraining(w http.ResponseWriter, r *http.Request, draining bool) {
	id := r.PathValue("id")
	if err := a.store.SetNodeDraining(r.Context(), id, draining); err != nil {
		writeErr(w, 404, "node not found: "+id)
		return
	}
	state := "ready"
	if draining {
		state = "draining"
	}
	slog.Info("node drain state changed", "node_id", id, "state", state)
	writeJSON(w, 200, map[string]string{"id": id, "status": state})
}

func (a *API) drainNode(w http.ResponseWriter, r *http.Request) {
	a.setNodeDraining(w, r, true)
}

func (a *API) readyNode(w http.ResponseWriter, r *http.Request) {
	a.setNodeDraining(w, r, false)
}
