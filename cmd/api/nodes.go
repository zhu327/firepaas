package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"

	"github.com/zhu327/firepaas/internal/controlplane/store"
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

// listCapabilities 返回集群能力汇总（v1.2-A，ADR-0023）：每项 feature 的
// 可放置（HEALTHY 且非 draining）节点数与节点 ID 列表；不把节点能力并集
// 伪装成“整个集群支持”。
func (a *API) listCapabilities(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	type capEntry struct {
		FeatureID     string   `json:"feature_id"`
		EligibleNodes int      `json:"eligible_nodes"`
		NodeIDs       []string `json:"node_ids"`
	}
	byFeature := map[string]*capEntry{}
	eligibleTotal := 0
	for i := range nodes {
		n := &nodes[i]
		eligible := n.Status == "HEALTHY" && !n.Draining
		if !eligible {
			continue
		}
		eligibleTotal++
		seen := map[string]bool{}
		for _, id := range n.FeatureIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			e, ok := byFeature[id]
			if !ok {
				e = &capEntry{FeatureID: id}
				byFeature[id] = e
			}
			e.EligibleNodes++
			e.NodeIDs = append(e.NodeIDs, n.ID)
		}
	}
	out := make([]capEntry, 0, len(byFeature))
	for _, e := range byFeature {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FeatureID < out[j].FeatureID })
	writeJSON(w, 200, map[string]any{
		"capabilities":   out,
		"eligible_nodes": eligibleTotal,
		"nodes_total":    len(nodes),
	})
}
