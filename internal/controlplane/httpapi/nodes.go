package httpapi

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
	// review 2026-09-10：节点排水属于集群级运维；项目 owner 的 admin scope
	// 不得触达（中间件只校验 capability，此处复核全局身份）。
	if !a.requireGlobalIdentity(w, r) {
		return
	}
	id := r.PathValue("id")
	if err := a.store.SetNodeDraining(r.Context(), id, draining, evacuate); err != nil {
		if errors.Is(err, store.ErrEvacuationBusy) {
			// 集群级单 evacuate 互斥（ADR-0021：不做并发多节点驱离）。
			writeErr(w, 409, "another node evacuation is already in progress")
			return
		}
		// 仅确证 not-found 才 404；PG 故障等内部错误不再伪装成“节点不存在”。
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, 404, "node not found: "+id)
			return
		}
		writeInternalErr(w, r, err)
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
//
// review 2026-09-10：node_ids 属于平台拓扑；受限 project key 只应看到
// 聚合计数（与 images coverage 的脱敏口径一致），全局身份保留完整视图。
func (a *API) listCapabilities(w http.ResponseWriter, r *http.Request) {
	global := identityIsGlobal(identFrom(r))
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeInternalErr(w, r, err)
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
		if !global {
			e.NodeIDs = nil
		}
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FeatureID < out[j].FeatureID })
	writeJSON(w, 200, map[string]any{
		"capabilities":   out,
		"eligible_nodes": eligibleTotal,
		"nodes_total":    len(nodes),
	})
}
