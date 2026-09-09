// eastwest.go：G2a API —— EastWestPolicy 租户自助 CRUD（ADR-0040 §15）。
//
// 授权语义（D1）：规则表达“谁能连谁”，开放动作作用于 dst 侧——调用方必须
// 拥有 dst_project（受限 key 按 clampBodyProject 钳制；unscoped admin 可
// 指定任意 project）。src_project 不钳制（dst owner 自主决定向谁开放）。
//
// generation 由服务端派生（既有行 +1，无行 1）：客户端无需协调水位；并发
// PUT 同键撞 fencing 时一方 409，重试即收敛。删除幂等（行不存在返回
// deleted=false），水位过期删除返回 409。
package main

import (
	"encoding/json"
	"errors"
	"net/http"

	agentv1 "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type eastWestRuleBody struct {
	SrcProject string   `json:"src_project"`
	SrcApp     string   `json:"src_app"`
	DstProject string   `json:"dst_project"`
	DstApp     string   `json:"dst_app"`
	DstService string   `json:"dst_service"`
	Ports      []uint32 `json:"ports"`
}

// parseEastWestRuleBody 校验并归一化一条规则（纯函数，无 DB，可单测）。
// 端口校验复用 contracts 口径（EastWestPolicySpec 单规则形态）。
func parseEastWestRuleBody(body eastWestRuleBody) (store.EastWestPolicy, error) {
	var zero store.EastWestPolicy
	// 输入端口先归一化（去重保序），再按 contracts 口径校验——API 层宽容，
	// 线上契约（agent 侧）保持严格。
	ports := make([]int32, 0, len(body.Ports))
	seen := map[uint32]bool{}
	wirePorts := make([]uint32, 0, len(body.Ports))
	for _, p := range body.Ports {
		if !seen[p] {
			seen[p] = true
			ports = append(ports, int32(p))
			wirePorts = append(wirePorts, p)
		}
	}
	spec := &pb.EastWestPolicySpec{
		Generation: 1,
		Rules: []*pb.EastWestPolicyRule{{
			SrcProject: body.SrcProject, SrcApp: body.SrcApp,
			DstProject: body.DstProject, DstApp: body.DstApp,
			DstService: body.DstService, Ports: wirePorts,
		}},
	}
	if err := agentv1.ValidateEastWestPolicy(spec); err != nil {
		return zero, err
	}
	return store.EastWestPolicy{
		SrcProject: body.SrcProject, SrcApp: body.SrcApp,
		DstProject: body.DstProject, DstApp: body.DstApp,
		DstService: body.DstService, Ports: ports,
	}, nil
}

type eastWestRuleResponse struct {
	SrcProject string  `json:"src_project"`
	SrcApp     string  `json:"src_app"`
	DstProject string  `json:"dst_project"`
	DstApp     string  `json:"dst_app"`
	DstService string  `json:"dst_service"`
	Ports      []int32 `json:"ports"`
	Generation int64   `json:"generation"`
}

func eastWestRuleResponseFrom(p store.EastWestPolicy) eastWestRuleResponse {
	return eastWestRuleResponse{
		SrcProject: p.SrcProject, SrcApp: p.SrcApp,
		DstProject: p.DstProject, DstApp: p.DstApp,
		DstService: p.DstService, Ports: p.Ports, Generation: p.Generation,
	}
}

// putEastWestPolicy upsert 一条东西向放行规则。
func (a *API) putEastWestPolicy(w http.ResponseWriter, r *http.Request) {
	var body eastWestRuleBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	// dst 侧归属钳制：受限调用方只能开放自己的 project。
	dstProject, ok := clampBodyProject(r, body.DstProject)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	body.DstProject = dstProject
	rule, err := parseEastWestRuleBody(body)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	ctx := r.Context()
	// generation 服务端派生：既有水位 +1（并发撞 fencing 返回 409，客户端重试）。
	var generation int64 = 1
	if existing, lerr := a.store.ListEastWestPolicies(ctx); lerr != nil {
		writeInternalErr(w, r, lerr)
		return
	} else {
		for _, e := range existing {
			if e.SrcProject == rule.SrcProject && e.SrcApp == rule.SrcApp &&
				e.DstProject == rule.DstProject && e.DstApp == rule.DstApp &&
				e.DstService == rule.DstService && e.Generation >= generation {
				generation = e.Generation + 1
			}
		}
	}
	rule.Generation = generation
	if err := a.store.PutEastWestPolicy(ctx, rule); err != nil {
		if errors.Is(err, store.ErrFabricGenerationStale) {
			w.Header().Set("Retry-After", "1")
			writeErr(w, 409, "concurrent eastwest write; retry")
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, eastWestRuleResponseFrom(rule))
}

// listEastWestPolicies 按 dst 列规则（dst_project 必填并钳制）。
func (a *API) listEastWestPolicies(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dstProject, ok := clampBodyProject(r, q.Get("dst_project"))
	if !ok || dstProject == "" {
		writeErr(w, 403, "dst_project is required and must be within your scope")
		return
	}
	dstApp := q.Get("dst_app")
	dstService := q.Get("dst_service")
	all, err := a.store.ListEastWestPolicies(r.Context())
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	out := make([]eastWestRuleResponse, 0)
	for _, p := range all {
		if p.DstProject != dstProject {
			continue
		}
		if dstApp != "" && p.DstApp != dstApp {
			continue
		}
		if dstService != "" && p.DstService != dstService {
			continue
		}
		out = append(out, eastWestRuleResponseFrom(p))
	}
	writeJSON(w, 200, out)
}

// deleteEastWestPolicy 删除一条规则（5 元组全必填 + dst 钳制；幂等）。
func (a *API) deleteEastWestPolicy(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	body := eastWestRuleBody{
		SrcProject: q.Get("src_project"), SrcApp: q.Get("src_app"),
		DstProject: q.Get("dst_project"), DstApp: q.Get("dst_app"),
		DstService: q.Get("dst_service"),
	}
	dstProject, ok := clampBodyProject(r, body.DstProject)
	if !ok || dstProject == "" {
		writeErr(w, 403, "dst_project is required and must be within your scope")
		return
	}
	body.DstProject = dstProject
	if body.SrcProject == "" || body.SrcApp == "" || body.DstApp == "" || body.DstService == "" {
		writeErr(w, 400, "src_project, src_app, dst_project, dst_app and dst_service are required")
		return
	}
	ctx := r.Context()
	// 删除水位取当前行（带 fencing 语义；行不存在即幂等成功）。
	var generation int64 = 1
	found := false
	if existing, lerr := a.store.ListEastWestPolicies(ctx); lerr != nil {
		writeInternalErr(w, r, lerr)
		return
	} else {
		for _, e := range existing {
			if e.SrcProject == body.SrcProject && e.SrcApp == body.SrcApp &&
				e.DstProject == body.DstProject && e.DstApp == body.DstApp &&
				e.DstService == body.DstService {
				generation, found = e.Generation, true
				break
			}
		}
	}
	if !found {
		writeJSON(w, 200, map[string]any{"deleted": false})
		return
	}
	rule := store.EastWestPolicy{
		SrcProject: body.SrcProject, SrcApp: body.SrcApp,
		DstProject: body.DstProject, DstApp: body.DstApp,
		DstService: body.DstService, Generation: generation,
	}
	if err := a.store.DeleteEastWestPolicy(ctx, rule); err != nil {
		if errors.Is(err, store.ErrFabricGenerationStale) {
			w.Header().Set("Retry-After", "1")
			writeErr(w, 409, "rule changed concurrently; retry")
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": true})
}
