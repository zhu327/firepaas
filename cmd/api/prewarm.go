// prewarm.go：v1.4-C（docs/v1.4-plan.md §7）显式镜像预热、覆盖率与 cache pin
// API。
//
// 端点：
//
//	POST   /v1/images/prewarm             派发 digest-pinned PullImage（write）
//	GET    /v1/images/coverage            逐节点缓存覆盖与预热状态（read）
//	POST   /v1/images/pins                短期 cache pin（write）
//	GET    /v1/images/pins                列出本项目 active pins（read）
//	DELETE /v1/images/pins/{id}           删除 pin（write）
//
// 限制（默认值可经环境变量覆盖）：目标节点数、最大 TTL、每项目 pinned
// bytes、prewarm 并发；磁盘 hard watermark 冲突时拒绝新 pin/prefetch。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/shared/pkg/id"
)

var imageDigestOnlyPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// prewarmLimits 是 v1.4-C 的准入上限（环境变量可覆盖默认值；只收紧运营
// 边界，不放松安全语义）。
type prewarmLimits struct {
	MaxTargetNodes    int
	MaxPinTTL         time.Duration
	MaxPinnedBytesMib int64
	MaxActivePrewarms int
	HardWatermarkFrac float64
	MaxPinsPerProject int
}

func defaultPrewarmLimits() prewarmLimits {
	l := prewarmLimits{
		MaxTargetNodes:    16,
		MaxPinTTL:         7 * 24 * time.Hour,
		MaxPinnedBytesMib: 16384,
		MaxActivePrewarms: 4,
		HardWatermarkFrac: 0.90,
		MaxPinsPerProject: 32,
	}
	if v := envInt("FIREPAAS_PREWARM_MAX_TARGET_NODES", 0); v > 0 {
		l.MaxTargetNodes = v
	}
	if v := envInt("FIREPAAS_PIN_MAX_TTL_SECONDS", 0); v > 0 {
		l.MaxPinTTL = time.Duration(v) * time.Second
	}
	if v := envInt("FIREPAAS_PIN_MAX_BYTES_MIB", 0); v > 0 {
		l.MaxPinnedBytesMib = int64(v)
	}
	if v := envInt("FIREPAAS_PREWARM_MAX_ACTIVE", 0); v > 0 {
		l.MaxActivePrewarms = v
	}
	if v := envFloat("FIREPAAS_PIN_HARD_WATERMARK", 0); v > 0 && v < 1 {
		l.HardWatermarkFrac = v
	}
	if v := envInt("FIREPAAS_PIN_MAX_PER_PROJECT", 0); v > 0 {
		l.MaxPinsPerProject = v
	}
	return l
}

// prewarmDigestFromRef extracts the digest from a digest-pinned image
// reference (registry/app@sha256:...) or validates a bare digest.
func prewarmDigestFromRef(ref string) (string, bool) {
	if i := strings.LastIndex(ref, "@"); i >= 0 && i < len(ref)-1 {
		digest := ref[i+1:]
		return digest, imageDigestOnlyPattern.MatchString(digest)
	}
	return ref, imageDigestOnlyPattern.MatchString(ref)
}

type prewarmBody struct {
	ProjectID string   `json:"project_id"`
	ImageRef  string   `json:"image_ref"`
	NodePool  string   `json:"node_pool"`
	NodeIDs   []string `json:"node_ids"`
}

type prewarmIntent struct {
	ImageRef string   `json:"image_ref"`
	NodePool string   `json:"node_pool,omitempty"`
	NodeIDs  []string `json:"node_ids,omitempty"`
}

type prewarmTargetView struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// eligiblePrewarmNodes resolves the target node set: explicit node IDs or all
// healthy non-draining nodes in a pool. Disk hard-watermark nodes are reported
// per node instead of silently dropped.
func (a *API) eligiblePrewarmNodes(r *http.Request, body prewarmBody, limits prewarmLimits) ([]store.Node, []prewarmTargetView, error) {
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		return nil, nil, err
	}
	var rejected []prewarmTargetView
	byID := map[string]store.Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	var wanted []store.Node
	if len(body.NodeIDs) > 0 {
		if len(body.NodeIDs) > limits.MaxTargetNodes {
			return nil, nil, fmt.Errorf("too many target nodes (%d > %d)", len(body.NodeIDs), limits.MaxTargetNodes)
		}
		for _, id := range body.NodeIDs {
			n, ok := byID[id]
			if !ok {
				return nil, nil, fmt.Errorf("unknown node %q", id)
			}
			wanted = append(wanted, n)
		}
	} else {
		for _, n := range nodes {
			if body.NodePool != "" && n.NodePool != body.NodePool {
				continue
			}
			wanted = append(wanted, n)
		}
		if len(wanted) > limits.MaxTargetNodes {
			return nil, nil, fmt.Errorf("too many target nodes (%d > %d); narrow the node pool", len(wanted), limits.MaxTargetNodes)
		}
	}
	var eligible []store.Node
	for _, n := range wanted {
		switch {
		case n.Status != "HEALTHY":
			rejected = append(rejected, prewarmTargetView{NodeID: n.ID, Status: "rejected", Reason: "node not healthy (" + n.Status + ")"})
		case n.Draining:
			rejected = append(rejected, prewarmTargetView{NodeID: n.ID, Status: "rejected", Reason: "node draining"})
		case n.DiskTotalMib > 0 && float64(n.DiskUsedMib)/float64(n.DiskTotalMib) >= limits.HardWatermarkFrac:
			rejected = append(rejected, prewarmTargetView{NodeID: n.ID, Status: "rejected", Reason: "disk hard watermark reached"})
		default:
			eligible = append(eligible, n)
		}
	}
	return eligible, rejected, nil
}

func (a *API) prewarmImage(w http.ResponseWriter, r *http.Request) {
	limits := defaultPrewarmLimits()
	var body prewarmBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	project, ok := clampBodyProject(r, body.ProjectID)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	if project == "" {
		project = "dev"
	}
	intentRaw, _ := json.Marshal(prewarmIntent{ImageRef: body.ImageRef, NodePool: body.NodePool, NodeIDs: body.NodeIDs})
	idemKey := r.Header.Get("Idempotency-Key")
	if replay, targets, err := a.store.FindPrewarmReplay(r.Context(), project, idemKey, intentRaw); err != nil {
		writeErr(w, 409, err.Error())
		return
	} else if replay != nil {
		writePrewarmResponse(w, replay, targets)
		return
	}
	digest, pinned := prewarmDigestFromRef(body.ImageRef)
	if !pinned || body.ImageRef == "" || !strings.Contains(body.ImageRef, "@") {
		writeErr(w, 400, "image_ref must be a digest-pinned reference (registry/app@sha256:...)")
		return
	}
	// v1.4-C：prewarm 是镜像入流路径，必须与 app/deploy 走同一镜像准入
	//（digest 形态 + registry allowlist），不得绕过 P1-2 控制。
	if _, err := a.images.Validate(body.ImageRef); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if len(body.NodeIDs) == 0 && body.NodePool == "" {
		writeErr(w, 400, "node_pool or node_ids is required")
		return
	}
	active, err := a.store.ActivePrewarmCount(r.Context(), project)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if active >= limits.MaxActivePrewarms {
		writeErr(w, 429, fmt.Sprintf("project has %d active prewarm operations (limit %d)", active, limits.MaxActivePrewarms))
		return
	}
	eligible, rejected, err := a.eligiblePrewarmNodes(r, body, limits)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if len(eligible) == 0 {
		writeJSON(w, 409, map[string]any{"error": "no eligible target nodes", "targets": rejected})
		return
	}
	nodeIDs := make([]string, 0, len(eligible))
	for _, n := range eligible {
		nodeIDs = append(nodeIDs, n.ID)
	}
	opID := "op-prewarm-" + id.New()
	request := map[string]any{"intent": json.RawMessage(intentRaw), "image_ref": body.ImageRef, "digest": digest, "targets": nodeIDs}
	raw, _ := json.Marshal(request)
	op, err := a.store.CreatePrewarmAndEnqueue(r.Context(), digest, idemKey, store.EnqueueOperationParams{
		OperationID: opID, ProjectID: project, Kind: "image_prewarm", Request: raw,
	}, nodeIDs, limits.MaxActivePrewarms)
	if err != nil {
		if errors.Is(err, store.ErrPrewarmNotAllowed) {
			writeErr(w, 429, fmt.Sprintf("project has reached the active prewarm limit (%d)", limits.MaxActivePrewarms))
			return
		}
		writeErr(w, 409, err.Error())
		return
	}
	persistedTargets, err := a.store.ListPrewarmTargets(r.Context(), op.ID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writePrewarmResponse(w, &op, persistedTargets)
}

func writePrewarmResponse(w http.ResponseWriter, op *store.Operation, persisted []store.PrewarmTarget) {
	var req struct {
		ImageRef string `json:"image_ref"`
		Digest   string `json:"digest"`
	}
	_ = json.Unmarshal(op.Request, &req)
	targets := make([]prewarmTargetView, 0, len(persisted))
	for _, target := range persisted {
		targets = append(targets, prewarmTargetView{NodeID: target.NodeID, Status: target.Status, Reason: target.Error})
	}
	writeJSON(w, 202, map[string]any{
		"operation_id": op.ID, "project_id": op.ProjectID, "digest": req.Digest,
		"image_ref": req.ImageRef, "targets": targets,
	})
}

// imageCoverage reports per-node cache state for a digest: eligible node count,
// cached/pending/failed and the latest per-node observation.
func (a *API) imageCoverage(w http.ResponseWriter, r *http.Request) {
	ref := r.URL.Query().Get("image_ref")
	digestParam := r.URL.Query().Get("digest")
	pool := r.URL.Query().Get("node_pool")
	digest, ok := prewarmDigestFromRef(ref)
	if !ok || digest == "" {
		if imageDigestOnlyPattern.MatchString(digestParam) {
			digest = digestParam
			ok = true
		}
	}
	if !ok {
		writeErr(w, 400, "digest or digest-pinned image_ref is required")
		return
	}
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	project := effectiveProjectID(r, "dev")
	prewarmByNode, err := a.store.PrewarmStatusByNode(r.Context(), project, digest)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	type nodeView struct {
		NodeID       string `json:"node_id"`
		NodePool     string `json:"node_pool,omitempty"`
		Cached       bool   `json:"cached"`
		Pending      bool   `json:"pending_prewarm"`
		Failed       bool   `json:"prewarm_failed"`
		LastObserved string `json:"last_observed"`
	}
	var views []nodeView
	eligible, cached, pending, failed := 0, 0, 0, 0
	for _, n := range nodes {
		if pool != "" && n.NodePool != pool {
			continue
		}
		// coverage 只统计满足部署硬约束的 eligible 节点：健康、未排水。
		if n.Status != "HEALTHY" || n.Draining {
			continue
		}
		eligible++
		isCached := false
		for _, d := range n.ImageCache {
			if d == digest {
				isCached = true
				break
			}
		}
		st := prewarmByNode[n.ID]
		view := nodeView{NodeID: n.ID, NodePool: n.NodePool, Cached: isCached,
			Pending: st.Pending, Failed: st.Failed, LastObserved: n.LastSeenAt.UTC().Format(time.RFC3339)}
		if isCached {
			cached++
		}
		if st.Pending {
			pending++
		}
		if st.Failed {
			failed++
		}
		views = append(views, view)
	}
	if views == nil {
		views = []nodeView{}
	}
	writeJSON(w, 200, map[string]any{
		"project_id": project, "digest": digest,
		"summary": map[string]any{
			"eligible": eligible, "cached": cached,
			"pending_prewarm": pending, "prewarm_failed": failed,
			"uncached": eligible - cached,
		},
		"nodes": views,
	})
}

type imagePinBody struct {
	ProjectID   string   `json:"project_id"`
	ImageDigest string   `json:"image_digest"`
	ImageRef    string   `json:"image_ref"`
	NodePool    string   `json:"node_pool"`
	NodeIDs     []string `json:"node_ids"`
	TTLSeconds  int      `json:"ttl_seconds"`
	Reason      string   `json:"reason"`
}

func (a *API) createImagePin(w http.ResponseWriter, r *http.Request) {
	limits := defaultPrewarmLimits()
	var body imagePinBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	project, ok := clampBodyProject(r, body.ProjectID)
	if !ok {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	if project == "" {
		project = "dev"
	}
	digest := body.ImageDigest
	if digest == "" && body.ImageRef != "" {
		digest, ok = prewarmDigestFromRef(body.ImageRef)
		if !ok {
			writeErr(w, 400, "image_ref must be digest-pinned")
			return
		}
		// pin 本身不拉取字节，但引用形态仍需过镜像准入（同 prewarm）。
		if _, err := a.images.Validate(body.ImageRef); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
	}
	if !imageDigestOnlyPattern.MatchString(digest) {
		writeErr(w, 400, "image_digest must be a lowercase sha256 digest")
		return
	}
	if body.TTLSeconds <= 0 || time.Duration(body.TTLSeconds)*time.Second > limits.MaxPinTTL {
		writeErr(w, 400, fmt.Sprintf("ttl_seconds must be in (0, %d]", int(limits.MaxPinTTL/time.Second)))
		return
	}
	intentRaw, _ := json.Marshal(body)
	idemKey := r.Header.Get("Idempotency-Key")
	if replay, ok, err := a.store.FindImagePinReplay(r.Context(), project, idemKey, intentRaw); err != nil {
		writeErr(w, 409, err.Error())
		return
	} else if ok {
		writeJSON(w, 201, map[string]any{"pins": replay})
		return
	}
	// Resolve selectors to explicit node IDs. Freezing a pool at creation avoids
	// unbounded quota growth when nodes are later added to that pool.
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	byID := map[string]store.Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	var selectors []string
	seenNodes := map[string]bool{}
	if len(body.NodeIDs) > 0 {
		if len(body.NodeIDs) > limits.MaxTargetNodes {
			writeErr(w, 400, fmt.Sprintf("too many target nodes (%d > %d)", len(body.NodeIDs), limits.MaxTargetNodes))
			return
		}
		for _, nodeID := range body.NodeIDs {
			if seenNodes[nodeID] {
				continue
			}
			seenNodes[nodeID] = true
			n, exists := byID[nodeID]
			if !exists {
				writeErr(w, 400, fmt.Sprintf("unknown node %q", nodeID))
				return
			}
			_ = n // store rechecks existence and watermark under the atomic admission transaction.
			selectors = append(selectors, "node:"+nodeID)
		}
	} else if body.NodePool != "" {
		for _, n := range nodes {
			if n.NodePool == body.NodePool {
				selectors = append(selectors, "node:"+n.ID)
			}
		}
		if len(selectors) == 0 {
			writeErr(w, 409, "node pool has no nodes")
			return
		}
		if len(selectors) > limits.MaxTargetNodes {
			writeErr(w, 400, fmt.Sprintf("node pool too large (%d > %d)", len(selectors), limits.MaxTargetNodes))
			return
		}
	} else {
		writeErr(w, 400, "node_pool or node_ids is required")
		return
	}
	owner := callerName(identFrom(r))
	if owner == "" {
		owner = "unknown"
	}
	batch := make([]store.ImagePin, 0, len(selectors))
	for _, selector := range selectors {
		batch = append(batch, store.ImagePin{ID: "pin-" + id.New(), ProjectID: project,
			ImageDigest: digest, Selector: selector, Owner: owner, Reason: body.Reason})
	}
	created, err := a.store.CreateImagePinsAtomic(r.Context(), batch, time.Duration(body.TTLSeconds)*time.Second, idemKey, intentRaw, store.ImagePinLimits{
		MaxPins: limits.MaxPinsPerProject, MaxBytesMib: limits.MaxPinnedBytesMib,
		MaxTargets: limits.MaxTargetNodes, HardWatermark: limits.HardWatermarkFrac,
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrImageSizeUnknown):
			writeErr(w, 409, "image size unknown; prewarm the digest before pinning")
		case errors.Is(err, store.ErrImagePinQuota):
			writeErr(w, 429, "project image pin quota exceeded")
		case errors.Is(err, store.ErrImagePinWatermark):
			writeErr(w, 409, "target node disk hard watermark reached")
		default:
			writeErr(w, 409, err.Error())
		}
		return
	}
	writeJSON(w, 201, map[string]any{"pins": created})
}

func (a *API) listImagePins(w http.ResponseWriter, r *http.Request) {
	project := effectiveProjectID(r, "")
	pins, err := a.store.ListImagePins(r.Context(), project)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(pins))
	for _, p := range pins {
		out = append(out, map[string]any{
			"id": p.ID, "project_id": p.ProjectID, "image_digest": p.ImageDigest,
			"selector": p.Selector, "owner": p.Owner, "reason": p.Reason,
			"expires_at": p.ExpiresAt, "created_at": p.CreatedAt,
		})
	}
	writeJSON(w, 200, map[string]any{"pins": out})
}

func (a *API) deleteImagePin(w http.ResponseWriter, r *http.Request) {
	pinID := r.PathValue("id")
	project := effectiveProjectID(r, "")
	request, _ := json.Marshal(map[string]string{"pin_id": pinID})
	if err := a.store.DeleteImagePin(r.Context(), pinID, project, r.Header.Get("Idempotency-Key"), request); err != nil {
		if errors.Is(err, store.ErrImagePinNotFound) {
			writeErr(w, 404, "pin not found")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": pinID})
}
