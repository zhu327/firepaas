// snapshots.go：v1.3-B（ADR-0028）snapshot API。
//
// 端点：
//
//	POST /v1/machines/{id}/snapshots     checkpoint（memory|filesystem）
//	GET  /v1/snapshots                   列表（project 隔离）
//	GET  /v1/snapshots/{id}              详情
//	DELETE /v1/snapshots/{id}            删除（→DELETING→DELETED）
//	POST /v1/machines/{id}/snapshot-schedules   创建/更新 schedule
//	GET  /v1/machines/{id}/snapshot-schedules   列表
//	DELETE /v1/machines/{id}/snapshot-schedules/{schedule}  删除 schedule
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/controller"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/shared/pkg/id"
)

type createSnapshotBody struct {
	Kind             string `json:"kind"` // memory | filesystem（默认 memory）
	Name             string `json:"name"`
	Compression      string `json:"compression"` // none|zstd|lz4（默认 none）
	CompressionLevel *int   `json:"compression_level"`
	RetentionClass   string `json:"retention_class"`
}

type snapshotScheduleBody struct {
	IntervalSeconds  int    `json:"interval_seconds"`
	JitterSeconds    int    `json:"jitter_seconds"`
	MaxCount         int    `json:"max_count"`
	MaxAgeSeconds    int    `json:"max_age_seconds"`
	Compression      string `json:"compression"`
	CompressionLevel *int   `json:"compression_level"`
	Enabled          *bool  `json:"enabled"`
}

// validateSnapshotCompressionValue 校验压缩声明（与契约一致）。
func validateSnapshotCompressionValue(compression string, level *int) error {
	switch compression {
	case "", "none":
		return nil
	case "zstd":
		if level != nil && (*level < 0 || *level > 19) {
			return fmt.Errorf("zstd level must be in [-1,19], got %d", *level)
		}
	case "lz4":
		if level != nil && (*level < 0 || *level > 9) {
			return fmt.Errorf("lz4 level must be in [-1,9], got %d", *level)
		}
	default:
		return fmt.Errorf("compression must be none, zstd or lz4")
	}
	return nil
}

func (a *API) createSnapshot(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	app, aerr := a.store.GetApp(r.Context(), m.AppID)
	if aerr != nil || app == nil {
		writeErr(w, 500, "resolve app")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && app.ProjectID != project {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	var body createSnapshotBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	kind := strings.ToUpper(body.Kind)
	if kind == "" {
		kind = "MEMORY"
	}
	if kind != "MEMORY" && kind != "FILESYSTEM" {
		writeErr(w, 400, "kind must be memory or filesystem")
		return
	}
	if body.Compression == "" {
		body.Compression = "none"
	}
	if err := validateSnapshotCompressionValue(body.Compression, body.CompressionLevel); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	// ADR-0024/0030: memory checkpoints cannot capture secret memory or a
	// writable volume/overlay whose independent lifecycle cannot be replayed.
	if kind == "MEMORY" {
		writable, err := a.store.WritableActiveAttachments(r.Context(), machineID, m.CurrentExecutionID)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if len(writable) > 0 {
			writeErr(w, 409, "memory checkpoint forbidden with writable volume attachments")
			return
		}
		if dep, err := a.store.ActiveDeploymentForApp(r.Context(), m.AppID); err == nil && dep != nil && len(dep.SecretRefs) > 0 {
			writeErr(w, 409, "memory checkpoint forbidden: execution may have received secrets (ADR-0024)")
			return
		}
	}
	if m.CurrentExecutionID == "" || m.NodeID == "" {
		writeErr(w, 409, "machine has no current execution/node yet")
		return
	}
	compatibilityKey := ""
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, "resolve snapshot compatibility: "+err.Error())
		return
	}
	for _, node := range nodes {
		if node.ID == m.NodeID {
			compatibilityKey = node.SnapshotCompatibilityKey
			break
		}
	}
	if compatibilityKey == "" {
		writeErr(w, 409, "origin node has no snapshot compatibility key")
		return
	}
	snapID := "snap-" + id.New()
	opID := "op-snap-" + snapID
	level := -1
	if body.CompressionLevel != nil {
		level = *body.CompressionLevel
	}
	// 请求体用 protojson 兼容的字段形态（controller 按 CreateSnapshotRequest 解析）。
	req := map[string]any{
		"machine_id": machineID, "execution_id": m.CurrentExecutionID,
		"generation": m.Generation, "operation_id": opID,
		"snapshot_id": snapID, "kind": "SNAPSHOT_" + kind, "name": body.Name,
		"compression": map[string]any{"algorithm": strings.ToUpper(body.Compression), "level": level},
	}
	raw, err := json.Marshal(req)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if _, _, err := a.store.CreateSnapshotAndEnqueue(r.Context(), store.Snapshot{
		ID: snapID, ProjectID: app.ProjectID, SourceMachineID: machineID,
		SourceExecutionID: m.CurrentExecutionID, SourceGeneration: m.Generation, Kind: kind, NodeID: m.NodeID,
		CompatibilityKey: compatibilityKey, Compression: body.Compression, CompressionLevel: body.CompressionLevel,
		CompressionState: "none", RetentionClass: body.RetentionClass,
	}, store.EnqueueOperationParams{
		OperationID: opID, ProjectID: app.ProjectID, MachineID: machineID,
		ExecutionID: m.CurrentExecutionID, Generation: m.Generation,
		Kind: "snapshot_create", Request: raw, DispatchNodeID: m.NodeID,
	}); err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, "snapshot operation conflict")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{
		"snapshot_id": snapID, "machine_id": machineID, "status": "CREATING",
	})
}

func snapshotDTO(s store.Snapshot) map[string]any {
	return map[string]any{"id": s.ID, "project_id": s.ProjectID, "source_machine_id": s.SourceMachineID, "source_execution_id": s.SourceExecutionID, "source_generation": s.SourceGeneration, "kind": s.Kind, "status": s.Status, "origin_node_id": s.NodeID, "locality": "node-local", "durability": "best-effort", "compatibility_key": s.CompatibilityKey, "size_bytes": s.SizeBytes, "checksum": s.Checksum, "integrity": s.Integrity, "created_at": s.CreatedAt}
}

// integrityBlocked（v1.4-B）：MISSING/CORRUPT 的产物不得 restore/fork/attach。
func integrityBlocked(integrity string) bool {
	integrity = strings.ToUpper(integrity)
	return integrity == "MISSING" || integrity == "CORRUPT"
}

func (a *API) listSnapshots(w http.ResponseWriter, r *http.Request) {
	project := effectiveProjectID(r, "")
	list, err := a.store.ListSnapshots(r.Context(), project)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, s := range list {
		out = append(out, snapshotDTO(s))
	}
	writeJSON(w, 200, map[string]any{"snapshots": out})
}

func (a *API) getSnapshot(w http.ResponseWriter, r *http.Request) {
	snapID := r.PathValue("id")
	snap, err := a.store.GetSnapshot(r.Context(), snapID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if snap == nil {
		writeErr(w, 404, "snapshot not found")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != snap.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	writeJSON(w, 200, map[string]any{"snapshot": snapshotDTO(*snap)})
}

func (a *API) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapID := r.PathValue("id")
	snap, err := a.store.GetSnapshot(r.Context(), snapID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if snap == nil {
		writeErr(w, 404, "snapshot not found")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != snap.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	if snap.Status == "DELETED" {
		writeJSON(w, 202, map[string]any{"snapshot_id": snapID, "already_deleted": true})
		return
	}
	// v1.4-A：对已进入 DELETING 的快照重试删除必须幂等收敛（复用在途操作；
	// 上次尝试已终结则按新 attempt 键重派），不能以 409 拒绝后永久挂在 DELETING。
	opID := "op-snap-del-" + snapID
	raw := jsonRawDeleteSnapshot(snap, opID)
	op, err := a.store.BeginSnapshotDeleteAndEnqueue(r.Context(), snapID, store.EnqueueOperationParams{
		OperationID: opID, ProjectID: snap.ProjectID, MachineID: snap.SourceMachineID,
		ExecutionID: snap.SourceExecutionID, Generation: snap.SourceGeneration,
		Kind: "snapshot_delete", Request: raw, DispatchNodeID: snap.NodeID,
	})
	if err != nil {
		if errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, "snapshot delete conflict")
			return
		}
		if errors.Is(err, store.ErrSnapshotStatusConflict) {
			writeErr(w, 409, "snapshot is in use or cannot be deleted")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"snapshot_id": snapID, "status": "DELETING", "operation_id": op.ID})
}

func jsonRawDeleteSnapshot(snap *store.Snapshot, opID string) []byte {
	return []byte(fmt.Sprintf(`{"snapshot_id":%q,"machine_id":%q,"execution_id":%q,"generation":%d,"operation_id":%q}`,
		snap.ID, snap.SourceMachineID, snap.SourceExecutionID, snap.SourceGeneration, opID))
}

type snapshotPreflightBody struct {
	RestoreMode string `json:"restore_mode"` // memory | filesystem | auto（默认 auto）
}

// snapshotPreflight（v1.4-D）：只读 restore 预检，不改变实际 restore/fork
// 状态机。结论是时点观测（observed_at）：节点变化后旧 preflight 不能当作
// 执行授权。兼容性结论与实际 rescue 派发共用 decideRestore。
func (a *API) snapshotPreflight(w http.ResponseWriter, r *http.Request) {
	snapID := r.PathValue("id")
	snap, err := a.store.GetSnapshot(r.Context(), snapID)
	if err != nil || snap == nil {
		writeErr(w, 404, "snapshot not found")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != snap.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	var body snapshotPreflightBody
	_ = json.NewDecoder(r.Body).Decode(&body)
	mode := strings.ToLower(body.RestoreMode)
	if mode == "" {
		mode = "auto"
	}
	if mode != "memory" && mode != "filesystem" && mode != "auto" {
		writeErr(w, 400, "restore_mode must be memory, filesystem or auto")
		return
	}

	// 目标节点 = origin node（node-local 快照唯一合法 restore 目标；rescue API
	// 同样要求 machine 与快照同节点）。能力/key 来自 PG nodes 投影。
	var origin *store.Node
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	for i := range nodes {
		if nodes[i].ID == snap.NodeID {
			origin = &nodes[i]
			break
		}
	}
	var blocking []string
	available := false
	var targetKey string
	var missing []string
	memoryCap, filesystemCap := false, false
	if origin == nil {
		blocking = append(blocking, "origin node is not registered")
	} else {
		available = origin.Status == "HEALTHY" && !origin.Draining
		targetKey = origin.SnapshotCompatibilityKey
		if origin.Status != "HEALTHY" {
			blocking = append(blocking, "origin node status is "+origin.Status)
		}
		if origin.Draining {
			blocking = append(blocking, "origin node is draining")
		}
		// 能力缺失（可观测信息；实际 dispatch 仍会 fail closed）。
		memoryCap = nodeHasFeature(*origin, capabilities.SnapshotMemoryV1)
		filesystemCap = nodeHasFeature(*origin, capabilities.SnapshotFilesystemV1)
		if !memoryCap {
			missing = append(missing, capabilities.SnapshotMemoryV1)
		}
		if !filesystemCap {
			missing = append(missing, capabilities.SnapshotFilesystemV1)
		}
	}
	if snap.Status != "READY" {
		blocking = append(blocking, "snapshot status is "+snap.Status)
	}
	if integrityBlocked(snap.Integrity) {
		blocking = append(blocking, "snapshot integrity is "+snap.Integrity+"; restore/fork are not allowed")
	}

	decision := controller.DecideRestore(snap.Kind, snap.CompatibilityKey, targetKey, mode)
	availableModes := controller.AvailableRestoreModes(decision, memoryCap, filesystemCap)
	if required, capable := controller.RestoreCapability(decision, memoryCap, filesystemCap); !capable {
		blocking = append(blocking, "resolved restore mode requires missing capability "+required)
	}
	if blocking == nil {
		blocking = []string{}
	}
	if missing == nil {
		missing = []string{}
	}
	writeJSON(w, 200, map[string]any{
		"snapshot_id":              snap.ID,
		"observed_at":              time.Now().UTC().Format(time.RFC3339),
		"locality":                 "node-local",
		"durability":               "best-effort",
		"origin_node_id":           snap.NodeID,
		"origin_node_available":    available,
		"snapshot_status":          snap.Status,
		"integrity":                snap.Integrity,
		"restore_mode_requested":   mode,
		"memory_compatible":        decision.MemoryCompatible,
		"degradable_to_filesystem": decision.Degradable,
		"resolved_mode":            decision.ResolvedMode,
		"reason":                   decision.Reason,
		"available_modes":          availableModes,
		"missing_capabilities":     missing,
		"blocking_issues":          blocking,
		"compatibility": map[string]any{
			// opaque compatibility key 仍保留；结构化字段只是可观测信息。
			"source_key": snap.CompatibilityKey,
			"target_key": targetKey,
			"kind":       snap.Kind,
			"structured": map[string]bool{
				"kind_is_memory":     strings.EqualFold(snap.Kind, "MEMORY"),
				"source_key_present": snap.CompatibilityKey != "",
				"target_key_present": targetKey != "",
				"keys_match":         snap.CompatibilityKey != "" && snap.CompatibilityKey == targetKey,
			},
		},
	})
}

func (a *API) upsertSnapshotSchedule(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	app, err := a.store.GetApp(r.Context(), m.AppID)
	if err != nil || app == nil {
		writeErr(w, 500, "resolve app")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != app.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	var body snapshotScheduleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.IntervalSeconds < 60 {
		writeErr(w, 400, "interval_seconds must be >= 60")
		return
	}
	if body.MaxCount < 1 {
		body.MaxCount = 10
	}
	if body.Compression == "" {
		body.Compression = "none"
	}
	if err := validateSnapshotCompressionValue(body.Compression, body.CompressionLevel); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	scheduleID := "sch-" + machineID
	if err := a.store.UpsertSnapshotSchedule(r.Context(), store.SnapshotSchedule{
		ID: scheduleID, ProjectID: app.ProjectID, AppID: m.AppID, MachineID: machineID,
		IntervalSeconds: body.IntervalSeconds, JitterSeconds: body.JitterSeconds,
		MaxCount: body.MaxCount, MaxAgeSeconds: body.MaxAgeSeconds,
		Compression: body.Compression, CompressionLevel: body.CompressionLevel,
		Enabled: enabled,
	}); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"schedule_id": scheduleID, "machine_id": machineID})
}

func (a *API) listSnapshotSchedules(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	list, err := a.store.ListSnapshotSchedules(r.Context(), m.AppID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	out := make([]store.SnapshotSchedule, 0, len(list))
	for _, s := range list {
		if s.MachineID == machineID {
			out = append(out, s)
		}
	}
	writeJSON(w, 200, map[string]any{"schedules": out})
}

func (a *API) deleteSnapshotSchedule(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	scheduleID := r.PathValue("schedule")
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	sc, err := a.store.GetSnapshotSchedule(r.Context(), scheduleID)
	if err != nil || sc == nil || sc.MachineID != machineID {
		writeErr(w, 404, "schedule not found")
		return
	}
	if err := a.store.DeleteSnapshotSchedule(r.Context(), scheduleID); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"schedule_id": scheduleID, "deleted": true})
}

// ---------------------------------------------------------------------------
// v1.3-C（ADR-0028）：受限 fork 与 filesystem rescue API
// ---------------------------------------------------------------------------

type forkSnapshotBody struct {
	AppID       string                     `json:"app_id"`      // 同 project 的既有 app（fork 的宿主）
	TTLSeconds  int64                      `json:"ttl_seconds"` // 必填：debug machine 生命周期
	SecretRefs  map[string]store.SecretRef `json:"secret_refs"` // 显式重新授权，可为空
	Env         map[string]string          `json:"env"`
	RestoreMode string                     `json:"restore_mode"` // rescue 用（memory|filesystem|auto）
}

type rescueMachineBody struct {
	SnapshotID  string `json:"snapshot_id"`
	RestoreMode string `json:"restore_mode"`
}

// forkSnapshot 从 READY snapshot fork 一台同 project 的 EPHEMERAL/DEBUG machine。
// 约束（ADR-0028）：TTL 必填、默认无 public route（不创建 route 投影）、
// 不继承 secret 值（显式 secret_refs 重新授权）、不继承 volume。
func (a *API) forkSnapshot(w http.ResponseWriter, r *http.Request) {
	snapID := r.PathValue("id")
	snap, err := a.store.GetSnapshot(r.Context(), snapID)
	if err != nil || snap == nil {
		writeErr(w, 404, "snapshot not found")
		return
	}
	if snap.Status != "READY" {
		writeErr(w, 409, "snapshot not READY (status="+snap.Status+")")
		return
	}
	if integrityBlocked(snap.Integrity) {
		writeErr(w, 409, "snapshot integrity is "+snap.Integrity+"; fork is not allowed")
		return
	}
	var body forkSnapshotBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if len(body.SecretRefs) > 0 {
		writeErr(w, 409, "fork secret_refs unsupported: secure re-authorization is not implemented")
		return
	}
	if body.TTLSeconds <= 0 {
		writeErr(w, 400, "ttl_seconds is required for debug fork machines")
		return
	}
	if body.AppID == "" {
		writeErr(w, 400, "app_id is required")
		return
	}
	app, err := a.store.GetApp(r.Context(), body.AppID)
	if err != nil || app == nil {
		writeErr(w, 404, "app not found")
		return
	}
	if app.ProjectID != snap.ProjectID {
		writeErr(w, 403, "fork must stay within the snapshot project")
		return
	}
	var target *store.Node
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, "resolve fork capability: "+err.Error())
		return
	}
	for i := range nodes {
		if nodes[i].ID == snap.NodeID {
			target = &nodes[i]
			break
		}
	}
	required := controller.SnapshotCapability(snap.Kind)
	if target == nil || target.Status != "HEALTHY" || target.Draining || !nodeHasFeature(*target, required) {
		writeErr(w, 409, "fork target node is not healthy or lacks capability "+required)
		return
	}
	if strings.EqualFold(snap.Kind, "MEMORY") && (snap.CompatibilityKey == "" ||
		target.SnapshotCompatibilityKey != snap.CompatibilityKey) {
		writeErr(w, 409, "fork memory snapshot compatibility mismatch")
		return
	}
	machineID := fmt.Sprintf("%s-fork-%s", snap.SourceMachineID, id.New()[:8])
	executionID := "exec-" + id.New()
	opID := "op-fork-" + id.New()
	expiresAt := time.Now().Add(time.Duration(body.TTLSeconds) * time.Second).UTC()
	// 机器行直接以 EPHEMERAL 语义创建：无 route 投影（desired_state=CREATED、
	// 不绑定 deployment 目标代 → controller 不发布 route）。
	spec := map[string]any{
		"project_id": snap.ProjectID, "app_id": body.AppID,
		"deployment_id": "", "execution_id": executionID,
		"image_ref": "", "env": body.Env, "secret_refs": body.SecretRefs,
	}
	specJSON, _ := json.Marshal(spec)
	req := map[string]any{
		"snapshot_id": snapID, "machine_id": machineID, "execution_id": executionID,
		"generation": 1, "operation_id": opID, "spec_json": string(specJSON),
		"source_machine_id": snap.SourceMachineID,
	}
	raw, _ := json.Marshal(req)
	if _, err := a.store.CreateForkMachineAndEnqueue(r.Context(), snapID, store.ForkMachineParams{
		ProjectID: snap.ProjectID, AppID: body.AppID, MachineID: machineID,
		ExecutionID: executionID, NodeID: snap.NodeID, ExpiresAt: &expiresAt,
		RequiredFeature: required, TargetCompatibilityKey: target.SnapshotCompatibilityKey,
		RequireMemoryCompatible: strings.EqualFold(snap.Kind, "MEMORY"),
	}, store.EnqueueOperationParams{
		OperationID: opID, ProjectID: snap.ProjectID, MachineID: machineID,
		ExecutionID: executionID, Generation: 1, Kind: "fork",
		Request: raw, DispatchNodeID: snap.NodeID,
	}); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{
		"machine_id": machineID, "execution_id": executionID,
		"snapshot_id": snapID, "ttl_seconds": body.TTLSeconds,
	})
}

// rescueMachine atomically replaces a machine execution from a READY snapshot.
func (a *API) rescueMachine(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	var body rescueMachineBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if body.SnapshotID == "" {
		writeErr(w, 400, "snapshot_id is required")
		return
	}
	mode := strings.ToLower(body.RestoreMode)
	if mode == "" {
		mode = "auto"
	}
	if mode != "memory" && mode != "filesystem" && mode != "auto" {
		writeErr(w, 400, "restore_mode must be memory, filesystem or auto")
		return
	}
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	app, err := a.store.GetApp(r.Context(), m.AppID)
	if err != nil || app == nil {
		writeErr(w, 500, "resolve app")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != app.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	snap, err := a.store.GetSnapshot(r.Context(), body.SnapshotID)
	if err != nil || snap == nil {
		writeErr(w, 404, "snapshot not found")
		return
	}
	if snap.ProjectID != app.ProjectID {
		writeErr(w, 403, "rescue must stay within the machine project")
		return
	}
	if snap.Status != "READY" {
		writeErr(w, 409, "snapshot not READY (status="+snap.Status+")")
		return
	}
	if integrityBlocked(snap.Integrity) {
		writeErr(w, 409, "snapshot integrity is "+snap.Integrity+"; restore is not allowed")
		return
	}
	if m.CurrentExecutionID == "" || m.NodeID == "" || snap.NodeID != m.NodeID {
		writeErr(w, 409, "snapshot and machine must be available on the same node")
		return
	}
	var target *store.Node
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeErr(w, 500, "resolve restore capability: "+err.Error())
		return
	}
	for i := range nodes {
		if nodes[i].ID == m.NodeID {
			target = &nodes[i]
			break
		}
	}
	if target == nil || target.Status != "HEALTHY" || target.Draining {
		writeErr(w, 409, "restore target node is not healthy and available")
		return
	}
	decision := controller.DecideRestore(snap.Kind, snap.CompatibilityKey, target.SnapshotCompatibilityKey, mode)
	required, capable := controller.RestoreCapability(decision,
		nodeHasFeature(*target, capabilities.SnapshotMemoryV1),
		nodeHasFeature(*target, capabilities.SnapshotFilesystemV1))
	if mode == "memory" && !decision.MemoryCompatible {
		writeErr(w, 409, "memory restore incompatible: "+decision.Reason)
		return
	}
	if !capable {
		writeErr(w, 409, "restore target lacks capability "+required)
		return
	}
	newExecution := "exec-" + id.New()
	newGeneration := m.Generation + 1
	opID := "op-rescue-" + id.New()
	req := map[string]any{
		"snapshot_id": snap.ID, "machine_id": machineID,
		"execution_id": newExecution, "generation": newGeneration,
		"operation_id": opID, "restore_mode": mode,
	}
	raw, err := json.Marshal(req)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	_, err = a.store.EnqueueRescueReplacement(r.Context(), store.RescueReplacementParams{
		ProjectID: app.ProjectID, MachineID: machineID,
		OldExecutionID: m.CurrentExecutionID, OldGeneration: m.Generation,
		NewExecutionID: newExecution, OperationID: opID, SnapshotID: snap.ID,
		Request: raw, DispatchNodeID: m.NodeID, RequiredFeature: required,
		TargetCompatibilityKey:  target.SnapshotCompatibilityKey,
		RequireMemoryCompatible: decision.ResolvedMode == "memory",
	})
	if err != nil {
		if errors.Is(err, store.ErrRescueConflict) || errors.Is(err, store.ErrSnapshotStatusConflict) || errors.Is(err, store.ErrRequestConflict) {
			writeErr(w, 409, err.Error())
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"operation_id": opID, "machine_id": machineID,
		"execution_id": newExecution, "generation": newGeneration, "snapshot_id": snap.ID})
}

// ---------------------------------------------------------------------------
// v1.3-D（ADR-0029）：LOCAL_RW volume API
// ---------------------------------------------------------------------------

type createVolumeBody struct {
	ProjectID        string `json:"project_id"`
	Name             string `json:"name"`
	Mode             string `json:"mode"` // LOCAL_RW | DATASET_RO
	SizeGib          int    `json:"size_gib"`
	NodeID           string `json:"node_id"`
	SourceURL        string `json:"source_url,omitempty"` // short-lived presigned HTTPS URL
	ContentDigest    string `json:"content_digest,omitempty"`
	MaxDownloadBytes uint64 `json:"max_download_bytes,omitempty"`
	MaxFiles         uint32 `json:"max_files,omitempty"`
}

var datasetDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// normalizeDatasetSourceURL validates the v1.4 credential-free HTTPS source
// policy (no userinfo/query/fragment) and returns the normalized URL. The
// loopback exception exists only for hermetic e2e runs.
func normalizeDatasetSourceURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawFragment != "" {
		return "", errors.New("source_url must be a credential-free HTTPS URL without userinfo, query or fragment")
	}
	loopback := u.Scheme == "http" && net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()
	if u.Scheme != "https" && !loopback {
		return "", errors.New("source_url must use https")
	}
	return u.String(), nil
}

// datasetSourceDigest returns a short stable digest of the normalized source
// URL for logs/events; the raw URL itself never leaves persistent facts.
func datasetSourceDigest(normalized string) string {
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])[:16]
}

func nodeHasFeature(n store.Node, feature string) bool {
	for _, f := range n.FeatureIDs {
		if f == feature {
			return true
		}
	}
	return false
}

func (a *API) selectVolumeNode(ctx context.Context, requested, feature string, sizeBytes int64) (string, error) {
	nodes, err := a.store.ListNodes(ctx)
	if err != nil {
		return "", err
	}
	for _, n := range nodes {
		if requested != "" && n.ID != requested {
			continue
		}
		if n.Status != "HEALTHY" || n.Draining || !nodeHasFeature(n, feature) {
			continue
		}
		if n.DiskTotalMib <= 0 || n.DiskUsedMib+(sizeBytes+1048575)/1048576 > n.DiskTotalMib {
			continue
		}
		return n.ID, nil
	}
	return "", errors.New("no healthy non-draining capable node with sufficient disk")
}

// createVolume 创建 LOCAL_RW 空卷（硬钉 origin node）。
func (a *API) createVolume(w http.ResponseWriter, r *http.Request) {
	var body createVolumeBody
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
	if body.Name == "" {
		writeErr(w, 400, "name is required")
		return
	}
	if body.SizeGib < 1 || body.SizeGib > 1024 {
		writeErr(w, 400, "size_gib must be in [1,1024]")
		return
	}
	mode := strings.ToUpper(body.Mode)
	if mode == "" {
		mode = "LOCAL_RW"
	}
	sizeBytes := int64(body.SizeGib) * 1024 * 1024 * 1024
	feature := capabilities.VolumeLocalRWV1
	if mode == "DATASET_RO" {
		feature = capabilities.VolumeDatasetROV1
	}
	nodeID, err := a.selectVolumeNode(r.Context(), body.NodeID, feature, sizeBytes)
	if err != nil {
		writeErr(w, 409, err.Error())
		return
	}
	volID := "vol-" + id.New()
	opID := "op-vol-" + volID
	params := store.EnqueueOperationParams{OperationID: opID, ProjectID: project, Generation: 1, DispatchNodeID: nodeID}
	v := store.Volume{ID: volID, ProjectID: project, Name: body.Name, NodeID: nodeID, SizeBytes: sizeBytes}
	if mode == "LOCAL_RW" {
		params.Kind = "volume_create"
		params.Request = []byte(fmt.Sprintf(`{"volume_id":%q,"size_bytes":%d,"operation_id":%q}`, volID, sizeBytes, opID))
		if _, _, err := a.store.CreateLocalRWVolumeAndEnqueue(r.Context(), v, params); err != nil {
			writeErr(w, 409, err.Error())
			return
		}
	} else if mode == "DATASET_RO" {
		if !datasetDigestPattern.MatchString(body.ContentDigest) || body.SourceURL == "" {
			writeErr(w, 400, "DATASET_RO requires source_url and lowercase sha256 content_digest")
			return
		}
		// v1.4（ADR-0030）：仅接受无 userinfo/query/fragment 的 credential-free
		// HTTPS 来源；规范化后持久化，摘要供观测，原文不进日志/事件/错误响应。
		normalized, derr := normalizeDatasetSourceURL(body.SourceURL)
		if derr != nil {
			writeErr(w, 400, derr.Error())
			return
		}
		body.SourceURL = normalized
		if body.MaxDownloadBytes == 0 {
			body.MaxDownloadBytes = uint64(sizeBytes)
		}
		if body.MaxFiles == 0 {
			body.MaxFiles = 100000
		}
		expires := time.Now().Add(10 * time.Minute).Unix()
		allowLoopback := false
		if os.Getenv("FIREPAAS_E2E_ALLOW_HTTP_LOOPBACK") == "1" {
			if parsed, perr := url.Parse(body.SourceURL); perr == nil && parsed.Scheme == "http" && parsed.Hostname() == "127.0.0.1" {
				allowLoopback = true
			}
		}
		req := map[string]any{"volume_id": volID, "source_url": body.SourceURL, "source_url_digest": datasetSourceDigest(body.SourceURL), "expected_digest": body.ContentDigest, "max_download_bytes": body.MaxDownloadBytes, "max_expanded_bytes": sizeBytes, "max_files": body.MaxFiles, "expires_at_unix": expires, "operation_id": opID, "allow_http_loopback_for_tests": allowLoopback}
		params.Kind = "dataset_import"
		params.Request, _ = json.Marshal(req)
		v.ContentDigest = body.ContentDigest
		if _, _, err := a.store.CreateDatasetAndEnqueue(r.Context(), v, params); err != nil {
			writeErr(w, 409, err.Error())
			return
		}
	} else {
		writeErr(w, 400, "mode must be LOCAL_RW or DATASET_RO")
		return
	}
	writeJSON(w, 202, map[string]any{"volume_id": volID, "project_id": project, "node_id": nodeID, "mode": mode, "status": "CREATING"})
}

func (a *API) listVolumes(w http.ResponseWriter, r *http.Request) {
	project := effectiveProjectID(r, "")
	list, err := a.store.ListVolumes(r.Context(), project)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"volumes": list})
}

func (a *API) getVolume(w http.ResponseWriter, r *http.Request) {
	volID := r.PathValue("id")
	v, err := a.store.GetVolume(r.Context(), volID)
	if err != nil || v == nil {
		writeErr(w, 404, "volume not found")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != v.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	writeJSON(w, 200, map[string]any{"volume": v})
}

func (a *API) deleteVolume(w http.ResponseWriter, r *http.Request) {
	volID := r.PathValue("id")
	v, err := a.store.GetVolume(r.Context(), volID)
	if err != nil || v == nil {
		writeErr(w, 404, "volume not found")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != v.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	if v.State == "DELETED" {
		writeJSON(w, 202, map[string]any{"volume_id": volID, "already_deleted": true})
		return
	}
	// v1.4-B：UNAVAILABLE 也允许删除——MISSING（完整 inventory 证明本地产物
	// 不存在）不得造成永久不可回收的墓碑；agent 删除对 NotFound 幂等。
	if v.State != "READY" && v.State != "UNAVAILABLE" && v.State != "DELETING" {
		writeErr(w, 409, "volume must be READY or UNAVAILABLE before deletion")
		return
	}
	active, err := a.store.ActiveAttachments(r.Context(), volID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if len(active) > 0 {
		writeErr(w, 409, "volume has active attachments; detach first")
		return
	}
	opID := "op-vol-del-" + volID
	raw := []byte(fmt.Sprintf(`{"volume_id":%q,"operation_id":%q}`, volID, opID))
	if _, err := a.store.BeginVolumeDeleteAndEnqueue(r.Context(), volID, store.EnqueueOperationParams{
		OperationID: opID, ProjectID: v.ProjectID, MachineID: "", ExecutionID: "",
		Generation: 1, Kind: "volume_delete", Request: raw, DispatchNodeID: v.NodeID,
	}); err != nil {
		writeErr(w, 409, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"volume_id": volID, "status": "DELETING"})
}

type attachVolumeBody struct {
	MountPath        string `json:"mount_path"`
	Readonly         bool   `json:"readonly"`
	OverlaySizeBytes int64  `json:"overlay_size_bytes"`
}

// attachVolume 把 LOCAL_RW 卷挂到 machine 当前 execution（单写 fencing）。
func (a *API) attachVolume(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	volID := r.URL.Query().Get("volume_id")
	if volID == "" {
		writeErr(w, 400, "volume_id is required")
		return
	}
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	v, err := a.store.GetVolume(r.Context(), volID)
	if err != nil || v == nil {
		writeErr(w, 404, "volume not found")
		return
	}
	if v.State != "READY" {
		writeErr(w, 409, "volume not READY (state="+v.State+")")
		return
	}
	if integrityBlocked(v.Integrity) {
		writeErr(w, 409, "volume integrity is "+v.Integrity+"; attach is not allowed")
		return
	}
	project := effectiveProjectID(r, "")
	if project != "" && project != v.ProjectID {
		writeErr(w, 403, "cross-project access denied")
		return
	}
	// locality 硬约束：LOCAL_RW 只能在 origin node 挂载。
	if m.NodeID != v.NodeID {
		writeErr(w, 409, "volume locality mismatch: machine node "+m.NodeID+" != volume node "+v.NodeID)
		return
	}
	if m.CurrentExecutionID == "" {
		writeErr(w, 409, "machine has no current execution")
		return
	}
	var body attachVolumeBody
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.OverlaySizeBytes < 0 {
		writeErr(w, 400, "overlay_size_bytes must not be negative")
		return
	}
	if v.Mode == "DATASET_RO" {
		body.Readonly = true
		if body.OverlaySizeBytes > 0 {
			nodes, nerr := a.store.ListNodes(r.Context())
			if nerr != nil {
				writeErr(w, 500, nerr.Error())
				return
			}
			capable := false
			for _, node := range nodes {
				if node.ID == v.NodeID && nodeHasFeature(node, capabilities.VolumeDatasetOverlayV1) {
					capable = true
					break
				}
			}
			if !capable {
				writeErr(w, 409, "dataset overlay is not supported by the volume node")
				return
			}
		}
	} else if body.OverlaySizeBytes != 0 {
		writeErr(w, 400, "overlay_size_bytes is only valid for DATASET_RO")
		return
	}
	opID := "op-vol-att-" + volID + "-" + m.CurrentExecutionID[:8]
	raw := []byte(fmt.Sprintf(`{"volume_id":%q,"machine_id":%q,"execution_id":%q,"generation":%d,"operation_id":%q,"mount_path":%q,"readonly":%v,"overlay":%v,"overlay_size_bytes":%d}`,
		volID, machineID, m.CurrentExecutionID, m.Generation, opID, body.MountPath, body.Readonly, body.OverlaySizeBytes > 0, body.OverlaySizeBytes))
	p := store.EnqueueOperationParams{OperationID: opID, ProjectID: v.ProjectID, MachineID: machineID, ExecutionID: m.CurrentExecutionID, Generation: m.Generation, Kind: "volume_attach", Request: raw, DispatchNodeID: v.NodeID}
	att := store.VolumeAttachment{VolumeID: volID, MachineID: machineID, ExecutionID: m.CurrentExecutionID, MountPath: body.MountPath, Readonly: body.Readonly, OverlaySizeBytes: body.OverlaySizeBytes, Status: "PENDING"}
	if v.Mode == "DATASET_RO" {
		_, err = a.store.ClaimDatasetAttachmentAndEnqueue(r.Context(), att, p)
	} else {
		_, err = a.store.ClaimLocalRWAttachmentAndEnqueue(r.Context(), att, p)
	}
	if err != nil {
		writeErr(w, 409, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"volume_id": volID, "machine_id": machineID, "status": "PENDING"})
}

// detachVolume 卸载卷（旧 execution 的 detach 不影响新代——按 execution fencing）。
func (a *API) detachVolume(w http.ResponseWriter, r *http.Request) {
	machineID := r.PathValue("id")
	volID := r.URL.Query().Get("volume_id")
	if volID == "" {
		writeErr(w, 400, "volume_id is required")
		return
	}
	m, err := a.store.GetMachine(r.Context(), machineID)
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	if m.CurrentExecutionID == "" {
		writeErr(w, 409, "machine has no current execution")
		return
	}
	opID := "op-vol-det-" + volID + "-" + m.CurrentExecutionID[:8]
	raw := []byte(fmt.Sprintf(`{"volume_id":%q,"machine_id":%q,"execution_id":%q,"generation":%d,"operation_id":%q}`,
		volID, machineID, m.CurrentExecutionID, m.Generation, opID))
	v, err := a.store.GetVolume(r.Context(), volID)
	if err != nil || v == nil {
		writeErr(w, 404, "volume not found")
		return
	}
	p := store.EnqueueOperationParams{OperationID: opID, ProjectID: v.ProjectID, MachineID: machineID, ExecutionID: m.CurrentExecutionID, Generation: m.Generation, Kind: "volume_detach", Request: raw, DispatchNodeID: m.NodeID}
	if _, err := a.store.BeginDetachVolumeAndEnqueue(r.Context(), volID, machineID, m.CurrentExecutionID, p); err != nil {
		writeErr(w, 409, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"volume_id": volID, "machine_id": machineID, "status": "DETACHING"})
}
