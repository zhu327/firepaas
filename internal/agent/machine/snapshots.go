// snapshots.go：v1.3-B（ADR-0028）agent 侧 snapshot 适配。
//
// firepaas snapshot ID 写入 hypeman snapshot tags（tagSnapshotID），agent 侧
// 按 tag 回读/解析。memory checkpoint = hypeman Standby snapshot（内部已做
// pause→capture→resume source）；filesystem-only = guest sync → stop → capture
// → start source（一致性标记显式返回；恢复失败不发布快照）。
package machine

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/snapshot"
	"github.com/kernel/hypeman/lib/tags"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const tagSnapshotID = "firepaas/snapshot_id"

// snapshotProvider 是 hypeman instances.Manager 的 snapshot 能力子集。
type snapshotProvider interface {
	CreateSnapshot(ctx context.Context, id string, req instances.CreateSnapshotRequest) (*instances.Snapshot, error)
	ListSnapshots(ctx context.Context, filter *instances.ListSnapshotsFilter) ([]instances.Snapshot, error)
	GetSnapshot(ctx context.Context, snapshotID string) (*instances.Snapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	StopInstance(ctx context.Context, id string) (*instances.Instance, error)
	StartInstance(ctx context.Context, id string, req instances.StartInstanceRequest) (*instances.Instance, error)
	ForkSnapshot(ctx context.Context, snapshotID string, req instances.ForkSnapshotRequest) (*instances.Instance, error)
	RestoreSnapshot(
		ctx context.Context,
		id string,
		snapshotID string,
		req instances.RestoreSnapshotRequest,
	) (*instances.Instance, error)
}

// ErrSnapshotNotFound 表示 agent 本地不存在该快照（tag 解析失败或已删）。
var (
	ErrSnapshotNotFound     = errors.New("snapshot not found at agent")
	ErrSnapshotUnsupported  = errors.New("snapshot operation unsupported by installed hypeman")
	ErrSnapshotIncompatible = errors.New("snapshot compatibility key mismatch")
)

// CreateSnapshot 执行 checkpoint（ADR-0028）：memory = pause→capture→resume；
// filesystem = guest sync → stop → capture → start（任何恢复失败都不发布）。
func (a *Adapter) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.SnapshotInfo, error) {
	if _, ok := a.instances.(snapshotProvider); !ok {
		return nil, ErrGuestOpsUnsupported
	}
	inst, err := a.instances.GetInstance(ctx, req.GetMachineId())
	if err != nil {
		if errors.Is(err, instances.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrMachineNotFound, req.GetMachineId())
		}
		return nil, fmt.Errorf("get instance %s: %w", req.GetMachineId(), err)
	}
	if inst.Tags[tagExecution] != req.GetExecutionId() {
		return nil, fmt.Errorf("%w: machine %s want %s got %s",
			ErrStaleExecution, req.GetMachineId(), req.GetExecutionId(), inst.Tags[tagExecution])
	}
	// ADR-0024 §9：接收过 secret 的 execution 禁止 memory snapshot。
	kind := req.GetKind()
	if kind == pb.SnapshotKind_SNAPSHOT_MEMORY {
		if _, hasSecret := inst.Tags[tagSecretLease]; hasSecret {
			return nil, ErrSecretSnapshotForbidden
		}
	}

	sp := a.instances.(snapshotProvider)
	compression, err := translateCompression(req.GetCompression())
	if err != nil {
		return nil, err
	}
	snap, err := sp.CreateSnapshot(ctx, inst.Id, instances.CreateSnapshotRequest{
		Kind: hypemanKind(kind), Name: req.GetName(),
		Tags: tags.Tags{tagSnapshotID: req.GetSnapshotId()}, Compression: compression,
	})
	if err != nil {
		return nil, fmt.Errorf("hypeman create snapshot: %w", err)
	}
	info := mapSnapshotInfo(snap, req)
	if kind == pb.SnapshotKind_SNAPSHOT_FILESYSTEM {
		info.FilesystemConsistency = "clean"
	}
	return info, nil
}

// ListSnapshots 按 machine 返回本地快照（tag 过滤）。
func (a *Adapter) ListSnapshots(ctx context.Context, machineID, snapshotID string) ([]*pb.SnapshotInfo, error) {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	filter := &instances.ListSnapshotsFilter{Tags: tags.Tags{}}
	if snapshotID != "" {
		filter.Tags[tagSnapshotID] = snapshotID
	}
	list, err := sp.ListSnapshots(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("hypeman list snapshots: %w", err)
	}
	out := make([]*pb.SnapshotInfo, 0, len(list))
	for i := range list {
		snap := &list[i]
		sid := snap.Tags[tagSnapshotID]
		if sid == "" {
			continue
		}
		if machineID != "" && snap.SourceName != machineID {
			continue
		}
		out = append(out, mapSnapshotInfo(snap, &pb.CreateSnapshotRequest{SnapshotId: sid}))
	}
	return out, nil
}

// RecoverSnapshot resolves a completed create by its stable firepaas tag.
func (a *Adapter) RecoverSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.SnapshotInfo, bool, error) {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, false, ErrSnapshotUnsupported
	}
	list, err := sp.ListSnapshots(
		ctx,
		&instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: req.GetSnapshotId()}},
	)
	if err != nil {
		return nil, false, fmt.Errorf("recover snapshot inventory: %w", err)
	}
	for i := range list {
		if list[i].SourceName == req.GetMachineId() {
			info := mapSnapshotInfo(&list[i], req)
			if req.GetKind() == pb.SnapshotKind_SNAPSHOT_FILESYSTEM {
				info.FilesystemConsistency = "clean"
			}
			return info, true, nil
		}
	}
	return nil, false, nil
}

// RecoverMachine returns a machine only when its durable runtime identity
// matches the operation's intended execution and generation.
func (a *Adapter) RecoverMachine(
	ctx context.Context,
	machineID, executionID string,
	generation uint64,
) (*pb.Machine, bool, error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if errors.Is(err, instances.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("recover machine inventory: %w", err)
	}
	if inst.Tags[tagExecution] != executionID || inst.Tags[tagGeneration] != strconv.FormatUint(generation, 10) {
		return nil, false, nil
	}
	return mapMachine(inst), true, nil
}

// RecoverRestore reconstructs the observable restore response from the target
// execution identity and immutable snapshot metadata.
func (a *Adapter) RecoverRestore(
	ctx context.Context,
	req *pb.RestoreSnapshotRequest,
) (*pb.Machine, string, string, bool, error) {
	m, found, err := a.RecoverMachine(ctx, req.GetMachineId(), req.GetExecutionId(), req.GetGeneration())
	if err != nil || !found {
		return nil, "", "", found, err
	}
	// A prior attempt may have restored the payload but failed while attaching
	// the slot. That path stops the replacement; retry the restore rather than
	// publishing a stopped execution as recovered success. A running recovery
	// still replays Attach to close the crash window before ledger completion.
	inst, err := a.instances.GetInstance(ctx, req.GetMachineId())
	if err != nil {
		return nil, "", "", false, err
	}
	if inst.State != instances.StateRunning {
		return nil, "", "", false, nil
	}
	if err := a.reattachSlot(ctx, req.GetMachineId(), inst); err != nil {
		return nil, "", "", false, err
	}
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, "", "", false, ErrSnapshotUnsupported
	}
	snaps, err := sp.ListSnapshots(
		ctx,
		&instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: req.GetSnapshotId()}},
	)
	if err != nil || len(snaps) != 1 {
		return nil, "", "", false, ErrSnapshotNotFound
	}
	mode := req.GetRestoreMode()
	if mode == "" {
		mode = "auto"
	}
	if mode == "auto" {
		if snaps[0].CompatibilityKey != "" && req.GetCompatibilityKey() != "" &&
			snaps[0].CompatibilityKey == req.GetCompatibilityKey() {
			mode = "memory"
		} else {
			mode = "filesystem"
		}
	}
	consistency := ""
	if mode == "filesystem" {
		consistency = "clean"
	}
	return m, mode, consistency, true, nil
}

// DeleteSnapshot 按 firepaas snapshot ID（tag）解析并删除本地快照。
func (a *Adapter) DeleteSnapshot(ctx context.Context, snapshotID, machineID string) error {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return ErrGuestOpsUnsupported
	}
	filter := &instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: snapshotID}}
	list, err := sp.ListSnapshots(ctx, filter)
	if err != nil {
		return fmt.Errorf("resolve snapshot by tag: %w", err)
	}
	for i := range list {
		if machineID != "" && list[i].SourceName != machineID {
			continue
		}
		if err := sp.DeleteSnapshot(ctx, list[i].Id); err != nil {
			if errors.Is(err, instances.ErrSnapshotNotFound) {
				continue
			}
			return fmt.Errorf("hypeman delete snapshot: %w", err)
		}
	}
	return nil
}

func hypemanKind(kind pb.SnapshotKind) snapshot.SnapshotKind {
	if kind == pb.SnapshotKind_SNAPSHOT_FILESYSTEM {
		return snapshot.SnapshotKindFilesystem
	}
	return snapshot.SnapshotKindStandby
}

// ForkSnapshot（v1.3-C，ADR-0028）：从 READY snapshot 在 origin node 本地
// fork 新 debug machine。fork 产物不继承 secret（tag 不复制 secret lease）、
// 不继承 volume；hypeman ForkSnapshot 生成全新 instance。返回后按需挂 slot。
func (a *Adapter) ForkSnapshot(ctx context.Context, req *pb.ForkSnapshotRequest) (*pb.Machine, error) {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, ErrSnapshotUnsupported
	}
	snaps, err := sp.ListSnapshots(
		ctx,
		&instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: req.GetSnapshotId()}},
	)
	if err != nil || len(snaps) != 1 {
		return nil, ErrSnapshotNotFound
	}
	var spec pb.MachineSpec
	if err := protojson.Unmarshal([]byte(req.GetSpecJson()), &spec); err != nil {
		return nil, fmt.Errorf("decode fork spec: %w", err)
	}
	forked, err := sp.ForkSnapshot(ctx, snaps[0].Id, instances.ForkSnapshotRequest{
		ID: req.GetMachineId(), Name: req.GetMachineId(), TargetState: instances.StateRunning, ClearVolumes: true,
		Tags: tags.Tags{
			tagMachine:    req.GetMachineId(),
			tagProject:    spec.GetProjectId(),
			tagApp:        spec.GetAppId(),
			tagDeployment: spec.GetDeploymentId(),
			tagExecution:  req.GetExecutionId(),
			tagGeneration: strconv.FormatUint(req.GetGeneration(), 10),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("hypeman fork snapshot: %w", err)
	}
	if err := a.reattachSlot(ctx, req.GetMachineId(), forked); err != nil {
		cleanupErr := a.instances.DeleteInstance(context.WithoutCancel(ctx), forked.Id)
		if a.slots != nil {
			cleanupErr = errors.Join(cleanupErr, a.slots.Release(context.WithoutCancel(ctx), req.GetMachineId()))
		}
		return nil, errors.Join(err, cleanupErr)
	}
	return mapMachine(forked), nil
}

// RestoreSnapshot（v1.3-C）：restore_mode=memory|filesystem|auto。memory 要求
// 目标节点 compatibility key 与快照来源一致；filesystem 冷启动 rootfs。
func (a *Adapter) RestoreSnapshot(
	ctx context.Context,
	req *pb.RestoreSnapshotRequest,
) (*pb.Machine, string, string, error) {
	sp, ok := a.instances.(snapshotProvider)
	if !ok {
		return nil, "", "", ErrSnapshotUnsupported
	}
	snaps, err := sp.ListSnapshots(
		ctx,
		&instances.ListSnapshotsFilter{Tags: tags.Tags{tagSnapshotID: req.GetSnapshotId()}},
	)
	if err != nil || len(snaps) != 1 {
		return nil, "", "", ErrSnapshotNotFound
	}
	inst, err := a.instances.GetInstance(ctx, req.GetMachineId())
	if err != nil {
		return nil, "", "", err
	}
	mode := req.GetRestoreMode()
	if mode == "" {
		mode = "auto"
	}
	filesystemOnly := mode == "filesystem"
	compatible := snaps[0].CompatibilityKey != "" && req.GetCompatibilityKey() != "" &&
		snaps[0].CompatibilityKey == req.GetCompatibilityKey()
	if mode == "auto" {
		if compatible {
			mode = "memory"
		} else {
			filesystemOnly = true
			mode = "filesystem"
		}
	}
	if mode == "memory" && !compatible {
		return nil, "", "", ErrSnapshotIncompatible
	}
	targetTags := tags.Clone(inst.Tags)
	targetTags[tagMachine] = req.GetMachineId()
	targetTags[tagExecution] = req.GetExecutionId()
	targetTags[tagGeneration] = strconv.FormatUint(req.GetGeneration(), 10)
	if inst.State == instances.StateRunning || inst.State == instances.StateInitializing {
		if _, err := sp.StopInstance(ctx, inst.Id); err != nil {
			return nil, "", "", fmt.Errorf("stop old execution before restore: %w", err)
		}
	}
	restored, err := sp.RestoreSnapshot(
		ctx,
		inst.Id,
		snaps[0].Id,
		instances.RestoreSnapshotRequest{
			TargetState:      instances.StateRunning,
			FilesystemOnly:   filesystemOnly,
			CompatibilityKey: req.GetCompatibilityKey(),
			Tags:             targetTags,
		},
	)
	if err != nil {
		// Compatibility is decided above from immutable artifact metadata. Do not
		// reinterpret corruption, checksum, permission or runtime errors as a
		// reason for auto filesystem fallback.
		return nil, "", "", fmt.Errorf("hypeman restore snapshot: %w", err)
	}
	// A restore is an execution replacement. Snapshot metadata contains the
	// source identity, so overwrite the externally observed identity with the
	// fenced target before returning it to the control plane.
	if restored.Tags == nil {
		restored.Tags = tags.Tags{}
	}
	restored.Tags[tagExecution] = req.GetExecutionId()
	restored.Tags[tagGeneration] = strconv.FormatUint(req.GetGeneration(), 10)
	if err := a.reattachSlot(ctx, req.GetMachineId(), restored); err != nil {
		cleanupCtx := context.WithoutCancel(ctx)
		_, stopErr := sp.StopInstance(cleanupCtx, restored.Id)
		var releaseErr error
		if a.slots != nil {
			releaseErr = a.slots.Release(cleanupCtx, req.GetMachineId())
		}
		return nil, "", "", errors.Join(err, stopErr, releaseErr)
	}
	consistency := ""
	if filesystemOnly {
		consistency = "clean"
	}
	return mapMachine(restored), mode, consistency, nil
}

func translateCompression(c *pb.SnapshotCompressionSpec) (*snapshot.SnapshotCompressionConfig, error) {
	if c == nil || c.GetAlgorithm() == pb.SnapshotCompressionSpec_ALGORITHM_UNSPECIFIED ||
		c.GetAlgorithm() == pb.SnapshotCompressionSpec_NONE {
		return nil, nil
	}
	switch c.GetAlgorithm() {
	case pb.SnapshotCompressionSpec_ZSTD:
		level := int(c.GetLevel())
		if level < 0 {
			level = snapshot.DefaultSnapshotCompressionZstdLevel
		}
		if level > snapshot.MaxSnapshotCompressionZstdLevel {
			return nil, fmt.Errorf("zstd level %d exceeds max %d", level, snapshot.MaxSnapshotCompressionZstdLevel)
		}
		return &snapshot.SnapshotCompressionConfig{
			Enabled:   true,
			Algorithm: snapshot.SnapshotCompressionAlgorithmZstd, Level: &level,
		}, nil
	case pb.SnapshotCompressionSpec_LZ4:
		level := int(c.GetLevel())
		if level < 0 {
			level = snapshot.DefaultSnapshotCompressionLz4Level
		}
		if level > snapshot.MaxSnapshotCompressionLz4Level {
			return nil, fmt.Errorf("lz4 level %d exceeds max %d", level, snapshot.MaxSnapshotCompressionLz4Level)
		}
		return &snapshot.SnapshotCompressionConfig{
			Enabled:   true,
			Algorithm: snapshot.SnapshotCompressionAlgorithmLz4, Level: &level,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm %v", c.GetAlgorithm())
	}
}

func mapSnapshotInfo(snap *instances.Snapshot, req *pb.CreateSnapshotRequest) *pb.SnapshotInfo {
	kind := req.GetKind()
	if kind == pb.SnapshotKind_SNAPSHOT_KIND_UNSPECIFIED {
		switch snap.Kind {
		case instances.SnapshotKindStandby:
			kind = pb.SnapshotKind_SNAPSHOT_MEMORY
		case instances.SnapshotKindFilesystem, instances.SnapshotKindStopped:
			kind = pb.SnapshotKind_SNAPSHOT_FILESYSTEM
		}
	}
	info := &pb.SnapshotInfo{
		Id:               req.GetSnapshotId(),
		ArtifactId:       snap.Id,
		MachineId:        req.GetMachineId(),
		ExecutionId:      req.GetExecutionId(),
		Kind:             kind,
		SizeBytes:        uint64(snap.SizeBytes),
		CompressionState: snap.CompressionState,
		CreatedAtUnix:    snap.CreatedAt.Unix(),
		ArtifactSha256:   snap.ArtifactSHA256,
		CompatibilityKey: snap.CompatibilityKey,
	}
	if snap.Compression != nil {
		info.CompressionAlgorithm = string(snap.Compression.Algorithm)
	}
	if snap.CompressedSizeBytes != nil {
		info.SizeBytes = uint64(*snap.CompressedSizeBytes)
	}
	return info
}
