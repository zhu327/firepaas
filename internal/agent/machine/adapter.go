// Package machine 是 agent 对 hypeman lib/instances 的 M1 最小适配层。
// 它把 firepaas 的 agent 契约（protos/agent/v1）映射为 hypeman 的 domain 请求，
// 并通过 tags 保存 firepaas 的业务标识（project/app/deployment/execution），
// 供 ListMachines 重建 spec。slot/bridge 切换只发生在本包与 network 包内。
package machine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/tags"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	tagProject    = "firepaas/project_id"
	tagApp        = "firepaas/app_id"
	tagDeployment = "firepaas/deployment_id"
	tagExecution  = "firepaas/execution_id"
	tagHealth     = "firepaas/health_check"
	tagHostname   = "firepaas/hostname"
	tagPort       = "firepaas/port"
)

// InstanceManager 是 Adapter 需要的 hypeman instance 能力子集。
// 用窄接口而非 instances.Manager，便于单测替身。
type InstanceManager interface {
	CreateInstance(ctx context.Context, req instances.CreateInstanceRequest) (*instances.Instance, error)
	ListInstances(ctx context.Context, filter *instances.ListInstancesFilter) ([]instances.Instance, error)
	GetInstance(ctx context.Context, idOrName string) (*instances.Instance, error)
	DeleteInstance(ctx context.Context, id string) error
}

// ImageManager 是 Adapter 需要的 hypeman image 能力子集。
type ImageManager interface {
	CreateImage(ctx context.Context, req images.CreateImageRequest) (*images.Image, error)
	WaitForReady(ctx context.Context, name string) error
}

// Adapter 包装 hypeman 的 instance/image manager。
type Adapter struct {
	instances InstanceManager
	images    ImageManager
}

// New 构造 Adapter。
func New(instances InstanceManager, images ImageManager) *Adapter {
	return &Adapter{instances: instances, images: images}
}

// Create 确保镜像就绪后创建 VM。返回的 Machine 只含回显安全字段。
func (a *Adapter) Create(ctx context.Context, req *pb.CreateMachineRequest) (*pb.Machine, error) {
	spec := req.Spec
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := a.ensureImageReady(waitCtx, spec.ImageRef); err != nil {
		return nil, err
	}

	env := map[string]string{}
	for k, v := range spec.Env {
		env[k] = v
	}
	for k, v := range req.SecretEnv {
		env[k] = v
	}

	sizeBytes := int64(spec.MemMib) * 1024 * 1024
	overlayBytes := int64(spec.DiskMib) * 1024 * 1024
	hreq := instances.CreateInstanceRequest{
		Name:           req.MachineId,
		Image:          spec.ImageRef,
		Size:           sizeBytes,
		OverlaySize:    overlayBytes,
		Vcpus:          int(spec.Vcpu),
		Env:            env,
		NetworkEnabled: true,
		Tags: tags.Tags{
			tagProject:    spec.ProjectId,
			tagApp:        spec.AppId,
			tagDeployment: spec.DeploymentId,
			tagExecution:  spec.ExecutionId,
			tagHealth:     healthTag(spec.HealthCheck),
			tagHostname:   spec.Hostname,
			tagPort:       ingressPort(spec.Network),
		},
	}

	inst, err := a.instances.CreateInstance(ctx, hreq)
	if err != nil {
		return nil, fmt.Errorf("hypeman create: %w", err)
	}
	return mapMachine(inst), nil
}

// List 返回全部（或按 project 过滤）的 machine。
func (a *Adapter) List(ctx context.Context, projectID string) ([]*pb.Machine, error) {
	listed, err := a.instances.ListInstances(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("hypeman list: %w", err)
	}
	out := make([]*pb.Machine, 0, len(listed))
	for _, inst := range listed {
		m := mapMachine(&inst)
		if projectID != "" && m.Spec.GetProjectId() != projectID {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// Delete 停止并删除 machine。machineID 是 firepaas 稳定 ID（hypeman Name）；
// hypeman DeleteInstance 只接受其内部 ID，因此先 GetInstance 解析。
func (a *Adapter) Delete(ctx context.Context, machineID string) error {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		return fmt.Errorf("hypeman get for delete: %w", err)
	}
	if err := a.instances.DeleteInstance(ctx, inst.Id); err != nil {
		return fmt.Errorf("hypeman delete: %w", err)
	}
	return nil
}

// GetEndpoint 解析 proxy 需要的 workload endpoint（M1 bridge guest IP）。
// 校验 execution_id 与 tags 一致，防止 edge 向 stale execution 转发。
func (a *Adapter) GetEndpoint(ctx context.Context, machineID, executionID string) (ip string, port int, err error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		return "", 0, fmt.Errorf("get instance %s: %w", machineID, err)
	}
	if executionID != "" && inst.Tags[tagExecution] != executionID {
		return "", 0, fmt.Errorf("execution mismatch for %s: want %s got %s",
			machineID, executionID, inst.Tags[tagExecution])
	}
	if inst.IP == "" {
		return "", 0, fmt.Errorf("instance %s has no workload endpoint yet", machineID)
	}
	port = 8080
	if portStr := inst.Tags[tagPort]; portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			port = p
		}
	}
	return inst.IP, port, nil
}

func (a *Adapter) ensureImageReady(ctx context.Context, imageRef string) error {
	if _, err := a.images.CreateImage(ctx, images.CreateImageRequest{Name: imageRef}); err != nil {
		// 已在队列/已存在等错误继续等待 ready。
	}
	if err := a.images.WaitForReady(ctx, imageRef); err != nil {
		return fmt.Errorf("image %s not ready: %w", imageRef, err)
	}
	return nil
}

func healthTag(h *pb.HealthCheckSpec) string {
	if h == nil {
		return "0"
	}
	return "1"
}

func ingressPort(n *pb.NetworkSpec) string {
	if n == nil || n.IngressPort == 0 {
		return "8080"
	}
	return strconv.FormatUint(n.IngressPort, 10)
}

func mapMachine(inst *instances.Instance) *pb.Machine {
	spec := &pb.MachineSpec{
		ProjectId:    inst.Tags[tagProject],
		AppId:        inst.Tags[tagApp],
		DeploymentId: inst.Tags[tagDeployment],
		ExecutionId:  inst.Tags[tagExecution],
		Hostname:     inst.Tags[tagHostname],
		ImageRef:     inst.Image,
		Vcpu:         uint64(inst.Vcpus),
		MemMib:       uint64(inst.Size / (1024 * 1024)),
		DiskMib:      uint64(inst.OverlaySize / (1024 * 1024)),
		Env:          inst.Env,
	}
	if portStr := inst.Tags[tagPort]; portStr != "" {
		if port, err := strconv.ParseUint(portStr, 10, 32); err == nil {
			spec.Network = &pb.NetworkSpec{IngressPort: port}
		}
	}
	if inst.Tags[tagHealth] == "1" {
		spec.HealthCheck = &pb.HealthCheckSpec{}
	}

	readiness := pb.MachineReadiness_UNKNOWN
	if spec.HealthCheck == nil {
		// ADR-0008：未声明 health_check 时上报 UNCONFIGURED，M1 降级为
		// RUNNING 即 READY。
		readiness = pb.MachineReadiness_UNCONFIGURED
	}

	// firepaas 的稳定 machine_id 就是 hypeman instance 的 Name；
	// hypeman 生成的 CUID2 仅作为本机内部句柄。
	machineID := inst.Name
	if machineID == "" {
		machineID = inst.Id
	}

	m := &pb.Machine{
		MachineId:   machineID,
		ExecutionId: spec.ExecutionId,
		State:       mapState(inst.State),
		Spec:        spec,
		SlotIp:      inst.IP,
		CreatedAt:   timestamppb.New(inst.CreatedAt),
		Readiness:   readiness,
	}
	return m
}

func mapState(s instances.State) pb.MachineState {
	switch s {
	case instances.StateCreated, instances.StateShutdown:
		return pb.MachineState_PENDING
	case instances.StateInitializing:
		return pb.MachineState_INITIALIZING
	case instances.StateRunning:
		return pb.MachineState_RUNNING
	case instances.StatePaused, instances.StateStandby:
		return pb.MachineState_PAUSED
	case instances.StateStopped:
		return pb.MachineState_STOPPED
	default:
		return pb.MachineState_MACHINE_STATE_UNSPECIFIED
	}
}
