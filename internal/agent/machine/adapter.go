// Package machine 是 agent 对 hypeman lib/instances 的 M1 最小适配层。
// 它把 firepaas 的 agent 契约（protos/agent/v1）映射为 hypeman 的 domain 请求，
// 并通过 tags 保存 firepaas 的业务标识（project/app/deployment/execution），
// 供 ListMachines 重建 spec。slot/bridge 切换只发生在本包与 network 包内。
package machine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kernel/hypeman/lib/autostandby"
	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/network"
	"github.com/kernel/hypeman/lib/tags"

	"github.com/example/firepaas/internal/agent/health"
	"github.com/example/firepaas/internal/agent/network/slot"
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
	tagGeneration = "firepaas/generation"
	// tagSecretKeys 是回显剔除用的键名列表（仅 opt-in 注入模式写入；值永不入 tag）。
	tagSecretKeys = "firepaas/secret_keys"
	// tagServices（v1.1，ADR-0022）：多端口 services 的紧凑 JSON
	//（[{"n":"http","p":80}]），base64url 编码（tag 值字符集限制，同 tagHealth）。
	// 只存主 service 之外的附加 service；主端口沿用 tagPort。
	tagServices = "firepaas/services"
)

// InstanceManager 是 Adapter 需要的 hypeman instance 能力子集。
// 用窄接口而非 instances.Manager，便于单测替身。
type InstanceManager interface {
	CreateInstance(ctx context.Context, req instances.CreateInstanceRequest) (*instances.Instance, error)
	ListInstances(ctx context.Context, filter *instances.ListInstancesFilter) ([]instances.Instance, error)
	GetInstance(ctx context.Context, idOrName string) (*instances.Instance, error)
	DeleteInstance(ctx context.Context, id string) error
	// M4.5（mvp-plan §8.4）：scale-to-zero。hypeman 语义：
	//   Standby = pause + snapshot + 删除 VMM（快照留在节点上）；
	//   Restore = 从 standby 快照恢复 VM 到 Running。
	// 两者对已处目标态的实例均幂等；Restore 在无快照时报错 → 由
	// controller 走 cold-start 重建降级。
	// hypeman.Manager 签名带 request 参数（保留压缩配置等），Adapter 包装为单参。
	StandbyInstance(ctx context.Context, id string, req instances.StandbyInstanceRequest) (*instances.Instance, error)
	RestoreInstance(ctx context.Context, id string) (*instances.Instance, error)
}

// ImageManager 是 Adapter 需要的 hypeman image 能力子集。
type ImageManager interface {
	CreateImage(ctx context.Context, req images.CreateImageRequest) (*images.Image, error)
	WaitForReady(ctx context.Context, name string) error
	// M5.1：列出全部镜像（ready 轮询 + 解包大小准入）。hypeman 对 OCI index
	// digest 引用（多平台镜像）的 GetImage 按平台 manifest 路径寻址会 404，
	// 而上游 ListImages 返回 metadata 原始 name（含 digest 形态）——firepaas
	// 只用这个稳定面。
	ListImages(ctx context.Context) ([]images.Image, error)
}

// CachedImageDigests 返回节点本地镜像缓存的 digest 列表（v1.1，ADR-0018）。
// 每个镜像贡献两个形态：name 里的 digest 引用（与控制面 image_ref 对齐）
// 与解析后的 manifest digest（M5 遗留：OCI index 引用两者不同，都入集合）。
// 排序：CreatedAt 降序（最新创建优先；hypeman 未暴露 last-used，因此不是 LRU），截断上限 cap。
func (a *Adapter) CachedImageDigests(ctx context.Context, cap int) []string {
	if cap <= 0 {
		cap = 512
	}
	ims, err := a.images.ListImages(ctx)
	if err != nil {
		return nil
	}
	type entry struct {
		name    string
		digest  string
		created time.Time
	}
	list := make([]entry, 0, len(ims))
	for _, img := range ims {
		if img.Status != images.StatusReady {
			continue
		}
		name := img.Name
		digest := img.Digest
		if i := strings.LastIndex(name, "@"); i >= 0 {
			if d := name[i+1:]; d != "" {
				list = append(list, entry{name: d, created: img.CreatedAt})
			}
		}
		if digest != "" {
			list = append(list, entry{name: digest, created: img.CreatedAt})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].created.After(list[j].created) })
	seen := make(map[string]bool, len(list))
	out := make([]string, 0, len(list))
	for _, e := range list {
		if seen[e.name] {
			continue
		}
		seen[e.name] = true
		out = append(out, e.name)
		if len(out) >= cap {
			break
		}
	}
	return out
}

// ErrImageTooBig：镜像解包大小超 FIREPAAS_IMAGE_MAX_UNPACK_MIB（永久错误）。
var ErrImageTooBig = errors.New("image unpack size exceeds limit")

// ErrSecretEnvInjectionUnsupported 表示 secret_env 注入被拒绝：默认 fail
// closed，因为 hypeman 的 Env 会明文持久化到节点 metadata.json，没有可
// 验证的 one-shot 注入 API（M5 评审决策：安全默认 + 受控 opt-in）。
// 受信/实验室环境可显式 FIREPAAS_SECRET_INJECTION=unsafe-persisted-env
// 恢复 M4 注入语义（明知 secret 会落盘）；v1.1 交付真 one-shot 通道后移除。
var ErrSecretEnvInjectionUnsupported = errors.New(
	"secret_env injection is unsupported: no safe one-shot injection API " +
		"(opt-in via FIREPAAS_SECRET_INJECTION=unsafe-persisted-env on trusted nodes)")

// SecretInjectionMode：secret_env 注入策略。
const (
	// SecretInjectionOff：拒绝携带 secret_env 的 create（默认，安全）。
	SecretInjectionOff = ""
	// SecretInjectionUnsafePersistedEnv：M4 语义——合并进 hypeman Env。
	// 明知 hypeman 会把 Env 明文持久化到 metadata.json，仅限受信环境。
	SecretInjectionUnsafePersistedEnv = "unsafe-persisted-env"
)

// Adapter 包装 hypeman 的 instance/image manager。slots 非空时启用 slot
// 网络后端（ADR-0004）：create 后把 hypeman TAP 移入 slot netns，delete 后回收。
type Adapter struct {
	instances InstanceManager
	images    ImageManager
	slots     *slot.Manager
	health    *health.Tracker
	// M4.5：GetEndpoint 遇 Standby 实例时同步唤醒（autoresume，<5s SLO 来自
	// M0 restore p95 基准）。默认开启；FIREPAAS_AGENT_AUTORESUME=false 关闭。
	autoResume bool
	// maxUnpackMib：镜像解包大小上限 MiB（0 = 不限制）。M5.1。
	maxUnpackMib int64
	// secretInjection：secret_env 注入策略（默认拒绝，见 ErrSecretEnvInjectionUnsupported）。
	secretInjection string
	// wakeObserver（v1.1，ADR-0017）：autoresume 唤醒观测（发生 standby
	// restore 时回调；nil = 不观测）。
	wakeObserver func(machineID string, took time.Duration)
}

// New 构造 Adapter。slotManager 为 nil 时保持 M1 bridge 行为；
// healthTracker 为 nil 时 readiness 退化为 UNKNOWN/UNCONFIGURED。
func New(instances InstanceManager, images ImageManager, slotManager *slot.Manager, healthTracker *health.Tracker) *Adapter {
	return &Adapter{instances: instances, images: images, slots: slotManager,
		health: healthTracker, autoResume: true}
}

// SetAutoResume 控制 GetEndpoint 的 standby 同步唤醔。
func (a *Adapter) SetAutoResume(v bool) { a.autoResume = v }

// SetMaxUnpackMib 配置镜像解包大小上限（FIREPAAS_IMAGE_MAX_UNPACK_MIB）。
func (a *Adapter) SetMaxUnpackMib(mib int64) { a.maxUnpackMib = mib }

// SetSecretInjection 配置 secret_env 注入策略（见 SecretInjectionMode*）。
func (a *Adapter) SetSecretInjection(mode string) { a.secretInjection = mode }

// SetWakeObserver 注入 autoresume 唤醒观测回调（v1.1，ADR-0017 metrics）。
func (a *Adapter) SetWakeObserver(fn func(machineID string, took time.Duration)) { a.wakeObserver = fn }

// EnsureImage 确保 image_ref 就绪（v1.1，ADR-0018 部署预取的 agent 侧入口）。
// 复用 create 路径的拉取与准入逻辑（digest 解析、LRU、解包上限）；
// 幂等（已在队列/已就绪直接等待 ready）。磁盘水位检查由调用方（server
// 的 PullImage）先行执行。
func (a *Adapter) EnsureImage(ctx context.Context, imageRef string) error {
	return a.ensureImageReady(ctx, imageRef)
}

// ImageInfo 返回已就绪镜像的 digest 与解包大小（MiB；未知为 0）。
func (a *Adapter) ImageInfo(ctx context.Context, imageRef string) (digest string, sizeMib uint64) {
	ims, err := a.images.ListImages(ctx)
	if err != nil {
		return "", 0
	}
	prefix := imageRef
	if i := strings.Index(prefix, "@"); i >= 0 {
		prefix = prefix[:i]
	}
	for _, img := range ims {
		if img.Name != imageRef && !strings.HasPrefix(img.Name, prefix+"@") {
			continue
		}
		if img.Status != images.StatusReady {
			continue
		}
		if img.SizeBytes != nil {
			sizeMib = uint64(*img.SizeBytes) >> 20
		}
		if img.Digest != "" {
			return img.Digest, sizeMib
		}
		if i := strings.LastIndex(img.Name, "@"); i >= 0 {
			return img.Name[i+1:], sizeMib
		}
	}
	return "", 0
}

// ErrImageNotFound 表示镜像引用无法解析/拉取（永久性业务错误：重试不会
// 改变结果）。controller 据此把 create 置为终态 FAILED，避免无限重派。
var ErrImageNotFound = errors.New("image not found")

// svcJSON 是 tagServices 的紧凑形态（控制 tag 长度）。
type svcJSON struct {
	Name string `json:"n,omitempty"`
	Port int    `json:"p"`
}

// ErrInvalidAutoStandby 表示 auto_standby 策略非法（永久错误）。
var ErrInvalidAutoStandby = errors.New("invalid auto_standby policy")

// ErrPortNotAllowed 表示请求端口不在 services 白名单内（ADR-0022）。
var ErrPortNotAllowed = errors.New("requested port is not declared by services")

// translateAutoStandby 把 proto AutoStandbyPolicy 翻译为 hypeman 策略。
// 探针排除不经由 IgnoreSourceCIDRs（slot 后端下与代理回流同源段，会误伤
// 真实流量）；agentd 用 probeflow registry 在 conntrack 源头精确剔除。
func translateAutoStandby(as *pb.AutoStandbyPolicy) (*autostandby.Policy, error) {
	if as == nil || !as.GetEnabled() {
		return nil, nil
	}
	if as.GetIdleTimeoutSeconds() < 5 {
		return nil, fmt.Errorf("%w: idle_timeout_seconds must be >= 5", ErrInvalidAutoStandby)
	}
	policy := &autostandby.Policy{
		Enabled:     true,
		IdleTimeout: fmt.Sprintf("%ds", as.GetIdleTimeoutSeconds()),
	}
	for _, cidr := range as.GetIgnoreSourceCidrs() {
		policy.IgnoreSourceCIDRs = append(policy.IgnoreSourceCIDRs, cidr)
	}
	for _, p := range as.GetIgnoreDestinationPorts() {
		if p == 0 || p > 65535 {
			return nil, fmt.Errorf("%w: ignore_destination_ports entry %d out of range", ErrInvalidAutoStandby, p)
		}
		policy.IgnoreDestinationPorts = append(policy.IgnoreDestinationPorts, uint16(p))
	}
	return autostandby.NormalizePolicy(policy)
}

// encodeServicesTag 把附加 services（除主端口外）编码为 base64url tag。
// 主 service 端口沿用 tagPort（单端口兼容）；空列表返回空串。
func encodeServicesTag(spec *pb.MachineSpec) (string, error) {
	if spec == nil {
		return "", nil
	}
	primary := ingressPortInt(spec.Network)
	var list []svcJSON
	for _, s := range spec.GetServices() {
		if s.GetInternalPort() == 0 {
			return "", fmt.Errorf("%w: service %q has no internal_port", ErrInvalidAutoStandby, s.GetName())
		}
		if int(s.GetInternalPort()) == primary {
			continue // 主端口在 tagPort
		}
		list = append(list, svcJSON{Name: s.GetName(), Port: int(s.GetInternalPort())})
	}
	if len(list) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(list)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	if len(encoded) > 200 {
		return "", fmt.Errorf("%w: services too large for tag", ErrInvalidAutoStandby)
	}
	return encoded, nil
}

// decodeServicesTag 解析 tagServices（附加 service）；容错返回 nil。
func decodeServicesTag(tag string) []svcJSON {
	if tag == "" {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(tag)
	if err != nil {
		return nil
	}
	var list []svcJSON
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil
	}
	return list
}

// Create 确保镜像就绪后创建 VM。返回的 Machine 只含回显安全字段。
func (a *Adapter) Create(ctx context.Context, req *pb.CreateMachineRequest) (*pb.Machine, error) {
	// 默认 fail closed（hypeman Env 明文落盘）；受信环境 opt-in 恢复 M4 语义。
	if len(req.GetSecretEnv()) != 0 && a.secretInjection != SecretInjectionUnsafePersistedEnv {
		return nil, ErrSecretEnvInjectionUnsupported
	}
	spec := req.Spec
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := a.ensureImageReady(waitCtx, spec.ImageRef); err != nil {
		if errors.Is(err, images.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrImageNotFound, err)
		}
		return nil, err
	}

	env := map[string]string{}
	for k, v := range spec.Env {
		env[k] = v
	}
	// secret_env 单向注入（opt-in 模式）：值进入 VM 启动配置（hypeman Env，
	// 明知会明文持久化到节点 metadata.json），键名记录在 tags 供回显剔除。
	secretKeys := make([]string, 0, len(req.SecretEnv))
	for k, v := range req.SecretEnv {
		env[k] = v
		secretKeys = append(secretKeys, k)
	}
	sort.Strings(secretKeys)
	// 完整探针策略 JSON 入 tag（ADR-0008）；EXEC 或非法 target 降级为
	// 未声明（UNCONFIGURED = RUNNING 即 READY）。
	healthTag := "0"
	if spec.HealthCheck != nil {
		if encoded, err := health.EncodePolicy(spec.HealthCheck); err == nil && encoded != "" {
			healthTag = encoded
		}
	}

	sizeBytes := int64(spec.MemMib) * 1024 * 1024
	overlayBytes := int64(spec.DiskMib) * 1024 * 1024
	// v1.1（ADR-0017）：auto-standby 策略翻译（默认关闭 = nil）。
	standbyPolicy, err := translateAutoStandby(spec.GetAutoStandby())
	if err != nil {
		return nil, err
	}
	// v1.1（ADR-0022）：附加 services 入 tag（主端口沿用 tagPort）。
	servicesTag, err := encodeServicesTag(spec)
	if err != nil {
		return nil, err
	}
	hreq := instances.CreateInstanceRequest{
		Name:           req.MachineId,
		Image:          spec.ImageRef,
		Size:           sizeBytes,
		OverlaySize:    overlayBytes,
		Vcpus:          int(spec.Vcpu),
		Env:            env,
		NetworkEnabled: true,
		AutoStandby:    standbyPolicy,
		Tags: tags.Tags{
			tagProject:    spec.ProjectId,
			tagApp:        spec.AppId,
			tagDeployment: spec.DeploymentId,
			tagExecution:  spec.ExecutionId,
			tagHealth:     healthTag,
			tagHostname:   spec.Hostname,
			tagPort:       ingressPort(spec.Network),
			tagGeneration: strconv.FormatUint(req.Generation, 10),
			tagSecretKeys: strings.Join(secretKeys, ","),
			tagServices:   servicesTag,
		},
	}

	inst, err := a.instances.CreateInstance(ctx, hreq)
	if err != nil {
		return nil, fmt.Errorf("hypeman create: %w", err)
	}
	if a.slots != nil {
		// slot 后端：把 hypeman 刚创建的 TAP 移入 slot netns。失败时回收
		// 刚创建的实例并返回错误（controller 会按退避重试）。
		tap := network.GenerateTAPName(inst.Id)
		if _, err := a.slots.Attach(ctx, req.MachineId, tap, inst.IP); err != nil {
			_ = a.instances.DeleteInstance(ctx, inst.Id)
			return nil, fmt.Errorf("slot attach: %w", err)
		}
	}
	return mapMachine(inst), nil
}

// List 返回全部（或按 project 过滤）的 machine。
// P1-1：探针执行已移入 health.Worker（agentd 后台循环），本路径只做
// O(1) 的 Observe 注册（刷新实例视图/策略/换代重置）与缓存读，不再有
// 网络 IO——不再与 gRPC 的 10s deadline 竞争。
func (a *Adapter) List(ctx context.Context, projectID string) ([]*pb.Machine, error) {
	listed, err := a.instances.ListInstances(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("hypeman list: %w", err)
	}
	out := make([]*pb.Machine, 0, len(listed))
	for _, inst := range listed {
		if a.health != nil {
			a.health.Observe(ctx, &inst)
		}
		m := mapMachine(&inst)
		if a.health != nil {
			id := inst.Name
			if id == "" {
				id = inst.Id
			}
			m.Readiness, m.LastReadinessChange = a.health.Readiness(id)
		}
		if projectID != "" && m.Spec.GetProjectId() != projectID {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// Delete 停止并删除 machine。machineID 是 firepaas 稳定 ID（hypeman Name）；
// hypeman DeleteInstance 只接受其内部 ID，因此先 GetInstance 解析。expectedExecution
// 非空时必须在删除前验证当前实例 tag；调用方须以 machine fence 串行这段
// 读取、验证、删除，避免旧 execution 删除新 execution。
func (a *Adapter) Delete(ctx context.Context, machineID, expectedExecution string) error {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		return fmt.Errorf("hypeman get for delete: %w", err)
	}
	if expectedExecution != "" && inst.Tags[tagExecution] != expectedExecution {
		return fmt.Errorf("execution mismatch for %s: want %s got %s",
			machineID, expectedExecution, inst.Tags[tagExecution])
	}
	if err := a.instances.DeleteInstance(ctx, inst.Id); err != nil {
		return fmt.Errorf("hypeman delete: %w", err)
	}
	if a.slots != nil {
		if err := a.slots.Release(ctx, machineID); err != nil {
			return fmt.Errorf("slot release: %w", err)
		}
	}
	if a.health != nil {
		a.health.Remove(machineID)
	}
	return nil
}

// GetEndpoint 解析 proxy 需要的 workload endpoint（M1 bridge guest IP）。
// 校验 execution_id 与 tags 一致，防止 edge 向 stale execution 转发。
func (a *Adapter) GetEndpoint(ctx context.Context, machineID, executionID string) (ip string, port int, err error) {
	return a.GetEndpointForPort(ctx, machineID, executionID, 0)
}

// GetEndpointForPort 是 GetEndpoint 的多端口形态（v1.1，ADR-0022）。
// wantPort == 0 → 主 service 端口（旧行为，向后兼容：edge 未带
// X-Firepaas-App-Port 头时走此分支）；wantPort > 0 → 必须在 services
// 白名单内（主端口 + tagServices 附加端口），否则 ErrPortNotAllowed
// （proxy 拒绝未声明端口）。
func (a *Adapter) GetEndpointForPort(ctx context.Context, machineID, executionID string, wantPort int) (ip string, port int, err error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		return "", 0, fmt.Errorf("get instance %s: %w", machineID, err)
	}
	if executionID != "" && inst.Tags[tagExecution] != executionID {
		return "", 0, fmt.Errorf("execution mismatch for %s: want %s got %s",
			machineID, executionID, inst.Tags[tagExecution])
	}
	// M4.5 autoresume：首个流量请求触发 standby→Running 同步恢复。
	// 失败返回错误（502/503 由 proxy 转化）；控制器侧不感知此路径——
	// 恢复后的 Running 实例随下次 sync 自然回到投影。
	// 注意 RestoreInstance 需要内部 ID（loadMetadata 按目录名加载）：
	// 用上面 GetInstance 已解析的 inst.Id，不能直接传 machineID（名字）。
	if inst.State == instances.StateStandby {
		if !a.autoResume {
			return "", 0, fmt.Errorf("instance %s is standby and autoresume is disabled", machineID)
		}
		wakeStart := time.Now()
		wakeCtx, wakeCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer wakeCancel()
		restored, rerr := a.instances.RestoreInstance(wakeCtx, inst.Id)
		if rerr != nil {
			return "", 0, fmt.Errorf("autoresume %s failed (cold-start will be needed): %w", machineID, rerr)
		}
		// restore 在 root ns 重建 TAP：重挂 slot 后才有数据面。
		if aerr := a.reattachSlot(wakeCtx, machineID, restored); aerr != nil {
			return "", 0, fmt.Errorf("autoresume %s: %w", machineID, aerr)
		}
		if a.wakeObserver != nil {
			a.wakeObserver(machineID, time.Since(wakeStart))
		}
		inst = restored
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
	if wantPort > 0 {
		if wantPort != port && !servicePortAllowed(inst.Tags[tagServices], wantPort) {
			return "", 0, fmt.Errorf("%w: %d (machine %s)", ErrPortNotAllowed, wantPort, machineID)
		}
		port = wantPort
	}
	return inst.IP, port, nil
}

// servicePortAllowed 判断 wantPort 是否在附加 services 白名单内。
func servicePortAllowed(servicesTag string, wantPort int) bool {
	for _, s := range decodeServicesTag(servicesTag) {
		if s.Port == wantPort {
			return true
		}
	}
	return false
}

func (a *Adapter) ensureImageReady(ctx context.Context, imageRef string) error {
	if _, err := a.images.CreateImage(ctx, images.CreateImageRequest{Name: imageRef}); err != nil {
		// 已在队列/已存在等错误继续等待 ready。
	}
	// M5.1：改为 ListImages 轮询取代 WaitForReady——hypeman 对 OCI index
	// digest 引用的 GetImage 寻址有缺陷（目录键是平台 manifest digest），
	// 而 ListImages 的 Name 保留请求时的 digest 形态，可用于匹配。
	return a.waitImageReady(ctx, imageRef)
}

// waitImageReady 轮询 ListImages 直到匹配 imageRef 的镜像 ready；
// ctx 超时或镜像进入 error 态则返回永久错误。匹配规则：
// Name 全等，或者 imageRef 为 digest 形态时 Name 的前缀（仓库路径）相同。
func (a *Adapter) waitImageReady(ctx context.Context, imageRef string) error {
	prefix := imageRef
	if i := strings.Index(prefix, "@"); i >= 0 {
		prefix = prefix[:i]
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		ims, err := a.images.ListImages(ctx)
		if err != nil {
			return fmt.Errorf("list images: %w", err)
		}
		for _, img := range ims {
			if img.Name != imageRef && !strings.HasPrefix(img.Name, prefix+"@") {
				continue
			}
			if img.Error != nil {
				return fmt.Errorf("image %s failed: %s", imageRef, *img.Error)
			}
			if img.Status == images.StatusReady {
				if a.maxUnpackMib > 0 && img.SizeBytes != nil &&
					*img.SizeBytes > a.maxUnpackMib<<20 {
					return fmt.Errorf("%w: %s unpacked %d MiB > limit %d MiB",
						ErrImageTooBig, imageRef, *img.SizeBytes>>20, a.maxUnpackMib)
				}
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("image %s not ready: %w", imageRef, ctx.Err())
		case <-tick.C:
		}
	}
}

// redactSecretEnv 从回显 env 中剔除 secret_env 注入的键。hypeman 侧保留
// 完整 env（VM 启动配置需要）；只有回显/持久化路径走本函数。
func redactSecretEnv(env map[string]string, secretKeysTag string) map[string]string {
	if len(env) == 0 || secretKeysTag == "" {
		return env
	}
	secret := make(map[string]bool)
	for _, k := range strings.Split(secretKeysTag, ",") {
		if k != "" {
			secret[k] = true
		}
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		if !secret[k] {
			out[k] = v
		}
	}
	return out
}

func ingressPort(n *pb.NetworkSpec) string {
	if n == nil || n.IngressPort == 0 {
		return "8080"
	}
	return strconv.FormatUint(n.IngressPort, 10)
}

func ingressPortInt(n *pb.NetworkSpec) int {
	if n == nil || n.IngressPort == 0 {
		return 8080
	}
	return int(n.IngressPort)
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
		// 回显前剔除 secret_env 注入的键（ADR-0013 不变量 3：secret 值不得
		// 出现在 Machine/MachineSpec、ListMachines 与持久化 operation result）。
		Env: redactSecretEnv(inst.Env, inst.Tags[tagSecretKeys]),
	}
	if portStr := inst.Tags[tagPort]; portStr != "" {
		if port, err := strconv.ParseUint(portStr, 10, 32); err == nil {
			spec.Network = &pb.NetworkSpec{IngressPort: port}
		}
	}
	if inst.Tags[tagHealth] != "" && inst.Tags[tagHealth] != "0" && inst.Tags[tagHealth] != "null" {
		// 探针策略已在 List 路径注册；回显的 HealthCheckSpec 只用于确认声明存在。
		spec.HealthCheck = &pb.HealthCheckSpec{}
	}

	readiness := pb.MachineReadiness_UNKNOWN
	if spec.HealthCheck == nil {
		// ADR-0008：未声明 health_check 时上报 UNCONFIGURED，等价 RUNNING 即 READY。
		readiness = pb.MachineReadiness_UNCONFIGURED
	}

	// firepaas 的稳定 machine_id 就是 hypeman instance 的 Name；
	// hypeman 生成的 CUID2 仅作为本机内部句柄。
	machineID := inst.Name
	if machineID == "" {
		machineID = inst.Id
	}

	generation := uint64(0)
	if g, err := strconv.ParseUint(inst.Tags[tagGeneration], 10, 64); err == nil {
		generation = g
	}
	m := &pb.Machine{
		MachineId:   machineID,
		ExecutionId: spec.ExecutionId,
		State:       mapState(inst.State),
		Spec:        spec,
		SlotIp:      inst.IP,
		CreatedAt:   timestamppb.New(inst.CreatedAt),
		Readiness:   readiness,
		Generation:  generation, // R6 orphan 清理按它下发 fence 安全的 delete（P1-2）
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

// Pause 将 machine 转入 standby（pause+snapshot+释放 VMM）。已 standby 直接
// 返回（幂等）。slot 后端无需改动：TAP/netns 保留，恢复后 IP/端口不变。
// executionID 非空时校验实例当前 execution 与之匹配（P3-18：旧代操作
// 不误停新代实例；与 GetEndpoint/Delete 同一纪律）。
func (a *Adapter) Pause(ctx context.Context, machineID, executionID string) (*pb.Machine, error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		if errors.Is(err, instances.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrMachineNotFound, machineID)
		}
		return nil, fmt.Errorf("get instance %s: %w", machineID, err)
	}
	if executionID != "" && inst.Tags[tagExecution] != executionID {
		return nil, fmt.Errorf("execution mismatch for %s: want %s got %s",
			machineID, executionID, inst.Tags[tagExecution])
	}
	inst, err = a.instances.StandbyInstance(ctx, inst.Id, instances.StandbyInstanceRequest{})
	if err != nil {
		return nil, fmt.Errorf("hypeman standby: %w", err)
	}
	return mapMachine(inst), nil
}

// reattachSlot 在 restore 后重建 slot 网络基座：hypeman standby 会释放网络，
// restore 在 root ns 重建同名 TAP；slot 后端要求 TAP 位于 slot netns 内。
// Attach 幂等（ensureKernel 会把 root ns 的 TAP 补移入 netns 并重加 /32 路由）。
// 不重挂则 VM Running 但流量 502（M4 真机验收发现的 autoresume 盲区）。
func (a *Adapter) reattachSlot(ctx context.Context, machineID string, inst *instances.Instance) error {
	if a.slots == nil || inst == nil {
		return nil
	}
	tap := network.GenerateTAPName(inst.Id)
	if _, err := a.slots.Attach(ctx, machineID, tap, inst.IP); err != nil {
		return fmt.Errorf("slot re-attach after restore: %w", err)
	}
	return nil
}

// Resume 从 standby 恢复到 Running。M0 基准 restore p95≈95ms + guest 启动；
// 失败由 controller 决定重试或 cold-start 重建。恢复后重挂 slot（standby
// 释放了网络，restore 在 root ns 重建 TAP）——失败则本 op 报错重试，
// RestoreInstance 对已 Running 实例幂等，重试安全。
func (a *Adapter) Resume(ctx context.Context, machineID, executionID string) (*pb.Machine, error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		if errors.Is(err, instances.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrMachineNotFound, machineID)
		}
		return nil, fmt.Errorf("get instance %s: %w", machineID, err)
	}
	if executionID != "" && inst.Tags[tagExecution] != executionID {
		return nil, fmt.Errorf("execution mismatch for %s: want %s got %s",
			machineID, executionID, inst.Tags[tagExecution])
	}
	inst, err = a.instances.RestoreInstance(ctx, inst.Id)
	if err != nil {
		return nil, fmt.Errorf("hypeman restore: %w", err)
	}
	if err := a.reattachSlot(ctx, machineID, inst); err != nil {
		return nil, err
	}
	return mapMachine(inst), nil
}

// ErrMachineNotFound 表示 machine 在 agent 侧不存在（实例已删/未建）。
// 与 ErrImageNotFound（永久性业务错误）区分：前者是生命周期状态，
// controller 的 reconcile 决策表会把 NotFound 收敛为幂等成功，不应
// 误判成镜像不可拉取的终态失败（P2-9）。
var ErrMachineNotFound = errors.New("machine not found at agent")
