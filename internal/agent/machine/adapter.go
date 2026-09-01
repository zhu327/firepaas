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
	"sync"
	"time"

	"github.com/kernel/hypeman/lib/autostandby"
	"github.com/kernel/hypeman/lib/guest"
	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/network"
	"github.com/kernel/hypeman/lib/tags"
	"github.com/kernel/hypeman/lib/vmconfig"

	"github.com/zhu327/firepaas/internal/agent/egress"
	"github.com/zhu327/firepaas/internal/agent/health"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	tagMachine    = "firepaas/machine_id"
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
	// tagSecretLease（v1.2-B，ADR-0024）：one-shot delivery lease 的 ID。
	// 存在即表示本 execution 接收过 secret（值永不入 tag）；用于 observed
	// secret_delivery_state 推导与 memory snapshot 防护。
	tagSecretLease = "firepaas/secret_lease"
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
	DeleteImage(ctx context.Context, name string) error
}

// CachedImages returns ready local image metadata for the image service.
func (a *Adapter) CachedImages(ctx context.Context) ([]images.Image, error) {
	ims, err := a.images.ListImages(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]images.Image, 0, len(ims))
	for _, img := range ims {
		if img.Status == images.StatusReady {
			out = append(out, img)
		}
	}
	return out, nil
}

// CachedImageDigests 返回节点本地镜像缓存的 digest 列表（v1.1，ADR-0018）。
// 每个镜像贡献两个形态：name 里的 digest 引用（与控制面 image_ref 对齐）
// 与解析后的 manifest digest（M5 遗留：OCI index 引用两者不同，都入集合）。
// 排序：CreatedAt 降序（最新创建优先；hypeman 未暴露 last-used，因此不是 LRU），截断上限 cap。

// DeleteImage deletes a cached image only when no local instance references it.
// The input may be a full OCI reference or a digest reported by CachedImageDigests.
// Listing and reference resolution are intentionally fail-closed: GC must not rely
// on the underlying image manager, which does not know about live instances.
func (a *Adapter) DeleteImage(ctx context.Context, imageRef string) error {
	if strings.TrimSpace(imageRef) == "" {
		return fmt.Errorf("image reference is empty")
	}
	// The exclusive lock makes this ListImages/ListInstances/DeleteImage sequence
	// the final, atomic reference check relative to pulls and machine creation.
	// In particular, an active pull cannot finish and become deletable underneath
	// this check, and a create cannot lose its image before its instance reference
	// is visible to ListInstances.
	a.imageUseMu.Lock()
	defer a.imageUseMu.Unlock()
	ims, err := a.images.ListImages(ctx)
	if err != nil {
		return fmt.Errorf("list images before delete: %w", err)
	}
	target := ""
	for _, img := range ims {
		if imageReferenceMatches(img, imageRef) {
			if target != "" && target != img.Name {
				return fmt.Errorf("image reference %q is ambiguous", imageRef)
			}
			target = img.Name
		}
	}
	if target == "" {
		return images.ErrNotFound
	}
	instancesList, err := a.instances.ListInstances(ctx, nil)
	if err != nil {
		return fmt.Errorf("list instances before image delete: %w", err)
	}
	for _, inst := range instancesList {
		if strings.TrimSpace(inst.Image) == "" {
			return fmt.Errorf("instance %q has an unresolvable empty image reference", inst.Name)
		}
		resolved := ""
		for _, img := range ims {
			if !imageReferenceMatches(img, inst.Image) {
				continue
			}
			if resolved != "" && resolved != img.Name {
				return fmt.Errorf("instance %q image reference %q is ambiguous", inst.Name, inst.Image)
			}
			resolved = img.Name
		}
		if resolved == "" {
			return fmt.Errorf("instance %q image reference %q cannot be resolved", inst.Name, inst.Image)
		}
		if resolved == target {
			return fmt.Errorf("image %q is referenced by live instance %q", imageRef, inst.Name)
		}
	}
	if err := a.images.DeleteImage(ctx, target); err != nil {
		if errors.Is(err, images.ErrNotFound) {
			return images.ErrNotFound
		}
		return fmt.Errorf("delete image: %w", err)
	}
	return nil
}

func imageReferenceMatches(img images.Image, ref string) bool {
	if ref == img.Name || ref == img.Digest {
		return true
	}
	if i := strings.LastIndex(img.Name, "@"); i >= 0 && ref == img.Name[i+1:] {
		return true
	}
	if i := strings.LastIndex(ref, "@"); i >= 0 {
		return ref[i+1:] == img.Digest || ref[i+1:] == imageDigestFromName(img.Name)
	}
	return false
}

func imageDigestFromName(name string) string {
	if i := strings.LastIndex(name, "@"); i >= 0 && i+1 < len(name) {
		return name[i+1:]
	}
	return ""
}

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
	// SecretInjectionOff：拒绝携带 secret_env 的 create（安全拒绝）。
	SecretInjectionOff = ""
	// SecretInjectionOneShot（v1.2-B，ADR-0024）：默认通道。secret 经 vsock
	// guest channel 写入 guest tmpfs（0700/0400），entrypoint 由 release gate
	// 阻塞到 marker 写入；值不进入 hypeman Env/metadata/config disk。
	SecretInjectionOneShot = "oneshot"
	// SecretInjectionUnsafePersistedEnv：M4 语义——合并进 hypeman Env。
	// 明知 hypeman 会把 Env 明文持久化到 metadata.json，仅限受信环境；
	// 已废弃（ADR-0024 §10），保留一个版本，下版本删除。
	SecretInjectionUnsafePersistedEnv = "unsafe-persisted-env"
)

// v1.2-B（ADR-0024）：one-shot secret 通道常量。
const (
	// secretDir：guest 内接收 secret 文件的 tmpfs 挂载点（init 在 guest
	// agent 启动前挂载，mode 0700）。
	secretDir = "/run/firepaas/secrets"
	// secretMarkerFile：release marker（最后写入，原子放行 entrypoint gate）。
	secretMarkerFile = secretDir + "/.delivered"
	// secretGateTimeoutSeconds：guest init 等待 marker 的上限；覆盖 agent
	// 侧 vsock 等待（含镜像拉取后的首次 boot）。
	secretGateTimeoutSeconds = 120
	// secretDeliverWaitForAgent：agent 侧等待 guest agent vsock 就绪的预算。
	secretDeliverWaitForAgent = 90 * time.Second
)

// secretFileMode：secret 文件权限（ADR-0024：0400）。
const secretFileMode = 0o400

// ErrSecretSnapshotForbidden（ADR-0024 §9）：接收过 secret 的 execution
// 禁止 memory snapshot/standby/checkpoint——tmpfs 属于 guest RAM，会进入
// Firecracker 内存快照；canary 扫描不能证明无副本。
var ErrSecretSnapshotForbidden = errors.New(
	"execution received one-shot secrets; memory snapshot/standby forbidden (ADR-0024)")

type slotManager interface {
	Attach(ctx context.Context, machineID, tap, guestIP string) (slot.Slot, error)
	Release(ctx context.Context, machineID string) error
	SlotFor(machineID string) (slot.Slot, bool)
}

// Adapter 包装 hypeman 的 instance/image manager。slots 非空时启用 slot
// 网络后端（ADR-0004）：create 后把 hypeman TAP 移入 slot netns，delete 后回收。
type Adapter struct {
	instances InstanceManager
	images    ImageManager
	slots     slotManager
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
	// egressMgr（v1.3-A，ADR-0027）：egress 策略执行（slot 规则 + 透明代理）。
	// nil = 未装配（桥接后端或单测）。
	egressMgr *egress.Manager
	// volumes（v1.3-D，ADR-0029）：hypeman volumes.Manager（nil = 未装配）。
	volumes volumeProvider
	// imageUseMu protects the transition between image readiness and a durable
	// instance reference. Pulls/creates are readers; deletion is an exclusive
	// final reference check plus delete. This is deliberately adapter-wide: an
	// unresolved active pull has no trustworthy digest key yet.
	imageUseMu sync.RWMutex
}

// New 构造 Adapter。slotManager 为 nil 时保持 M1 bridge 行为；
// healthTracker 为 nil 时 readiness 退化为 UNKNOWN/UNCONFIGURED。
func New(
	instances InstanceManager,
	images ImageManager,
	slotManager slotManager,
	healthTracker *health.Tracker,
) *Adapter {
	return &Adapter{
		instances: instances, images: images, slots: slotManager,
		health: healthTracker, autoResume: true,
	}
}

// SetAutoResume 控制 GetEndpoint 的 standby 同步唤醔。
func (a *Adapter) SetAutoResume(v bool) { a.autoResume = v }

// SetMaxUnpackMib 配置镜像解包大小上限（FIREPAAS_IMAGE_MAX_UNPACK_MIB）。
func (a *Adapter) SetMaxUnpackMib(mib int64) { a.maxUnpackMib = mib }

// SetSecretInjection 配置 secret_env 注入策略（见 SecretInjectionMode*）。
func (a *Adapter) SetSecretInjection(mode string) { a.secretInjection = mode }

// SetWakeObserver 注入 autoresume 唤醒观测回调（v1.1，ADR-0017 metrics）。
func (a *Adapter) SetWakeObserver(fn func(machineID string, took time.Duration)) { a.wakeObserver = fn }

// SetEgressManager（v1.3-A，ADR-0027）注入 egress 策略执行层（nil = 禁用）。
func (a *Adapter) SetEgressManager(mgr *egress.Manager) { a.egressMgr = mgr }

// RebuildEgress（v1.3-A 重启恢复）：agentd 启动后按 hypeman 实例 tags 与
// slot 持久化规则重建代理注册 + 幂等重放内核规则。策略不可重建（tags 只存
// 身份）时记日志跳过，绝不静默放行：slot 侧规则仍按持久化状态生效。
func (a *Adapter) RebuildEgress(ctx context.Context) error {
	if a.egressMgr == nil || a.slots == nil {
		return nil
	}
	listed, err := a.instances.ListInstances(ctx, nil)
	if err != nil {
		return fmt.Errorf("rebuild egress: list instances: %w", err)
	}
	for i := range listed {
		inst := &listed[i]
		machineID := inst.Name
		if machineID == "" {
			machineID = inst.Id
		}
		s, ok := a.slots.SlotFor(machineID)
		if !ok || s.Egress.Mode == "" {
			continue
		}
		policy, perr := egress.FromRuleSet(s.Egress)
		if perr != nil {
			return fmt.Errorf("rebuild egress %s: %w", machineID, perr)
		}
		if err := a.egressMgr.Apply(ctx, machineID, inst.Tags[tagExecution],
			inst.Tags[tagProject], inst.Tags[tagApp], inst.IP, policy); err != nil {
			return fmt.Errorf("rebuild egress %s: %w", machineID, err)
		}
	}
	return nil
}

// EnsureImage 确保 image_ref 就绪（v1.1，ADR-0018 部署预取的 agent 侧入口）。
// 复用 create 路径的拉取与准入逻辑（digest 解析、LRU、解包上限）；
// 幂等（已在队列/已就绪直接等待 ready）。磁盘水位检查由调用方（server
// 的 PullImage）先行执行。
func (a *Adapter) EnsureImage(ctx context.Context, imageRef string) error {
	a.imageUseMu.RLock()
	defer a.imageUseMu.RUnlock()
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
	policy.IgnoreSourceCIDRs = append(policy.IgnoreSourceCIDRs, as.GetIgnoreSourceCidrs()...)
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
	// Keep the image protected until CreateInstance has made its local reference
	// observable. DeleteImage takes the exclusive side and repeats its checks.
	a.imageUseMu.RLock()
	imageProtected := true
	defer func() {
		if imageProtected {
			a.imageUseMu.RUnlock()
		}
	}()
	// secret_env 模式分派（ADR-0024）：oneshot = vsock tmpfs 通道（默认）；
	// unsafe-persisted-env = M4 明文 Env（已废弃，保留一个版本）；其余拒绝。
	oneShot := false
	if len(req.GetSecretEnv()) != 0 {
		switch a.secretInjection {
		case SecretInjectionOneShot:
			oneShot = true
		case SecretInjectionUnsafePersistedEnv:
			// legacy 路径不支持 lease：unsafe 模式无投递状态上报，控制面
			// 无法推进 lease 状态机（ADR-0024）；组合即拒绝。
			if req.GetSecretLeaseId() != "" {
				return nil, fmt.Errorf("unsafe-persisted-env does not support secret leases; use oneshot mode")
			}
		default:
			return nil, ErrSecretEnvInjectionUnsupported
		}
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
	// legacy（unsafe-persisted-env）：secret_env 合并进 hypeman Env（明知会
	// 明文持久化到节点 metadata.json）；oneshot 模式下值不进 Env，经 vsock
	// 通道写入 guest tmpfs（见下）。
	secretKeys := make([]string, 0, len(req.SecretEnv))
	if !oneShot {
		for k, v := range req.SecretEnv {
			env[k] = v
			secretKeys = append(secretKeys, k)
		}
		sort.Strings(secretKeys)
	}
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
			tagMachine:    req.MachineId,
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
	// v1.2-B（ADR-0024 §9）：接收过 secret 的 execution 禁止 memory snapshot。
	// auto-standby = pause+snapshot+释放 VMM，在 create 时直接拒绝。
	if oneShot && standbyPolicy != nil && standbyPolicy.Enabled {
		return nil, ErrSecretSnapshotForbidden
	}
	if oneShot {
		// release gate：init 在 guest agent 启动前挂 secret tmpfs，等待
		// marker 出现才启动 entrypoint（ADR-0024）。lease id 入 tag（非敏感
		// 标识）供 observed state 推导投递状态。
		hreq.Tags[tagSecretLease] = req.GetSecretLeaseId()
		hreq.SecretDelivery = &vmconfig.SecretDelivery{
			Dir:            secretDir,
			MarkerFile:     secretMarkerFile,
			ExportAsEnv:    true,
			TimeoutSeconds: secretGateTimeoutSeconds,
		}
	}

	inst, err := a.instances.CreateInstance(ctx, hreq)
	if err != nil {
		return nil, fmt.Errorf("hypeman create: %w", err)
	}
	// The instance manager has now durably published the image reference, so
	// DeleteImage's final ListInstances check is sufficient protection.
	a.imageUseMu.RUnlock()
	imageProtected = false
	if a.slots != nil {
		// slot 后端：把 hypeman 刚创建的 TAP 移入 slot netns。失败时回收
		// 刚创建的实例并返回错误（controller 会按退避重试）。
		tap := network.GenerateTAPName(inst.Id)
		if _, err := a.slots.Attach(ctx, req.MachineId, tap, inst.IP); err != nil {
			_ = a.instances.DeleteInstance(ctx, inst.Id)
			return nil, fmt.Errorf("slot attach: %w", err)
		}
	}
	// v1.3-A（ADR-0027）：egress 策略落地（slot 规则 + 代理注册）。失败即
	// 回收实例（fail closed：带策略的 execution 不能在没有执行层的情况下
	// 运行，否则等于静默无防护）。
	if a.egressMgr != nil && spec.GetNetwork().GetEgress() != nil {
		policy, perr := egress.FromProto(spec.GetNetwork().GetEgress())
		if perr != nil {
			_ = a.instances.DeleteInstance(ctx, inst.Id)
			return nil, fmt.Errorf("egress policy: %w", perr)
		}
		if err := a.egressMgr.Apply(ctx, req.MachineId, spec.GetExecutionId(),
			spec.GetProjectId(), spec.GetAppId(), inst.IP, policy); err != nil {
			_ = a.instances.DeleteInstance(ctx, inst.Id)
			return nil, fmt.Errorf("egress apply: %w", err)
		}
	}
	if oneShot {
		// 同步投递（ADR-0024）：值只在本函数内存与 guest tmpfs 存在；
		// 失败即销毁实例（gate 未放行，无泄漏面），控制面换新 execution 重试。
		if err := a.deliverSecrets(ctx, inst.Id, req.SecretEnv); err != nil {
			_ = a.instances.DeleteInstance(ctx, inst.Id)
			return nil, fmt.Errorf("secret delivery: %w", err)
		}
		// 明文生命周期收口：投递完成立即清零，缩短明文在 agent 内存的停留。
		for k := range req.SecretEnv {
			req.SecretEnv[k] = ""
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
		// v1.3-A（ADR-0027）：egress 审计聚合随 observed 上报（控制面入 PG 摘要）。
		if a.egressMgr != nil {
			if id := m.MachineId; id != "" {
				m.EgressAudit = a.egressMgr.Stats(id)
			}
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
	// v1.3-A（ADR-0027）：egress 策略随 machine 删除清除（代理注册 + slot 规则）。
	if a.egressMgr != nil {
		if err := a.egressMgr.Remove(ctx, machineID); err != nil {
			return fmt.Errorf("egress remove: %w", err)
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
func (a *Adapter) GetEndpointForPort(
	ctx context.Context,
	machineID, executionID string,
	wantPort int,
) (ip string, port int, err error) {
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
	// 已在队列/已存在等错误继续等待 ready。
	_, _ = a.images.CreateImage(ctx, images.CreateImageRequest{Name: imageRef})
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
	// v1.2-D（ADR-0026）：execution-bound exit 报告（ON_FAILURE restart 输入）。
	if inst.ExitCode != nil {
		code := int32(*inst.ExitCode)
		m.ExitCode = &code
	}
	// v1.2-B（ADR-0024）：one-shot 投递观测状态（仅状态，无任何元数据）。
	// tagSecretLease 存在 = 本 execution 接收过 secret；entrypoint 启动
	//（ProgramStartedAt）即 guest 已消费（init 读入并 unlink）→ ACKED。
	if _, hasSecret := inst.Tags[tagSecretLease]; hasSecret {
		if inst.ProgramStartedAt != nil {
			m.SecretDeliveryState = pb.SecretDeliveryState_SECRET_DELIVERY_ACKED
		} else {
			m.SecretDeliveryState = pb.SecretDeliveryState_SECRET_DELIVERY_DELIVERED
		}
	} else {
		m.SecretDeliveryState = pb.SecretDeliveryState_SECRET_DELIVERY_NONE
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

// deliverSecrets（v1.2-B，ADR-0024）：经 vsock guest channel 把 secret 写入
// guest tmpfs 并释放 entrypoint gate。幂等（重试重写同值）；marker 最后
// 写入，其出现即原子放行。值不进入错误信息与日志。
func (a *Adapter) deliverSecrets(ctx context.Context, instanceID string, env map[string]string) error {
	vp, ok := a.instances.(vsockProvider)
	if !ok {
		return fmt.Errorf("%w: secret delivery needs vsock guest channel", ErrGuestOpsUnsupported)
	}
	dialer, err := vp.GetVsockDialer(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("vsock dialer: %w", err)
	}
	files := make(map[string][]byte, len(env))
	for k, v := range env {
		files[k] = []byte(v)
	}
	return guest.DeliverSecretFiles(ctx, dialer, guest.DeliverSecretsOptions{
		Dir:          secretDir,
		Files:        files,
		FileMode:     secretFileMode,
		MarkerFile:   secretMarkerFile,
		WaitForAgent: secretDeliverWaitForAgent,
	})
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
	// v1.2-B（ADR-0024 §9）：接收过 secret 的 execution 禁止 memory snapshot。
	// standby = pause+snapshot+释放 VMM，快照会捕获 tmpfs 内的 secret。
	if _, hasSecret := inst.Tags[tagSecretLease]; hasSecret {
		return nil, ErrSecretSnapshotForbidden
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
