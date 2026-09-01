// Package agentv1 是 M1.2 冻结的 agent 契约校验（ADR-0013）。
// 实现方（agent/control-plane）必须复用这里的校验，不得各自手写语义。
package agentv1

import (
	"errors"
	"fmt"
	"net"
	"path"
	"strings"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// ValidateFencing 校验所有状态变更请求必须携带的 fencing/幂等键。
// 同一 operation_id 重试必须返回已记录结果；同 ID 不同 request hash 由
// agent operation ledger 拒绝。
func ValidateFencing(machineID, executionID string, generation uint64, operationID string) error {
	switch {
	case machineID == "":
		return errors.New("machine_id is required")
	case executionID == "":
		return errors.New("execution_id is required")
	case generation == 0:
		return errors.New("generation must be > 0")
	case operationID == "":
		return errors.New("operation_id is required")
	}
	return nil
}

// ValidateMachineSpecForCreate 校验 CreateMachine 的 spec 最小集合。
// 只覆盖 M1 冻结子集需要的内容，不校验实验能力。
func ValidateMachineSpecForCreate(spec *pb.MachineSpec) error {
	if spec == nil {
		return errors.New("spec is required")
	}
	switch {
	case spec.ProjectId == "":
		return errors.New("spec.project_id is required")
	case spec.AppId == "":
		return errors.New("spec.app_id is required")
	case spec.DeploymentId == "":
		return errors.New("spec.deployment_id is required")
	case spec.ExecutionId == "":
		return errors.New("spec.execution_id is required")
	case spec.ImageRef == "":
		return errors.New("spec.image_ref is required")
	case spec.Vcpu == 0:
		return errors.New("spec.vcpu must be > 0")
	case spec.MemMib == 0:
		return errors.New("spec.mem_mib must be > 0")
	}
	// v1.1（ADR-0017）：auto_standby 策略校验（enabled 时 idle_timeout 必须 > 0）。
	if as := spec.GetAutoStandby(); as != nil && as.GetEnabled() && as.GetIdleTimeoutSeconds() == 0 {
		return errors.New("spec.auto_standby.idle_timeout_seconds must be > 0 when enabled")
	}
	// v1.1（ADR-0022）：services 校验（端口合法、deployment 内端口唯一；
	// 主 service 端口与 network.ingress_port 对齐）。
	if svcs := spec.GetServices(); len(svcs) > 0 {
		if len(svcs) > 8 {
			return errors.New("spec.services supports at most 8 entries in v1.1")
		}
		if spec.GetNetwork() == nil {
			return errors.New("spec.network is required when services are declared")
		}
		if svcs[0].GetInternalPort() != uint32(spec.GetNetwork().GetIngressPort()) {
			return errors.New("spec.services[0].internal_port must equal spec.network.ingress_port")
		}
		seenPort := map[uint32]bool{}
		seenName := map[string]bool{}
		for i, s := range svcs {
			if s.GetName() == "" {
				return fmt.Errorf("spec.services[%d].name is required", i)
			}
			if seenName[s.GetName()] {
				return fmt.Errorf("spec.services[%d].name %q duplicated", i, s.GetName())
			}
			if s.GetInternalPort() == 0 || s.GetInternalPort() > 65535 {
				return errors.New("spec.services[].internal_port must be in [1,65535]")
			}
			if seenPort[s.GetInternalPort()] {
				return fmt.Errorf("spec.services[%d].internal_port %d duplicated", i, s.GetInternalPort())
			}
			seenPort[s.GetInternalPort()] = true
			seenName[s.GetName()] = true
		}
	}
	// v1.3-A（ADR-0027）：egress policy 契约校验（agent 侧 fail closed）。
	if spec.GetNetwork() != nil {
		if err := ValidateEgressPolicy(spec.GetNetwork().GetEgress()); err != nil {
			return err
		}
	}
	return nil
}

// ValidateCreateRequest 校验 CreateMachineRequest 的 fencing 与 spec。
func ValidateCreateRequest(req *pb.CreateMachineRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := ValidateFencing(req.MachineId, req.Spec.GetExecutionId(), req.Generation, req.OperationId); err != nil {
		return fmt.Errorf("create fencing: %w", err)
	}
	// v1.2-B（ADR-0024）：one-shot 下发必须绑定 lease（状态机可审计、
	// 可幂等重放；无 lease 的 secret 会被 agent fail-closed 拒绝）。
	if len(req.GetSecretEnv()) != 0 && req.GetSecretLeaseId() == "" {
		return errors.New("secret_lease_id is required when secret_env is present")
	}
	return ValidateMachineSpecForCreate(req.Spec)
}

// DefaultDiskMib（v1.2-E，ADR-0035）：spec.disk_mib=0 时的有效磁盘承诺，
// 与 hypeman 默认 overlay 大小（10GiB）对齐。调度、预约与 agent 准入
// 三处必须用同一有效值，避免 accounting 偏差。
const DefaultDiskMib = 10 * 1024

// EffectiveDiskMib 返回 machine 的有效磁盘承诺（MiB）。
func EffectiveDiskMib(diskMib uint64) uint64 {
	if diskMib == 0 {
		return DefaultDiskMib
	}
	return diskMib
}

// ValidateDeleteRequest 校验 DeleteMachineRequest。
func ValidateDeleteRequest(req *pb.DeleteMachineRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	return ValidateFencing(req.MachineId, req.ExecutionId, req.Generation, req.OperationId)
}

func ValidateScrubSnapshotRequest(req *pb.ScrubSnapshotRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot_id is required")
	}
	if req.GetExpectedRevision() == "" {
		return errors.New("expected_revision is required")
	}
	return nil
}

func ValidateQuarantineImageRequest(req *pb.QuarantineImageRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if req.GetImageRef() == "" || req.GetClaimId() == "" || req.GetOperationId() == "" ||
		req.GetExpectedRevision() == "" {
		return errors.New("image_ref, claim_id, operation_id and expected_revision are required")
	}
	return nil
}

func ValidateImageQuarantineActionRequest(req *pb.ImageQuarantineActionRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if req.GetClaimId() == "" || req.GetToken() == "" || req.GetOperationId() == "" {
		return errors.New("claim_id, token and operation_id are required")
	}
	return nil
}

func ValidateQuarantineVolumeRequest(req *pb.QuarantineVolumeRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if req.GetVolumeId() == "" || req.GetClaimId() == "" || req.GetOperationId() == "" ||
		req.GetExpectedRevision() == "" {
		return errors.New("volume_id, claim_id, operation_id and expected_revision are required")
	}
	if req.GetMode() != "DATASET_RO" || !req.GetRebuildable() {
		return errors.New("only rebuildable DATASET_RO may be quarantined")
	}
	return nil
}

func ValidateVolumeQuarantineActionRequest(req *pb.VolumeQuarantineActionRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if req.GetClaimId() == "" || req.GetToken() == "" || req.GetOperationId() == "" {
		return errors.New("claim_id, token and operation_id are required")
	}
	return nil
}

// ValidateCreateSnapshotRequest 校验快照创建（ADR-0028：machine/execution/
// generation/operation 全量 fencing + snapshot_id + 合法 kind/压缩）。
func ValidateCreateSnapshotRequest(req *pb.CreateSnapshotRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := ValidateFencing(req.GetMachineId(), req.GetExecutionId(), req.GetGeneration(), req.GetOperationId()); err != nil {
		return fmt.Errorf("snapshot fencing: %w", err)
	}
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot_id is required")
	}
	switch req.GetKind() {
	case pb.SnapshotKind_SNAPSHOT_MEMORY, pb.SnapshotKind_SNAPSHOT_FILESYSTEM:
	default:
		return errors.New("snapshot kind must be memory or filesystem")
	}
	return ValidateSnapshotCompression(req.GetCompression())
}

// ValidateDeleteSnapshotRequest 校验快照删除。
func ValidateDeleteSnapshotRequest(req *pb.DeleteSnapshotRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := ValidateFencing(req.GetMachineId(), req.GetExecutionId(), req.GetGeneration(), req.GetOperationId()); err != nil {
		return fmt.Errorf("snapshot delete fencing: %w", err)
	}
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot_id is required")
	}
	return nil
}

// ValidateSnapshotCompression 校验压缩声明（nil 合法 = none）。
func ValidateSnapshotCompression(c *pb.SnapshotCompressionSpec) error {
	if c == nil {
		return nil
	}
	switch c.GetAlgorithm() {
	case pb.SnapshotCompressionSpec_ALGORITHM_UNSPECIFIED, pb.SnapshotCompressionSpec_NONE:
		return nil
	case pb.SnapshotCompressionSpec_ZSTD:
		if l := c.GetLevel(); l < 0 || l > 19 {
			return fmt.Errorf("zstd level must be in [-1,19], got %d", l)
		}
	case pb.SnapshotCompressionSpec_LZ4:
		if l := c.GetLevel(); l < 0 || l > 9 {
			return fmt.Errorf("lz4 level must be in [-1,9], got %d", l)
		}
	default:
		return fmt.Errorf("unsupported compression algorithm %v", c.GetAlgorithm())
	}
	return nil
}

// ValidateForkSnapshotRequest 校验受限 fork（ADR-0028 §fork）。
func ValidateForkSnapshotRequest(req *pb.ForkSnapshotRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := ValidateFencing(req.GetMachineId(), req.GetExecutionId(), req.GetGeneration(), req.GetOperationId()); err != nil {
		return fmt.Errorf("fork fencing: %w", err)
	}
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot_id is required")
	}
	return nil
}

// ValidateRestoreSnapshotRequest 校验 rescue（restore_mode 枚举）。
func ValidateRestoreSnapshotRequest(req *pb.RestoreSnapshotRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := ValidateFencing(req.GetMachineId(), req.GetExecutionId(), req.GetGeneration(), req.GetOperationId()); err != nil {
		return fmt.Errorf("restore fencing: %w", err)
	}
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot_id is required")
	}
	switch req.GetRestoreMode() {
	case "", "memory", "filesystem", "auto":
		return nil
	default:
		return fmt.Errorf("restore_mode must be memory, filesystem or auto, got %q", req.GetRestoreMode())
	}
}

// ValidateOperationRequest 校验 MachineOperationRequest 信封。
func ValidateOperationRequest(op *pb.MachineOperationRequest) error {
	if op == nil {
		return errors.New("operation is required")
	}
	return ValidateFencing(op.MachineId, op.ExecutionId, op.Generation, op.OperationId)
}

// ValidateRuntimeSessionFence 校验 v1.2-C（ADR-0025）logs/exec/cp 的会话绑定：
// machine + execution 必填；旧 execution 由 agent 侧 tags 比对拒绝。
func ValidateRuntimeSessionFence(machineID, executionID string) error {
	switch {
	case machineID == "":
		return errors.New("machine_id is required")
	case executionID == "":
		return errors.New("execution_id is required")
	}
	return nil
}

// ValidateGuestPath 校验 cp 的 guest 路径（与 agent machine 包同规则，
// 保持两端语义一致）。
func ValidateGuestPath(p string) error {
	if p == "" {
		return errors.New("path is required")
	}
	if !strings.HasPrefix(p, "/") {
		return errors.New("path must be absolute")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return errors.New("path traversal not allowed")
		}
	}
	if path.Clean(p) == "/" {
		return errors.New("path must be a file, not root")
	}
	return nil
}

// ---------------------------------------------------------------------------
// v1.3-A（ADR-0027）：egress policy 契约校验（控制面 API 与 agent 共用）。
// ---------------------------------------------------------------------------

// NormalizeEgressDomain 归一化域名条目：小写、去尾点；exact 或
// *.example.com（单层 wildcard）形态合法。IP 字面量视为 exact 域名（只用于
// 匹配字面量，解析仍走 trusted resolver 流程对域名型生效）。
func NormalizeEgressDomain(raw string) (string, error) {
	d := strings.TrimSpace(strings.ToLower(raw))
	d = strings.TrimSuffix(d, ".")
	if d == "" {
		return "", errors.New("egress domain must be non-empty")
	}
	if strings.ContainsAny(d, "/?#:@") {
		return "", fmt.Errorf("egress domain %q must not contain scheme/path/port", raw)
	}
	if strings.HasPrefix(d, "*.") {
		suffix := strings.TrimPrefix(d, "*.")
		if suffix == "" || strings.Contains(suffix, "*") {
			return "", fmt.Errorf("egress wildcard domain %q must be *.example.com (single label)", raw)
		}
		if !validEgressDomainLabelSet(suffix) {
			return "", fmt.Errorf("egress wildcard domain %q has invalid suffix", raw)
		}
		return "*." + suffix, nil
	}
	if strings.Contains(d, "*") {
		return "", fmt.Errorf("egress domain %q has unsupported wildcard form", raw)
	}
	if ip := net.ParseIP(d); ip != nil {
		if ip.To4() == nil {
			return "", fmt.Errorf("egress domain %q: IPv6 egress is not supported", raw)
		}
		return d, nil
	}
	if !validEgressDomainLabelSet(d) {
		return "", fmt.Errorf("egress domain %q is not a valid hostname", raw)
	}
	return d, nil
}

func validEgressDomainLabelSet(d string) bool {
	if d == "" || len(d) > 253 {
		return false
	}
	labels := strings.Split(d, ".")
	for _, l := range labels {
		if l == "" || len(l) > 63 {
			return false
		}
		for i, c := range l {
			ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
			if !ok || (i == 0 || i == len(l)-1) && c == '-' {
				return false
			}
		}
	}
	return true
}

// ValidateEgressPolicy 校验 EgressPolicySpec（nil = 未声明，合法）。
// 控制面与 agent 复用：模式合法、CIDR 可解析、域名条目归一化后非空、
// policy_generation 必须 > 0。
func ValidateEgressPolicy(p *pb.EgressPolicySpec) error {
	if p == nil {
		return nil
	}
	switch p.GetMode() {
	case pb.EgressPolicySpec_MODE_UNSPECIFIED, pb.EgressPolicySpec_UNRESTRICTED,
		pb.EgressPolicySpec_DENY_ALL, pb.EgressPolicySpec_ALLOWLIST:
	default:
		return fmt.Errorf("egress mode %d is invalid", p.GetMode())
	}
	for i, cidr := range p.GetAllowedCidrs() {
		ip, _, err := net.ParseCIDR(strings.TrimSpace(cidr))
		if err != nil {
			return fmt.Errorf("egress allowed_cidrs[%d] %q: %v", i, cidr, err)
		}
		if ip.To4() == nil {
			return fmt.Errorf("egress allowed_cidrs[%d] %q: IPv6 egress is not supported", i, cidr)
		}
	}
	for i, cidr := range p.GetDeniedCidrs() {
		ip, _, err := net.ParseCIDR(strings.TrimSpace(cidr))
		if err != nil {
			return fmt.Errorf("egress denied_cidrs[%d] %q: %v", i, cidr, err)
		}
		if ip.To4() == nil {
			return fmt.Errorf("egress denied_cidrs[%d] %q: IPv6 egress is not supported", i, cidr)
		}
	}
	seen := map[string]bool{}
	for i, d := range p.GetAllowedDomains() {
		normalized, err := NormalizeEgressDomain(d)
		if err != nil {
			return fmt.Errorf("egress allowed_domains[%d]: %v", i, err)
		}
		if seen[normalized] {
			return fmt.Errorf("egress allowed_domains[%d] %q duplicated", i, normalized)
		}
		seen[normalized] = true
	}
	if p.GetPolicyGeneration() == 0 {
		return errors.New("egress policy_generation must be > 0")
	}
	return nil
}
