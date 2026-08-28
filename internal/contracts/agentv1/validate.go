// Package agentv1 是 M1.2 冻结的 agent 契约校验（ADR-0013）。
// 实现方（agent/control-plane）必须复用这里的校验，不得各自手写语义。
package agentv1

import (
	"errors"
	"fmt"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
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
	return ValidateMachineSpecForCreate(req.Spec)
}

// ValidateDeleteRequest 校验 DeleteMachineRequest。
func ValidateDeleteRequest(req *pb.DeleteMachineRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	return ValidateFencing(req.MachineId, req.ExecutionId, req.Generation, req.OperationId)
}

// ValidateOperationRequest 校验 MachineOperationRequest 信封。
func ValidateOperationRequest(op *pb.MachineOperationRequest) error {
	if op == nil {
		return errors.New("operation is required")
	}
	return ValidateFencing(op.MachineId, op.ExecutionId, op.Generation, op.OperationId)
}
