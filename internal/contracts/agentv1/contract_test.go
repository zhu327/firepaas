package agentv1

import (
	"testing"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestValidateEgressPolicyRejectsIPv6(t *testing.T) {
	for _, spec := range []*pb.EgressPolicySpec{
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedCidrs: []string{"2001:db8::/32"}},
		{Mode: pb.EgressPolicySpec_UNRESTRICTED, PolicyGeneration: 1, DeniedCidrs: []string{"::1/128"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedDomains: []string{"2001:db8::1"}},
	} {
		if err := ValidateEgressPolicy(spec); err == nil {
			t.Fatalf("IPv6 policy must be rejected: %+v", spec)
		}
	}
}

func descriptor(t *testing.T, name protoreflect.FullName) protoreflect.MessageDescriptor {
	t.Helper()
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	if err != nil {
		t.Fatalf("descriptor %s not found: %v", name, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("descriptor %s is not a message", name)
	}
	return md
}

func fieldNames(md protoreflect.MessageDescriptor) map[string]protoreflect.FieldDescriptor {
	out := map[string]protoreflect.FieldDescriptor{}
	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		out[string(fd.Name())] = fd
	}
	return out
}

// TestFencingEnvelopeFrozen 校验所有状态变更请求都带完整的 fencing 信封
// （ADR-0013 不变量 1）。
func TestFencingEnvelopeFrozen(t *testing.T) {
	required := []string{"machine_id", "execution_id", "generation", "operation_id"}
	for _, msgName := range []string{
		"firepaas.agent.v1.DeleteMachineRequest",
		"firepaas.agent.v1.MachineOperationRequest",
	} {
		fields := fieldNames(descriptor(t, protoreflect.FullName(msgName)))
		for _, name := range required {
			if _, ok := fields[name]; !ok {
				t.Errorf("%s must keep field %q (fencing envelope frozen)", msgName, name)
			}
		}
	}

	// CreateMachineRequest 的 execution_id 在 MachineSpec 内（machine 自身的
	// execution 代），顶层保留 machine_id/generation/operation_id。
	createFields := fieldNames(descriptor(t, "firepaas.agent.v1.CreateMachineRequest"))
	for _, name := range []string{"machine_id", "generation", "operation_id"} {
		if _, ok := createFields[name]; !ok {
			t.Errorf("CreateMachineRequest must keep field %q", name)
		}
	}
	specField, ok := createFields["spec"]
	if !ok || specField.Message() == nil || string(specField.Message().FullName()) != "firepaas.agent.v1.MachineSpec" {
		t.Fatal("CreateMachineRequest.spec must be MachineSpec")
	}
	if _, ok := fieldNames(specField.Message())["execution_id"]; !ok {
		t.Error("MachineSpec must keep execution_id (create 的 fencing 键之一)")
	}

	for _, msgName := range []string{
		"firepaas.agent.v1.StartMachineRequest",
		"firepaas.agent.v1.StopMachineRequest",
		"firepaas.agent.v1.PauseMachineRequest",
		"firepaas.agent.v1.ResumeMachineRequest",
		"firepaas.agent.v1.CheckpointMachineRequest",
	} {
		fields := fieldNames(descriptor(t, protoreflect.FullName(msgName)))
		fd, ok := fields["operation"]
		if !ok {
			t.Errorf("%s must keep field \"operation\"", msgName)
			continue
		}
		if fd.Message() == nil || string(fd.Message().FullName()) != "firepaas.agent.v1.MachineOperationRequest" {
			t.Errorf("%s.operation must be MachineOperationRequest", msgName)
		}
	}
}

// TestNoSensitiveFieldsOnEchoMessages 校验响应/回显结构不出现 secret 值、
// 代理凭证、traffic token（ADR-0013 不变量 2/3）。
func TestNoSensitiveFieldsOnEchoMessages(t *testing.T) {
	blacklist := map[string]bool{
		"secret_env":       true,
		"secret_value":     true,
		"secret_values":    true,
		"proxy_credential": true,
		"traffic_token":    true,
		"access_token":     true,
	}
	for _, msgName := range []string{
		"firepaas.agent.v1.Machine",
		"firepaas.agent.v1.MachineSpec",
		"firepaas.agent.v1.CreateMachineResponse",
		"firepaas.agent.v1.ListMachinesResponse",
	} {
		for name := range fieldNames(descriptor(t, protoreflect.FullName(msgName))) {
			if blacklist[name] {
				t.Errorf("%s must not contain sensitive field %q", msgName, name)
			}
		}
	}

	// Create 请求允许单向下发，但不能出现在回显结构。
	createFields := fieldNames(descriptor(t, "firepaas.agent.v1.CreateMachineRequest"))
	for _, name := range []string{"secret_env", "proxy_credential"} {
		if _, ok := createFields[name]; !ok {
			t.Errorf("CreateMachineRequest must keep one-way field %q", name)
		}
	}
	for _, message := range []string{"firepaas.agent.v1.ForkSnapshotRequest", "firepaas.agent.v1.RestoreSnapshotRequest"} {
		if _, ok := fieldNames(descriptor(t, protoreflect.FullName(message)))["proxy_credential"]; !ok {
			t.Errorf("%s must keep one-way field proxy_credential", message)
		}
	}
}

// TestMachineReadinessFrozen 校验 readiness 四值语义（ADR-0008/ADR-0013）。
func TestMachineReadinessFrozen(t *testing.T) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName("firepaas.agent.v1.MachineReadiness")
	if err != nil {
		t.Fatalf("MachineReadiness descriptor not found: %v", err)
	}
	ed, ok := d.(protoreflect.EnumDescriptor)
	if !ok {
		t.Fatalf("MachineReadiness is not an enum")
	}
	values := map[string]bool{}
	for i := 0; i < ed.Values().Len(); i++ {
		values[string(ed.Values().Get(i).Name())] = true
	}
	for _, name := range []string{"READINESS_UNSPECIFIED", "UNKNOWN", "NOT_READY", "READY", "UNCONFIGURED"} {
		if !values[name] {
			t.Errorf("MachineReadiness must keep enum value %s", name)
		}
	}
}

// TestMachineObservedFieldsFrozen 校验 Machine 的 observed state 关键字段
// （slot_ip 仅供 control-plane observed state 消费，edge 不得读取）。
func TestMachineObservedFieldsFrozen(t *testing.T) {
	fields := fieldNames(descriptor(t, "firepaas.agent.v1.Machine"))
	for _, name := range []string{"slot_ip", "readiness", "execution_id", "state"} {
		if _, ok := fields[name]; !ok {
			t.Errorf("Machine must keep observed field %q", name)
		}
	}
}

// TestStableServiceMethodsFrozen 校验 M1 稳定 RPC 子集存在。
func TestStableServiceMethodsFrozen(t *testing.T) {
	want := map[string][]string{
		"firepaas.agent.v1.InfoService":    {"ServiceInfo"},
		"firepaas.agent.v1.MachineService": {"CreateMachine", "ListMachines", "DeleteMachine"},
	}
	for svcName, methods := range want {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svcName))
		if err != nil {
			t.Fatalf("service %s not found: %v", svcName, err)
		}
		sd := d.(protoreflect.ServiceDescriptor)
		got := map[string]bool{}
		for i := 0; i < sd.Methods().Len(); i++ {
			got[string(sd.Methods().Get(i).Name())] = true
		}
		for _, m := range methods {
			if !got[m] {
				t.Errorf("service %s must keep method %s", svcName, m)
			}
		}
	}
}

func TestValidateFencing(t *testing.T) {
	if err := ValidateFencing("m", "e", 1, "op"); err != nil {
		t.Fatalf("valid fencing rejected: %v", err)
	}
	for _, tc := range []struct {
		name                         string
		machineID, executionID, opID string
		generation                   uint64
	}{
		{"missing machine", "", "e", "op", 1},
		{"missing execution", "m", "", "op", 1},
		{"zero generation", "m", "e", "op", 0},
		{"missing operation", "m", "e", "", 1},
	} {
		if err := ValidateFencing(tc.machineID, tc.executionID, tc.generation, tc.opID); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

func TestValidateMachineSpecForCreate(t *testing.T) {
	valid := &pb.MachineSpec{
		ProjectId:    "p",
		AppId:        "a",
		DeploymentId: "d",
		ExecutionId:  "e",
		ImageRef:     "registry.local/nginx:1.27",
		Vcpu:         1,
		MemMib:       512,
	}
	if err := ValidateMachineSpecForCreate(valid); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	if err := ValidateMachineSpecForCreate(&pb.MachineSpec{ProjectId: "p"}); err == nil {
		t.Fatal("expected error for incomplete spec")
	}
}

func TestLocalIntegrityContractsFailClosed(t *testing.T) {
	if err := ValidateScrubSnapshotRequest(&pb.ScrubSnapshotRequest{SnapshotId: "s", ExpectedRevision: "sha256:x"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateScrubSnapshotRequest(&pb.ScrubSnapshotRequest{SnapshotId: "s"}); err == nil {
		t.Fatal("scrub without revision must fail")
	}
	if err := ValidateQuarantineImageRequest(&pb.QuarantineImageRequest{ImageRef: "repo@sha256:x", ClaimId: "c", OperationId: "op", ExpectedRevision: "sha256:x"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateQuarantineVolumeRequest(&pb.QuarantineVolumeRequest{VolumeId: "v", ClaimId: "c", OperationId: "op", ExpectedRevision: "r", Mode: "LOCAL_RW", Rebuildable: true}); err == nil {
		t.Fatal("LOCAL_RW quarantine must fail")
	}
	if err := ValidateQuarantineVolumeRequest(&pb.QuarantineVolumeRequest{VolumeId: "v", ClaimId: "c", OperationId: "op", ExpectedRevision: "r", Mode: "DATASET_RO", Rebuildable: false}); err == nil {
		t.Fatal("non-rebuildable dataset quarantine must fail")
	}
}

func TestValidateCreateRequest(t *testing.T) {
	req := &pb.CreateMachineRequest{
		MachineId:   "m",
		Generation:  1,
		OperationId: "op",
		Spec: &pb.MachineSpec{
			ProjectId:    "p",
			AppId:        "a",
			DeploymentId: "d",
			ExecutionId:  "e",
			ImageRef:     "registry.local/nginx:1.27",
			Vcpu:         1,
			MemMib:       512,
		},
	}
	if err := ValidateCreateRequest(req); err != nil {
		t.Fatalf("valid create rejected: %v", err)
	}
	req.OperationId = ""
	if err := ValidateCreateRequest(req); err == nil {
		t.Fatal("expected error for missing operation_id")
	}
}
