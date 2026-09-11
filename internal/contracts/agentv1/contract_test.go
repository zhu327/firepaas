package agentv1

import (
	"testing"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/proto"
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

func TestValidateEastWestPolicy(t *testing.T) {
	valid := &pb.EastWestPolicySpec{
		Generation: 3,
		Rules: []*pb.EastWestPolicyRule{
			{
				SrcProject: "p1",
				SrcApp:     "web",
				DstProject: "p2",
				DstApp:     "db",
				DstService: "pg",
				Ports:      []uint32{5432, 6432},
			},
			{
				SrcProject: "p1",
				SrcApp:     "web",
				DstProject: "p2",
				DstApp:     "cache",
				DstService: "redis",
				Ports:      []uint32{6379},
			},
		},
	}
	if err := ValidateEastWestPolicy(valid); err != nil {
		t.Fatalf("valid eastwest rejected: %v", err)
	}
	if err := ValidateEastWestPolicy(nil); err != nil {
		t.Fatalf("nil eastwest must be legal (default deny): %v", err)
	}
	for name, mutate := range map[string]func(*pb.EastWestPolicySpec){
		"generation zero": func(p *pb.EastWestPolicySpec) { p.Generation = 0 },
		"empty src_app":   func(p *pb.EastWestPolicySpec) { p.Rules[0].SrcApp = "" },
		"empty dst service": func(p *pb.EastWestPolicySpec) {
			p.Rules[1].DstService = ""
		},
		"empty ports": func(p *pb.EastWestPolicySpec) { p.Rules[0].Ports = nil },
		"port zero":   func(p *pb.EastWestPolicySpec) { p.Rules[0].Ports = []uint32{0} },
		"port dup":    func(p *pb.EastWestPolicySpec) { p.Rules[0].Ports = []uint32{80, 80} },
		"rule dup dst": func(p *pb.EastWestPolicySpec) {
			p.Rules = append(p.Rules, &pb.EastWestPolicyRule{
				SrcProject: "p1", SrcApp: "web", DstProject: "p2", DstApp: "db", DstService: "pg", Ports: []uint32{5432},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := &pb.EastWestPolicySpec{Generation: valid.Generation}
			p.Rules = append(p.Rules, valid.Rules...)
			mutate(p)
			if err := ValidateEastWestPolicy(p); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestValidateEastWestPolicyTable(t *testing.T) {
	// 全表允许多 src 声明同一 dst（与单快照的 dst 唯一不同，G2a）。
	valid := &pb.EastWestPolicySpec{
		Generation: 5,
		Rules: []*pb.EastWestPolicyRule{
			{SrcProject: "p1", SrcApp: "web", DstProject: "p1", DstApp: "db", DstService: "pg", Ports: []uint32{5432}},
			{
				SrcProject: "p1",
				SrcApp:     "worker",
				DstProject: "p1",
				DstApp:     "db",
				DstService: "pg",
				Ports:      []uint32{5432},
			},
		},
	}
	if err := ValidateEastWestPolicyTable(valid); err != nil {
		t.Fatalf("multi-src table rejected: %v", err)
	}
	if err := ValidateEastWestPolicyTable(nil); err != nil {
		t.Fatalf("nil table must be legal (default deny): %v", err)
	}
	for name, mutate := range map[string]func(*pb.EastWestPolicySpec){
		"generation zero": func(p *pb.EastWestPolicySpec) { p.Generation = 0 },
		"dup src+dst": func(p *pb.EastWestPolicySpec) {
			p.Rules = append(p.Rules, &pb.EastWestPolicyRule{
				SrcProject: "p1", SrcApp: "web", DstProject: "p1", DstApp: "db", DstService: "pg", Ports: []uint32{5433},
			})
		},
		"empty ports": func(p *pb.EastWestPolicySpec) { p.Rules[0].Ports = nil },
	} {
		t.Run(name, func(t *testing.T) {
			p := &pb.EastWestPolicySpec{Generation: valid.Generation}
			p.Rules = append(p.Rules, valid.Rules...)
			mutate(p)
			if err := ValidateEastWestPolicyTable(p); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestValidateApplyFabricRequest(t *testing.T) {
	valid := &pb.ApplyFabricRequest{
		NodeId:           "node-1",
		FabricGeneration: 7,
		OperationId:      "op-fabric-7",
		NodePrefix:       "fd7a:9a55:1::/64",
		Peers: []*pb.FabricPeer{
			{
				NodeId: "node-2", Pubkey: "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=", Endpoint: "10.0.0.2:51820",
				NodePrefix: "fd7a:9a55:2::/64", FabricGeneration: 6,
			},
		},
		Identities: []*pb.IdentityMapping{
			{
				IdentityId: 1, TrustDomain: "firepaas.local", ProjectId: "p", AppId: "a",
				Service: "api", Ula: "fd7a:9a55:1::5", MachineId: "m1", ExecutionId: "e1", Generation: 3,
			},
		},
	}
	if err := ValidateApplyFabricRequest(valid); err != nil {
		t.Fatalf("valid fabric rejected: %v", err)
	}
	// G2a 增量：eastwest 全表 + mesh_direct 声明必须通过。
	withPolicy := proto.Clone(valid).(*pb.ApplyFabricRequest)
	withPolicy.Identities[0].MeshDirect = true
	withPolicy.Eastwest = &pb.EastWestPolicySpec{
		Generation: 2,
		Rules: []*pb.EastWestPolicyRule{
			{SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "a", DstService: "api", Ports: []uint32{8080}},
			{SrcProject: "p", SrcApp: "cron", DstProject: "p", DstApp: "a", DstService: "api", Ports: []uint32{8080}},
		},
	}
	if err := ValidateApplyFabricRequest(withPolicy); err != nil {
		t.Fatalf("fabric with eastwest table rejected: %v", err)
	}
	// G2c 增量：.internal 记录全表必须通过。
	withDNS := proto.Clone(withPolicy).(*pb.ApplyFabricRequest)
	withDNS.Dns = []*pb.DnsRecord{
		{Name: "a.p.internal", Aaaa: []string{"fd7a:9a55:1::5", "fd7a:9a55:2::5"}, Generation: 4},
		{Name: "web.q.internal", Aaaa: []string{"fd7a:9a55:2::6"}, Generation: 9},
	}
	if err := ValidateApplyFabricRequest(withDNS); err != nil {
		t.Fatalf("fabric with dns records rejected: %v", err)
	}
	for name, mutate := range map[string]func(*pb.ApplyFabricRequest){
		"missing operation": func(r *pb.ApplyFabricRequest) { r.OperationId = "" },
		"zero generation":   func(r *pb.ApplyFabricRequest) { r.FabricGeneration = 0 },
		"bad node prefix":   func(r *pb.ApplyFabricRequest) { r.NodePrefix = "fd7a:9a55:1::/48" },
		"non ULA prefix":    func(r *pb.ApplyFabricRequest) { r.NodePrefix = "2001:db8:1::/64" },
		"fc00 reserved":     func(r *pb.ApplyFabricRequest) { r.NodePrefix = "fc00:1::/64" },
		"host bits set":     func(r *pb.ApplyFabricRequest) { r.NodePrefix = "fd7a:9a55:1::5/64" },
		"dup peer": func(r *pb.ApplyFabricRequest) {
			r.Peers = append(r.Peers, &pb.FabricPeer{
				NodeId: "node-2", Pubkey: "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=",
				Endpoint: "10.0.0.9:51820", NodePrefix: "fd7a:9a55:9::/64", FabricGeneration: 1,
			})
		},
		"bad peer prefix": func(r *pb.ApplyFabricRequest) { r.Peers[0].NodePrefix = "fd7a:9a55:2::/63" },
		"ula not ipv6":    func(r *pb.ApplyFabricRequest) { r.Identities[0].Ula = "10.0.0.5" },
		"dup binding": func(r *pb.ApplyFabricRequest) {
			r.Identities = append(r.Identities, &pb.IdentityMapping{
				IdentityId:  2,
				TrustDomain: "firepaas.local", ProjectId: "p", AppId: "a", Service: "api",
				Ula: "fd7a:9a55:1::6", MachineId: "m1", ExecutionId: "e1", Generation: 3,
			})
		},
		"bad eastwest rule": func(r *pb.ApplyFabricRequest) {
			r.Eastwest = &pb.EastWestPolicySpec{Generation: 1, Rules: []*pb.EastWestPolicyRule{
				{SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "a", DstService: "api"},
			}}
		},
		"bad dns name": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{{Name: "App.P.internal", Aaaa: []string{"fd7a:9a55:0:1::5"}, Generation: 1}}
		},
		"bad dns shape": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{{Name: "app.internal", Aaaa: []string{"fd7a:9a55:0:1::5"}, Generation: 1}}
		},
		"dns non-ula": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{{Name: "app.p.internal", Aaaa: []string{"2001:db8::1"}, Generation: 1}}
		},
		"dns no aaaa": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{{Name: "app.p.internal", Generation: 1}}
		},
		"dns zero gen": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{{Name: "app.p.internal", Aaaa: []string{"fd7a:9a55:0:1::5"}}}
		},
		"dup dns name": func(r *pb.ApplyFabricRequest) {
			r.Dns = []*pb.DnsRecord{
				{Name: "app.p.internal", Aaaa: []string{"fd7a:9a55:0:1::5"}, Generation: 1},
				{Name: "app.p.internal", Aaaa: []string{"fd7a:9a55:0:1::6"}, Generation: 1},
			}
		},
		"eastwest dup src+dst": func(r *pb.ApplyFabricRequest) {
			r.Eastwest = &pb.EastWestPolicySpec{Generation: 1, Rules: []*pb.EastWestPolicyRule{
				{SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "a", DstService: "api", Ports: []uint32{8080}},
				{SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "a", DstService: "api", Ports: []uint32{8081}},
			}}
		},
		"eastwest zero generation": func(r *pb.ApplyFabricRequest) {
			r.Eastwest = &pb.EastWestPolicySpec{Rules: []*pb.EastWestPolicyRule{
				{SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "a", DstService: "api", Ports: []uint32{8080}},
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(valid).(*pb.ApplyFabricRequest)
			mutate(r)
			if err := ValidateApplyFabricRequest(r); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestValidateMachineSpecWithEastWestAndMeshDirect(t *testing.T) {
	spec := &pb.MachineSpec{
		ProjectId: "p", AppId: "a", DeploymentId: "d", ExecutionId: "e",
		ImageRef: "registry.local/nginx:1.27", Vcpu: 1, MemMib: 512,
		Network: &pb.NetworkSpec{
			IngressPort: 8080,
			Eastwest: &pb.EastWestPolicySpec{
				Generation: 2,
				Rules: []*pb.EastWestPolicyRule{
					{
						SrcProject: "p",
						SrcApp:     "a",
						DstProject: "p",
						DstApp:     "db",
						DstService: "pg",
						Ports:      []uint32{5432},
					},
				},
			},
		},
		Services: []*pb.ServiceSpec{
			{Name: "http", InternalPort: 8080, MeshDirect: true},
		},
	}
	if err := ValidateMachineSpecForCreate(spec); err != nil {
		t.Fatalf("valid mesh spec rejected: %v", err)
	}
	spec.Network.Eastwest.Generation = 0
	if err := ValidateMachineSpecForCreate(spec); err == nil {
		t.Fatal("eastwest generation 0 must fail")
	}
}

// TestValidateEgressPolicyDomainMode：allowed_domains 只在 ALLOWLIST 模式由
// 代理执行；unrestricted/deny_all 下域名会被静默忽略（unrestricted 时等于
// 无限制），必须在部署期拒绝。
func TestValidateEgressPolicyDomainMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    pb.EgressPolicySpec_Mode
		domains []string
		wantErr bool
	}{
		{"unrestricted with domains", pb.EgressPolicySpec_UNRESTRICTED, []string{"example.com"}, true},
		{"deny_all with domains", pb.EgressPolicySpec_DENY_ALL, []string{"example.com"}, true},
		{"unspecified with domains", pb.EgressPolicySpec_MODE_UNSPECIFIED, []string{"example.com"}, true},
		{"allowlist with domains", pb.EgressPolicySpec_ALLOWLIST, []string{"example.com"}, false},
		{"allowlist without domains", pb.EgressPolicySpec_ALLOWLIST, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &pb.EgressPolicySpec{
				Mode: tc.mode, AllowedDomains: tc.domains, PolicyGeneration: 1,
			}
			err := ValidateEgressPolicySubmission(spec)
			if tc.wantErr && err == nil {
				t.Fatalf("want error for mode=%v domains=%v", tc.mode, tc.domains)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 结构校验对存量行保持宽容（placement/controller 消费路径），
			// 否则升级后既有 unrestricted+domains 会被硬过滤成不可调度。
			if err := ValidateEgressPolicy(spec); err != nil {
				t.Fatalf("structural validator must stay tolerant for stored rows: %v", err)
			}
		})
	}
}
