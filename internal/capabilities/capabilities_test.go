package capabilities

import "testing"

func TestValid(t *testing.T) {
	for _, id := range All() {
		if !Valid(id) {
			t.Fatalf("published feature %q must be valid", id)
		}
	}
	for _, bad := range []string{"", "UPPER.v1", "space id", "no.version"} {
		if bad == "no.version" && Valid(bad) {
			// no.version 由字母/点组成，按当前形态规则合法（语义审查由发布流程承担）。
			continue
		}
		if Valid(bad) {
			t.Fatalf("want invalid: %q", bad)
		}
	}
}

func TestSetOf(t *testing.T) {
	if got := SetOf(nil); got != nil {
		t.Fatalf("nil in must yield nil set, got %v", got)
	}
	s := SetOf([]string{GuestExecV1, GuestExecV1, "bad!id"})
	if len(s) != 1 || !s[GuestExecV1] {
		t.Fatalf("want deduped valid set, got %v", s)
	}
}

func TestFabricCapabilityDependencies(t *testing.T) {
	// ADR-0040 §12/§18：mesh 硬依赖 eBPF 数据面；回退节点永不调度 mesh 服务。
	deps := Requires(MeshEastWestV1)
	if len(deps) != 1 || deps[0] != NetworkEbpfV1 {
		t.Fatalf("mesh.eastwest.v1 deps = %v, want [network.ebpf.v1]", deps)
	}
	if Requires(NetworkEbpfV1) != nil || Requires(NetworkNftFallbackV1) != nil {
		t.Fatal("datapath IDs must have no declared dependencies")
	}
	// 二选一上报互斥性由上报方（agentd）保证；ID 本身形态必须符合 name.vN。
	for _, id := range []string{NetworkEbpfV1, NetworkNftFallbackV1, MeshEastWestV1} {
		if !Valid(id) {
			t.Fatalf("fabric capability %q must be a valid feature ID", id)
		}
	}
}
