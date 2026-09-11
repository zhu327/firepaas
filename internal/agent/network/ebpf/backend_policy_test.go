package ebpf

import (
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// TestMergePolicyEntriesBidirectional：同一 (src,dst) 同时存在正向（dport）
// 与回程（sport）条目时必须按位合并 flags，端口按 dir 分开——后写覆盖前写
// 会丢约束（回程变任意端口）或误拒（正向端口白名单丢失）。
func TestMergePolicyEntriesBidirectional(t *testing.T) {
	entries := []api.FabricPolicyEntry{
		// A→B 正向：dport 8080。
		{SrcIdentity: 1, DstIdentity: 2, Generation: 3, Ports: []uint32{8080}},
		// B→A 规则的对称回程落在 (A,B)：sport 9090。
		{SrcIdentity: 1, DstIdentity: 2, Generation: 4, Ports: []uint32{9090}, SrcPorts: true},
		// 另一对，仅回程。
		{SrcIdentity: 2, DstIdentity: 1, Generation: 3, Ports: []uint32{7070}, SrcPorts: true},
	}
	policies, ports := mergePolicyEntries(entries)
	if len(policies) != 2 {
		t.Fatalf("policy keys = %d, want 2", len(policies))
	}
	v := policies[policyKey{Src: 1, Dst: 2}]
	if v.Flags != policyFlagDport|policyFlagSport {
		t.Fatalf("flags = %b, want dport|sport", v.Flags)
	}
	if v.Generation != 4 {
		t.Fatalf("generation = %d, want max 4", v.Generation)
	}
	if policies[policyKey{Src: 2, Dst: 1}].Flags != policyFlagSport {
		t.Fatalf("reverse-only entry flags = %b", policies[policyKey{Src: 2, Dst: 1}].Flags)
	}
	type want struct {
		src, dst uint32
		port     uint16
		dir      uint16
	}
	wants := map[want]bool{
		{1, 2, 8080, 0}: true,
		{1, 2, 9090, 1}: true,
		{2, 1, 7070, 1}: true,
	}
	if len(ports) != len(wants) {
		t.Fatalf("ports = %+v", ports)
	}
	for _, p := range ports {
		if !wants[want{p.Src, p.Dst, p.Port, p.Dir}] {
			t.Fatalf("unexpected port key %+v", p)
		}
	}
}
