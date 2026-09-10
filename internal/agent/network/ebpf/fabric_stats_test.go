package ebpf

import (
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// TestSummarizeFabricPolicy 断言状态面统计折叠（纯函数，无需内核/BTF）。
func TestSummarizeFabricPolicy(t *testing.T) {
	now := time.Now().UTC()
	snap := api.FabricPolicySnapshot{
		Generation: 9,
		Identities: []api.IdentityMapEntry{
			{IdentityID: 1, ULA: "fd7a:9a55:1::1"},
			{IdentityID: 2, ULA: "fd7a:9a55:1::2"},
		},
		Entries: []api.FabricPolicyEntry{
			{SrcIdentity: 1, DstIdentity: 2, Generation: 9, Ports: []uint32{80, 443}},
			{SrcIdentity: 2, DstIdentity: 1, Generation: 9}, // 对称回程：无端口
		},
		NodeULA: "fd7a:9a55:1::",
	}
	got := summarizeFabricPolicy(snap, now)
	if got.Generation != 9 {
		t.Errorf("generation = %d, want 9", got.Generation)
	}
	if got.IdentityEntries != 2 {
		t.Errorf("identities = %d, want 2", got.IdentityEntries)
	}
	if got.PolicyEntries != 2 {
		t.Errorf("entries = %d, want 2", got.PolicyEntries)
	}
	if got.PortEntries != 2 {
		t.Errorf("ports = %d, want 2", got.PortEntries)
	}
	if !got.UpdatedAt.Equal(now) {
		t.Errorf("updated_at = %v, want %v", got.UpdatedAt, now)
	}
	// 空快照（默认 deny）折叠为零条目而非零值混淆：generation 保留。
	empty := summarizeFabricPolicy(api.FabricPolicySnapshot{Generation: 10}, now)
	if empty.PolicyEntries != 0 || empty.PortEntries != 0 || empty.Generation != 10 {
		t.Errorf("empty snapshot stats = %+v", empty)
	}
}
