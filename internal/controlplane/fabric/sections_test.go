package fabric

import (
	"net/netip"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func sectionTestSnapshot() (netip.Prefix, []store.WGPeer, []store.FabricIdentityRow, *pb.EastWestPolicySpec, []*pb.DnsRecord) {
	prefix := netip.MustParsePrefix("fd7a:9a55:1::/64")
	peers := []store.WGPeer{{
		NodeID: "node-b", Pubkey: "cGsta2V5", Endpoint: "10.0.0.2:51820",
		NodePrefix: netip.MustParsePrefix("fd7a:9a55:2::/64"), FabricGeneration: 3,
	}}
	ids := []store.FabricIdentityRow{{
		IdentityID: 7, TrustDomain: "td", ProjectID: "p", AppID: "a", Service: "web",
		ULA:       netip.MustParseAddr("fd7a:9a55:1::1"),
		MachineID: "m1", ExecutionID: "e1", Generation: 1, MeshDirect: true,
	}}
	ew := &pb.EastWestPolicySpec{Generation: 2, Rules: []*pb.EastWestPolicyRule{{
		SrcProject: "p", SrcApp: "a", DstProject: "p", DstApp: "a",
		DstService: "web", Ports: []uint32{80},
	}}}
	dns := []*pb.DnsRecord{{Name: "a.p.internal", Aaaa: []string{"fd7a:9a55:1::1"}, Generation: 5}}
	return prefix, peers, ids, ew, dns
}

// TestSectionHashesIsolateChange 断言分面哈希的隔离性：只改 DNS 时，只有
// dns（及首轮的全量）变化，peers/identities/eastwest 保持稳定。
func TestSectionHashesIsolateChange(t *testing.T) {
	prefix, peers, ids, ew, dns := sectionTestSnapshot()
	base := snapshotSectionHashes(prefix, peers, ids, ew, dns)
	for _, k := range []string{"node_prefix", "peers", "identities", "eastwest", "dns"} {
		if base[k] == "" {
			t.Fatalf("section %s has empty hash", k)
		}
	}
	// 首轮：无历史 → 全视为变化。
	if got := changedSections(nil, base); len(got) != 5 {
		t.Fatalf("first round changed = %v, want all 5 sections", got)
	}
	// 仅 DNS 变化。
	dns2 := []*pb.DnsRecord{{Name: "a.p.internal", Aaaa: []string{"fd7a:9a55:1::2"}, Generation: 6}}
	next := snapshotSectionHashes(prefix, peers, ids, ew, dns2)
	if got := changedSections(base, next); len(got) != 1 || got[0] != "dns" {
		t.Fatalf("dns-only changed = %v, want [dns]", got)
	}
	// 无变化。
	if got := changedSections(base, snapshotSectionHashes(prefix, peers, ids, ew, dns)); len(got) != 0 {
		t.Fatalf("identical changed = %v, want []", got)
	}
}

// TestReconcilerStatusRoundTrip 断言状态面的写入/读取（纯内存，不依赖 PG）。
func TestReconcilerStatusRoundTrip(t *testing.T) {
	r, err := New(Config{
		Store:      &store.Store{},
		NodeSource: &fakeNodeSource{},
		CellPrefix: "fd7a:9a55::/40",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Status("node-a"); ok {
		t.Fatal("unknown node should report ok=false")
	}
	_, peers, ids, ew, dns := sectionTestSnapshot()
	prefix := netip.MustParsePrefix("fd7a:9a55:1::/64")
	sections := snapshotSectionHashes(prefix, peers, ids, ew, dns)
	r.recordStatus(NodeFabricStatus{
		NodeID: "node-a", DesiredGeneration: 4, AppliedGeneration: 3,
		LastHash: "abc", SectionHashes: sections, ChangedSections: []string{"dns"},
		LastError: "dial timeout",
	})
	st, ok := r.Status("node-a")
	if !ok {
		t.Fatal("recorded node should report ok=true")
	}
	if st.DesiredGeneration != 4 || st.AppliedGeneration != 3 || st.LastError != "dial timeout" {
		t.Fatalf("status roundtrip = %+v", st)
	}
	if len(st.ChangedSections) != 1 || st.ChangedSections[0] != "dns" {
		t.Fatalf("changed sections = %v", st.ChangedSections)
	}
	// 返回的是拷贝：修改返回值不影响内部。
	st.SectionHashes["dns"] = "mutated"
	again, _ := r.Status("node-a")
	if again.SectionHashes["dns"] == "mutated" {
		t.Fatal("Status must return a copy")
	}
	if all := r.Statuses(); len(all) != 1 {
		t.Fatalf("Statuses len = %d, want 1", len(all))
	}
}
