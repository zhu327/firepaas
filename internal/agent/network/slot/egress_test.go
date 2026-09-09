package slot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

func TestEgressTableScriptIsCompleteAndLimitsBeforeAccepts(t *testing.T) {
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: t.TempDir() + "/slots.json"})
	if err != nil {
		t.Fatal(err)
	}
	script, err := egressTableScript(m.mustRef(Slot{Index: 0}), &api.PolicySnapshot{
		Mode: "allowlist", AllowedCIDRs: []string{"203.0.113.0/24"},
		DeniedCIDRs: []string{"198.51.100.0/24"}, ProxyPort80: 18080,
		ProxyPort443: 18443, MaxTCPConns: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"delete table ip fp-slot", "add table ip fp-slot", "add chain ip fp-slot post",
		"add set ip fp-slot egress-allow4", "dnat to 10.12.0.1:18080",
		"meta l4proto tcp ct state new ct count over 7 counter drop", "ip daddr @egress-allow4 accept",
		"add rule ip fp-slot egress-fwd drop",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
	limit := strings.Index(script, "ct count over")
	allowed := strings.Index(script, "@egress-allow4 accept")
	proxy := strings.Index(script, "ip daddr 10.12.0.1 accept")
	if limit < 0 || limit > allowed || limit > proxy {
		t.Fatalf("connection limit must precede all accepts:\n%s", script)
	}
	if strings.Contains(script, "daddr != @egress-allow4") {
		t.Fatal("allowed CIDR must not bypass Host/SNI proxy on ports 80/443")
	}
}

func TestApplySnapshotPersistsForRestart(t *testing.T) {
	oldRun := runNftBatch
	t.Cleanup(func() { runNftBatch = oldRun })
	runNftBatch = func(context.Context, string, string) error { return nil }
	state := t.TempDir() + "/slots.json"
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	m.slots["m1"] = Slot{Index: 3, MachineID: "m1", GuestIP: "10.100.0.7"}
	want := api.PolicySnapshot{Mode: "allowlist", Domains: []string{"ok.example.com"}, Generation: 2}
	if err := m.ApplySnapshot(context.Background(), "m1", want); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Load(); err != nil {
		t.Fatal(err)
	}
	got, ok := reloaded.CurrentSnapshot("m1")
	if !ok || got.Generation != want.Generation || len(got.Domains) != 1 || got.Domains[0] != want.Domains[0] {
		t.Fatalf("persisted policy = %+v, present=%v", got, ok)
	}
}

func TestApplySnapshotNftFailurePreservesPersistedSlot(t *testing.T) {
	oldRun := runNftBatch
	t.Cleanup(func() { runNftBatch = oldRun })
	dir := t.TempDir()
	state := dir + "/slots.json"
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	old := api.PolicySnapshot{Mode: "allowlist", Generation: 1}
	m.slots["m1"] = Slot{Index: 3, MachineID: "m1", GuestIP: "10.100.0.7", Egress: old}
	if err := m.persistLocked(); err != nil {
		t.Fatal(err)
	}
	runNftBatch = func(context.Context, string, string) error { return errors.New("nft stage rejected") }
	if err := m.ApplySnapshot(context.Background(), "m1", api.PolicySnapshot{Mode: "deny_all", Generation: 2}); err == nil {
		t.Fatal("expected nft failure")
	}
	if got, _ := m.CurrentSnapshot("m1"); got.Generation != 1 {
		t.Fatalf("memory policy changed: %+v", got)
	}
	raw, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	var slots []Slot
	if err := json.Unmarshal(raw, &slots); err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 || slots[0].Egress.Generation != 1 {
		t.Fatalf("persisted policy changed: %+v", slots)
	}
}

func TestLegacySlotsJSONRemainsParseable(t *testing.T) {
	// ADR-0040 §11 把 slot.EgressRuleSet 收敛为 api.PolicySnapshot：JSON 字段
	// 必须与 v1.3-A 持久化完全兼容，升级后旧 slots.json 直接可读、策略不丢。
	legacy := `{"index":4,"machine_id":"m-legacy","tap":"fp-tap1","guest_ip":"10.100.0.9",` +
		`"egress":{"mode":"allowlist","allowed_cidrs":["203.0.113.0/24"],` +
		`"domains":["ok.example.com"],"proxy_port80":18080,"proxy_port443":18443,` +
		`"max_tcp_conns":7,"audit_all":true,"generation":5}}`
	var s Slot
	if err := json.Unmarshal([]byte(legacy), &s); err != nil {
		t.Fatal(err)
	}
	got := s.Egress
	if got.Mode != "allowlist" || got.Generation != 5 || len(got.Domains) != 1 ||
		got.ProxyPort80 != 18080 || got.ProxyPort443 != 18443 ||
		got.MaxTCPConns != 7 || !got.AuditAll ||
		len(got.AllowedCIDRs) != 1 || got.AllowedCIDRs[0] != "203.0.113.0/24" {
		t.Fatalf("legacy snapshot lost fields: %+v", got)
	}
	// 往返后 JSON 保持同形态（外部消费方/回滚二进制可读）。
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var round Slot
	if err := json.Unmarshal(raw, &round); err != nil {
		t.Fatal(err)
	}
	if round.Egress.Generation != 5 || round.Egress.Mode != "allowlist" {
		t.Fatalf("round-trip mismatch: %+v", round.Egress)
	}
}

func TestEnsureSlotEgressUsesSingleBatchAndPropagatesFailure(t *testing.T) {
	old := runNftBatch
	t.Cleanup(func() { runNftBatch = old })
	calls := 0
	runNftBatch = func(_ context.Context, ns, script string) error {
		calls++
		if ns != "fp-slot-3" || !strings.Contains(script, "delete table ip fp-slot") {
			t.Fatalf("unexpected batch: ns=%s script=%s", ns, script)
		}
		return errors.New("rejected")
	}
	ref3 := SlotRef{
		Index:     3,
		Netns:     "fp-slot-3",
		VethHost:  "fp-vp3",
		VethGuest: "fp-vpg3",
		HostAddr:  "10.12.0.13",
		NsAddr:    "10.12.0.14",
	}
	if err := ensureSlotEgress(context.Background(), ref3, api.PolicySnapshot{Mode: "deny_all"}); err == nil {
		t.Fatal("batch failure must be returned")
	}
	if calls != 1 {
		t.Fatalf("nft calls = %d, want one transaction", calls)
	}
}
