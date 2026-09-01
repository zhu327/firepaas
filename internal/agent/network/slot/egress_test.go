package slot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestEgressTableScriptIsCompleteAndLimitsBeforeAccepts(t *testing.T) {
	script, err := egressTableScript(0, &EgressRuleSet{
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

func TestApplyEgressPolicyPersistsForRestart(t *testing.T) {
	oldRun := runNftBatch
	t.Cleanup(func() { runNftBatch = oldRun })
	runNftBatch = func(context.Context, string, string) error { return nil }
	state := t.TempDir() + "/slots.json"
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	m.slots["m1"] = Slot{Index: 3, MachineID: "m1", GuestIP: "10.100.0.7"}
	want := EgressRuleSet{Mode: "allowlist", Domains: []string{"ok.example.com"}, Generation: 2}
	if err := m.ApplyEgressPolicy(context.Background(), "m1", want); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Load(); err != nil {
		t.Fatal(err)
	}
	got, ok := reloaded.CurrentEgressPolicy("m1")
	if !ok || got.Generation != want.Generation || len(got.Domains) != 1 || got.Domains[0] != want.Domains[0] {
		t.Fatalf("persisted policy = %+v, present=%v", got, ok)
	}
}

func TestApplyEgressPolicyNftFailurePreservesPersistedSlot(t *testing.T) {
	oldRun := runNftBatch
	t.Cleanup(func() { runNftBatch = oldRun })
	dir := t.TempDir()
	state := dir + "/slots.json"
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	old := EgressRuleSet{Mode: "allowlist", Generation: 1}
	m.slots["m1"] = Slot{Index: 3, MachineID: "m1", GuestIP: "10.100.0.7", Egress: old}
	if err := m.persistLocked(); err != nil {
		t.Fatal(err)
	}
	runNftBatch = func(context.Context, string, string) error { return errors.New("nft stage rejected") }
	if err := m.ApplyEgressPolicy(context.Background(), "m1", EgressRuleSet{Mode: "deny_all", Generation: 2}); err == nil {
		t.Fatal("expected nft failure")
	}
	if got, _ := m.CurrentEgressPolicy("m1"); got.Generation != 1 {
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
	if err := ensureSlotEgress(context.Background(), 3, EgressRuleSet{Mode: "deny_all"}); err == nil {
		t.Fatal("batch failure must be returned")
	}
	if calls != 1 {
		t.Fatalf("nft calls = %d, want one transaction", calls)
	}
}
