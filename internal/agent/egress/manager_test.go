package egress

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type fakeSlots struct {
	current             slot.EgressRuleSet
	present             bool
	applyErr, removeErr error
	onApply             func()
	applies             []slot.EgressRuleSet
	removes             int
	rollbacks           int
}

func (f *fakeSlots) ApplyEgressPolicy(_ context.Context, _ string, rs slot.EgressRuleSet) error {
	f.applies = append(f.applies, rs)
	if f.applyErr != nil {
		return f.applyErr
	}
	f.current, f.present = rs, true
	if f.onApply != nil {
		f.onApply()
	}
	return nil
}

func (f *fakeSlots) RemoveEgressPolicy(context.Context, string) error {
	f.removes++
	if f.removeErr != nil {
		return f.removeErr
	}
	f.current, f.present = slot.EgressRuleSet{}, false
	return nil
}

func (f *fakeSlots) CurrentEgressPolicy(string) (slot.EgressRuleSet, bool) {
	return f.current, f.present
}

func (f *fakeSlots) RollbackEgressPolicy(_ context.Context, _ string, rs slot.EgressRuleSet, present bool) error {
	f.rollbacks++
	f.current, f.present = rs, present
	return nil
}

func (*fakeSlots) RestoreEgress(context.Context, string) error { return nil }

func managerProxy(t *testing.T) *Proxy {
	t.Helper()
	resolver, err := NewResolver([]string{"127.0.0.1:53"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := NewReservedChecker()
	if err != nil {
		t.Fatal(err)
	}
	return NewProxy(18080, 18443, resolver, reserved, nil)
}

func managerPolicy(generation uint64) *Policy {
	p := policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"ok.example.com"})
	p.Generation = generation
	return p
}

func TestManagerApplyNftFailureKeepsProxyGenerationAndBinding(t *testing.T) {
	p := managerProxy(t)
	old := managerPolicy(1)
	if err := p.Register("m1", "e1", "p", "a", old); err != nil {
		t.Fatal(err)
	}
	if err := p.BindIP("10.100.0.7", "m1"); err != nil {
		t.Fatal(err)
	}
	fs := &fakeSlots{
		current:  slot.EgressRuleSet{Mode: "allowlist", Generation: 1},
		present:  true,
		applyErr: errors.New("nft failed"),
	}
	m := NewManager(p, fs)
	if err := m.Apply(context.Background(), "m1", "e2", "p", "a", "10.100.0.8", managerPolicy(2)); err == nil {
		t.Fatal("expected failure")
	}
	if got := p.byMach["m1"]; got.ExecutionID != "e1" || got.Policy.Generation != 1 {
		t.Fatalf("proxy changed: %+v", got)
	}
	if p.byIP[netip.MustParseAddr("10.100.0.7")] == nil || p.byIP[netip.MustParseAddr("10.100.0.8")] != nil {
		t.Fatal("IP binding changed before nft commit")
	}
}

func TestManagerRemoveClearsNftBeforeUnregister(t *testing.T) {
	p := managerProxy(t)
	if err := p.Register("m1", "e1", "p", "a", managerPolicy(1)); err != nil {
		t.Fatal(err)
	}
	fs := &fakeSlots{removeErr: errors.New("nft failed")}
	m := NewManager(p, fs)
	if err := m.Remove(context.Background(), "m1"); err == nil {
		t.Fatal("expected failure")
	}
	if p.byMach["m1"] == nil {
		t.Fatal("proxy unregistered while nft remained")
	}
	fs.removeErr = nil
	if err := m.Remove(context.Background(), "m1"); err != nil {
		t.Fatal(err)
	}
	if p.byMach["m1"] != nil {
		t.Fatal("proxy remains after nft clear")
	}
}

func TestManagerApplyBindIPValidationDoesNotTouchNft(t *testing.T) {
	p := managerProxy(t)
	fs := &fakeSlots{}
	m := NewManager(p, fs)
	if err := m.Apply(context.Background(), "m1", "e1", "p", "a", "bad-ip", managerPolicy(1)); err == nil {
		t.Fatal("expected bind validation failure")
	}
	if len(fs.applies) != 0 {
		t.Fatal("nft touched before BindIP validation")
	}
	if p.byMach["m1"] != nil {
		t.Fatal("staged registration became visible")
	}
}
