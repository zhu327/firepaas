package egress

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func policyFor(mode pb.EgressPolicySpec_Mode, allowedCidrs, deniedCidrs, domains []string) *Policy {
	p, err := FromProto(&pb.EgressPolicySpec{
		Mode: mode, AllowedCidrs: allowedCidrs, DeniedCidrs: deniedCidrs,
		AllowedDomains: domains, PolicyGeneration: 1,
	})
	if err != nil {
		panic(err)
	}
	return p
}

func TestFromProtoDomainNormalization(t *testing.T) {
	p := policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"Example.COM", "*.Example.com", "api.example.com."})
	if len(p.AllowedDomains) != 3 {
		t.Fatalf("domains = %v", p.AllowedDomains)
	}
	for _, host := range []string{"example.com", "EXAMPLE.COM", "example.com."} {
		if !p.matchesDomain(host) {
			t.Fatalf("exact %q must match", host)
		}
	}
	// ADR-0027：单层 wildcard（*.example.com 匹配 api.example.com，
	// 不匹配 a.b.example.com）。
	if !p.matchesDomain("api.example.com") {
		t.Fatal("wildcard must match single-label prefix")
	}
	if p.matchesDomain("a.b.example.com") {
		t.Fatal("wildcard must not match multi-label prefix")
	}
	if p.matchesDomain("example.net") || p.matchesDomain("badexample.com") || p.matchesDomain("") {
		t.Fatal("non-matching hosts must not match")
	}
}

func TestFromProtoValidationErrors(t *testing.T) {
	cases := []*pb.EgressPolicySpec{
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedCidrs: []string{"not-a-cidr"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedDomains: []string{"https://evil.com/path"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedDomains: []string{"*.*.com"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 0},
		{Mode: 99, PolicyGeneration: 1},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedCidrs: []string{"2001:db8::/32"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, DeniedCidrs: []string{"2001:db8::/32"}},
		{Mode: pb.EgressPolicySpec_ALLOWLIST, PolicyGeneration: 1, AllowedDomains: []string{"2001:db8::1"}},
	}
	for i, spec := range cases {
		if _, err := FromProto(spec); err == nil {
			t.Fatalf("case %d must fail: %+v", i, spec)
		}
	}
}

func TestDecideForProxiedUnrestricted(t *testing.T) {
	p := policyFor(pb.EgressPolicySpec_UNRESTRICTED, nil, []string{"203.0.113.0/24"}, nil)
	public := []netip.Addr{netip.MustParseAddr("93.184.216.34")}
	if d := p.DecideForProxied("example.com", public); !d.Allow {
		t.Fatalf("unrestricted allow: %+v", d)
	}
	if d := p.DecideForProxied("example.com", []netip.Addr{netip.MustParseAddr("203.0.113.5")}); d.Allow || d.MatchType != "cidr_denied" {
		t.Fatalf("denied cidr must deny: %+v", d)
	}
}

func TestDecideForProxiedDenyAll(t *testing.T) {
	p := policyFor(pb.EgressPolicySpec_DENY_ALL, []string{"93.184.216.0/24"}, nil, nil)
	if d := p.DecideForProxied("", []netip.Addr{netip.MustParseAddr("93.184.216.34")}); !d.Allow || d.MatchType != "cidr_allowed" {
		t.Fatalf("allowed cidr must allow: %+v", d)
	}
	if d := p.DecideForProxied("example.com", []netip.Addr{netip.MustParseAddr("1.2.3.4")}); d.Allow {
		t.Fatalf("deny_all default deny: %+v", d)
	}
}

func TestDecideForProxiedAllowlist(t *testing.T) {
	p := policyFor(pb.EgressPolicySpec_ALLOWLIST, []string{"203.0.113.0/24"}, []string{"198.51.100.0/24"}, []string{"*.example.com"})
	cases := []struct {
		name     string
		host     string
		resolved []netip.Addr
		allow    bool
		match    string
	}{
		{"domain match", "api.example.com", []netip.Addr{netip.MustParseAddr("93.184.216.34")}, true, "domain"},
		{"exact case-insensitive", "API.EXAMPLE.COM", []netip.Addr{netip.MustParseAddr("93.184.216.34")}, true, "domain"},
		{"cidr allowed without host", "", []netip.Addr{netip.MustParseAddr("203.0.113.9")}, true, "cidr_allowed"},
		{"no host allowlist default deny", "", []netip.Addr{netip.MustParseAddr("93.184.216.34")}, false, "no_host"},
		{"host not listed", "evil.com", []netip.Addr{netip.MustParseAddr("93.184.216.34")}, false, "mode_default"},
		{"denied wins over domain", "api.example.com", []netip.Addr{netip.MustParseAddr("198.51.100.4")}, false, "cidr_denied"},
		{"denied wins over allowed cidr", "", []netip.Addr{netip.MustParseAddr("198.51.100.4")}, false, "cidr_denied"},
		{"empty resolved with host", "api.example.com", nil, false, "mode_default"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := p.DecideForProxied(c.host, c.resolved)
			if d.Allow != c.allow || (c.match != "" && d.MatchType != c.match) {
				t.Fatalf("decision = %+v", d)
			}
		})
	}
}

func TestDecideByCIDRMatrix(t *testing.T) {
	target := netip.MustParseAddr("203.0.113.5")
	allowed := netip.MustParseAddr("93.184.216.34")
	tests := []struct {
		mode  pb.EgressPolicySpec_Mode
		allow bool
	}{
		{pb.EgressPolicySpec_UNRESTRICTED, true},
		{pb.EgressPolicySpec_DENY_ALL, false},
		{pb.EgressPolicySpec_ALLOWLIST, false},
	}
	for _, tc := range tests {
		p := policyFor(tc.mode, []string{"93.184.216.0/24"}, nil, nil)
		if d := p.DecideByCIDR(target); d.Allow != tc.allow {
			t.Fatalf("mode %v target: %+v", tc.mode, d)
		}
		if d := p.DecideByCIDR(allowed); !d.Allow {
			t.Fatalf("mode %v allowed cidr: %+v", tc.mode, d)
		}
	}
	// denied 优先于 allowed。
	p := policyFor(pb.EgressPolicySpec_UNRESTRICTED, []string{"93.184.216.0/24"}, []string{"93.184.216.0/24"}, nil)
	if d := p.DecideByCIDR(allowed); d.Allow {
		t.Fatalf("denied must win: %+v", d)
	}
}

func TestProxyNeededOnlyForAllowlistDomains(t *testing.T) {
	if policyFor(pb.EgressPolicySpec_UNRESTRICTED, nil, nil, []string{"example.com"}).ProxyNeeded() {
		t.Fatal("unrestricted must not proxy")
	}
	if policyFor(pb.EgressPolicySpec_DENY_ALL, nil, nil, []string{"example.com"}).ProxyNeeded() {
		t.Fatal("deny_all must not proxy")
	}
	if !policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"example.com"}).ProxyNeeded() {
		t.Fatal("allowlist with domains must proxy")
	}
	if policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, nil).ProxyNeeded() {
		t.Fatal("allowlist without domains must not proxy")
	}
}

func TestNormalizeHost(t *testing.T) {
	cases := map[string]string{
		"Example.COM:443":  "example.com",
		"[::1]:8080":       "::1",
		"API.example.com.": "api.example.com",
		"  HOST ":          "host",
	}
	for in, want := range cases {
		if got := NormalizeHost(in); got != want {
			t.Fatalf("NormalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRuleSetRoundTrip(t *testing.T) {
	p := policyFor(pb.EgressPolicySpec_ALLOWLIST,
		[]string{"93.184.216.0/24"}, []string{"198.51.100.0/24"}, []string{"*.example.com"})
	p.MaxTCPConns = 128
	p.AuditAll = true
	rs := (&Manager{port80: 18080, port443: 18443}).ruleSetFor(p)
	if rs.Mode != "allowlist" || rs.ProxyPort80 != 18080 || rs.ProxyPort443 != 18443 ||
		rs.MaxTCPConns != 128 || !rs.AuditAll {
		t.Fatalf("rule set = %+v", rs)
	}
	back, err := FromRuleSet(rs)
	if err != nil {
		t.Fatal(err)
	}
	if !back.ProxyNeeded() || back.Generation != 1 ||
		!back.matchesDomain("x.example.com") || !containsCIDR(back.AllowedCIDRs, netip.MustParseAddr("93.184.216.4")) {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
	if _, err := FromRuleSet(slot.EgressRuleSet{Mode: "bogus"}); err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("unknown mode must error: %v", err)
	}
	if p2, err := FromRuleSet(slot.EgressRuleSet{}); err != nil || p2 != nil {
		t.Fatalf("empty rule set must yield nil policy")
	}
}
