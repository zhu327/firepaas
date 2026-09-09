package ulanet

import (
	"net/netip"
	"testing"
)

func TestIsULAPrefix(t *testing.T) {
	good := []string{
		"fd7a:9a55::/40",
		"fd7a:9a55:1::/64",
		"fd00::/8",
	}
	for _, raw := range good {
		if !IsULAPrefix(netip.MustParsePrefix(raw)) {
			t.Fatalf("%s must be ULA", raw)
		}
	}
	bad := []string{
		"fc00::/8",            // L=0 保留，不是 ULA（评审 P1-3 曾误收）
		"fe80::/10",           // link-local
		"2001:db8::/32",       // 文档地址
		"10.0.0.0/8",          // v4
		"::ffff:10.0.0.0/104", // v4-mapped
	}
	for _, raw := range bad {
		if IsULAPrefix(netip.MustParsePrefix(raw)) {
			t.Fatalf("%s must not be ULA", raw)
		}
	}
	if IsULAPrefix(netip.Prefix{}) {
		t.Fatal("invalid prefix must not be ULA")
	}
	if IsULAAddr(netip.MustParseAddr("fd7a:9a55:1::5")) == false {
		t.Fatal("fd7a:9a55:1::5 must be a ULA addr")
	}
	if IsULAAddr(netip.MustParseAddr("fc00::5")) {
		t.Fatal("fc00::5 must not be a ULA addr")
	}
}

func TestValidatePrefix(t *testing.T) {
	// 规范化 /64 ULA。
	p, err := ValidatePrefix("fd7a:9a55:1::/64", 64)
	if err != nil || p.String() != "fd7a:9a55:1::/64" {
		t.Fatalf("valid node prefix rejected: %v %s", err, p)
	}
	// host bits 置位 → 拒绝（非规范前缀不得静默错位）。
	for _, raw := range []string{
		"fd7a:9a55:1::5/64",
		"fc00::/64",        // L=0 保留段
		"2001:db8:1::/64",  // 非 ULA
		"fd7a:9a55:1::/48", // 位宽不符
		"fd7a:9a55:1::/63",
		"not-a-prefix",
	} {
		if _, err := ValidatePrefix(raw, 64); err == nil {
			t.Fatalf("%q must be rejected", raw)
		}
	}
}
