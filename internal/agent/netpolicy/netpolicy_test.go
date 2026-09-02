package netpolicy

import (
	"net/netip"
	"strings"
	"testing"
)

// canonical 集合的硬约束：CGNAT 与 loopback 必须同时出现在 Go matcher 与
// nftables 集合文本里（两者同源是本包存在的意义）。
func TestCanonicalSetCoversCGNATAndLoopback(t *testing.T) {
	text := IPv4NftSetText()
	for _, cidr := range []string{"100.64.0.0/10", "127.0.0.0/8", "10.0.0.0/8", "169.254.0.0/16"} {
		if !strings.Contains(text, cidr) {
			t.Fatalf("nft set text missing %s: %s", cidr, text)
		}
		if !Contains(netip.MustParseAddr(strings.Split(cidr, "/")[0])) {
			t.Fatalf("matcher rejects canonical %s", cidr)
		}
	}
}

func TestContainsBoundarySemantics(t *testing.T) {
	// CGNAT 边界：100.127.255.255 在内，100.128.0.0 在外。
	if !Contains(netip.MustParseAddr("100.127.255.255")) {
		t.Fatal("100.127.255.255 must be reserved (CGNAT /10)")
	}
	if Contains(netip.MustParseAddr("100.128.0.0")) {
		t.Fatal("100.128.0.0 is outside 100.64.0.0/10")
	}
	// 公网单播不属于保留段。
	if Contains(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("8.8.8.8 must not be reserved")
	}
	// IPv6 链路本地与 ULA 保留；公网 v6 不保留。
	if !Contains(netip.MustParseAddr("fe80::1")) || !Contains(netip.MustParseAddr("fd00::1")) {
		t.Fatal("v6 link-local/ULA must be reserved")
	}
	if Contains(netip.MustParseAddr("2606:4700:4700::1111")) {
		t.Fatal("2606:4700:4700::1111 must not be reserved")
	}
}

func TestPrefixesReturnsCopy(t *testing.T) {
	p := Prefixes()
	p[0] = netip.MustParsePrefix("1.2.3.4/32")
	if Prefixes()[0] == p[0] {
		t.Fatal("Prefixes must return a copy")
	}
}
