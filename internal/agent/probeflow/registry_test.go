package probeflow

import (
	"net/netip"
	"testing"
	"time"
)

func ap(s string) netip.AddrPort {
	a, err := netip.ParseAddrPort(s)
	if err != nil {
		panic(err)
	}
	return a
}

func TestRegistryMatchAndExpiry(t *testing.T) {
	r := NewRegistry(time.Hour)
	r.Record(40000, ap("10.100.0.5:8080"))
	if !r.Match(40000, ap("10.100.0.5:8080")) {
		t.Fatal("recorded flow must match")
	}
	if r.Match(40001, ap("10.100.0.5:8080")) {
		t.Fatal("different source port must not match (per-connection precision)")
	}
	if r.Match(40000, ap("10.100.0.6:8080")) {
		t.Fatal("different destination must not match")
	}
	if r.Size() != 1 {
		t.Fatalf("size = %d, want 1", r.Size())
	}
	// 键 = (源端口, 目的四元组)：目的端口不同不命中。
	if r.Match(40000, ap("10.100.0.5:8081")) {
		t.Fatal("different destination port must not match")
	}
}

func TestRegistryFlowLifetimeIsExplicit(t *testing.T) {
	r := NewRegistry(10 * time.Millisecond) // legacy TTL is deliberately ignored
	dst := ap("10.100.0.5:8080")
	r.Record(40000, dst)
	time.Sleep(20 * time.Millisecond)
	if !r.Match(40000, dst) {
		t.Fatal("live keepalive flow must not expire by TTL")
	}
	r.Release(40000, dst)
	if r.Match(40000, dst) {
		t.Fatal("released flow must not match: a reused port is real traffic")
	}
	if r.Size() != 0 {
		t.Fatalf("released flow must be dropped, size = %d", r.Size())
	}
}

func TestRegistryInvalidInputs(t *testing.T) {
	r := NewRegistry(0)
	r.Record(0, netip.AddrPort{})
	r.Record(40000, netip.AddrPort{})
	if r.Match(0, netip.AddrPort{}) || r.Match(40000, netip.AddrPort{}) {
		t.Fatal("invalid keys must never match")
	}
	if r.Size() != 0 {
		t.Fatalf("invalid record must be ignored, size = %d", r.Size())
	}
}
