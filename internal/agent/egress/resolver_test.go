package egress

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/miekg/dns"
)

// startTestDNS 启动本地权威 DNS：example.com A + AAAA；alias CNAME 链；私有
// rebind 目标 private.example.com→10.0.0.7；nx 返回 NXDOMAIN。
func startTestDNS(t *testing.T) (upstreams []string) {
	t.Helper()
	mux := dns.NewServeMux()
	handler := func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		switch q.Name {
		case "example.com.":
			if q.Qtype == dns.TypeA {
				rr, _ := dns.NewRR("example.com. 30 IN A 93.184.216.34")
				m.Answer = append(m.Answer, rr)
			} else if q.Qtype == dns.TypeAAAA {
				rr, _ := dns.NewRR("example.com. 30 IN AAAA 2606:2800:220:1:248:1893:25c8:1946")
				m.Answer = append(m.Answer, rr)
			}
		case "alias.example.com.":
			if q.Qtype == dns.TypeA || q.Qtype == dns.TypeAAAA {
				rr, _ := dns.NewRR("alias.example.com. 20 IN CNAME example.com.")
				m.Answer = append(m.Answer, rr)
			}
		case "chain.example.com.":
			if q.Qtype == dns.TypeA || q.Qtype == dns.TypeAAAA {
				rr, _ := dns.NewRR("chain.example.com. 20 IN CNAME alias.example.com.")
				m.Answer = append(m.Answer, rr)
			}
		case "private.example.com.":
			if q.Qtype == dns.TypeA {
				rr, _ := dns.NewRR("private.example.com. 30 IN A 10.0.0.7")
				m.Answer = append(m.Answer, rr)
			}
		default:
			m.Rcode = dns.RcodeNameError
		}
		_ = w.WriteMsg(m)
	}
	mux.HandleFunc(".", handler)
	srv := &dns.Server{Addr: "127.0.0.1:0", Net: "udp", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Shutdown() })
	deadline := time.Now().Add(3 * time.Second)
	for srv.PacketConn == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.PacketConn == nil {
		t.Fatal("test dns server did not start")
	}
	port := srv.PacketConn.LocalAddr().(*net.UDPAddr).Port
	return []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
}

func TestResolverCNAMEChainAndTTLCache(t *testing.T) {
	upstreams := startTestDNS(t)
	r, err := NewResolver(upstreams, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := r.Resolve(context.Background(), "chain.example.com")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range addrs {
		if a == netip.MustParseAddr("93.184.216.34") {
			found = true
		}
	}
	if !found {
		t.Fatalf("cname chain must resolve to example.com A: %v", addrs)
	}
	if _, err := r.Resolve(context.Background(), "nx.example.com"); err == nil {
		t.Fatal("NXDOMAIN must fail closed")
	}
	if _, err := r.Resolve(context.Background(), ""); err == nil {
		t.Fatal("empty host must fail")
	}
}

func TestResolverTTLCap(t *testing.T) {
	upstreams := startTestDNS(t)
	r, err := NewResolver(upstreams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), "example.com"); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	e := r.cache["example.com"]
	r.mu.Unlock()
	if e.expires.IsZero() || time.Until(e.expires) > time.Second+50*time.Millisecond {
		t.Fatalf("ttl must be capped at 1s, got %v", time.Until(e.expires))
	}
}

func TestReservedChecker(t *testing.T) {
	rc, err := NewReservedChecker("10.12.0.0/16", "10.100.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	reserved := []string{
		"127.0.0.1", "10.1.2.3", "172.16.5.5", "192.168.1.1",
		"169.254.169.254", "169.254.10.10", "224.0.0.1", "0.0.0.0",
		"100.64.0.1", "198.18.0.1", "255.255.255.255", "203.0.113.9",
		"::1", "fe80::1", "fc00::1", "ff02::1", "2001:db8::1",
		"10.12.0.1", "10.100.99.7",
	}
	for _, ip := range reserved {
		if !rc.IsReserved(netip.MustParseAddr(ip)) {
			t.Fatalf("%s must be reserved", ip)
		}
	}
	public := []string{"93.184.216.34", "8.8.8.8", "2606:2800:220:1:248:1893:25c8:1946"}
	for _, ip := range public {
		if rc.IsReserved(netip.MustParseAddr(ip)) {
			t.Fatalf("%s must not be reserved", ip)
		}
	}
	if _, err := NewReservedChecker("bad-cidr"); err == nil {
		t.Fatal("bad platform cidr must error")
	}
}

func TestReservedCheckerFilterPublic(t *testing.T) {
	rc, err := NewReservedChecker("10.12.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	out := rc.FilterPublic([]netip.Addr{
		netip.MustParseAddr("10.0.0.7"), netip.MustParseAddr("93.184.216.34"),
	})
	if len(out) != 1 || out[0] != netip.MustParseAddr("93.184.216.34") {
		t.Fatalf("filter = %v", out)
	}
}
