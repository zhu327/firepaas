package egress

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/miekg/dns"
)

// fakeDNSServer 解析固定域名 → 指定 A 记录（无记录 = NXDOMAIN）。
func fakeDNSServer(t *testing.T, records map[string][]string) []string {
	t.Helper()
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		ips, ok := records[q.Name]
		if !ok {
			m.Rcode = dns.RcodeNameError
			_ = w.WriteMsg(m)
			return
		}
		for _, ip := range ips {
			rr, _ := dns.NewRR(q.Name + " 30 IN A " + ip)
			m.Answer = append(m.Answer, rr)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{Addr: "127.0.0.1:0", Net: "udp", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Shutdown() })
	deadline := time.Now().Add(3 * time.Second)
	for srv.PacketConn == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.PacketConn == nil {
		t.Fatal("dns did not start")
	}
	port := srv.PacketConn.LocalAddr().(*net.UDPAddr).Port
	return []string{net.JoinHostPort("127.0.0.1", itoaPort(port))}
}

func itoaPort(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

type collectSink struct {
	mu   sync.Mutex
	recs []AuditRecord
}

func (c *collectSink) Record(r AuditRecord) {
	c.mu.Lock()
	c.recs = append(c.recs, r)
	c.mu.Unlock()
}

func testProxy(t *testing.T) (*Proxy, *collectSink) {
	t.Helper()
	upstreams := fakeDNSServer(t, map[string][]string{
		"ok.example.com.":      {"93.184.216.34"},
		"private.example.com.": {"10.0.0.7"},
	})
	resolver, err := NewResolver(upstreams, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := NewReservedChecker()
	if err != nil {
		t.Fatal(err)
	}
	sink := &collectSink{}
	p := NewProxy(18080, 18443, resolver, reserved, sink)
	policy := policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"ok.example.com"})
	policy.Generation = 2
	if err := p.Register("m1", "exec-1", "dev", "app-1", policy); err != nil {
		t.Fatal(err)
	}
	if err := p.BindIP("10.100.0.7", "m1"); err != nil {
		t.Fatal(err)
	}
	return p, sink
}

func TestProxyFencingKeepsOldPolicy(t *testing.T) {
	p, _ := testProxy(t)
	lower := policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"other.example.com"})
	lower.Generation = 1
	if err := p.Register("m1", "exec-1", "dev", "app-1", lower); err == nil {
		t.Fatal("lower generation must be rejected")
	}
	p.mu.RLock()
	e := p.byMach["m1"]
	p.mu.RUnlock()
	if e == nil || e.Policy.Generation != 2 || !e.Policy.matchesDomain("ok.example.com") {
		t.Fatalf("old policy must survive fencing: %+v", e)
	}
}

func TestProxyUnregisteredSourceFailClosed(t *testing.T) {
	p, _ := testProxy(t)
	server, client := net.Pipe()
	defer server.Close()
	go p.handle(context.Background(), server, 0)
	_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: ok.example.com\r\n\r\n"))
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, _ = client.Read(buf) // 未注册来源 → 直接关闭（EOF）
}

func TestProxyReservedSegmentProtection(t *testing.T) {
	p, _ := testProxy(t)
	// 解析到私网段的域名必须被过滤（DNS rebinding 防护）。
	addrs, err := p.resolver.Resolve(context.Background(), "private.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got := p.reserved.FilterPublic(addrs); len(got) != 0 {
		t.Fatalf("reserved filtering failed: %v", got)
	}
	decision := p.byMach["m1"].Policy.DecideForProxied("private.example.com", addrs)
	if decision.Allow {
		t.Fatal("rebinding target must be denied")
	}
}

func TestProxyAuditMode(t *testing.T) {
	p, sink := testProxy(t)
	e := p.byMach["m1"]
	allowed := AuditRecord{Allowed: true}
	denied := AuditRecord{Allowed: false}
	p.recordAudit(e, allowed)
	p.recordAudit(e, denied)
	if len(sink.recs) != 1 || sink.recs[0].Allowed {
		t.Fatalf("default audit must emit denials only: %+v", sink.recs)
	}
	e.Policy.AuditAll = true
	p.recordAudit(e, allowed)
	if len(sink.recs) != 2 || !sink.recs[1].Allowed {
		t.Fatalf("audit_all must emit allows: %+v", sink.recs)
	}
}

func TestProxyDecisionAuditAndStats(t *testing.T) {
	p, sink := testProxy(t)
	e := p.byMach["m1"]
	d := e.Policy.DecideForProxied("evil.example.com", []netip.Addr{netip.MustParseAddr("93.184.216.34")})
	if d.Allow {
		t.Fatal("non-listed host must be denied")
	}
	// 审计与分桶。
	e.recordDeny("http", 80)
	e.recordDeny("http", 80)
	e.recordDeny("tls", 443)
	stats := p.Stats("m1")
	if stats == nil || stats.DeniedConnections != 3 || len(stats.DenyBuckets) != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.DenyBuckets[0].Protocol == "" {
		t.Fatal("bucket protocol missing")
	}
	if sink == nil {
		t.Fatal("sink must be wired")
	}
}

func TestProxyConnectionLimit(t *testing.T) {
	upstreams := fakeDNSServer(t, map[string][]string{"ok.example.com.": {"93.184.216.34"}})
	resolver, _ := NewResolver(upstreams, time.Hour)
	reserved, _ := NewReservedChecker()
	p := NewProxy(18080, 18443, resolver, reserved, nil)
	policy := policyFor(pb.EgressPolicySpec_ALLOWLIST, nil, nil, []string{"ok.example.com"})
	policy.MaxTCPConns = 2
	if err := p.Register("m1", "exec-1", "dev", "app-1", policy); err != nil {
		t.Fatal(err)
	}
	if err := p.BindIP("10.100.0.7", "m1"); err != nil {
		t.Fatal(err)
	}
	e := p.byMach["m1"]
	e.active.Store(2)
	e.limitReject.Add(1)
	stats := p.Stats("m1")
	if stats.LimitRejections != 1 {
		t.Fatalf("limit rejections = %d", stats.LimitRejections)
	}
}

func TestDialFirstFallsThrough(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	conn, err := dialFirst([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, uint32(port))
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if _, err := dialFirst(nil, 80); err == nil {
		t.Fatal("empty set must error")
	}
}

func TestHandleAllowlistNoHostDenied(t *testing.T) {
	p, _ := testProxy(t)
	e := p.byMach["m1"]
	decision := e.Policy.DecideForProxied("", nil)
	if decision.Allow || decision.MatchType != "no_host" {
		t.Fatalf("no-host allowlist must deny: %+v", decision)
	}
}

// relay 单元验证：prefix 回放 + 双向拷贝（两对 pipe，避免同 pipe 双读者）。
func TestRelayReplaysPrefix(t *testing.T) {
	upSrv, upCli := net.Pipe()
	downSrv, downCli := net.Pipe()
	defer upSrv.Close()
	defer downSrv.Close()
	msg := []byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	prefix := []byte("PREFIX:")
	done := make(chan error, 1)
	go func() {
		done <- relay(upCli, downCli, bufio.NewReader(strings.NewReader(string(msg))), prefix)
	}()
	// upstream 端应收到 prefix + msg。
	want := append(append([]byte{}, prefix...), msg...)
	got := make([]byte, len(want))
	_ = upSrv.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(upSrv, got); err != nil {
		t.Fatalf("read relayed: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("relayed = %q, want %q", got, want)
	}
	// 反向：upstream 响应回送到 downstream 端。
	resp := []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	go func() { _, _ = upSrv.Write(resp) }()
	buf := make([]byte, len(resp))
	_ = downSrv.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(downSrv, buf); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(buf) != string(resp) {
		t.Fatalf("response = %q, want %q", buf, resp)
	}
	upSrv.Close()
	downSrv.Close()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("relay error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not return")
	}
}
