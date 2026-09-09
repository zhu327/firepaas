package dnsserver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/zhu327/firepaas/internal/agent/state"
)

// recordSet 是测试用线程安全快照：DNS handler 在 serve goroutine 中读取，
// 测试主 goroutine 替换快照，必须同步避免 data race。
type recordSet struct {
	mu      sync.Mutex
	records []state.DnsRecord
}

func (s *recordSet) get() []state.DnsRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records
}

func (s *recordSet) set(records []state.DnsRecord) {
	s.mu.Lock()
	s.records = records
	s.mu.Unlock()
}

// freeUDPAddr 取一个可绑定的 [::1]:0 地址（同端口 UDP+TCP 双监听用；
// 取端口后立即释放存在竞态，实测可接受——失败即重试一次）。
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	for i := 0; i < 5; i++ {
		c, err := net.ListenPacket("udp", "[::1]:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := c.LocalAddr().String()
		_ = c.Close()
		// 拿到端口后校验 UDP 可绑（Ensure 需要 UDP+TCP 同端口）。
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			return addr
		}
	}
	t.Fatal("no free port")
	return ""
}

func query(t *testing.T, addr, name string, qtype uint16) *dns.Msg {
	t.Helper()
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), qtype)
	resp, err := dns.Exchange(m, addr)
	if err != nil {
		t.Fatalf("exchange %s: %v", name, err)
	}
	return resp
}

func TestNodeLocalDNS(t *testing.T) {
	current := &recordSet{records: []state.DnsRecord{
		{Name: "web.dev.internal", AAAA: []string{"fd7a:9a55:0:1::5", "fd7a:9a55:0:1::6"}, Generation: 3},
	}}
	srv := New(current.get)
	defer srv.Close()
	addr := freeUDPAddr(t)
	if err := srv.Ensure(context.Background(), addr); err != nil {
		t.Fatal(err)
	}

	// 命中：AAAA 集 + 权威 + TTL。
	resp := query(t, addr, "web.dev.internal", dns.TypeAAAA)
	if resp.Rcode != dns.RcodeSuccess || !resp.Authoritative || len(resp.Answer) != 2 {
		t.Fatalf("hit = rcode %d auth %v answers %d", resp.Rcode, resp.Authoritative, len(resp.Answer))
	}
	got := map[string]bool{}
	for _, rr := range resp.Answer {
		if a, ok := rr.(*dns.AAAA); ok {
			got[a.AAAA.String()] = true
			if a.Hdr.Ttl != uint32(answerTTL.Seconds()) {
				t.Fatalf("ttl = %d", a.Hdr.Ttl)
			}
		}
	}
	if !got["fd7a:9a55:0:1::5"] || !got["fd7a:9a55:0:1::6"] {
		t.Fatalf("aaaa set = %v", got)
	}

	// 大小写不敏感 + 尾点归一。
	if r := query(t, addr, "WEB.dev.internal.", dns.TypeAAAA); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 2 {
		t.Fatalf("case-insensitive lookup failed: %d/%d", r.Rcode, len(r.Answer))
	}

	// 区内未命中 → NXDOMAIN（权威负应答）。
	if r := query(t, addr, "missing.dev.internal", dns.TypeAAAA); r.Rcode != dns.RcodeNameError {
		t.Fatalf("miss rcode = %d, want NXDOMAIN", r.Rcode)
	}

	// 区内 A 查询 → NOERROR 空应答（IPv4-only 不支持，不构造 IPv4）。
	if r := query(t, addr, "web.dev.internal", dns.TypeA); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 {
		t.Fatalf("in-zone A rcode = %d answers = %d, want NOERROR/0", r.Rcode, len(r.Answer))
	}

	// 区外 → REFUSED（split-horizon：客户端转下一 nameserver）。
	if r := query(t, addr, "example.com", dns.TypeA); r.Rcode != dns.RcodeRefused {
		t.Fatalf("out-of-zone rcode = %d, want REFUSED", r.Rcode)
	}

	// 快照变更即时生效（记录摘除 → NXDOMAIN；新记录 → 命中）。
	current.set([]state.DnsRecord{{Name: "api.dev.internal", AAAA: []string{"fd7a:9a55:0:2::9"}, Generation: 4}})
	if r := query(t, addr, "web.dev.internal", dns.TypeAAAA); r.Rcode != dns.RcodeNameError {
		t.Fatalf("stale record still served: %d", r.Rcode)
	}
	if r := query(t, addr, "api.dev.internal", dns.TypeAAAA); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 1 {
		t.Fatalf("new record missing: %d/%d", r.Rcode, len(r.Answer))
	}

	// 空快照（全部下线）→ 区内一律 NXDOMAIN，区外仍 REFUSED。
	current.set(nil)
	if r := query(t, addr, "api.dev.internal", dns.TypeAAAA); r.Rcode != dns.RcodeNameError {
		t.Fatalf("empty snapshot must NXDOMAIN: %d", r.Rcode)
	}
	if r := query(t, addr, "example.com", dns.TypeA); r.Rcode != dns.RcodeRefused {
		t.Fatalf("out-of-zone after empty: %d", r.Rcode)
	}
}

// TestEnsureIdempotentAndRebind：同 addr 重复 Ensure no-op；addr 变更重绑后
// 旧监听停、新监听服。空 addr 不启动。
func TestEnsureIdempotentAndRebind(t *testing.T) {
	srv := New(func() []state.DnsRecord {
		return []state.DnsRecord{{Name: "a.p.internal", AAAA: []string{"fd7a:9a55::1"}}}
	})
	defer srv.Close()
	if err := srv.Ensure(context.Background(), ""); err != nil {
		t.Fatalf("empty addr must be no-op: %v", err)
	}
	addr1 := freeUDPAddr(t)
	if err := srv.Ensure(context.Background(), addr1); err != nil {
		t.Fatal(err)
	}
	if err := srv.Ensure(context.Background(), addr1); err != nil { // 幂等
		t.Fatal(err)
	}
	addr2 := freeUDPAddr(t)
	if addr2 == addr1 {
		t.Skip("port reuse; skip rebind assertion")
	}
	if err := srv.Ensure(context.Background(), addr2); err != nil {
		t.Fatal(err)
	}
	if r, err := queryErr(t, addr1, "a.p.internal", dns.TypeAAAA); err == nil {
		t.Fatalf("old listener still answering: %+v", r)
	}
	if r := query(t, addr2, "a.p.internal", dns.TypeAAAA); r.Rcode != dns.RcodeSuccess {
		t.Fatalf("rebind failed: %d", r.Rcode)
	}
}

func queryErr(t *testing.T, addr, name string, qtype uint16) (*dns.Msg, error) {
	t.Helper()
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), qtype)
	return dns.Exchange(m, addr)
}

// TestServeStaleBudgetDocumented：断流预算由 Redis TTL 链路承载（键过期
// → reconciler 摘除 → 快照收敛），本服务无独立计时器——这里锁定语义：
// 快照不变更时持续应答（与 fabric 其余投影一致），快照摘除即停。
func TestServeStaleBudgetDocumented(t *testing.T) {
	current := &recordSet{records: []state.DnsRecord{{Name: "x.p.internal", AAAA: []string{"fd7a:9a55::2"}}}}
	srv := New(current.get)
	defer srv.Close()
	addr := freeUDPAddr(t)
	if err := srv.Ensure(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if r := query(t, addr, "x.p.internal", dns.TypeAAAA); r.Rcode != dns.RcodeSuccess {
		t.Fatalf("frozen snapshot must keep serving: %d", r.Rcode)
	}
	current.set(nil) // 快照收敛（预算耗尽后的形态）
	if r := query(t, addr, "x.p.internal", dns.TypeAAAA); r.Rcode != dns.RcodeNameError {
		t.Fatalf("converged snapshot must stop serving: %d", r.Rcode)
	}
}
