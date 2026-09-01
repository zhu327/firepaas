// resolver.go：v1.3-A（ADR-0027）可信 DNS resolver。
//
// 只使用 agent 宿主配置的可信上游（/etc/resolv.conf 或 FIREPAAS_EGRESS_DNS），
// 绝不使用 guest 提供的解析结果。完整跟随 CNAME 链（有界深度），A/AAAA 合并
// 去重；缓存按记录 TTL 且封顶（受限 TTL）。解析失败/空集合由调用方 fail
// closed。连接前保留段检查由 ReservedChecker 提供。
package egress

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

const (
	defaultTTLCap       = 60 * time.Second
	defaultResolveDepth = 8
	defaultResolveTO    = 5 * time.Second
)

// ErrResolutionFailed 表示可信解析失败或集合为空（连接必须拒绝）。
var ErrResolutionFailed = errors.New("trusted resolution failed")

// Resolver 是带 TTL 上限缓存的可信解析器。
type Resolver struct {
	upstreams []string
	ttlCap    time.Duration
	timeout   time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	addrs   []netip.Addr
	expires time.Time
}

// NewResolver 构造解析器。upstreams 空 = 读 /etc/resolv.conf（fail closed：
// 读不到即返回 error）。ttlCap <= 0 用默认上限。
func NewResolver(upstreams []string, ttlCap time.Duration) (*Resolver, error) {
	if len(upstreams) == 0 {
		up, err := systemUpstreams()
		if err != nil {
			return nil, err
		}
		upstreams = up
	}
	if ttlCap <= 0 {
		ttlCap = defaultTTLCap
	}
	return &Resolver{upstreams: upstreams, ttlCap: ttlCap, timeout: defaultResolveTO,
		cache: map[string]cacheEntry{}}, nil
}

func systemUpstreams() ([]string, error) {
	f, err := os.Open("/etc/resolv.conf")
	if err != nil {
		return nil, fmt.Errorf("read /etc/resolv.conf: %w", err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "nameserver" {
			out = append(out, fields[1])
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no nameserver in /etc/resolv.conf")
	}
	return out, nil
}

// Resolve 解析规范化 host，返回可信 A/AAAA 集合（已去重、排序）。跟随
// CNAME 链（深度上限 defaultResolveDepth），NXDOMAIN/无地址/超时均为错误。
func (r *Resolver) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if host == "" {
		return nil, fmt.Errorf("%w: empty host", ErrResolutionFailed)
	}
	r.mu.Lock()
	if e, ok := r.cache[host]; ok && time.Now().Before(e.expires) {
		out := append([]netip.Addr(nil), e.addrs...)
		r.mu.Unlock()
		return out, nil
	}
	r.mu.Unlock()

	addrs, ttl, err := r.resolveChain(ctx, host, defaultResolveDepth)
	if err != nil {
		return nil, err
	}
	addrs = sortedIPs(addrs)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w: no A/AAAA for %s", ErrResolutionFailed, host)
	}
	if ttl <= 0 || ttl > r.ttlCap {
		ttl = r.ttlCap
	}
	r.mu.Lock()
	r.cache[host] = cacheEntry{addrs: addrs, expires: time.Now().Add(ttl)}
	r.mu.Unlock()
	return addrs, nil
}

// resolveChain 跟随 CNAME 链；返回合并地址与链上最小 TTL。
func (r *Resolver) resolveChain(ctx context.Context, host string, depth int) ([]netip.Addr, time.Duration, error) {
	if depth <= 0 {
		return nil, 0, fmt.Errorf("%w: cname chain too deep for %s", ErrResolutionFailed, host)
	}
	var (
		all  []netip.Addr
		minT = time.Duration(0)
	)
	for name, hop := host, 0; name != "" && hop < defaultResolveDepth; hop++ {
		msg, err := r.query(ctx, name)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %s: %v", ErrResolutionFailed, name, err)
		}
		if msg.Rcode != dns.RcodeSuccess {
			return nil, 0, fmt.Errorf("%w: %s: rcode=%s", ErrResolutionFailed, name, dns.RcodeToString[msg.Rcode])
		}
		var next string
		for _, rr := range msg.Answer {
			switch rec := rr.(type) {
			case *dns.A:
				if a, ok := netip.AddrFromSlice(rec.A); ok {
					all = append(all, a)
				}
			case *dns.AAAA:
				if a, ok := netip.AddrFromSlice(rec.AAAA); ok {
					all = append(all, a)
				}
			case *dns.CNAME:
				if next == "" {
					next = strings.ToLower(strings.TrimSuffix(rec.Target, "."))
				}
			}
			ttl := time.Duration(rr.Header().Ttl) * time.Second
			if ttl > 0 && (minT == 0 || ttl < minT) {
				minT = ttl
			}
		}
		if len(all) > 0 {
			return all, minT, nil
		}
		if next == "" || next == name {
			return nil, 0, fmt.Errorf("%w: no A/AAAA for %s", ErrResolutionFailed, name)
		}
		name = next
	}
	return nil, 0, fmt.Errorf("%w: cname chain unresolved for %s", ErrResolutionFailed, host)
}

// query 向上游之一发起 A+AAAA 查询（依次尝试，全部失败即错误）。
func (r *Resolver) query(ctx context.Context, host string) (*dns.Msg, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var lastErr error
	for _, upstream := range r.upstreams {
		server := upstream
		if _, _, err := net.SplitHostPort(server); err != nil {
			server = net.JoinHostPort(strings.Trim(server, "[]"), "53")
		}
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn(host), dns.TypeA)
		// 用支持 SetDeadline 的 UDP 连接；TCP 回退由 client 完成。
		c := &dns.Client{Net: "udp"}
		resp, _, err := c.ExchangeContext(ctx, m, server)
		if err != nil {
			lastErr = err
			continue
		}
		if resp == nil {
			lastErr = errors.New("empty dns response")
			continue
		}
		// A 与 AAAA 合并：A 失败（SERVFAIL 等）也不阻塞 AAAA 结果。
		m6 := new(dns.Msg)
		m6.SetQuestion(dns.Fqdn(host), dns.TypeAAAA)
		resp6, _, err6 := c.ExchangeContext(ctx, m6, server)
		if err6 == nil && resp6 != nil && resp6.Rcode == dns.RcodeSuccess {
			resp.Answer = append(resp.Answer, resp6.Answer...)
		}
		return resp, nil
	}
	return nil, lastErr
}

// ReservedChecker 判断解析地址是否属于必须拒绝的保留段：
// loopback、link-local（含 metadata）、私网、组播、未指定、文档段、
// CGNAT、平台 veth 子网。平台段由 NewReservedChecker 注入。
type ReservedChecker struct {
	extra []netip.Prefix
}

// NewReservedChecker 构造 checker；platformCIDRs 为平台内部段（slot veth
// 范围、guest 子网等），连接前一律拒绝。
func NewReservedChecker(platformCIDRs ...string) (*ReservedChecker, error) {
	rc := &ReservedChecker{}
	for _, cidr := range platformCIDRs {
		if strings.TrimSpace(cidr) == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
		if err != nil {
			return nil, fmt.Errorf("reserved platform cidr %q: %w", cidr, err)
		}
		rc.extra = append(rc.extra, prefix.Masked())
	}
	return rc, nil
}

// IsReserved 判断地址是否在保留段（连接前检查）。
func (rc *ReservedChecker) IsReserved(addr netip.Addr) bool {
	a := addr.Unmap()
	if !a.IsValid() {
		return true
	}
	if a.IsLoopback() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsPrivate() || a.IsMulticast() || a.IsUnspecified() {
		return true
	}
	// 显式补充：CGNAT、IETF 保留、benchmark、broadcast、文档段（IPv4）。
	if a.Is4() {
		ip := a.As4()
		switch {
		case ip[0] == 0, ip[0] == 100 && ip[1]&0xC0 == 64, // 0/8, 100.64/10
			ip[0] == 192 && ip[1] == 0 && ip[2] == 0,     // 192.0.0.0/24
			ip[0] == 198 && (ip[1] == 18 || ip[1] == 19), // 198.18/15
			ip[0] == 255,                                // broadcast
			ip[0] == 192 && ip[1] == 0 && ip[2] == 2,    // TEST-NET-1
			ip[0] == 198 && ip[1] == 51 && ip[2] == 100, // TEST-NET-2
			ip[0] == 203 && ip[1] == 0 && ip[2] == 113:  // TEST-NET-3
			return true
		}
	} else {
		// IPv6 文档段 2001:db8::/32。
		if a.Is6() && a.As16()[0] == 0x20 && a.As16()[1] == 0x01 && a.As16()[2] == 0x0d && a.As16()[3] == 0xb8 {
			return true
		}
	}
	for _, prefix := range rc.extra {
		if prefix.Contains(a) {
			return true
		}
	}
	return false
}

// FilterPublic 过滤保留段，返回可连接地址子集；全部保留 = nil（拒绝）。
func (rc *ReservedChecker) FilterPublic(addrs []netip.Addr) []netip.Addr {
	out := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		if !rc.IsReserved(a) {
			out = append(out, a)
		}
	}
	return out
}
