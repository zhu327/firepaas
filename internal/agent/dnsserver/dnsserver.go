// Package dnsserver 是节点本地 .internal DNS（ADR-0040 §16，G2c）。
//
// 职责与边界：
//   - 只权威服务 .internal 区（AAAA 唯一记录形态；IPv4-only 应用不发布
//     .internal 记录——上游同源门控决定，本服务只按快照应答）；
//   - 非 .internal 查询一律 REFUSED：guest resolv.conf 把本服务排在首位
//     （split-horizon），公网名立刻落到第二 nameserver（公网解析不受影响）；
//   - 记录源 = 本节点 fabric 快照的 DNS 全表（控制面权威：route backend
//     set 同源 READY 非 draining mesh_direct 门控 → dns:internal:* Redis
//     投影（TTL = serve-stale 预算 120s，与 FIREPAAS_EDGE_STALE_WINDOW
//     同值）→ fabric 快照下发）。断流预算由 Redis TTL 承载：发布器停更
//     → 键 120s 过期 → reconciler 下一轮快照摘除 → 节点停服；
//   - agent 进程挂 = 本服务消失：guest 的公网 nameserver 不受影响
//     （.internal 降级为 NXDOMAIN/超时，符合 §16 “agent 挂则降级”）。
//
// 快照冻结语义：控制面整体断联时 fabric 快照不再更新（identities/policy
// 同样冻结），本服务继续按最后快照应答——与 fabric 其余投影一致，
// 预算收敛依赖 Redis TTL 链路（见上）。
package dnsserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/zhu327/firepaas/internal/agent/state"
)

// answerTTL 是应答记录 TTL：客户端缓存上限（收敛 = 同步周期 + TTL）。
const answerTTL = 30 * time.Second

// RecordsFunc 返回当前快照的 .internal 记录全量（nil 安全）。
type RecordsFunc func() []state.DnsRecord

// Server 是节点本地 DNS 服务（UDP+TCP 同一 handler）。
type Server struct {
	records RecordsFunc

	mu      sync.Mutex
	bind    string
	udp     *dns.Server
	tcp     *dns.Server
	closed  bool
	handler dns.Handler
}

// New 构造服务（未监听；Ensure 按快照 NodePrefix 幂等绑定）。
func New(records RecordsFunc) *Server {
	s := &Server{records: records}
	s.handler = dns.HandlerFunc(s.serveDNS)
	return s
}

// Ensure 幂等确保监听在 addr（"[ula]:53"）。addr 为空 = 未就绪（首拍快照
// 前）不启动。addr 变化（节点 /64 重规划）时重绑。已在 addr 上监听则
// no-op。绑定失败同步返回错误（调用方降级记日志，绝不让 DNS 失败阻断
// agentd）；serve 循环异步（Shutdown 停止）。
func (s *Server) Ensure(ctx context.Context, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("dnsserver: closed")
	}
	if addr == "" {
		return nil
	}
	if s.bind == addr && s.udp != nil {
		return nil
	}
	s.closeLocked()
	// 同步绑定（错误即返回）；serve 异步。
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("dnsserver: udp %s: %w", addr, err)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("dnsserver: tcp %s: %w", addr, err)
	}
	udp := &dns.Server{PacketConn: pc, Handler: s.handler}
	tcp := &dns.Server{Listener: l, Handler: s.handler}
	go func() { _ = udp.ActivateAndServe() }()
	go func() { _ = tcp.ActivateAndServe() }()
	s.bind, s.udp, s.tcp = addr, udp, tcp
	slog.Info("node-local .internal dns listening", "addr", addr)
	return nil
}

// Close 停止监听（幂等）。
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closeLocked()
}

func (s *Server) closeLocked() {
	if s.udp != nil {
		_ = s.udp.Shutdown()
		// Shutdown 在 serve goroutine 尚未 ActivateAndServe 时不会关闭
		// PacketConn（返回 "server not started"），直接关闭兜底，避免重绑
		// 后旧 socket 仍应答。
		if s.udp.PacketConn != nil {
			_ = s.udp.PacketConn.Close()
		}
	}
	if s.tcp != nil {
		_ = s.tcp.Shutdown()
		if s.tcp.Listener != nil {
			_ = s.tcp.Listener.Close()
		}
	}
	s.udp, s.tcp, s.bind = nil, nil, ""
}

// serveDNS 处理单个查询：
//   - 仅 .internal 区内名字（任意 qtype）进入记录查找；
//   - AAAA 命中 → NOERROR + AAAA 集；区内未命中 → NXDOMAIN（权威负应答）；
//   - 区内其它 qtype（A/SRV/TXT…）→ NOERROR 空应答（.internal 只承载
//     AAAA，A 查询不构造 IPv4——IPv4-only 明确不支持）；
//   - 区外名字 → REFUSED（split-horizon：客户端立即转下一 nameserver）。
func (s *Server) serveDNS(w dns.ResponseWriter, req *dns.Msg) {
	if len(req.Question) != 1 {
		// 单问题协议约定（miekg handler 也按首个问题应答）；多问题按
		// FORMERR 拒绝，不猜语义。
		_ = w.WriteMsg(formerr(req))
		return
	}
	q := req.Question[0]
	name := strings.ToLower(strings.TrimSuffix(q.Name, "."))
	if !strings.HasSuffix(name, ".internal") {
		_ = w.WriteMsg(refused(req))
		return
	}
	if q.Qtype != dns.TypeAAAA {
		// 区内非 AAAA（A/SRV/TXT…）：.internal 只承载 AAAA——A 查询
		// 返回 NOERROR 空应答（IPv4-only 明确不支持，不构造 IPv4），
		// 其余类型同理空应答。
		resp := new(dns.Msg)
		resp.SetReply(req)
		resp.Authoritative = true
		_ = w.WriteMsg(resp)
		return
	}
	aaaa := s.lookupAAAA(name)
	if len(aaaa) == 0 {
		// 区内未命中：权威负应答（NXDOMAIN），客户端不再重试。
		_ = w.WriteMsg(nxdomain(req))
		return
	}
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Authoritative = true
	for _, ip := range aaaa {
		resp.Answer = append(resp.Answer, &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   q.Name,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET,
				Ttl:    uint32(answerTTL.Seconds()),
			},
			AAAA: ip,
		})
	}
	_ = w.WriteMsg(resp)
}

// lookupAAAA 返回名字的 ULA 集合（快照全量线性扫；记录数与 app 同阶，
// 节点本地无热路径问题）。名字匹配大小写不敏感（快照名已小写）。
func (s *Server) lookupAAAA(name string) []net.IP {
	records := s.records()
	var out []net.IP
	for _, r := range records {
		if strings.ToLower(r.Name) != name {
			continue
		}
		for _, a := range r.AAAA {
			if ip := net.ParseIP(a); ip != nil && ip.To16() != nil && ip.To4() == nil {
				out = append(out, ip)
			}
		}
	}
	return out
}

func formerr(req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetRcode(req, dns.RcodeFormatError)
	return m
}

func refused(req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetRcode(req, dns.RcodeRefused)
	return m
}

func nxdomain(req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetRcode(req, dns.RcodeNameError)
	m.Authoritative = true
	return m
}
