// proxy.go：v1.3-A（ADR-0027）透明 TCP 代理。
//
// 拓扑：slot netns 内 nftables 把 guest 的 TCP/80、TCP/443 DNAT 到本代理
// （root ns 监听 0.0.0.0:<port80>/<port443>）。代理按来源 guest IP 找到
// execution policy，嗅探 Host/SNI，经可信 resolver 解析后只连接保留段检查
// 通过的 A/AAAA 集合（不回退 guest DNS）；无 Host/SNI 时回退原始目标并按
// CIDR 矩阵判定。per-execution TCP 连接限额 + 结构化审计。
package egress

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

const dialTimeout = 5 * time.Second

// AuditSink 接收连接决策审计记录（不含 URL path/query/header/body）。
type AuditSink interface {
	Record(AuditRecord)
}

// AuditRecord 是一次连接决策（ADR-0027 §审计）。
type AuditRecord struct {
	ProjectID        string
	AppID            string
	MachineID        string
	ExecutionID      string
	PolicyGeneration uint64
	Protocol         string // http | tls | tcp
	Port             uint32
	Host             string
	ResolvedIP       string
	Allowed          bool
	MatchType        string
	Reason           string
}

type slogAuditSink struct {
	log *slog.Logger
}

func (s *slogAuditSink) Record(r AuditRecord) {
	lvl := slog.LevelWarn
	if r.Allowed {
		lvl = slog.LevelDebug
	}
	s.log.Log(context.Background(), lvl, "egress connection decision",
		"project_id", r.ProjectID,
		"app_id", r.AppID,
		"machine_id", r.MachineID,
		"execution_id", r.ExecutionID,
		"policy_generation", r.PolicyGeneration,
		"protocol", r.Protocol,
		"port", r.Port,
		"host", r.Host,
		"resolved_ip", r.ResolvedIP,
		"allowed", r.Allowed,
		"match_type", r.MatchType,
		"reason", r.Reason)
}

// Entry 是 proxy 注册的 per-execution 策略与统计。
type Entry struct {
	MachineID   string
	ExecutionID string
	ProjectID   string
	AppID       string
	Policy      *Policy

	active      atomic.Int64
	allowed     atomic.Uint64
	denied      atomic.Uint64
	limitReject atomic.Uint64

	mu     sync.Mutex
	bucket map[string]*bucketStat
}

type bucketStat struct {
	protocol string
	port     uint32
	denied   uint64
}

// Proxy 是透明 egress 代理：两个监听端口（80→HTTP Host、443→TLS SNI）。
type Proxy struct {
	port80 int
	port43 int

	resolver *Resolver
	reserved *ReservedChecker
	sink     AuditSink

	mu      sync.RWMutex
	byIP    map[netip.Addr]*Entry // guest IP → entry
	byMach  map[string]*Entry     // machineID → entry
	entries []*Entry

	listeners []net.Listener
	closed    atomic.Bool
}

// NewProxy 构造代理。resolver/reserved 不能为 nil；sink nil 时退化为 slog。
func NewProxy(port80, port443 int, resolver *Resolver, reserved *ReservedChecker, sink AuditSink) *Proxy {
	if sink == nil {
		sink = &slogAuditSink{log: slog.Default()}
	}
	return &Proxy{
		port80: port80, port43: port443, resolver: resolver, reserved: reserved, sink: sink,
		byIP: map[netip.Addr]*Entry{}, byMach: map[string]*Entry{},
	}
}

// Start 启动两个监听（0.0.0.0）。失败返回 error（agentd 按能力缺失处理）。
func (p *Proxy) Start(ctx context.Context) error {
	var err error
	p.listeners, err = p.listen(ctx, p.port80, 0)
	if err != nil {
		return err
	}
	l43, err := p.listen(ctx, p.port43, 1)
	if err != nil {
		_ = p.Shutdown(ctx)
		return err
	}
	p.listeners = append(p.listeners, l43...)
	return nil
}

func (p *Proxy) listen(ctx context.Context, port, idx int) ([]net.Listener, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("egress proxy listen :%d: %w", port, err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if p.closed.Load() {
					return
				}
				slog.Warn("egress proxy accept", "port", port, "error", err)
				continue
			}
			go p.handle(ctx, conn, idx)
		}
	}()
	return []net.Listener{ln}, nil
}

// Shutdown 关闭监听（连接由 ctx 取消收敛）。
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.closed.Store(true)
	for _, ln := range p.listeners {
		_ = ln.Close()
	}
	return nil
}

type stagedEntry struct {
	entry   *Entry
	guestIP netip.Addr
}

// stage validates a complete proxy generation without making it visible.
func (p *Proxy) stage(machineID, executionID, projectID, appID, guestIP string, policy *Policy) (*stagedEntry, error) {
	if policy == nil {
		return nil, errors.New("egress stage: nil policy")
	}
	var addr netip.Addr
	var err error
	if guestIP != "" {
		addr, err = netip.ParseAddr(guestIP)
		if err != nil {
			return nil, fmt.Errorf("egress bind ip %q: %w", guestIP, err)
		}
		addr = addr.Unmap()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if existing := p.byMach[machineID]; existing != nil && existing.ExecutionID == executionID && existing.Policy.Generation > policy.Generation {
		return nil, fmt.Errorf("egress policy generation fencing: machine %s has newer policy generation %d, reject %d",
			machineID, existing.Policy.Generation, policy.Generation)
	}
	if old := p.byIP[addr]; addr.IsValid() && old != nil && old.MachineID != machineID {
		return nil, fmt.Errorf("egress bind ip: guest ip %s already bound to machine %s", guestIP, old.MachineID)
	}
	return &stagedEntry{entry: &Entry{MachineID: machineID, ExecutionID: executionID,
		ProjectID: projectID, AppID: appID, Policy: policy, bucket: map[string]*bucketStat{}}, guestIP: addr}, nil
}

// current returns a staging-compatible snapshot of the currently published entry.
func (p *Proxy) current(machineID string) (*stagedEntry, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e := p.byMach[machineID]
	if e == nil {
		return nil, false
	}
	var guestIP netip.Addr
	for ip, cur := range p.byIP {
		if cur == e {
			guestIP = ip
			break
		}
	}
	return &stagedEntry{entry: e, guestIP: guestIP}, true
}

// swap publishes the already validated policy and IP binding in one critical section.
func (p *Proxy) swap(staged *stagedEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := staged.entry
	if existing := p.byMach[e.MachineID]; existing != nil && existing.ExecutionID == e.ExecutionID && existing.Policy.Generation > e.Policy.Generation {
		return fmt.Errorf("egress policy generation changed while staged")
	}
	if old := p.byIP[staged.guestIP]; staged.guestIP.IsValid() && old != nil && old.MachineID != e.MachineID {
		return fmt.Errorf("egress guest ip binding changed while staged")
	}
	if old := p.byMach[e.MachineID]; old != nil {
		for ip, cur := range p.byIP {
			if cur == old {
				delete(p.byIP, ip)
			}
		}
	}
	p.byMach[e.MachineID] = e
	if staged.guestIP.IsValid() {
		p.byIP[staged.guestIP] = e
	}
	p.entries = append(p.entries, e)
	return nil
}

// Register 注册/更新 machine 的 execution policy。Manager 使用 stage+swap
// 将 policy 与 IP 一次发布；直接调用时保留已有 IP 绑定。
func (p *Proxy) Register(machineID, executionID, projectID, appID string, policy *Policy) error {
	if policy == nil {
		return p.Unregister(machineID)
	}
	guestIP := ""
	p.mu.RLock()
	if existing := p.byMach[machineID]; existing != nil {
		for ip, entry := range p.byIP {
			if entry == existing {
				guestIP = ip.String()
				break
			}
		}
	}
	p.mu.RUnlock()
	staged, err := p.stage(machineID, executionID, projectID, appID, guestIP, policy)
	if err != nil {
		return err
	}
	return p.swap(staged)
}

// BindIP 绑定 guest IP → entry（slot attach 后调用）。
func (p *Proxy) BindIP(guestIP string, machineID string) error {
	addr, err := netip.ParseAddr(guestIP)
	if err != nil {
		return fmt.Errorf("egress bind ip %q: %w", guestIP, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byMach[machineID]
	if !ok {
		return fmt.Errorf("egress bind ip: machine %s not registered", machineID)
	}
	if old, ok := p.byIP[addr]; ok && old != e {
		return fmt.Errorf("egress bind ip: guest ip %s already bound to machine %s", guestIP, old.MachineID)
	}
	p.byIP[addr] = e
	return nil
}

// Unregister 移除 machine（含 guest IP 绑定）。
func (p *Proxy) Unregister(machineID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byMach[machineID]
	if !ok {
		return nil
	}
	for ip, cur := range p.byIP {
		if cur == e {
			delete(p.byIP, ip)
		}
	}
	delete(p.byMach, machineID)
	return nil
}

func (e *Entry) reset() {
	e.allowed.Store(0)
	e.denied.Store(0)
	e.limitReject.Store(0)
	e.mu.Lock()
	e.bucket = map[string]*bucketStat{}
	e.mu.Unlock()
}

func (e *Entry) recordDeny(protocol string, port uint32) {
	e.denied.Add(1)
	key := protocol + ":" + strconv.FormatUint(uint64(port), 10)
	e.mu.Lock()
	if e.bucket == nil {
		e.bucket = map[string]*bucketStat{}
	}
	if len(e.bucket) >= 32 {
		if _, ok := e.bucket[key]; !ok {
			return // 分桶上限截断（防高基数）
		}
	}
	b, ok := e.bucket[key]
	if !ok {
		b = &bucketStat{protocol: protocol, port: port}
		e.bucket[key] = b
	}
	b.denied++
	e.mu.Unlock()
}

// Stats 返回当前 execution 的审计聚合（Machine.EgressAudit 用）。
func (p *Proxy) Stats(machineID string) *pb.EgressAuditStats {
	p.mu.RLock()
	e := p.byMach[machineID]
	p.mu.RUnlock()
	if e == nil {
		return nil
	}
	e.mu.Lock()
	buckets := make([]*pb.EgressDenyBucket, 0, len(e.bucket))
	for _, b := range e.bucket {
		buckets = append(buckets, &pb.EgressDenyBucket{Protocol: b.protocol, Port: b.port, Denied: b.denied})
	}
	e.mu.Unlock()
	return &pb.EgressAuditStats{
		PolicyGeneration:   e.Policy.Generation,
		AllowedConnections: e.allowed.Load(),
		DeniedConnections:  e.denied.Load(),
		LimitRejections:    e.limitReject.Load(),
		DenyBuckets:        buckets,
	}
}

// entryForIP 返回来源 guest IP 的 entry（无 → nil）。
func (p *Proxy) entryForIP(ip net.IP) *Entry {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.byIP[addr.Unmap()]
}

// handle 处理一条代理连接。idx: 0=80(HTTP), 1=443(TLS)。
func (p *Proxy) handle(ctx context.Context, conn net.Conn, idx int) {
	defer conn.Close()
	remote := conn.RemoteAddr()
	tcpAddr, ok := remote.(*net.TCPAddr)
	if !ok {
		return
	}
	entry := p.entryForIP(tcpAddr.IP)
	if entry == nil {
		// 未注册来源（slot 未挂策略 / 清理竞态）：fail closed。
		slog.Warn("egress proxy: unregistered source", "remote", remote.String())
		return
	}

	// per-execution TCP 连接限额（ADR-0027 §8）。
	if max := entry.Policy.MaxTCPConns; max > 0 {
		if cur := entry.active.Add(1); cur > int64(max) {
			entry.active.Add(-1)
			entry.limitReject.Add(1)
			entry.recordDeny(protoLabel(idx), protoPort(idx))
			p.recordAudit(entry, AuditRecord{
				ProjectID: entry.ProjectID, AppID: entry.AppID,
				MachineID: entry.MachineID, ExecutionID: entry.ExecutionID,
				PolicyGeneration: entry.Policy.Generation,
				Protocol:         protoLabel(idx), Port: protoPort(idx),
				Allowed: false, MatchType: "connection_limit",
				Reason: fmt.Sprintf("per-execution tcp connection limit %d reached", max),
			})
			return
		}
		defer entry.active.Add(-1)
	}

	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	br := bufio.NewReader(conn)
	decision := Decision{Allow: false, MatchType: "mode_default", Reason: "fail closed"}
	record := AuditRecord{
		ProjectID: entry.ProjectID, AppID: entry.AppID,
		MachineID: entry.MachineID, ExecutionID: entry.ExecutionID,
		PolicyGeneration: entry.Policy.Generation,
		Protocol:         protoLabel(idx), Port: protoPort(idx),
	}
	var (
		prefix   []byte
		upstream net.Conn
	)
	switch idx {
	case 0:
		peek, err := PeekHTTPHost(br)
		if err != nil && !errors.Is(err, ErrNoHostInfo) {
			record.Reason = "read http request: " + err.Error()
			p.finishDeny(entry, record, decision, conn)
			return
		}
		record.Host = peek.Host
		prefix = peek.Prefix
		upstream, decision = p.dialFor(entry.Policy, peek.Host, protoPort(idx), peek.Prefix)
	case 1:
		peek, err := PeekTLSSNI(br)
		if err != nil && !errors.Is(err, ErrNoHostInfo) {
			record.Reason = "read tls clienthello: " + err.Error()
			p.finishDeny(entry, record, decision, conn)
			return
		}
		record.Host = peek.Host
		prefix = peek.Prefix
		upstream, decision = p.dialFor(entry.Policy, peek.Host, protoPort(idx), peek.Prefix)
	}
	_ = conn.SetDeadline(time.Time{})

	record.ResolvedIP = resolvedIPOf(upstream)
	record.MatchType = decision.MatchType
	record.Reason = decision.Reason
	record.Allowed = decision.Allow

	if !decision.Allow {
		entry.recordDeny(protoLabel(idx), protoPort(idx))
		p.recordAudit(entry, record)
		return
	}
	if upstream == nil {
		record.Allowed = false
		entry.recordDeny(protoLabel(idx), protoPort(idx))
		p.recordAudit(entry, record)
		return
	}
	defer upstream.Close()
	entry.allowed.Add(1)
	p.recordAudit(entry, record)
	_ = relay(upstream, conn, br, prefix)
}

func (p *Proxy) finishDeny(entry *Entry, record AuditRecord, decision Decision, conn net.Conn) {
	record.MatchType = decision.MatchType
	record.Reason = decision.Reason
	record.Allowed = false
	entry.recordDeny(record.Protocol, record.Port)
	p.recordAudit(entry, record)
}

// recordAudit implements audit_mode: denials are always emitted; allowed
// decisions are emitted only when audit_all is enabled. nft-only traffic
// (non-80/443 TCP and UDP) cannot carry Host/SNI and is enforced/counted only
// by kernel rules; nft has no safe per-connection userspace event delivery here.
func (p *Proxy) recordAudit(entry *Entry, record AuditRecord) {
	if record.Allowed && !entry.Policy.AuditAll {
		return
	}
	p.sink.Record(record)
}

// protoLabel / protoPort 把代理端口映射为协议标签与目标端口。
func protoLabel(idx int) string {
	if idx == 0 {
		return "http"
	}
	return "tls"
}

func protoPort(idx int) uint32 {
	if idx == 0 {
		return 80
	}
	return 443
}

// dialFor 是 80/443 判定 + 连接的核心：解析（有 host）、CIDR/域名矩阵、
// 保留段过滤、dial resolved 集合。无 host（ECH/无 SNI/非 HTTP）时只按
// 策略矩阵判定：allowlist/deny_all 默认拒绝（no_host）。
func (p *Proxy) dialFor(policy *Policy, host string, port uint32, _ []byte) (net.Conn, Decision) {
	if host == "" {
		decision := policy.DecideForProxied("", nil)
		return nil, decision
	}
	addrs, err := p.resolver.Resolve(context.Background(), host)
	if err != nil {
		return nil, Decision{Allow: false, MatchType: "resolution_failed",
			Reason: "trusted resolution failed: " + err.Error()}
	}
	public := p.reserved.FilterPublic(addrs)
	if len(public) == 0 {
		return nil, Decision{Allow: false, MatchType: "reserved",
			Reason: "all resolved addresses are in reserved segments"}
	}
	decision := policy.DecideForProxied(host, public)
	if !decision.Allow {
		return nil, decision
	}
	conn, err := dialFirst(public, port)
	if err != nil {
		return nil, Decision{Allow: false, MatchType: "dial_failed",
			Reason: "connect to resolved set failed: " + err.Error()}
	}
	decision.Reason = decision.Reason + " (resolved " + conn.RemoteAddr().(*net.TCPAddr).IP.String() + ")"
	return conn, decision
}

// dialFirst 依次尝试 resolved 集合，返回首个成功连接。当前 slot 数据面仅
// 配置 IPv4；优先尝试 IPv4，避免 DNS 返回可解析但本机不可达的 AAAA 时，
// 单次 IPv6 connect timeout 吃掉整个代理请求预算。
func dialFirst(addrs []netip.Addr, port uint32) (net.Conn, error) {
	ordered := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		if a.Is4() {
			ordered = append(ordered, a)
		}
	}
	for _, a := range addrs {
		if !a.Is4() {
			ordered = append(ordered, a)
		}
	}
	var lastErr error
	for _, a := range ordered {
		target := net.JoinHostPort(a.String(), strconv.FormatUint(uint64(port), 10))
		d := net.Dialer{Timeout: dialTimeout}
		conn, err := d.Dial("tcp", target)
		if err != nil {
			lastErr = err
			continue
		}
		return conn, nil
	}
	if lastErr == nil {
		lastErr = errors.New("empty resolved set")
	}
	return nil, lastErr
}

// resolvedIPOf 提取已建立上游连接的远端 IP（审计用；nil conn → ""）。
func resolvedIPOf(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return conn.RemoteAddr().String()
	}
	return tcpAddr.IP.String()
}

// relay 双向拷贝：先回放 prefix 到 upstream，再双向流。
func relay(upstream, downstream net.Conn, br *bufio.Reader, prefix []byte) error {
	if len(prefix) > 0 {
		if _, err := upstream.Write(prefix); err != nil {
			return err
		}
	}
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstream, br)
		if tc, ok := upstream.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		errc <- err
	}()
	go func() {
		_, err := io.Copy(downstream, upstream)
		if tc, ok := downstream.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		errc <- err
	}()
	return <-errc
}
