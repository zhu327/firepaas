// Package egress 是 agent 的 v1.3-A（ADR-0027）出站策略执行层。
//
// 职责：
//   - 解析/归一 EgressPolicySpec（mode、CIDR、域名矩阵）；
//   - 透明 TCP 代理执行 TCP/80（HTTP Host）与 TCP/443（TLS ClientHello SNI）
//     的域名决策；不做 TLS 解密；
//   - 可信 resolver 解析规范化 Host/SNI（完整 CNAME 链、TTL 上限缓存），
//     连接前拒绝 loopback/link-local/metadata/私网/平台保留段；
//   - per-execution TCP 连接限额；
//   - 连接决策审计（结构化 sink；不含 URL path/query/header/body）。
//
// nftables 强制由 internal/agent/network/slot 完成：slot 把 80/443 重定向到
// 本包 Proxy 的监听端口，其余 TCP/UDP 由 CIDR 规则与模式默认拒绝执行。
package egress

import (
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Mode 与 proto EgressPolicySpec.Mode 对齐。
type Mode int

const (
	ModeUnrestricted Mode = iota
	ModeDenyAll
	ModeAllowlist
)

// DomainEntry 是归一后的域名条目（exact 或单层 wildcard 后缀）。
type DomainEntry struct {
	exact          string // 无通配；IP 字面量也在 exact
	wildcardSuffix string // "*.example.com" → ".example.com"
}

func (d DomainEntry) matches(host string) bool {
	if d.exact != "" {
		return host == d.exact
	}
	if !strings.HasSuffix(host, d.wildcardSuffix) {
		return false
	}
	prefix := strings.TrimSuffix(host, d.wildcardSuffix)
	return prefix != "" && !strings.Contains(prefix, ".")
}

// Policy 是归一化后的 egress 策略（纯数据 + 判定，不依赖内核）。
type Policy struct {
	Mode            Mode
	AllowedCIDRs    []netip.Prefix
	DeniedCIDRs     []netip.Prefix
	AllowedDomains  []DomainEntry
	MaxTCPConns     uint32 // 0 = 不限
	Generation      uint64
	AuditAll        bool
	DomainProtected bool // 存在域名规则（决定 80/443 是否走代理判定）
}

// FromProto 解析并归一 proto 策略。返回 error 时 p 为 nil。
func FromProto(p *pb.EgressPolicySpec) (*Policy, error) {
	if p == nil {
		return nil, nil
	}
	var out Policy
	switch p.GetMode() {
	case pb.EgressPolicySpec_MODE_UNSPECIFIED, pb.EgressPolicySpec_UNRESTRICTED:
		out.Mode = ModeUnrestricted
	case pb.EgressPolicySpec_DENY_ALL:
		out.Mode = ModeDenyAll
	case pb.EgressPolicySpec_ALLOWLIST:
		out.Mode = ModeAllowlist
	default:
		return nil, fmt.Errorf("invalid egress mode %d", p.GetMode())
	}
	for i, raw := range p.GetAllowedCidrs() {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("egress allowed_cidrs[%d] %q: %w", i, raw, err)
		}
		if prefix.Addr().Is6() {
			return nil, fmt.Errorf("egress allowed_cidrs[%d] %q: IPv6 egress is not supported", i, raw)
		}
		out.AllowedCIDRs = append(out.AllowedCIDRs, prefix.Masked())
	}
	for i, raw := range p.GetDeniedCidrs() {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("egress denied_cidrs[%d] %q: %w", i, raw, err)
		}
		if prefix.Addr().Is6() {
			return nil, fmt.Errorf("egress denied_cidrs[%d] %q: IPv6 egress is not supported", i, raw)
		}
		out.DeniedCIDRs = append(out.DeniedCIDRs, prefix.Masked())
	}
	for i, raw := range p.GetAllowedDomains() {
		normalized, err := normalizeDomain(raw)
		if err != nil {
			return nil, fmt.Errorf("egress allowed_domains[%d]: %w", i, err)
		}
		if strings.HasPrefix(normalized, "*.") {
			out.AllowedDomains = append(out.AllowedDomains,
				DomainEntry{wildcardSuffix: "." + strings.TrimPrefix(normalized, "*.")})
		} else {
			out.AllowedDomains = append(out.AllowedDomains, DomainEntry{exact: normalized})
		}
	}
	out.MaxTCPConns = p.GetMaxTcpConnections()
	out.Generation = p.GetPolicyGeneration()
	out.AuditAll = p.GetAuditAll()
	out.DomainProtected = len(out.AllowedDomains) > 0
	if p.GetPolicyGeneration() == 0 {
		return nil, errors.New("egress policy_generation must be > 0")
	}
	sort.Slice(
		out.AllowedCIDRs,
		func(i, j int) bool { return out.AllowedCIDRs[i].String() < out.AllowedCIDRs[j].String() },
	)
	sort.Slice(
		out.DeniedCIDRs,
		func(i, j int) bool { return out.DeniedCIDRs[i].String() < out.DeniedCIDRs[j].String() },
	)
	return &out, nil
}

func normalizeDomain(raw string) (string, error) {
	d := strings.TrimSpace(strings.ToLower(raw))
	d = strings.TrimSuffix(d, ".")
	if d == "" {
		return "", errors.New("domain must be non-empty")
	}
	if strings.ContainsAny(d, "/?#:@") {
		return "", fmt.Errorf("domain %q must not contain scheme/path/port", raw)
	}
	if strings.HasPrefix(d, "*.") {
		suffix := strings.TrimPrefix(d, "*.")
		if suffix == "" || strings.Contains(suffix, "*") {
			return "", fmt.Errorf("wildcard domain %q must be *.example.com (single label)", raw)
		}
		if !validDomain(suffix) {
			return "", fmt.Errorf("wildcard domain %q has invalid suffix", raw)
		}
		return "*." + suffix, nil
	}
	if strings.Contains(d, "*") {
		return "", fmt.Errorf("domain %q has unsupported wildcard form", raw)
	}
	if addr, err := netip.ParseAddr(d); err == nil && addr.IsValid() {
		if addr.Is6() {
			return "", fmt.Errorf("domain %q: IPv6 egress is not supported", raw)
		}
		return d, nil
	}
	if !validDomain(d) {
		return "", fmt.Errorf("domain %q is not a valid hostname", raw)
	}
	return d, nil
}

func validDomain(d string) bool {
	if d == "" || len(d) > 253 {
		return false
	}
	for _, label := range strings.Split(d, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for i, c := range label {
			ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
			if !ok || (i == 0 || i == len(label)-1) && c == '-' {
				return false
			}
		}
	}
	return true
}

// NormalizeHost 归一 Host/SNI 字符串（小写、去尾点、剥 [] 与端口）。
func NormalizeHost(raw string) string {
	h := strings.TrimSpace(raw)
	// 括号 IPv6 字面量：[::1]:8080 → ::1。
	if strings.HasPrefix(h, "[") {
		if i := strings.Index(h, "]"); i > 0 {
			inner := h[1:i]
			rest := h[i+1:]
			if rest == "" || strings.HasPrefix(rest, ":") {
				h = inner
			} else {
				return strings.ToLower(inner)
			}
		}
	}
	// host:port（IPv4 或裸域名）。
	if i := strings.LastIndex(h, ":"); i > 0 && strings.Count(h, ":") == 1 {
		h = h[:i]
	}
	h = strings.TrimSuffix(h, ".")
	return strings.ToLower(h)
}

// matchesDomain 判断 host 是否命中任意域名条目（大小写不敏感；host 需先归一）。
func (p *Policy) matchesDomain(host string) bool {
	host = NormalizeHost(host)
	for _, d := range p.AllowedDomains {
		if d.matches(host) {
			return true
		}
	}
	return false
}

// containsCIDR 判断 addr 是否落在集合内。
func containsCIDR(list []netip.Prefix, addr netip.Addr) bool {
	u := addr.Unmap()
	for _, prefix := range list {
		if prefix.Contains(u) {
			return true
		}
	}
	return false
}

// Decision 是一次连接判定结果（审计用）。
type Decision struct {
	Allow     bool
	MatchType string // cidr_allowed | cidr_denied | domain | mode_default | no_host | reserved
	Reason    string
}

// DecideForProxied 判定经透明代理的 80/443 连接。host 空 = 无 Host/SNI
// （ECH/非标准请求）；resolved 是本连接可信解析得到的 A/AAAA 集合（可为空）。
// 调用方保证已对 resolved 逐项做过保留段检查；这里只负责策略矩阵。
// 空 resolved = fail closed（ADR-0027：解析失败/集合为空必须拒绝）。
func (p *Policy) DecideForProxied(host string, resolved []netip.Addr) Decision {
	host = NormalizeHost(host)
	denied := func() bool {
		for _, a := range resolved {
			if containsCIDR(p.DeniedCIDRs, a) {
				return true
			}
		}
		return false
	}
	allowedByCIDR := false
	for _, a := range resolved {
		if containsCIDR(p.AllowedCIDRs, a) {
			allowedByCIDR = true
			break
		}
	}
	switch p.Mode {
	case ModeUnrestricted:
		if denied() {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "resolved address in denied_cidrs"}
		}
		return Decision{Allow: true, MatchType: "mode_default", Reason: "unrestricted mode"}
	case ModeDenyAll:
		if denied() {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "resolved address in denied_cidrs"}
		}
		if allowedByCIDR {
			return Decision{Allow: true, MatchType: "cidr_allowed", Reason: "resolved address in allowed_cidrs"}
		}
		return Decision{Allow: false, MatchType: "mode_default", Reason: "deny_all mode"}
	case ModeAllowlist:
		if denied() {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "resolved address in denied_cidrs"}
		}
		if allowedByCIDR {
			return Decision{Allow: true, MatchType: "cidr_allowed", Reason: "resolved address in allowed_cidrs"}
		}
		if host == "" {
			// 无 Host/SNI/ECH：域名规则无法保护，allowlist-only 默认拒绝。
			return Decision{
				Allow:     false,
				MatchType: "no_host",
				Reason:    "no host/sni to authorize (allowlist default deny)",
			}
		}
		if len(resolved) == 0 {
			// 解析失败/集合为空：fail closed，不回退 guest DNS。
			return Decision{Allow: false, MatchType: "mode_default", Reason: "empty resolved set (fail closed)"}
		}
		if p.matchesDomain(host) {
			return Decision{Allow: true, MatchType: "domain", Reason: "host matches allowed_domains"}
		}
		return Decision{Allow: false, MatchType: "mode_default", Reason: "host not in allowed_domains"}
	}
	return Decision{Allow: false, MatchType: "mode_default", Reason: "unknown mode"}
}

// DecideByCIDR 判定非 80/443 TCP 与 UDP（只按 CIDR 矩阵；deny_all/allowlist
// 默认拒绝，unrestricted 默认放行）。audit 用。
func (p *Policy) DecideByCIDR(addr netip.Addr) Decision {
	switch p.Mode {
	case ModeUnrestricted:
		if containsCIDR(p.DeniedCIDRs, addr) {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "destination in denied_cidrs"}
		}
		return Decision{Allow: true, MatchType: "mode_default", Reason: "unrestricted mode"}
	case ModeDenyAll:
		if containsCIDR(p.DeniedCIDRs, addr) {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "destination in denied_cidrs"}
		}
		if containsCIDR(p.AllowedCIDRs, addr) {
			return Decision{Allow: true, MatchType: "cidr_allowed", Reason: "destination in allowed_cidrs"}
		}
		return Decision{Allow: false, MatchType: "mode_default", Reason: "deny_all mode"}
	case ModeAllowlist:
		if containsCIDR(p.DeniedCIDRs, addr) {
			return Decision{Allow: false, MatchType: "cidr_denied", Reason: "destination in denied_cidrs"}
		}
		if containsCIDR(p.AllowedCIDRs, addr) {
			return Decision{Allow: true, MatchType: "cidr_allowed", Reason: "destination in allowed_cidrs"}
		}
		return Decision{
			Allow:     false,
			MatchType: "mode_default",
			Reason:    "allowlist default deny (no host/sni protection)",
		}
	}
	return Decision{Allow: false, MatchType: "mode_default", Reason: "unknown mode"}
}

// ProxyNeeded 判断该策略是否需要透明代理：只有 allowlist 模式携带域名规则
// 时 80/443 才需要经代理判定（unrestricted/deny_all 只按 CIDR 矩阵）。
func (p *Policy) ProxyNeeded() bool {
	return p.Mode == ModeAllowlist && p.DomainProtected
}

// ModeString 返回模式字符串（slot 规则集持久化用）。
func (p *Policy) ModeString() string {
	switch p.Mode {
	case ModeDenyAll:
		return "deny_all"
	case ModeAllowlist:
		return "allowlist"
	default:
		return "unrestricted"
	}
}

// CIDRStrings / DomainStrings 返回归一化后的声明（slot 持久化/审计用）。
func (p *Policy) CIDRStrings() (allowed, denied []string) {
	for _, prefix := range p.AllowedCIDRs {
		allowed = append(allowed, prefix.String())
	}
	for _, prefix := range p.DeniedCIDRs {
		denied = append(denied, prefix.String())
	}
	return allowed, denied
}

// DomainStrings 返回归一化域名条目。
func (p *Policy) DomainStrings() []string {
	out := make([]string, 0, len(p.AllowedDomains))
	for _, d := range p.AllowedDomains {
		if d.exact != "" {
			out = append(out, d.exact)
		} else {
			out = append(out, "*"+d.wildcardSuffix)
		}
	}
	sort.Strings(out)
	return out
}

// UnmarshalProto 解析 protojson 形式的策略（控制面固化、slot 持久化与测试用）。
func UnmarshalProto(raw []byte) (*Policy, error) {
	var spec pb.EgressPolicySpec
	if err := protojson.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}
	return FromProto(&spec)
}

// FromRuleSet（v1.3-A 重启恢复）：从 slot 持久化的规则集重建 Policy。
// 只用于在役 machine 的幂等重放；生成无效时返回 error（fail closed）。
func FromRuleSet(rs slot.EgressRuleSet) (*Policy, error) {
	if rs.Mode == "" {
		return nil, nil
	}
	var spec pb.EgressPolicySpec
	switch rs.Mode {
	case "unrestricted":
		spec.Mode = pb.EgressPolicySpec_UNRESTRICTED
	case "deny_all":
		spec.Mode = pb.EgressPolicySpec_DENY_ALL
	case "allowlist":
		spec.Mode = pb.EgressPolicySpec_ALLOWLIST
	default:
		return nil, fmt.Errorf("egress restore: unknown mode %q", rs.Mode)
	}
	spec.AllowedCidrs = append([]string(nil), rs.AllowedCIDRs...)
	spec.DeniedCidrs = append([]string(nil), rs.DeniedCIDRs...)
	spec.AllowedDomains = append([]string(nil), rs.Domains...)
	spec.MaxTcpConnections = rs.MaxTCPConns
	spec.PolicyGeneration = rs.Generation
	spec.AuditAll = rs.AuditAll
	return FromProto(&spec)
}

// sortedIPs 去重排序 resolved 集合（确定性 dial 顺序）。
func sortedIPs(addrs []netip.Addr) []netip.Addr {
	seen := map[netip.Addr]bool{}
	out := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		u := a.Unmap()
		if !u.IsValid() || seen[u] {
			continue
		}
		seen[u] = true
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out
}
