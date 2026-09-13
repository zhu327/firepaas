// nft.go：slot 数据面的 nftables 后端（现役，ADR-0040 §12 fallback）。
//
// 结构从 slot.go/egress.go 平移（M3/v1.3-A 语义原样保留）：
//   - root ns fp-isolation 表（ip+ip6）：INPUT 只放行 established/related 与
//     egress 代理端口；FORWARD 放行 established、drop 私网/保留目标；
//     POSTROUTING masquerade VethRange（二级 NAT 出口半段）；
//   - slot netns fp-slot 表：egress masquerade + 策略快照全量替换
//     （deny → allow → mode 默认；redirect 在 prerouting DNAT）。
package slot

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os/exec"
	"strings"

	"github.com/zhu327/firepaas/internal/agent/netpolicy"
	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// NftBackend 实现 Backend（nftables）。vethCIDR 用于 POSTROUTING
// masquerade 源段（默认 10.12.0.0/16；同主机多 agent 时随 Config.VethCIDR
// 隔离）。注意：root 级 fp-isolation 表名全局共享，同主机双 agent 必须
// 双双使用 eBPF 后端（nft fallback 仅单 agent 主机支持）。
type NftBackend struct {
	port80   int
	port81   int
	vethCIDR string
}

// NewNftBackend 构造 nft 后端（端口 0 时用默认 18080/18443；vethCIDR
// 空串 = 默认 VethRange）。
func NewNftBackend(port80, port81 int, vethCIDR string) *NftBackend {
	if port80 == 0 {
		port80 = 18080
	}
	if port81 == 0 {
		port81 = 18443
	}
	if vethCIDR == "" {
		vethCIDR = VethRange
	}
	return &NftBackend{port80: port80, port81: port81, vethCIDR: vethCIDR}
}

var _ Backend = (*NftBackend)(nil)

// PrivateCIDRs 返回 canonical IPv4 保留段文本集（netpolicy 同源；
// eBPF 后端 private4 LPM 与 nft 规则文本共用）。
func PrivateCIDRs() []string {
	var out []string
	for _, p := range netpolicy.Prefixes() {
		if p.Addr().Is4() {
			out = append(out, p.String())
		}
	}
	return out
}

// EnsureNode 幂等创建 root 侧隔离表（ip+ip6）。
func (b *NftBackend) EnsureNode(ctx context.Context) error {
	if err := exec.Command("nft", "list", "table", "ip", "fp-isolation").Run(); err != nil {
		for _, args := range nftIsolationStepsIPv4(b.port80, b.port81, b.vethCIDR) {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: nft setup (%s): %w", strings.Join(args, " "), err)
			}
		}
	}
	if err := exec.Command("nft", "list", "table", "ip6", "fp-isolation").Run(); err != nil {
		for _, args := range ip6IsolationSteps() {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: nft6 setup (%s): %w", strings.Join(args, " "), err)
			}
		}
	}
	return nil
}

// AttachSlot 把 veth 加入隔离集合（两个 family）并补齐代理 INPUT accept。
func (b *NftBackend) AttachSlot(ctx context.Context, ref SlotRef) error {
	if err := b.EnsureNode(ctx); err != nil {
		return err
	}
	for _, family := range []string{"ip", "ip6"} {
		out, err := exec.Command("nft", "add", "element", family, "fp-isolation", "slot-veths",
			"{", ref.VethHost, "}").CombinedOutput()
		if err != nil && !strings.Contains(string(out), "exists") {
			return fmt.Errorf("slot: nft add veth %s (%s): %w (%s)",
				ref.VethHost, family, err, strings.TrimSpace(string(out)))
		}
	}
	return b.ensureEgressProxyInputRule(ctx)
}

// DetachSlot 摘除集合元素（best-effort）。
func (b *NftBackend) DetachSlot(ctx context.Context, ref SlotRef) error {
	_ = exec.Command("nft", "delete", "element", "ip", "fp-isolation", "slot-veths",
		"{", ref.VethHost, "}").Run()
	_ = exec.Command("nft", "delete", "element", "ip6", "fp-isolation", "slot-veths",
		"{", ref.VethHost, "}").Run()
	return nil
}

// EnsureSlotNAT 幂等创建 slot 内一级 NAT（出口 masquerade）。
func (b *NftBackend) EnsureSlotNAT(ctx context.Context, ref SlotRef) error {
	return EnsureNetnsNAT(ctx, ref, 0, 0)
}

// ApplyEgress 全量替换策略快照（snap nil = 清除）。
func (b *NftBackend) ApplyEgress(ctx context.Context, ref SlotRef, snap *api.PolicySnapshot) error {
	if snap == nil {
		return clearSlotEgress(ctx, ref)
	}
	return ensureSlotEgress(ctx, ref, *snap)
}

// ensureSlotEgress replaces the complete fp-slot table in one nft transaction.
// nft validates the whole batch before committing it, so a syntax/runtime failure
// leaves the previous policy and NAT rules active.
func ensureSlotEgress(ctx context.Context, ref SlotRef, snap api.PolicySnapshot) error {
	script, err := egressTableScript(ref, &snap)
	if err != nil {
		return err
	}
	if err := runNftBatch(ctx, ref.Netns, script); err != nil {
		return fmt.Errorf("slot: atomically replace egress policy: %w", err)
	}
	return nil
}

func clearSlotEgress(ctx context.Context, ref SlotRef) error {
	script, err := egressTableScript(ref, nil)
	if err != nil {
		return err
	}
	if err := runNftBatch(ctx, ref.Netns, script); err != nil {
		return fmt.Errorf("slot: atomically clear egress policy: %w", err)
	}
	return nil
}

func egressTableScript(ref SlotRef, snap *api.PolicySnapshot) (string, error) {
	hostAddr, vethGuest := ref.HostAddr, ref.VethGuest
	if hostAddr == "" || vethGuest == "" {
		return "", fmt.Errorf("slot: egress table: incomplete ref %+v", ref)
	}
	var b strings.Builder
	b.WriteString("delete table ip fp-slot\n")
	b.WriteString("add table ip fp-slot\n")
	b.WriteString("add chain ip fp-slot post { type nat hook postrouting priority srcnat; policy accept; }\n")
	fmt.Fprintf(&b, "add rule ip fp-slot post oifname %q ip daddr != %s masquerade\n", vethGuest, hostAddr)
	if snap == nil {
		return b.String(), nil
	}
	b.WriteString("add set ip fp-slot egress-allow4 { type ipv4_addr; flags interval; }\n")
	b.WriteString("add set ip fp-slot egress-deny4 { type ipv4_addr; flags interval; }\n")
	if len(snap.AllowedCIDRs) > 0 {
		fmt.Fprintf(&b, "add element ip fp-slot egress-allow4 { %s }\n", strings.Join(snap.AllowedCIDRs, ", "))
	}
	if len(snap.DeniedCIDRs) > 0 {
		fmt.Fprintf(&b, "add element ip fp-slot egress-deny4 { %s }\n", strings.Join(snap.DeniedCIDRs, ", "))
	}
	b.WriteString("add chain ip fp-slot egress-pre { type nat hook prerouting priority dstnat; policy accept; }\n")
	b.WriteString("add chain ip fp-slot egress-fwd { type filter hook forward priority filter; policy accept; }\n")
	if snap.ProxyPort80 > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot egress-pre %s\n", proxyDNATRule(vethGuest, hostAddr, 80, snap.ProxyPort80))
	}
	if snap.ProxyPort443 > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot egress-pre %s\n", proxyDNATRule(vethGuest, hostAddr, 443, snap.ProxyPort443))
	}
	b.WriteString("add rule ip fp-slot egress-fwd ct state established,related accept\n")
	// The limit precedes every new-connection accept, including allowed CIDRs and
	// proxy DNAT, so neither path can bypass the per-execution cap.
	if snap.MaxTCPConns > 0 {
		fmt.Fprintf(
			&b,
			"add rule ip fp-slot egress-fwd meta l4proto tcp ct state new ct count over %d counter drop comment \"firepaas-tcp-limit\"\n",
			snap.MaxTCPConns,
		)
	}
	fmt.Fprintf(&b, "add rule ip fp-slot egress-fwd ip daddr %s accept\n", hostAddr)
	b.WriteString("add rule ip fp-slot egress-fwd ip daddr @egress-deny4 drop\n")
	b.WriteString("add rule ip fp-slot egress-fwd ip daddr @egress-allow4 accept\n")
	if snap.Mode == "deny_all" || snap.Mode == "allowlist" {
		b.WriteString("add rule ip fp-slot egress-fwd drop\n")
	}
	return b.String(), nil
}

var runNftBatch = func(ctx context.Context, ns, script string) error {
	cmd := exec.CommandContext(ctx, "ip", "netns", "exec", ns, "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(script)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// nftIsolationStepsIPv4 生成 fp-isolation（ip family）的幂等建表步骤。
// 私网目标集合来自 netpolicy 的 canonical 集合（Go matcher 与规则文本同源）。
func nftIsolationStepsIPv4(port80, port81 int, vethCIDR string) [][]string {
	return [][]string{
		{"nft", "add", "table", "ip", "fp-isolation"},
		{"nft", "add", "set", "ip", "fp-isolation", "slot-veths", "{", "type", "ifname;", "}"},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "in",
			"{", "type", "filter", "hook", "input", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "fwdchain",
			"{", "type", "filter", "hook", "forward", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "post",
			"{", "type", "nat", "hook", "postrouting", "priority", "srcnat;", "}",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths",
			"ct", "state", "established,related", "accept",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths",
			"tcp", "dport", "{", fmt.Sprintf("%d,%d", port80, port81), "}", "accept",
		},
		{"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths", "drop"},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths",
			"ct", "state", "established,related", "accept",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths",
			"ip", "daddr", netpolicy.IPv4NftSetText(), "drop",
		},
		{"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "accept"},
		{"nft", "add", "rule", "ip", "fp-isolation", "post", "ip", "saddr", vethCIDR, "masquerade"},
	}
}

func ip6IsolationSteps() [][]string {
	return [][]string{
		{"nft", "add", "table", "ip6", "fp-isolation"},
		{"nft", "add", "set", "ip6", "fp-isolation", "slot-veths", "{", "type", "ifname;", "}"},
		{
			"nft", "add", "chain", "ip6", "fp-isolation", "in",
			"{", "type", "filter", "hook", "input", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip6", "fp-isolation", "fwdchain",
			"{", "type", "filter", "hook", "forward", "priority", "filter;", "}",
		},
		// NDP（RS/RA/NS/NA/redirect）先行放行：v6 邻居解析是 slot v6 接线
		//（网关 fe80::1）与 mesh 链路的前提，无此前 nft fallback 下 slot 发的
		// NS 被默认 drop 杀死， NDP 恒 FAILED（真机 G2c 验收抓到）。
		{
			"nft", "add", "rule", "ip6", "fp-isolation", "in", "iifname", "@slot-veths",
			"icmpv6", "type", "{", "nd-router-solicit,", "nd-router-advert,",
			"nd-neighbor-solicit,", "nd-neighbor-advert,", "nd-redirect", "}", "accept",
		},
		{"nft", "add", "rule", "ip6", "fp-isolation", "in", "iifname", "@slot-veths", "drop"},
		{"nft", "add", "rule", "ip6", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "drop"},
	}
}

// ensureEgressProxyInputRule 幂等保证 INPUT 链中存在 slot→egress 代理端口的
// accept 规则（位于 drop 规则之前）。nft 的 position 参数语义是**handle**而非
// 序号，因此先带 handle 列出链，定位 drop 规则后在其前插入。
func (b *NftBackend) ensureEgressProxyInputRule(ctx context.Context) error {
	marker := fmt.Sprintf("%d", b.port80)
	out, err := exec.Command("nft", "-a", "list", "chain", "ip", "fp-isolation", "in").Output()
	if err != nil {
		return fmt.Errorf("slot: list input chain: %w", err)
	}
	listing := string(out)
	if strings.Contains(listing, marker) {
		return nil
	}
	dropHandle := ""
	for _, line := range strings.Split(listing, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "@slot-veths") || !strings.HasSuffix(line, "drop") {
			continue
		}
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "handle" {
				dropHandle = fields[i+1]
			}
		}
	}
	if dropHandle == "" {
		return fmt.Errorf("slot: egress input accept: drop rule handle not found in fp-isolation")
	}
	if err := execCmd(ctx, "nft", "insert", "rule", "ip", "fp-isolation", "in", "position", dropHandle,
		"iifname", "@slot-veths", "tcp", "dport", "{",
		fmt.Sprintf("%d,%d", b.port80, b.port81), "}", "accept"); err != nil {
		return fmt.Errorf("slot: insert egress proxy input rule: %w", err)
	}
	return nil
}

// EnsureRootEgressNAT 幂等确保 root ns 存在对 vethCIDR 的出口 masquerade
// （ADR-0040 §7：南北出口仍节点 SNAT）。eBPF 后端使用：nft 后端的等价
// 规则在 fp-isolation 表的 post 链内，eBPF 后端不建该表，缺此规则时
// slot 内一级 NAT 改写后的源（链路地址）无法获得公网回程（全新 eBPF
// 节点上非代理南北向流量断流；升级节点靠旧 nft 表残留掩盖）。
//
// 多 agent 同主机：规则按 CIDR 标记判定存在性，同表各自补自己的 CIDR，
// 互不覆盖；table 默认 "fp-egress"，测试/多实例可传独立表名。
func EnsureRootEgressNAT(ctx context.Context, table, vethCIDR string) error {
	table = strings.TrimSpace(table)
	vethCIDR = strings.TrimSpace(vethCIDR)
	if table == "" || vethCIDR == "" {
		return fmt.Errorf("slot: root egress NAT needs table and veth CIDR (got %q/%q)", table, vethCIDR)
	}
	// 规范化（带主机位的输入如 10.12.5.0/16 → 10.12.0.0/16）：nft 列出的是
	// 规范化文本，存在性判定若用原串会永不命中——每次 EnsureNode（每次
	// attach/reconcile）追加一条等价规则，无界增长。
	cidr, err := normalizeVethCIDR(vethCIDR)
	if err != nil {
		return err
	}
	if err := exec.Command("nft", "list", "table", "ip", table).Run(); err != nil {
		steps := [][]string{
			{"nft", "add", "table", "ip", table},
			{
				"nft", "add", "chain", "ip", table, "post",
				"{", "type", "nat", "hook", "postrouting", "priority", "srcnat;", "policy", "accept;", "}",
			},
		}
		for _, step := range steps {
			if err := execCmd(ctx, step[0], step[1:]...); err != nil {
				return fmt.Errorf("slot: root egress NAT setup (%s): %w", strings.Join(step, " "), err)
			}
		}
	}
	out, err := exec.Command("nft", "-a", "list", "chain", "ip", table, "post").Output()
	if err != nil {
		return fmt.Errorf("slot: list root egress NAT chain: %w", err)
	}
	if strings.Contains(string(out), "ip saddr "+cidr+" masquerade") {
		return nil // 该 CIDR 的规则已存在（幂等）
	}
	if err := execCmd(ctx, "nft", "add", "rule", "ip", table, "post", "ip", "saddr", cidr, "masquerade"); err != nil {
		return fmt.Errorf("slot: add root egress NAT rule: %w", err)
	}
	return nil
}

// normalizeVethCIDR 规范化 veth 源段（Masked，仅 IPv4）。
func normalizeVethCIDR(raw string) (string, error) {
	p, err := netip.ParsePrefix(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("slot: root egress NAT veth cidr %q: %w", raw, err)
	}
	if !p.Addr().Is4() {
		return "", fmt.Errorf("slot: root egress NAT veth cidr %q must be IPv4", raw)
	}
	return p.Masked().String(), nil
}

// EnsureNetnsTCPLimit 幂等替换 slot netns 的 per-execution TCP 新连接上限
// （W2-1）：内核 conntrack 的 `meta l4proto tcp ct state new ct count over N
// counter drop`，与 nft 后端 egress-fwd 链内的规则同一口径，随 conntrack
// 超时自愈（BPF 聚合计数在去重表淘汰后会漏减，已在 tc.c 移除）。
// limit=0 = 清除规则。实现：确保 fp-slot 表与 tcp_limit 链存在，然后单事务
// flush + add（原子替换；失败保留旧规则）。
func EnsureNetnsTCPLimit(ctx context.Context, ref SlotRef, limit uint32) error {
	if ref.Netns == "" {
		return fmt.Errorf("slot: tcp limit: empty netns")
	}
	// 表/基础链不存在时补齐（与 EnsureNetnsNAT 同源，不装 DNAT）。
	if err := EnsureNetnsNAT(ctx, ref, 0, 0); err != nil {
		return err
	}
	// 回收 nft 后端遗留策略（切换/升级窗口），避免旧链与 tc 裁决叠加。
	cleanupLegacyNftPolicy(ctx, ref)
	if _, err := exec.Command("ip", "netns", "exec", ref.Netns,
		"nft", "list", "chain", "ip", "fp-slot", tcpLimitChain).CombinedOutput(); err != nil {
		if err := execCmd(ctx, "ip", "netns", "exec", ref.Netns,
			"nft", "add", "chain", "ip", "fp-slot", tcpLimitChain,
			"{", "type", "filter", "hook", "forward", "priority", "filter;", "policy", "accept;", "}"); err != nil {
			return fmt.Errorf("slot: add tcp limit chain: %w", err)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "flush chain ip fp-slot %s\n", tcpLimitChain)
	if limit > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot %s %s\n", tcpLimitChain, tcpLimitRule(limit))
	}
	if err := runNftBatch(ctx, ref.Netns, b.String()); err != nil {
		return fmt.Errorf("slot: apply tcp limit: %w", err)
	}
	return nil
}

// tcpLimitChain/tcpLimitComment：限额链名（`limit` 是 nft 关键字，不可用）
// 与规则注释标记（注释仅便于排障/审计）。
const (
	tcpLimitChain   = "tcp_limit"
	tcpLimitComment = "firepaas-tcp-limit"
)

// tcpLimitRule 是一条 per-execution 新 TCP 上限规则的规则体（不含链名）。
// nft 后端（egress-fwd 内）与 eBPF 后端（tcp_limit 链）共用同一文本，
// 避免两处手写同一条 ct count 规则再次漂移。
func tcpLimitRule(limit uint32) string {
	return fmt.Sprintf(
		"meta l4proto tcp ct state new ct count over %d counter drop comment \"%s\"",
		limit, tcpLimitComment,
	)
}

// cleanupLegacyNftPolicy 尽力删除 nft 后端遗留的 slot 内策略链/集合。
// 后端切换（ADR-0040 §12）要求重建 execution，但 agent 升级或 Reconcile
// 复用旧 netns 时，egress-fwd/egress-allow4/egress-deny4 会与 tc 裁决叠加
// （旧 CIDR/限额继续生效）。eBPF 后端在每次 ApplyEgress 前顺手回收。
func cleanupLegacyNftPolicy(ctx context.Context, ref SlotRef) {
	for _, args := range [][]string{
		{"nft", "delete", "chain", "ip", "fp-slot", "egress-fwd"},
		{"nft", "delete", "set", "ip", "fp-slot", "egress-allow4"},
		{"nft", "delete", "set", "ip", "fp-slot", "egress-deny4"},
	} {
		full := append([]string{"ip", "netns", "exec", ref.Netns}, args...)
		_, _ = exec.CommandContext(ctx, full[0], full[1:]...).CombinedOutput()
	}
}

// proxyDNATRule 是一条透明代理 DNAT 规则的规则体（不含链名前缀）。
// slot 全量替换脚本（egress-pre 链）与 EnsureNetnsNAT（pre 链）共用同一
// 文本，避免两处手写同一条 dnat 规则再次漂移（仿 tcpLimitRule）。
func proxyDNATRule(vethGuest, hostAddr string, dport, proxyPort int) string {
	return fmt.Sprintf("iifname != %q tcp dport %d ip daddr != %s dnat to %s:%d",
		vethGuest, dport, hostAddr, hostAddr, proxyPort)
}

// EnsureNetnsNAT 幂等创建 slot 内一级 NAT：出口 masquerade +（port>0 时）
// prerouting DNAT（tcp 80/443 → hostAddr:port，conntrack 反向 NAT——代理回流
// 不改写）。proxyPort 传 0 = 只建 masquerade（nft 后端在 ApplyEgress 的
// 全量替换脚本里自带 DNAT）。导出：eBPF 后端复用。
func EnsureNetnsNAT(ctx context.Context, ref SlotRef, proxyPort80, proxyPort443 int) error {
	ns := ref.Netns
	if err := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", "ip", "fp-slot").Run(); err != nil {
		steps := [][]string{
			{"ip", "netns", "exec", ns, "nft", "add", "table", "ip", "fp-slot"},
			{
				"ip",
				"netns",
				"exec",
				ns,
				"nft",
				"add",
				"chain",
				"ip",
				"fp-slot",
				"post",
				"{",
				"type",
				"nat",
				"hook",
				"postrouting",
				"priority",
				"srcnat;",
				"}",
			},
			{
				"ip",
				"netns",
				"exec",
				ns,
				"nft",
				"add",
				"rule",
				"ip",
				"fp-slot",
				"post",
				"oifname",
				ref.VethGuest,
				"ip",
				"daddr",
				"!=",
				ref.HostAddr,
				"masquerade",
			},
		}
		if proxyPort80 > 0 || proxyPort443 > 0 {
			steps = append(steps, []string{
				"ip", "netns", "exec", ns, "nft", "add", "chain", "ip", "fp-slot", "pre",
				"{", "type", "nat", "hook", "prerouting", "priority", "dstnat;", "}",
			})
			// iifname != veth：透明代理 DNAT 只捕获 guest 发起的出口流量
			//（经 bridge 进入）；host 发起的转送流（探针/代理拨号到 guest:80/443，
			// 经 veth 进入）不得被重定向回代理——否则探针/回程死循环
			//（真机 G2c 验收抓到：健康探针回包 DNAT 回代理 → 恒 NOT_READY）。
			if proxyPort80 > 0 {
				steps = append(steps, append([]string{
					"ip", "netns", "exec", ns, "nft", "add", "rule", "ip", "fp-slot", "pre",
				}, strings.Fields(proxyDNATRule(ref.VethGuest, ref.HostAddr, 80, proxyPort80))...))
			}
			if proxyPort443 > 0 {
				steps = append(steps, append([]string{
					"ip", "netns", "exec", ns, "nft", "add", "rule", "ip", "fp-slot", "pre",
				}, strings.Fields(proxyDNATRule(ref.VethGuest, ref.HostAddr, 443, proxyPort443))...))
			}
		}
		for _, step := range steps {
			if err := execCmd(ctx, step[0], step[1:]...); err != nil {
				return fmt.Errorf("slot: netns NAT setup (%s): %w", strings.Join(step, " "), err)
			}
		}
	}
	return nil
}
