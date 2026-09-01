// egress.go：v1.3-A（ADR-0027）slot 内 egress nftables 执行。
//
// 在 slot netns 的 fp-slot 表内落地：
//   - pre（nat prerouting）：TCP/80、TCP/443 且目标非本机桥网关时 DNAT 到
//     root ns 透明代理（ProxyPort80/443，0 = 不代理）；
//   - fwd（filter forward）：established/related 放行 → 新 TCP 连接限额 →
//     代理回流放行 → denied CIDR drop → allowed CIDR accept → mode 默认；
//   - ct count：per-execution TCP 连接数上限（MaxTCPConns>0）。
//
// 全量替换 + generation fencing 由 Manager.ApplyEgressPolicy 保证；本文件
// 将完整替换作为单个 nft batch 提交，失败时内核保留旧规则。
package slot

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// ensureSlotEgress replaces the complete fp-slot table in one nft transaction.
// nft validates the whole batch before committing it, so a syntax/runtime failure
// leaves the previous policy and NAT rules active.
func ensureSlotEgress(ctx context.Context, idx int, rs EgressRuleSet) error {
	script, err := egressTableScript(idx, &rs)
	if err != nil {
		return err
	}
	if err := runNftBatch(ctx, nsName(idx), script); err != nil {
		return fmt.Errorf("slot: atomically replace egress policy: %w", err)
	}
	return nil
}

func clearSlotEgress(ctx context.Context, idx int) error {
	script, err := egressTableScript(idx, nil)
	if err != nil {
		return err
	}
	if err := runNftBatch(ctx, nsName(idx), script); err != nil {
		return fmt.Errorf("slot: atomically clear egress policy: %w", err)
	}
	return nil
}

func egressTableScript(idx int, rs *EgressRuleSet) (string, error) {
	hostAddr, _, err := vethAddrs(idx)
	if err != nil {
		return "", err
	}
	_, vethGuest := vethNames(idx)
	var b strings.Builder
	b.WriteString("delete table ip fp-slot\n")
	b.WriteString("add table ip fp-slot\n")
	b.WriteString("add chain ip fp-slot post { type nat hook postrouting priority srcnat; policy accept; }\n")
	fmt.Fprintf(&b, "add rule ip fp-slot post oifname %q ip daddr != %s masquerade\n", vethGuest, hostAddr)
	if rs == nil {
		return b.String(), nil
	}
	b.WriteString("add set ip fp-slot egress-allow4 { type ipv4_addr; flags interval; }\n")
	b.WriteString("add set ip fp-slot egress-deny4 { type ipv4_addr; flags interval; }\n")
	if len(rs.AllowedCIDRs) > 0 {
		fmt.Fprintf(&b, "add element ip fp-slot egress-allow4 { %s }\n", strings.Join(rs.AllowedCIDRs, ", "))
	}
	if len(rs.DeniedCIDRs) > 0 {
		fmt.Fprintf(&b, "add element ip fp-slot egress-deny4 { %s }\n", strings.Join(rs.DeniedCIDRs, ", "))
	}
	b.WriteString("add chain ip fp-slot egress-pre { type nat hook prerouting priority dstnat; policy accept; }\n")
	b.WriteString("add chain ip fp-slot egress-fwd { type filter hook forward priority filter; policy accept; }\n")
	if rs.ProxyPort80 > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot egress-pre tcp dport 80 ip daddr != %s dnat to %s:%d\n", hostAddr, hostAddr, rs.ProxyPort80)
	}
	if rs.ProxyPort443 > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot egress-pre tcp dport 443 ip daddr != %s dnat to %s:%d\n", hostAddr, hostAddr, rs.ProxyPort443)
	}
	b.WriteString("add rule ip fp-slot egress-fwd ct state established,related accept\n")
	// The limit precedes every new-connection accept, including allowed CIDRs and
	// proxy DNAT, so neither path can bypass the per-execution cap.
	if rs.MaxTCPConns > 0 {
		fmt.Fprintf(&b, "add rule ip fp-slot egress-fwd meta l4proto tcp ct state new ct count over %d counter drop comment \"firepaas-tcp-limit\"\n", rs.MaxTCPConns)
	}
	fmt.Fprintf(&b, "add rule ip fp-slot egress-fwd ip daddr %s accept\n", hostAddr)
	b.WriteString("add rule ip fp-slot egress-fwd ip daddr @egress-deny4 drop\n")
	b.WriteString("add rule ip fp-slot egress-fwd ip daddr @egress-allow4 accept\n")
	if rs.Mode == "deny_all" || rs.Mode == "allowlist" {
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
