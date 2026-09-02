// Package netpolicy 是节点网络策略的单一事实来源（R2 评审加固）。
//
// 私网/保留 CIDR 集合此前在 dataset SSRF 校验（machine/volumes.go）与 slot
// 内核隔离规则（fp-isolation 的 privateDst）各自维护并已开始漂移（如
// 100.64.0.0/10 CGNAT、127.0.0.0/8 未进 slot 前向 drop）。本包集中维护
// canonical 集合，同时提供 Go matcher 与 nftables 集合文本，两个消费方
// 不再各自抄写。
package netpolicy

import (
	"fmt"
	"net/netip"
	"strings"
)

// canonicalReservedIPv4 是 IPv4 保留段黑名单（RFC 6890 全集 + CGNAT
// 100.64.0.0/10）。语义基线沿用原 datasetReservedPrefixes 的 IPv4 子集。
var canonicalReservedIPv4 = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
	"172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.31.196.0/24", "192.52.193.0/24",
	"192.88.99.0/24", "192.168.0.0/16", "192.175.48.0/24", "198.18.0.0/15",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
}

// canonicalReservedIPv6 是 IPv6 保留段黑名单（沿用原 datasetReservedPrefixes
// 的 IPv6 子集）。
var canonicalReservedIPv6 = []string{
	"::/128", "::1/128", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64",
	"2001::/23", "2001:db8::/32", "3fff::/20", "2620:4f:8000::/48", "fc00::/7", "fe80::/10", "ff00::/8",
}

// reservedPrefixes 是解析后的 canonical 集合（IPv4 在前、IPv6 在后）。
// 列表全部为本文件内的静态字面量：ParsePrefix 只会因字面量笔误失败，属于
// 包初始化即应暴露的编程错误（netip.MustParse 语义，init 时 fail fast），
// 不处理任何运行期输入，因此 panic 是可接受的。
var reservedPrefixes = func() []netip.Prefix {
	all := append(append([]string(nil), canonicalReservedIPv4...), canonicalReservedIPv6...)
	out := make([]netip.Prefix, 0, len(all))
	for _, cidr := range all {
		block, err := netip.ParsePrefix(cidr)
		if err != nil {
			panic(fmt.Sprintf("netpolicy: static reserved prefix %q is invalid: %v", cidr, err))
		}
		out = append(out, block)
	}
	return out
}()

// Prefixes 返回 canonical 保留段集合的副本（调用方不得依赖顺序外的语义）。
func Prefixes() []netip.Prefix {
	return append([]netip.Prefix(nil), reservedPrefixes...)
}

// Contains 报告 addr 是否落在任一 canonical 保留段（按族匹配；未解析的
// 地址返回 false，由调用方另行判非法）。
func Contains(addr netip.Addr) bool {
	for _, prefix := range reservedPrefixes {
		if prefix.Addr().BitLen() == addr.BitLen() && prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// IPv4NftSetText 返回 nftables 规则的 IPv4 字面集合文本
// （"{ 0.0.0.0/8, 10.0.0.0/8, ... }"）。fp-isolation 的私网目标 drop 规则
// 直接内联它——规则文本与本包的 Go matcher 永远同源，不会再次漂移。
func IPv4NftSetText() string {
	return "{ " + strings.Join(canonicalReservedIPv4, ", ") + " }"
}
