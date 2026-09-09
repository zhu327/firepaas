// Package ulanet 是 RFC 4193 ULA（fd00::/8）前缀的单一校验实现。
//
// 消费方：controlplane/store（IPAM 权威表）与 contracts/agentv1（fabric 下发
// 契约）。两处此前各自手写位运算且同错（0xfc/0xfd 都通过），本包收敛为
// 一处实现，禁止再次复制。
package ulanet

import (
	"fmt"
	"net/netip"
	"strings"
)

// IsULAPrefix 报告 p 是否落在 RFC 4193 ULA 空间 fd00::/8（L=1）。
// fc00::/8（L=0，保留）不是 ULA，必须拒绝。
func IsULAPrefix(p netip.Prefix) bool {
	if !p.IsValid() {
		return false
	}
	a := p.Addr()
	return a.Is6() && !a.Is4In6() && a.As16()[0] == 0xfd
}

// IsULAAddr 报告 addr 是否为 IPv6 且落在 fd00::/8。
func IsULAAddr(addr netip.Addr) bool {
	return addr.IsValid() && IsULAPrefix(netip.PrefixFrom(addr, 128))
}

// ValidatePrefix 解析并校验 ULA 前缀：IPv6、fd00::/8、bits 与 wantBits 一致、
// host bits 未置位（非规范形态 fail closed）。成功返回 Masked() 规范化前缀。
func ValidatePrefix(raw string, wantBits int) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("parse %q: %v", raw, err)
	}
	return CanonicalizePrefix(prefix, wantBits)
}

// CanonicalizePrefix 对已解析前缀做同 ValidatePrefix 的校验与规范化。
func CanonicalizePrefix(prefix netip.Prefix, wantBits int) (netip.Prefix, error) {
	if prefix.Bits() != wantBits {
		return netip.Prefix{}, fmt.Errorf("%s must be a /%d", prefix, wantBits)
	}
	if !IsULAPrefix(prefix) {
		return netip.Prefix{}, fmt.Errorf("%s is outside RFC 4193 fd00::/8", prefix)
	}
	masked := prefix.Masked()
	if prefix.Addr() != masked.Addr() {
		return netip.Prefix{}, fmt.Errorf("%s has host bits set (canonical form %s)", prefix, masked)
	}
	return masked, nil
}
