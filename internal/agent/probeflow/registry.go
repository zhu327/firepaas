// Package probeflow 登记 host 侧 readiness 探针连接的四元组，供 agentd 的
// auto-standby ConnectionSource 过滤（v1.1，ADR-0017 决策 3 的实现形态）。
//
// 为什么不用 IgnoreSourceCIDRs 排除探针源段：slot 后端下探针（health worker
// → slot IP）与代理回流（agent proxy → slot IP）共享同一源地址段（root 侧
// veth 主机地址，10.12.0.0/16），按源段忽略会同时排除真实流量——busy 实例
// 会在 established 连接仍存活时被判空闲并 standby（掐断在途请求），且
// wake-on-traffic 语义退化为“每请求唤醒”。因此改为精确按连接排除：
//
//   - 探针 dialer 在 Control 回调里显式 bind 固定本地端口，并在 connect
//     发出 SYN 之前把 (srcPort → dst) 登记到本 registry；
//   - agentd 用 filteredSource 包装 autostandby 的 conntrack
//     ConnectionSource，ListConnections dump 与事件流均丢弃命中的连接。
//
// 键形态说明：源地址在 connect 前不可知（内核按路由选择，getsockname 在
// Control 阶段返回 0.0.0.0），因此匹配键是 (源端口 → 目的四元组)——临时
// 端口全局唯一，精度等同四元组。登记的生命周期严格绑定 socket：dial 成功
// 后保持到 Conn.Close，dial 失败立即撤销。这样长 keepalive 不会因 TTL 失效，
// 而连接结束后端口复用也不会继续误过滤真实流量。
package probeflow

import (
	"net/netip"
	"sync"
	"time"
)

type flowKey struct {
	srcPort uint16
	dst     netip.AddrPort
}

// Registry 是探针连接登记表。并发安全。
type Registry struct {
	mu    sync.Mutex
	flows map[flowKey]struct{}
}

// NewRegistry constructs a registry. ttl is retained for source compatibility;
// flow lifetime is socket-driven and intentionally does not expire by time.
func NewRegistry(_ time.Duration) *Registry {
	return &Registry{flows: map[flowKey]struct{}{}}
}

// Record registers a probe connection before its SYN is sent.
func (r *Registry) Record(srcPort uint16, dst netip.AddrPort) {
	if srcPort == 0 || !dst.IsValid() {
		return
	}
	r.mu.Lock()
	r.flows[flowKey{srcPort: srcPort, dst: dst}] = struct{}{}
	r.mu.Unlock()
}

// Release removes a probe flow when its socket closes or dialing fails.
func (r *Registry) Release(srcPort uint16, dst netip.AddrPort) {
	if srcPort == 0 || !dst.IsValid() {
		return
	}
	r.mu.Lock()
	delete(r.flows, flowKey{srcPort: srcPort, dst: dst})
	r.mu.Unlock()
}

// Match 判断连接是否命中当前仍打开的探针流。
func (r *Registry) Match(srcPort uint16, dst netip.AddrPort) bool {
	if srcPort == 0 || !dst.IsValid() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.flows[flowKey{srcPort: srcPort, dst: dst}]
	return ok
}

// Size 返回当前登记数（观测/测试用）。
func (r *Registry) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.flows)
}
