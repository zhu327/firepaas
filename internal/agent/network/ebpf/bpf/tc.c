// tc.c —— firepaas eBPF datapath（ADR-0040 §10 最小闭包）。
//
// 两个程序（挂载点/port 对照图，本文件为唯一权威）：
//
//   tc_ingress   host 侧 veth（fp-vpN）clsact ingress
//     - v4：proxy 端口新连接放行；私网/保留目标（private4 LPM，netpolicy
//       canonical 集合同源）drop；其余放行（等价 nft fwdchain）
//     - v6：源必须是 ipcache 已知 ULA（v6 源绑定，fail closed）；
//       dst = node_ula（本节点平台流量）放行；东西向 = policy 条目
//       （谁连谁，默认 deny）+ TCP 纯 SYN 逐端口白名单（policy_ports）
//       + policy_flows 已授权流表（回包/中间流；授权时登记反向四元组，
//       300s 懒过期）；其余 v6 全部 drop
//   tc_egress    slot netns 内 veth（fp-vgN）clsact egress
//     - conn_count：per-execution 新 TCP 上限（SYN 增、FIN/RST 减，syn_seen
//       去重 SYN 重传）
//     - deny→allow→mode 默认：per-slot 策略（egress_allow/deny 按 guest IP
//       分片的 map-in-map + egress_mode；无条目 = 未下发快照 = unrestricted，
//       与 nft 无 fp-slot 表 = accept 同语义）
//     - 网关可达 accept（等价 nft：ip daddr hostAddr accept）
//
// 代理 redirect 不走 eBPF：TC 位置重写 L3/L4 无法可靠维护校验和（
// CHECKSUM_PARTIAL 的 finalize 发生在 qdisc 之后，见 T5 真机验证记录）。
// redirect 由 slot 内 nft prerouting DNAT 提供（与现役 nft 后端同一语义，
// conntrack 自动反向 NAT——"代理回流不 masquerade"断言在该路径上成立）。
// 两个后端共享同一 DNAT 实现（slot.EnsureNetnsNAT）。
//
// maps（Go 侧 pin 到 /sys/fs/bpf/firepaas/ 并维护）：
//   proxy_ports     [80, 443] 代理端口（tc_ingress INPUT 放行用）
//   host4           规范 slot key → host 网关地址（tc_egress 网关可达判定）
//   private4        LPM：canonical 私网集（netpolicy 同源）
//   egress_slot     saddr → 规范 slot key（guest IP/链路地址双条目；未知源 drop）
//   egress_allow/deny HASH_OF_MAPS：per-slot egress 快照（key = 规范 slot key，
//     内层 LPM 做 CIDR 匹配）。
//     无外层条目 = 该 slot 未下发快照 = unrestricted（与 nft 无表 = accept
//     同语义；全局单例会让多 slot 互抹，见 W1）。
//   egress_mode     规范 slot key → {drop, proxy80, proxy443}：per-slot 默认动作
//     + 快照代理声明（nft 只在声明时装 DNAT，eBPF bypass 同条件）。
//   conn_cap        规范 slot key → 新 TCP 上限（0 = 不限）
//   conn_count      聚合计数（key = 规范 key 的 {saddr,0,0,0}；条目按 slot 生命
//     周期存在，Detach 删除；零值保留不删——删条目会与并发持锁产生 UAF；且
//     bpf_spin_lock 仅允许于 HASH/ARRAY 值类型）
//   syn_seen        {四元组} → SYN 去重标记
//   ipcache         ULA(/128)→identity_id（G2 前为空 = v6 全拒）
//   policy          {src_id,dst_id}→{generation,has_ports}（谁连谁；has_ports
//                    = 正向端口规则标记，纯对称回程条目为 0。默认 deny）
//   policy_ports    {src_id,dst_id,dport}→1（TCP 纯 SYN 与 UDP 的服务端口白名单；
//                    TCP 回包凭 entry 放行，UDP 回包凭对向纯回程条目放行）
//   node_ula        本节点自身 ULA →1（平台流量放行）
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// vmlinux.h 只含类型不含宏（UAPI 头与 vmlinux.h 类型重复，不可并 include）。
#define ETH_P_IP 0x0800
#define ETH_P_IPV6 0x86DD
#define ETH_HLEN 14
#define TC_ACT_OK 0
#define TC_ACT_SHOT 2
#define IPPROTO_TCP 6
#define BPF_ANY 0
#define BPF_F_NO_PREALLOC (1U << 0)
#define BPF_F_RECOMPUTE_CSUM (1ULL << 0)
#define BPF_F_INGRESS (1ULL << 0)

char LICENSE[] SEC("license") = "GPL";

#define MAP_ENTRIES 65536

#define TCPHDR_SYN 0x02
#define TCPHDR_FIN 0x01
#define TCPHDR_RST 0x04

struct lpm4_key {
	__u32 prefixlen;
	__u32 data; // 与 Go 侧同口径：IPv4 按 LE u32 比较（内存序，非网络字节序）
};

struct conn_key {
	__u32 saddr;
	__u32 daddr;
	__u16 sport;
	__u16 dport;
};

// conn_val 是 conn_count 的值类型：count 的读写必须在自旋锁下完成。
// 背景：旧代码读-改-写非原子，多 CPU 并发 SYN 会 lost-update（两个已建立
// 连接只计 1，导致限额漏放——真机偶发 238 即此因）。
struct conn_val {
	struct bpf_spin_lock lock;
	__u64 count;
};

/* ---- 共享 maps ---- */

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 2);
	__type(key, __u32);
	__type(value, __u16);
} proxy_ports SEC(".maps");

// host4: 规范 slot key（链路地址，见 egress_slot 归一）→ host 侧 veth 地址
//（BE）。tc_egress 网关可达判定用。
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, __u32);
	__type(value, __u32);
} host4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__uint(max_entries, 256);
	__type(key, struct lpm4_key);
	__type(value, __u8);
} private4 SEC(".maps");

/* egress_allow/deny：per-slot 快照（W1）。外层 key = guest IPv4（host 字节序），
 * 内层 LPM 做 CIDR 匹配。Go 侧按 slot 建内层表并挂入外层（map-in-map）；
 * 外层无条目 = 未下发快照 = unrestricted（与 nft 无表 = accept 同语义）。
 * 内层模板经 __array(values) 由 loader 推导（cilium/ebpf map-in-map）。 */
struct egress_lpm_def {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__uint(max_entries, 512);
	__type(key, struct lpm4_key);
	__type(value, __u8);
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH_OF_MAPS);
	__uint(max_entries, 1024);
	__type(key, __u32);
	__array(values, struct egress_lpm_def);
} egress_allow SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH_OF_MAPS);
	__uint(max_entries, 1024);
	__type(key, __u32);
	__array(values, struct egress_lpm_def);
} egress_deny SEC(".maps");

/* egress_slot：saddr → 规范 slot key（本 slot 链路地址，host 字节序）。
 * guest IP 与链路地址双条目（Go 侧 Attach 写入，Detach 删除）。未命中 =
 * 非本 slot 源（伪造/残留），调用方 fail closed。见 tc_egress 头注释的
 * hook-order 说明。 */
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 2048);
	__type(key, __u32);
	__type(value, __u32);
} egress_slot SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, __u32); // 规范 slot key（链路地址，见 egress_slot）
	__type(value, __u32); // per-execution 新 TCP 上限（0 = 不限）
} conn_cap SEC(".maps");

/* egress_mode：per-slot 默认动作 + 快照代理声明（与 nft 快照同源）。
 * drop=1 → deny_all/allowlist 默认拒绝；proxy80/proxy443=1 → 快照声明了
 * 透明代理端口（nft 只在声明时装 DNAT；eBPF 的 80/443 bypass 必须同条件，
 * 否则无代理快照的 80/443 会被错误放行）。无条目 = 未下发快照。 */
struct egress_mode_val {
	__u8 drop;
	__u8 proxy80;
	__u8 proxy443;
	__u8 _pad;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 1024);
	__type(key, __u32); // guest IPv4 host 字节序
	__type(value, struct egress_mode_val);
} egress_mode SEC(".maps");

// conn_count：per-slot 聚合计数（key = 规范 slot key 的 {saddr,0,0,0}）。常规
// HASH（非 LRU）：条目按 slot 生命周期存在，Detach 删除；零值保留不删——
// 删条目会与并发持锁产生 UAF；且 bpf_spin_lock 仅允许于 HASH/ARRAY 值类型。
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, struct conn_key);
	__type(value, struct conn_val);
} conn_count SEC(".maps");

// syn_seen：去重 SYN 重传（conn_count 聚合计数只对首个 SYN 递增）。
struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, struct conn_key);
	__type(value, __u8);
} syn_seen SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, __u128);
	__type(value, __u32);
} ipcache SEC(".maps");

struct policy_key {
	__u32 src;
	__u32 dst;
};

struct policy_val {
	__u64 generation;
	/* has_ports：正向端口规则标记（1 = 该方向有 ports 白名单，0 = 纯对称
	 * 回程条目）。UDP 凭此区分“新连接发起”（查 policy_ports）与“回包”
	 * （对向纯回程条目直接放行）；TCP 回包凭 SYN 标志区分，不读本字段。 */
	__u64 has_ports;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, struct policy_key);
	__type(value, struct policy_val);
} policy SEC(".maps");

// policy_ports：东西向逐端口放行（G2a §15）。key 命中 = 该 (src,dst)
// 允许此 TCP 目的端口；policy 条目存在但端口无 key = drop（规则最小化）。
// 非 TCP 无端口语义 → 默认 deny；含 v6 扩展头的报文不解析 L4 → 保守 deny。
struct policy_port_key {
	__u32 src;
	__u32 dst;
	__u16 dport; // host 字节序
};
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAP_ENTRIES);
	__type(key, struct policy_port_key);
	__type(value, __u8);
} policy_ports SEC(".maps");

// node_ula：本节点自身 ULA（/64 基址，wg 设备地址；agent 随快照写入）。
// dst = 节点自身的 v6 = 平台流量（spike 探活、G2c 节点本地 DNS、健康检查
// 等），不是东西向 workload 流量——源绑定后直接放行（默认 deny 只约束
// workload↔workload）。
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 1);
	__type(key, __u8[16]);
	__type(value, __u8);
} node_ula SEC(".maps");

/* flow_events（G3，§21）：东西向决策事件 ringbuf（Hubble 式 flow 源）。
 * 事件只在 v6 东西向路径的决策点发射（allow/deny 各分支），字段为固定
 * 尺寸（verifier 友好）；用户态读取后按 identity_id 关联
 * project/app/machine/execution（fabric 快照），聚合计数保持低基数
 *（域名/IP 不进 label）。丢弃策略：ringbuf 满 → 丢弃（观测不得反压
 * 数据面）。 */
struct flow_event {
	__u32 src_id;
	__u32 dst_id;
	__u16 dport;
	__u8 proto;
	__u8 verdict; /* 1=allow 2=deny-policy 3=deny-src 4=deny-port 5=deny-dst */
	__u32 pkt_len;
	__u64 pad; /* 对齐/前向兼容 */
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 14 /* 16 KiB ≈ 512 事件 */);
} flow_events SEC(".maps");

#define FLOW_ALLOW 1
#define FLOW_DENY_POLICY 2
#define FLOW_DENY_SRC 3
#define FLOW_DENY_PORT 4
#define FLOW_DENY_DST 5

static __always_inline void emit_flow(struct __sk_buff *skb, __u32 src,
	__u32 dst, __u16 dport, __u8 proto, __u8 verdict)
{
	struct flow_event *ev = bpf_ringbuf_reserve(&flow_events, sizeof(*ev), 0);
	if (!ev)
		return; /* 满 → 丢弃（观测不反压数据面） */
	ev->src_id = src;
	ev->dst_id = dst;
	ev->dport = dport;
	ev->proto = proto;
	ev->verdict = verdict;
	ev->pkt_len = skb->len;
	ev->pad = 0;
	bpf_ringbuf_submit(ev, 0);
}

/* ---- helpers ---- */

#define CTX_BOUND(skb, p) ((void *)(p) <= (void *)(long)(skb)->data_end)

/* host_addrs4：根 netns 已分配的 IPv4 地址集（agent 启动时枚举写入）。
 * dst 命中 = 主机自身流量（INPUT 语义）；未命中且私网 = 跨 slot/平台段
 *（FORWARD 语义，drop）。替代 nft INPUT 的 conntrack established accept：
 * 内核无 conntrack kfunc（6.8 实测 bpf_ct_lookup_tcp 不支持），用
 * “dst 是本机地址 ∧ TCP 带 ACK”识别 host 发起连接的回程（健康探针/运维
 * 通道回包；伪造 ACK 到本机栈只会被 RST，无服务面）。 */
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 128);
	__type(key, __u32);
	__type(value, __u8);
} host_addrs4 SEC(".maps");

static __always_inline int lpm4_hit(void *map, __u32 addr)
{
	struct lpm4_key key = {
		.prefixlen = 32,
		.data = addr,
	};
	return bpf_map_lookup_elem(map, &key) != NULL;
}

/* egress_lpm_hit：per-slot CIDR 命中。外层无条目（未下发快照）= 不命中
 *（调用方按 unrestricted 处理，与 nft 无表 = accept 同语义）。 */
static __always_inline int egress_lpm_hit(void *outer, __u32 slot, __u32 addr)
{
	void *inner = bpf_map_lookup_elem(outer, &slot);
	if (!inner)
		return 0;
	struct lpm4_key key = {
		.prefixlen = 32,
		.data = addr,
	};
	return bpf_map_lookup_elem(inner, &key) != NULL;
}

static __always_inline struct egress_mode_val *egress_mode_of(__u32 slot)
{
	return bpf_map_lookup_elem(&egress_mode, &slot);
}

static __always_inline __u16 proxy_port(__u32 idx)
{
	__u32 k = idx;
	__u16 *p = bpf_map_lookup_elem(&proxy_ports, &k);
	return p ? *p : 0;
}

/* host_bound_established：dst 是本机地址且（TCP 回包带 ACK 或即代理端
 * 口）。等价 nft INPUT 的 established+proxy accept：host 发起连接（探针
 * 等）的回包 dst 是主机私网地址，无此前被 private4 drop 误杀（真机 G2c
 * 验收抓到探针回包全丢 → readiness 恒 NOT_READY）。guest 发起的新连接
 *（纯 SYN 到非代理端口）仍拒，与 nft INPUT 语义一致；跨 guest 流量
 * dst 非本机地址，不进本分支，private drop 不变。 */
static __always_inline bool host_bound_established(struct iphdr *ip,
	struct tcphdr *tcp, __u16 dport)
{
	if (!bpf_map_lookup_elem(&host_addrs4, &ip->daddr))
		return false;
	if (dport == proxy_port(0) || dport == proxy_port(1))
		return true; /* 代理回流（INPUT 显式 accept） */
	/* ACK 位（位 4，偏移 13）：回程标志。SYN-only = 新连接，非回程。 */
	return ((__u8 *)tcp)[13] & 0x10;
}

/* ---- tc_ingress：host 侧 veth ingress ---- */

SEC("tc")
int tc_ingress(struct __sk_buff *skb)
{
	void *data = (void *)(long)skb->data;
	struct ethhdr *eth = data;
	if (!CTX_BOUND(skb, eth + 1))
		return TC_ACT_OK;

	if (eth->h_proto == bpf_htons(ETH_P_IP)) {
		struct iphdr *ip = (void *)(eth + 1);
		if (!CTX_BOUND(skb, ip + 1))
			return TC_ACT_OK;
		// 代理回流入口（等价 nft INPUT: tcp dport {proxy80,proxy443} accept）
		if (ip->protocol == IPPROTO_TCP) {
			struct tcphdr *tcp = (void *)ip + ip->ihl * 4;
			if (CTX_BOUND(skb, tcp + 1)) {
				__u16 dport = bpf_ntohs(tcp->dest);
				if (dport == proxy_port(0) || dport == proxy_port(1))
					return TC_ACT_OK;
				// host 发起连接的回程（dst 本机地址 ∧ ACK）：先于私网
				// drop 放行（探针/运维通道回包）。
				if (host_bound_established(ip, tcp, dport))
					return TC_ACT_OK;
			}
		}
		// 私网/保留目标（跨 slot、平台保留段）→ drop（等价 nft fwdchain）
		if (lpm4_hit(&private4, ip->daddr))
			return TC_ACT_SHOT;
		return TC_ACT_OK;
	}

	if (eth->h_proto == bpf_htons(ETH_P_IPV6)) {
		struct ipv6hdr *ip6 = (void *)(eth + 1);
		if (!CTX_BOUND(skb, ip6 + 1))
			return TC_ACT_OK;
		// NDP/ICMPv6 链路必需报文（RS/RA/NS/NA/redirect）先行放行：邻居解析
		// 是 ULA 可达的前提；ipcache 只约束 workload 数据面，不约束链路层
		// 发现——否则 mesh 链路 NDP 全灭，跨节点 ULA 永远不可达（slot 解析
		// 网关失败约 3s 后内核回 ICMPv6 Addr-Unreachable，表现为 EHOSTUNREACH）。
		if (ip6->nexthdr == 58) { /* IPPROTO_ICMPV6（vmlinux.h 仅类型，无常量） */
			struct icmp6hdr *ic6 = (void *)(ip6 + 1);
			if (CTX_BOUND(skb, ic6 + 1)) {
				/* ND 类型 133-137 = RS/RA/NS/NA/redirect */
				if (ic6->icmp6_type >= 133 && ic6->icmp6_type <= 137)
					return TC_ACT_OK;
			}
		}
		// v6 源绑定：src 必须是 ipcache 已知 ULA，否则 drop（fail closed）
		__u32 *src_id = bpf_map_lookup_elem(&ipcache, &ip6->saddr);
		if (!src_id) {
			emit_flow(skb, 0, 0, 0, ip6->nexthdr, FLOW_DENY_SRC);
			return TC_ACT_SHOT;
		}
		// dst = 本节点自身 ULA（平台流量：探活/DNS/健康检查）→ 放行。
		{
			__u8 *self = bpf_map_lookup_elem(&node_ula, &ip6->daddr);
			if (self)
				return TC_ACT_OK;
		}
		// 东西向：dst 也必须是已知 ULA 且 policy 有放行条目（默认 deny）
		__u32 *dst_id = bpf_map_lookup_elem(&ipcache, &ip6->daddr);
		if (!dst_id) {
			emit_flow(skb, *src_id, 0, 0, ip6->nexthdr, FLOW_DENY_DST);
			return TC_ACT_SHOT;
		}
		// L4 解析（无扩展头；带扩展头 → 无可靠 L4 偏移，保守 deny）。
		__u8 proto = ip6->nexthdr;
		__u16 dport = 0;
		__u8 tcp_syn = 0;
		if (proto == 6) { /* IPPROTO_TCP */
			struct tcphdr *tcp = (void *)(ip6 + 1);
			if ((void *)tcp + 20 > (void *)(long)skb->data_end)
				return TC_ACT_SHOT;
			dport = bpf_ntohs(tcp->dest);
			tcp_syn = ((__u8 *)tcp)[13] & 0x12; /* SYN|ACK 位图 */
		} else if (proto == 17 || proto == 58) { /* UDP / ICMPv6 */
			if (proto == 17) {
				struct udphdr *udp = (void *)(ip6 + 1);
				if ((void *)udp + 8 > (void *)(long)skb->data_end)
					return TC_ACT_SHOT;
				dport = bpf_ntohs(udp->dest);
			}
		} else {
			return TC_ACT_SHOT; // 其余协议无端口语义，deny
		}
		// 东西向授权（§15）：entry {src,dst} 存在 = “谁连谁”成立。回程由
		// 快照组装方写入的对称 entry（无 ports 条目，has_ports=0）承担——
		// 跨节点时请求与回包在不同节点分别执行，无法共享运行时流表，故回
		// 程授权必须是静态对称条目而非逐流登记（真机 spike 实测教训）。
		struct policy_key key = { .src = *src_id, .dst = *dst_id };
		struct policy_val *pol = bpf_map_lookup_elem(&policy, &key);
		if (!pol) {
			emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_POLICY);
			return TC_ACT_SHOT;
		}
		if (proto == 6 && tcp_syn == 0x02) { /* 纯 SYN（无 ACK）才做端口检查 */
			struct policy_port_key pkey = {
				.src = *src_id, .dst = *dst_id, .dport = dport,
			};
			if (!bpf_map_lookup_elem(&policy_ports, &pkey)) {
				emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
				return TC_ACT_SHOT;
			}
		} else if (proto == 17 && pol->has_ports) {
			/* UDP 无 SYN 语义：正向规则（has_ports=1）逐包做端口白名单；
			 * 回包命中对向纯回程条目（has_ports=0）直接放行。空 ports 的
			 * 正向 UDP 规则无意义（契约要求 ports 必填），此处保守 deny。 */
			struct policy_port_key pkey = {
				.src = *src_id, .dst = *dst_id, .dport = dport,
			};
			if (!bpf_map_lookup_elem(&policy_ports, &pkey)) {
				emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
				return TC_ACT_SHOT;
			}
		}
		emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_ALLOW);
	}

	return TC_ACT_OK;
}

/* ---- tc_egress：slot netns veth egress ---- */

SEC("tc")
int tc_egress(struct __sk_buff *skb)
{
	void *data = (void *)(long)skb->data;
	struct ethhdr *eth = data;
	if (!CTX_BOUND(skb, eth + 1))
		return TC_ACT_OK;
	if (eth->h_proto != bpf_htons(ETH_P_IP))
		return TC_ACT_OK; // v6 由 host 侧 tc_ingress 统一执行（源绑定/策略）
	struct iphdr *ip = (void *)(eth + 1);
	if (!CTX_BOUND(skb, ip + 1))
		return TC_ACT_OK;

	__u32 saddr = ip->saddr;
	__u32 daddr = ip->daddr;
	/* W1 hook-order 事实：tc egress 位于 POSTROUTING 之后，看到的是
	 * masquerade 后的 saddr（出向包源 = 本 slot 链路地址；网关/代理包
	 * 未被 masquerade，源仍是 guest IP）。nft fwd 看到的是 pre-NAT saddr。
	 * 因此全部 per-slot 状态（egress 策略/mode/conn 上限）按规范 key
	 * （egress_slot 映射：guest IP 与链路地址都归一到链路地址）索引，
	 * 两端 key 不同但 1:1 对应同一 slot，裁决 outcome 一致。未知源
	 * （伪造/IP 复用残留）直接 drop——附带的出口源绑定。 */
	__u32 *pslot = bpf_map_lookup_elem(&egress_slot, &saddr);
	if (!pslot)
		return TC_ACT_SHOT;
	__u32 guest_key = *pslot;

	if (ip->protocol == IPPROTO_TCP) {
		struct tcphdr *tcp = (void *)ip + ip->ihl * 4;
		// flags 在 tcphdr 偏移 13：bounds 必须覆盖到该字节（verifier 拒绝
		// scalar 越界访问）。
		if ((void *)tcp + 20 > (void *)(long)skb->data_end)
			return TC_ACT_OK;
		__u16 dport = bpf_ntohs(tcp->dest);
		__u16 sport = bpf_ntohs(tcp->source);
		__u8 flags = ((__u8 *)tcp)[13];

		// 1) 连接数限额：per-execution 聚合计数（key = 仅 saddr）。
		//    SYN 重传经 syn_seen 去重只增一次；FIN/RST 饱和递减。
		//    检查+递增在 conn_val 自旋锁下原子完成（多 CPU 并发 SYN 的
		//    lost-update 会漏放超限连接）。拒绝的 SYN 不入 syn_seen、
		//    不污染计数（重传仍被拒绝）。
		__u32 *cap = bpf_map_lookup_elem(&conn_cap, &guest_key);
		if (cap && *cap > 0) {
			struct conn_key flow = {
				.saddr = saddr,
				.daddr = daddr,
				.sport = bpf_htons(sport),
				.dport = bpf_htons(dport),
			};
			struct conn_key agg = { .saddr = guest_key }; // 规范 key 聚合（双源 flavor 合并计数，与 nft 单计数器同口径）
			if (flags & TCPHDR_SYN) {
				if (!bpf_map_lookup_elem(&syn_seen, &flow)) {
					struct conn_val *cv = bpf_map_lookup_elem(&conn_count, &agg);
					if (!cv) {
						// 首建竞态：并发双方同时 NOEXIST 插入，败者回读。
						struct conn_val init = {};
						bpf_map_update_elem(&conn_count, &agg, &init, BPF_NOEXIST);
						cv = bpf_map_lookup_elem(&conn_count, &agg);
						if (!cv)
							return TC_ACT_SHOT; // 保守拒绝（fail closed）
					}
					bpf_spin_lock(&cv->lock);
					__u64 v = cv->count + 1;
					if (v > *cap) {
						bpf_spin_unlock(&cv->lock);
						return TC_ACT_SHOT;
					}
					cv->count = v;
					bpf_spin_unlock(&cv->lock);
					__u8 one = 1;
					bpf_map_update_elem(&syn_seen, &flow, &one, BPF_ANY);
				}
			} else if (flags & (TCPHDR_FIN | TCPHDR_RST)) {
				if (bpf_map_delete_elem(&syn_seen, &flow) == 0) {
					struct conn_val *cv = bpf_map_lookup_elem(&conn_count, &agg);
					if (cv) {
						bpf_spin_lock(&cv->lock);
						if (cv->count > 0)
							cv->count--;
						bpf_spin_unlock(&cv->lock);
					}
				}
			}
		}
	}

	// 2) 网关可达（等价 nft：ip daddr hostAddr accept——代理回流方向）。
	{
		__u32 *host = bpf_map_lookup_elem(&host4, &guest_key);
		if (host && *host != 0 && daddr == *host)
			return TC_ACT_OK;
		// proxy redirect 语义（等价 nft：prerouting DNAT 先于 deny/allow
		// 评估）——tcp 80/443 且 dst≠host 的流会被后续 nft DNAT 接走，
		// 这里必须放行，与 nft 后端「deny/allow 看到的是重写后 dst」一致。
		// 与 nft 同条件：只在快照声明了代理端口时 bypass（无声明时 80/443
		// 走正常 deny/allow/mode 裁决，见 egress_mode）。
		if (ip->protocol == IPPROTO_TCP && host && *host != 0 && daddr != *host) {
			struct egress_mode_val *mode = egress_mode_of(guest_key);
			struct tcphdr *t2 = (void *)ip + ip->ihl * 4;
			if ((void *)t2 + 4 <= (void *)(long)skb->data_end) {
				__u16 dp = bpf_ntohs(t2->dest);
				if ((dp == 80 && mode && mode->proxy80) ||
				    (dp == 443 && mode && mode->proxy443))
					return TC_ACT_OK;
			}
		}
	}
	// 3) per-slot deny → allow → mode 默认（等价 nft egress-fwd 链顺序）。
	//    外层无条目 = 未下发快照 = unrestricted（与 nft 无 fp-slot 表 =
	//    accept 同语义；fail-closed 会让快照前的 slot 断网）。
	if (egress_lpm_hit(&egress_deny, guest_key, daddr))
		return TC_ACT_SHOT;
	if (egress_lpm_hit(&egress_allow, guest_key, daddr))
		return TC_ACT_OK;
	{
		struct egress_mode_val *mode = egress_mode_of(guest_key);
		if (mode && mode->drop)
			return TC_ACT_SHOT;
		return TC_ACT_OK;
	}
}
