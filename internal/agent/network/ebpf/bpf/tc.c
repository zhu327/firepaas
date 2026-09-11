// tc.c —— firepaas eBPF datapath（ADR-0040 §10 最小闭包）。
//
// 两个程序（挂载点/port 对照图，本文件为唯一权威）：
//
//   tc_ingress   host 侧 veth（fp-vpN）clsact ingress
//     - v4：**目的为本机地址**的 proxy 端口新连接放行（等价 nft INPUT，
//       跨 slot 目标仍走 private4 drop）；私网/保留目标（private4 LPM，
//       netpolicy canonical 集合同源）drop；其余放行（等价 nft fwdchain）
//     - v6：源必须等于本 veth 的期望 ULA（slot_ula per-veth 源绑定，
//       防同节点 identity 冒用）且是 ipcache 已知 ULA（fail closed）；
//       dst = node_ula（本节点平台流量）放行；东西向 = policy 条目
//       （谁连谁，默认 deny）+ 端口白名单（policy_ports：正向查 dport、
//       回程查 sport）+ ICMPv6 仅链路发现放行；其余 v6 全部 drop
//   tc_egress    slot netns 内 veth（fp-vgN）clsact egress
//     - deny→allow→mode 默认：per-slot 策略（egress_allow/deny 按 guest IP
//       分片的 map-in-map + egress_mode；无条目 = 未下发快照 = unrestricted，
//       与 nft 无 fp-slot 表 = accept 同语义）
//     - 网关可达 accept（等价 nft：ip daddr hostAddr accept）
//
// 代理 redirect 不走 eBPF：TC 位置重写 L3/L4 无法可靠维护校验和
//（CHECKSUM_PARTIAL 的 finalize 发生在 qdisc 之后，见 T5 真机验证记录）。
// redirect 由 slot 内 nft prerouting DNAT 提供（与现役 nft 后端同一语义，
// conntrack 自动反向 NAT——"代理回流不 masquerade"断言在该路径上成立）。
// 两个后端共享同一 DNAT 实现（slot.EnsureNetnsNAT）。
//
// 连接限额（per-execution 新 TCP 上限）不在本程序：改用 slot netns 的
// nft `meta l4proto tcp ct state new ct count over N`（与 nft 后端同一
// 实现，随 conntrack 超时自愈）。BPF 聚合计数在 LRU 去重表淘汰后会漏减
//（长连接关闭不被计数），最终拒绝合法新连接，故移除。
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
//   fabric_active   ARRAY[1]：当前活动的 fabric 策略集（0/1）。fabric 三表
//     双缓冲（*_v0/*_v1），Go 写非活动集后一次翻转，datapath 只看到完整集。
//   ipcache_v0/v1   ULA(/128)→identity_id（空 = v6 全拒）
//   slot_ula        host veth ifindex→本 slot 期望 ULA（per-veth v6 源绑定；
//                   无条目 = 该 veth v6 全拒）
//   policy_v0/v1    {src_id,dst_id}→{generation,flags}（谁连谁；flags 标注
//                   dport/sport 白名单方向；默认 deny）
//   policy_ports_v0/v1 {src_id,dst_id,port}→1（正向查 dport、回程查 sport）
//   node_ula_v0/v1  本节点自身 ULA →1（平台流量放行）
//   flow_sample     ARRAY[1]：allow 事件采样模数（1 = 全量；deny 始终全量）
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
#define TCPHDR_ACK 0x10

struct lpm4_key {
	__u32 prefixlen;
	__u32 data; // 与 Go 侧同口径：IPv4 按 LE u32 比较（内存序，非网络字节序）
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

// slot_ula：host veth ifindex → 该 slot 的期望 workload ULA（/128 原字节）。
// per-veth 源绑定：ipcache 命中只证明 ULA 存在，不证明报文来自拥有它的
// slot——同节点 workload 可用他人 ULA 当源地址冒用其 identity 通过策略。
// Go 侧 AttachSlot 写入、DetachSlot 删除；无条目（纯 v4 slot）→ 该 veth
// 的 v6 一律 drop（fail closed）。
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 2048);
	__type(key, __u32);
	__type(value, __u8[16]);
} slot_ula SEC(".maps");

/* fabric_active：当前活动的 fabric 策略集（0/1）。三组策略表双缓冲：
 * Go 在非活动集写全量快照，完成后一次 Put 翻转本选择子；datapath 每次
 * 查表前读它，因此只会看到完整旧集或完整新集（替换原子；写失败保持旧集）。 */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u32);
} fabric_active SEC(".maps");

#define FABRIC_SET(NAME, VAL, MAX_ENTRIES)                                     \
	struct {                                                               \
		__uint(type, BPF_MAP_TYPE_HASH);                               \
		__uint(max_entries, MAX_ENTRIES);                              \
		__type(key, __u128);                                           \
		__type(value, VAL);                                            \
	} NAME##_v0 SEC(".maps");                                              \
	struct {                                                               \
		__uint(type, BPF_MAP_TYPE_HASH);                               \
		__uint(max_entries, MAX_ENTRIES);                              \
		__type(key, __u128);                                           \
		__type(value, VAL);                                            \
	} NAME##_v1 SEC(".maps")

FABRIC_SET(ipcache, __u32, MAP_ENTRIES);

struct policy_key {
	__u32 src;
	__u32 dst;
};

/* policy_val.flags：端口白名单方向。
 *   POLICY_FLAG_DPORT：该 (src,dst) 的 ports 是目的端口（正向规则），
 *     TCP 纯 SYN 与 UDP 逐包查 dport。
 *   POLICY_FLAG_SPORT：ports 是源端口（对称回程规则），TCP 非 SYN 与
 *     UDP 逐包查 sport——把回程从“任意端口”收窄到声明服务端口。
 * flags=0 的条目只放行 ICMPv6（无端口语义）；UDP 遇 flags=0 保守 deny。 */
#define POLICY_FLAG_DPORT (1ULL << 0)
#define POLICY_FLAG_SPORT (1ULL << 1)

struct policy_val {
	__u64 generation;
	__u64 flags;
};

#define FABRIC_POLICY_SET(NAME, KEY, VAL)                                      \
	struct {                                                               \
		__uint(type, BPF_MAP_TYPE_HASH);                               \
		__uint(max_entries, MAP_ENTRIES);                              \
		__type(key, KEY);                                              \
		__type(value, VAL);                                            \
	} NAME##_v0 SEC(".maps");                                              \
	struct {                                                               \
		__uint(type, BPF_MAP_TYPE_HASH);                               \
		__uint(max_entries, MAP_ENTRIES);                              \
		__type(key, KEY);                                              \
		__type(value, VAL);                                            \
	} NAME##_v1 SEC(".maps")

FABRIC_POLICY_SET(policy, struct policy_key, struct policy_val);

// policy_ports：东西向逐端口放行（G2a §15）。key 命中 = 该 (src,dst)
// 在指定方向上允许此端口：dir=0 为目的端口（正向），dir=1 为源端口
//（回程）。双向都声明时同一 (src,dst) 会同时存在两个方向的端口集，
// 必须靠 dir 区分——不能把两个方向混在一张表里（会互相覆盖或过宽）。
// 含 v6 扩展头的报文不解析 L4 → 保守 deny。
struct policy_port_key {
	__u32 src;
	__u32 dst;
	__u16 port; // host 字节序
	__u16 dir;  // 0 = dport 白名单；1 = sport 白名单
};
FABRIC_POLICY_SET(policy_ports, struct policy_port_key, __u8);

// node_ula：本节点自身 ULA（/64 基址，wg 设备地址；agent 随快照写入）。
// dst = 节点自身的 v6 = 平台流量（spike 探活、G2c 节点本地 DNS、健康检查
// 等），不是东西向 workload 流量——源绑定后直接放行（默认 deny 只约束
// workload↔workload）。
FABRIC_SET(node_ula, __u8, 1);

/* flow_sample：allow 事件采样模数（ARRAY[1]，1 = 全量）。deny 事件始终
 * 全量发射（审计）。ARP/ND 级噪声不在此列的由 deny 分支自己 emit。 */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u32);
} flow_sample SEC(".maps");

/* flow_events（G3，§21）：东西向决策事件 ringbuf（Hubble 式 flow 源）。
 * 事件只在 v6 东西向路径的决策点发射（allow 采样/deny 各分支），字段为
 * 固定尺寸（verifier 友好）；用户态读取后按 identity_id 关联
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
	/* allow 采样：per-packet ringbuf 在高 pps 下是纯开销（读者跟不上、
	 * 事件被丢弃），按 flow_sample 模数抽样；deny 全量保留审计。 */
	if (verdict == FLOW_ALLOW) {
		__u32 k = 0;
		__u32 *every = bpf_map_lookup_elem(&flow_sample, &k);
		if (every && *every > 1 && (bpf_get_prandom_u32() % *every) != 0)
			return;
	}
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

/* ula_eq：比较 16 字节 ULA（网络字节序原样比较；展开为常量循环，
 * verifier 可静态展开且不依赖指针对齐）。 */
static __always_inline bool ula_eq(const __u8 *a, const struct in6_addr *b)
{
	const __u8 *bb = (const __u8 *)b;
#pragma clang loop unroll(full)
	for (int i = 0; i < 16; i++) {
		if (a[i] != bb[i])
			return false;
	}
	return true;
}

/* fabric 双缓冲读取：先读选择子再查对应集合。每个 map 一个 helper（分支 +
 * 两次静态 lookup），verifier 友好且切换对所有查询原子可见。 */
static __always_inline __u32 fabric_active_idx(void)
{
	__u32 k = 0;
	__u32 *v = bpf_map_lookup_elem(&fabric_active, &k);
	return (v && *v == 1) ? 1 : 0;
}

static __always_inline __u32 *ipcache_lookup(__u32 set, const void *addr)
{
	if (set == 1)
		return bpf_map_lookup_elem(&ipcache_v1, addr);
	return bpf_map_lookup_elem(&ipcache_v0, addr);
}

static __always_inline __u8 *node_ula_lookup(__u32 set, const void *addr)
{
	if (set == 1)
		return bpf_map_lookup_elem(&node_ula_v1, addr);
	return bpf_map_lookup_elem(&node_ula_v0, addr);
}

static __always_inline struct policy_val *policy_lookup(__u32 set, const struct policy_key *key)
{
	if (set == 1)
		return bpf_map_lookup_elem(&policy_v1, key);
	return bpf_map_lookup_elem(&policy_v0, key);
}

static __always_inline __u8 *policy_port_lookup(__u32 set, const struct policy_port_key *key)
{
	if (set == 1)
		return bpf_map_lookup_elem(&policy_ports_v1, key);
	return bpf_map_lookup_elem(&policy_ports_v0, key);
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
	return ((__u8 *)tcp)[13] & TCPHDR_ACK;
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
		// 本机目的（代理回流 / host 发起连接的回程）先于私网 drop 放行，
		// 判定集中在 host_bound_established（daddr 是本机 ∧（代理端口 ∨
		// ACK））；跨 slot 目标不在 host_addrs4，落 private4 drop。代理
		// 端口特判保留 host-bound 条件，等价 nft INPUT 链。
		if (ip->protocol == IPPROTO_TCP) {
			struct tcphdr *tcp = (void *)ip + ip->ihl * 4;
			if (CTX_BOUND(skb, tcp + 1)) {
				__u16 dport = bpf_ntohs(tcp->dest);
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
		// 本包只读一次活动集：ipcache/node_ula/policy/policy_ports 必须来自同一代，
		// 否则翻转落在同一包的多次查表之间时会混读两代（逐查询原子不足）。
		__u32 set = fabric_active_idx();
		// NDP/ICMPv6 链路必需报文先行放行：邻居解析是 ULA 可达的前提；
		// ipcache 只约束 workload 数据面，不约束链路层发现——否则 mesh
		// 链路 NDP 全灭，跨节点 ULA 永远不可达（slot 解析网关失败约 3s
		// 后内核回 ICMPv6 Addr-Unreachable，表现为 EHOSTUNREACH）。
		// 只放行 RS(133)/NS(135)/NA(136)：RA(134) 与 redirect(137) 对
		// slot 不必要，放行会让 guest 可向宿主注入路由/重定向报文。
		if (ip6->nexthdr == 58) { /* IPPROTO_ICMPV6（vmlinux.h 仅类型，无常量） */
			struct icmp6hdr *ic6 = (void *)(ip6 + 1);
			if (CTX_BOUND(skb, ic6 + 1)) {
				if (ic6->icmp6_type == 133 /* nd-router-solicit */ ||
				    ic6->icmp6_type == 135 /* nd-neighbor-solicit */ ||
				    ic6->icmp6_type == 136 /* nd-neighbor-advert */)
					return TC_ACT_OK;
			}
		}
		// v6 源绑定两级：src 必须等于**本 veth 的期望 ULA**（slot_ula，防同
		// 节点冒用），再要求该 ULA 在 ipcache（全局身份表）。只查 ipcache
		// 时，同节点 workload 可用他人 ULA 当源、借他人 identity 通过策略。
		{
			__u32 ifindex = skb->ifindex;
			__u8 *expect = bpf_map_lookup_elem(&slot_ula, &ifindex);
			if (!expect || !ula_eq(expect, &ip6->saddr)) {
				emit_flow(skb, 0, 0, 0, ip6->nexthdr, FLOW_DENY_SRC);
				return TC_ACT_SHOT;
			}
		}
		__u32 *src_id = ipcache_lookup(set, &ip6->saddr);
		if (!src_id) {
			emit_flow(skb, 0, 0, 0, ip6->nexthdr, FLOW_DENY_SRC);
			return TC_ACT_SHOT;
		}
		// dst = 本节点自身 ULA（平台流量：探活/DNS/健康检查）→ 放行。
		{
			__u8 *self = node_ula_lookup(set, &ip6->daddr);
			if (self)
				return TC_ACT_OK;
		}
		// 东西向：dst 也必须是已知 ULA 且 policy 有放行条目（默认 deny）
		__u32 *dst_id = ipcache_lookup(set, &ip6->daddr);
		if (!dst_id) {
			emit_flow(skb, *src_id, 0, 0, ip6->nexthdr, FLOW_DENY_DST);
			return TC_ACT_SHOT;
		}
		// L4 解析（无扩展头；带扩展头 → 无可靠 L4 偏移，保守 deny）。
		__u8 proto = ip6->nexthdr;
		__u16 dport = 0;
		__u16 sport = 0;
		__u8 tcp_flags = 0;
		if (proto == 6) { /* IPPROTO_TCP */
			struct tcphdr *tcp = (void *)(ip6 + 1);
			if ((void *)tcp + 20 > (void *)(long)skb->data_end)
				return TC_ACT_SHOT;
			dport = bpf_ntohs(tcp->dest);
			sport = bpf_ntohs(tcp->source);
			tcp_flags = ((__u8 *)tcp)[13];
		} else if (proto == 17 || proto == 58) { /* UDP / ICMPv6 */
			if (proto == 17) {
				struct udphdr *udp = (void *)(ip6 + 1);
				if ((void *)udp + 8 > (void *)(long)skb->data_end)
					return TC_ACT_SHOT;
				dport = bpf_ntohs(udp->dest);
				sport = bpf_ntohs(udp->source);
			}
		} else {
			return TC_ACT_SHOT; // 其余协议无端口语义，deny
		}
		// 东西向授权（§15）：entry {src,dst} 存在 = “谁连谁”成立。回程由
		// 快照组装方写入的对称 entry（flags=SPORT，携带同一端口集）承担——
		// 跨节点时请求与回包在不同节点分别执行，无法共享运行时流表，故回
		// 程授权必须是静态对称条目而非逐流登记（真机 spike 实测教训）。
		struct policy_key key = { .src = *src_id, .dst = *dst_id };
		struct policy_val *pol = policy_lookup(set, &key);
		if (!pol) {
			emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_POLICY);
			return TC_ACT_SHOT;
		}
		if (proto == 6) {
			__u8 pure_syn = (tcp_flags & TCPHDR_SYN) && !(tcp_flags & TCPHDR_ACK);
			if (pure_syn) {
				// 新连接只认正向 dport 白名单；反向条目（SPORT）不接受
				// 反向新建，B→A 的纯 SYN 一律 deny。
				if (!(pol->flags & POLICY_FLAG_DPORT)) {
					emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
					return TC_ACT_SHOT;
				}
				struct policy_port_key pkey = {
					.src = *src_id, .dst = *dst_id, .port = dport, .dir = 0,
				};
				if (!policy_port_lookup(set, &pkey)) {
					emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
					return TC_ACT_SHOT;
				}
			} else if (pol->flags) {
				/* 非纯 SYN（已建立流的数据/ACK，或被放行的回包）：
				 * dport 与 sport 两个方向任一命中即放行。双向声明的
				 * (src,dst) 同时带 DPORT|SPORT：A 发起的连接其 ACK/数据
				 * 的 sport 是临时端口，只查 sport 会把已建立流全部误丢。
				 * 两方向都有端口规则但都不命中才 deny。 */
				__u8 ok = 0;
				if (pol->flags & POLICY_FLAG_DPORT) {
					struct policy_port_key pkey = {
						.src = *src_id, .dst = *dst_id, .port = dport, .dir = 0,
					};
					if (policy_port_lookup(set, &pkey))
						ok = 1;
				}
				if (!ok && (pol->flags & POLICY_FLAG_SPORT)) {
					struct policy_port_key pkey = {
						.src = *src_id, .dst = *dst_id, .port = sport, .dir = 1,
					};
					if (policy_port_lookup(set, &pkey))
						ok = 1;
				}
				if (!ok) {
					emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
					return TC_ACT_SHOT;
				}
			}
		} else if (proto == 17) {
			/* UDP 无 SYN 语义，逐包按方向查端口表：正向查 dport(dir0)，
			 * 回程查 sport(dir1)；双向都声明时任一方向命中即放行（回程
			 * 本身无状态，与分成两条独立条目时的语义一致）。两方向都
			 * 没有的脏条目保守 deny。 */
			__u8 ok = 0;
			if (pol->flags & POLICY_FLAG_DPORT) {
				struct policy_port_key pkey = {
					.src = *src_id, .dst = *dst_id, .port = dport, .dir = 0,
				};
				if (policy_port_lookup(set, &pkey))
					ok = 1;
			}
			if (!ok && (pol->flags & POLICY_FLAG_SPORT)) {
				struct policy_port_key pkey = {
					.src = *src_id, .dst = *dst_id, .port = sport, .dir = 1,
				};
				if (policy_port_lookup(set, &pkey))
					ok = 1;
			}
			if (!ok) {
				emit_flow(skb, *src_id, *dst_id, dport, proto, FLOW_DENY_PORT);
				return TC_ACT_SHOT;
			}
		}
		// ICMPv6：条目存在即放行（PMTU/echo 无端口语义）。
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
	 * 因此全部 per-slot 状态（egress 策略/mode）按规范 key（egress_slot
	 * 映射：guest IP 与链路地址都归一到链路地址）索引，两端 key 不同但
	 * 1:1 对应同一 slot，裁决 outcome 一致。未知源（伪造/IP 复用残留）
	 * 直接 drop——附带的出口源绑定。 */
	__u32 *pslot = bpf_map_lookup_elem(&egress_slot, &saddr);
	if (!pslot)
		return TC_ACT_SHOT;
	__u32 guest_key = *pslot;

	// 网关可达（等价 nft：ip daddr hostAddr accept——代理回流方向）。
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
	// per-slot deny → allow → mode 默认（等价 nft egress-fwd 链顺序）。
	// 外层无条目 = 未下发快照 = unrestricted（与 nft 无 fp-slot 表 =
	// accept 同语义；fail-closed 会让快照前的 slot 断网）。
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
