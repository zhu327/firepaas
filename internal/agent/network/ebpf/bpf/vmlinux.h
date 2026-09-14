// vmlinux.h —— tc.c 所需类型的最小闭包（CO-RE 布局与内核一致）。
//
// 只保留 tc.c 实际使用的部分：基础整数类型 + enum bpf_map_type
//（map 定义的 type 参数用）+ 报文头结构 + __sk_buff。
// 不含 UAPI 宏/协议常量（由 tc.c 自行 #define，避免与内核
// UAPI 头重复）。各 struct 体与内核 vmlinux/BTF 逐字段一致（直接从
// bpftool 生成的完整 vmlinux.h 裁剪，未改字段顺序/类型/位域），
// 因此 preserve_access_index 下的 CO-RE 重定位行为不变。
#ifndef __VMLINUX_H__
#define __VMLINUX_H__

#ifndef BPF_NO_PRESERVE_ACCESS_INDEX
#pragma clang attribute push (__attribute__((preserve_access_index)), apply_to = record)
#endif

typedef unsigned char __u8;

typedef short unsigned int __u16;

typedef int __s32;

typedef unsigned int __u32;

typedef long long int __s64;

typedef long long unsigned int __u64;

typedef __int128 unsigned __u128;

typedef __u16 __be16;

typedef __u32 __be32;

typedef __u16 __sum16;

typedef __u32 __wsum;

enum {
	false = 0,
	true = 1,
};

typedef _Bool bool;

enum bpf_map_type {
	BPF_MAP_TYPE_UNSPEC = 0,
	BPF_MAP_TYPE_HASH = 1,
	BPF_MAP_TYPE_ARRAY = 2,
	BPF_MAP_TYPE_PROG_ARRAY = 3,
	BPF_MAP_TYPE_PERF_EVENT_ARRAY = 4,
	BPF_MAP_TYPE_PERCPU_HASH = 5,
	BPF_MAP_TYPE_PERCPU_ARRAY = 6,
	BPF_MAP_TYPE_STACK_TRACE = 7,
	BPF_MAP_TYPE_CGROUP_ARRAY = 8,
	BPF_MAP_TYPE_LRU_HASH = 9,
	BPF_MAP_TYPE_LRU_PERCPU_HASH = 10,
	BPF_MAP_TYPE_LPM_TRIE = 11,
	BPF_MAP_TYPE_ARRAY_OF_MAPS = 12,
	BPF_MAP_TYPE_HASH_OF_MAPS = 13,
	BPF_MAP_TYPE_DEVMAP = 14,
	BPF_MAP_TYPE_SOCKMAP = 15,
	BPF_MAP_TYPE_CPUMAP = 16,
	BPF_MAP_TYPE_XSKMAP = 17,
	BPF_MAP_TYPE_SOCKHASH = 18,
	BPF_MAP_TYPE_CGROUP_STORAGE_DEPRECATED = 19,
	BPF_MAP_TYPE_CGROUP_STORAGE = 19,
	BPF_MAP_TYPE_REUSEPORT_SOCKARRAY = 20,
	BPF_MAP_TYPE_PERCPU_CGROUP_STORAGE_DEPRECATED = 21,
	BPF_MAP_TYPE_PERCPU_CGROUP_STORAGE = 21,
	BPF_MAP_TYPE_QUEUE = 22,
	BPF_MAP_TYPE_STACK = 23,
	BPF_MAP_TYPE_SK_STORAGE = 24,
	BPF_MAP_TYPE_DEVMAP_HASH = 25,
	BPF_MAP_TYPE_STRUCT_OPS = 26,
	BPF_MAP_TYPE_RINGBUF = 27,
	BPF_MAP_TYPE_INODE_STORAGE = 28,
	BPF_MAP_TYPE_TASK_STORAGE = 29,
	BPF_MAP_TYPE_BLOOM_FILTER = 30,
	BPF_MAP_TYPE_USER_RINGBUF = 31,
	BPF_MAP_TYPE_CGRP_STORAGE = 32,
};

struct in6_addr {
	union {
		__u8 u6_addr8[16];
		__be16 u6_addr16[8];
		__be32 u6_addr32[4];
	} in6_u;
};

struct ethhdr {
	unsigned char h_dest[6];
	unsigned char h_source[6];
	__be16 h_proto;
};

struct iphdr {
	__u8 ihl: 4;
	__u8 version: 4;
	__u8 tos;
	__be16 tot_len;
	__be16 id;
	__be16 frag_off;
	__u8 ttl;
	__u8 protocol;
	__sum16 check;
	union {
		struct {
			__be32 saddr;
			__be32 daddr;
		};
		struct {
			__be32 saddr;
			__be32 daddr;
		} addrs;
	};
};

struct ipv6hdr {
	__u8 priority: 4;
	__u8 version: 4;
	__u8 flow_lbl[3];
	__be16 payload_len;
	__u8 nexthdr;
	__u8 hop_limit;
	union {
		struct {
			struct in6_addr saddr;
			struct in6_addr daddr;
		};
		struct {
			struct in6_addr saddr;
			struct in6_addr daddr;
		} addrs;
	};
};

struct tcphdr {
	__be16 source;
	__be16 dest;
	__be32 seq;
	__be32 ack_seq;
	__u16 res1: 4;
	__u16 doff: 4;
	__u16 fin: 1;
	__u16 syn: 1;
	__u16 rst: 1;
	__u16 psh: 1;
	__u16 ack: 1;
	__u16 urg: 1;
	__u16 ece: 1;
	__u16 cwr: 1;
	__be16 window;
	__sum16 check;
	__be16 urg_ptr;
};

struct udphdr {
	__be16 source;
	__be16 dest;
	__be16 len;
	__sum16 check;
};

struct icmpv6_echo {
	__be16 identifier;
	__be16 sequence;
};

struct icmpv6_nd_advt {
	__u32 reserved: 5;
	__u32 override: 1;
	__u32 solicited: 1;
	__u32 router: 1;
	__u32 reserved2: 24;
};

struct icmpv6_nd_ra {
	__u8 hop_limit;
	__u8 reserved: 3;
	__u8 router_pref: 2;
	__u8 home_agent: 1;
	__u8 other: 1;
	__u8 managed: 1;
	__be16 rt_lifetime;
};

struct icmp6hdr {
	__u8 icmp6_type;
	__u8 icmp6_code;
	__sum16 icmp6_cksum;
	union {
		__be32 un_data32[1];
		__be16 un_data16[2];
		__u8 un_data8[4];
		struct icmpv6_echo u_echo;
		struct icmpv6_nd_advt u_nd_advt;
		struct icmpv6_nd_ra u_nd_ra;
	} icmp6_dataun;
};

struct bpf_flow_keys;
struct bpf_sock;

struct __sk_buff {
	__u32 len;
	__u32 pkt_type;
	__u32 mark;
	__u32 queue_mapping;
	__u32 protocol;
	__u32 vlan_present;
	__u32 vlan_tci;
	__u32 vlan_proto;
	__u32 priority;
	__u32 ingress_ifindex;
	__u32 ifindex;
	__u32 tc_index;
	__u32 cb[5];
	__u32 hash;
	__u32 tc_classid;
	__u32 data;
	__u32 data_end;
	__u32 napi_id;
	__u32 family;
	__u32 remote_ip4;
	__u32 local_ip4;
	__u32 remote_ip6[4];
	__u32 local_ip6[4];
	__u32 remote_port;
	__u32 local_port;
	__u32 data_meta;
	union {
		struct bpf_flow_keys *flow_keys;
	};
	__u64 tstamp;
	__u32 wire_len;
	__u32 gso_segs;
	union {
		struct bpf_sock *sk;
	};
	__u32 gso_size;
	__u8 tstamp_type;
	__u64 hwtstamp;
};

#ifndef BPF_NO_PRESERVE_ACCESS_INDEX
#pragma clang attribute pop
#endif

#endif /* __VMLINUX_H__ */
