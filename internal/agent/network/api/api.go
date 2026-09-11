// Package api 是 agent 网络数据面的唯一接口包（ADR-0040 §11 G1）。
//
// 三个插件缝：
//
//	Datapath      —— workload netns 的挂载/摘除/对账（现役实现：slot nft 后端；
//	                 后续实现：eBPF tc-ingress/tc-egress datapath）
//	PolicyEngine  —— 一套策略快照的落地（同一 PolicySnapshot 输入，nft 或
//	                 eBPF 行为必须一致，ADR-0040 §10）
//	Underlay      —— 跨节点 underlay（WG mesh；按 ADR-0040 §0.3 管理拍板
//	                 全量推进，FIREPAAS_MESH=eastwest 时装配 wg.Manager）
//
// 依赖纪律（ADR-0040 §11）：machine / proxy / egress / server 只允许 import
// 本包，不得直接 import 具体后端包（如 internal/agent/network/slot）；
// 进程装配（cmd/agentd）是唯一允许同时见到接口与具体实现的组合点。
// LB（SelectBackend）刻意不属于本包——least-inflight 是 edge 职责（ADR-0020）。
package api

import (
	"context"
	"net"
	"net/netip"
)

// SlotBridgeLL6 是 slot bridge 的 v6 链路本地网关（ADR-0040 §7，T6）：
// guest 默认 v6 网关经 hypeman vmconfig GuestGW6 下发同值；slot 在此地址上
// 应答 NDP 并经默认路由上送 root。LL 作用域限于 slot netns，跨 slot 无冲突。
const SlotBridgeLL6 = "fe80::1"

// NetnsSpec 描述一次 workload 挂网（execution 域）所需的全部输入。
// 具体后端用其建立 netns/veth/TAP 接线、隔离与地址。
type NetnsSpec struct {
	MachineID string // firepaas 稳定 machine_id（必填）
	Tap       string // hypeman TAP 名（可为空：无 VM 的泄漏测试）
	GuestIP   string // legacy IPv4 guest 地址（新 slot 必填）
	// GuestIP6 / GuestPrefix6 / GuestGW6 承载 ULA（ADR-0040 §7）。
	// firepaas 在 mesh 启用且控制面已分配 ULA 时下发；后端的 v6 数据面
	// （eBPF ipcache/policy）在 G2 前默认拒绝未知 ULA（fail closed）。
	// GuestIP6 空串语义 = 保持既有 ULA（Resume/复挂不携带），不是清空；
	// 显式清空走 DetachNetns 或新 execution 的完整 attach。
	GuestIP6     string
	GuestPrefix6 int
	GuestGW6     string
}

// NetnsState 是 machine 当前 netns 的观察视图（含已应用策略快照）。
// 由 CurrentNetns 返回；调用方只读，不用于反推业务结论（ADR-0003）。
type NetnsState struct {
	Index     int
	MachineID string
	Tap       string
	GuestIP   string
	Snapshot  PolicySnapshot
}

// LiveInstance 是 Reconcile 时 agent 提供的存活实例视图（ADR-0040 §11：
// “三方对齐”的 live 输入，来自 hypeman 实例清单）。
type LiveInstance struct {
	MachineID string
	Tap       string
	GuestIP   string
	// GuestIP6 是 hypeman 已持久化的 workload ULA（空 = 无 v6）。
	GuestIP6 string
}

// PolicySnapshot 是一套策略快照（ADR-0040 §6：双层身份 + 一套快照，同时管
// 南北与东西）。G1 阶段快照只承载南北 egress 字段（ADR-0027），与 legacy
// slots.json 持久化 JSON 完全兼容；G2 在此结构上扩展 identity/east-west
// 字段（identity_id、EastWest 规则），旧二进制按未知字段忽略（ADR-0023）。
//
// Mode 语义（ADR-0027）："" = 未声明快照；unrestricted | deny_all | allowlist。
// Generation 是 fencing 水位（只升不降；同代幂等）。
type PolicySnapshot struct {
	Mode         string   `json:"mode"`
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`
	DeniedCIDRs  []string `json:"denied_cidrs,omitempty"`
	Domains      []string `json:"domains,omitempty"`       // 归一化域名（重启重建代理用）
	ProxyPort80  int      `json:"proxy_port80,omitempty"`  // 0 = 不代理
	ProxyPort443 int      `json:"proxy_port443,omitempty"` // 0 = 不代理
	MaxTCPConns  uint32   `json:"max_tcp_conns,omitempty"` // 0 = 不限
	AuditAll     bool     `json:"audit_all,omitempty"`
	Generation   uint64   `json:"generation,omitempty"`
}

// Datapath 是 workload 网络数据面的挂载/摘除/对账插件缝。
type Datapath interface {
	// AttachNetns 为 machine 建立（或幂等补齐）slot netns。同一 machine
	// 重复调用幂等：复用已有 slot，仅重放内核接线（CNI ADD 语义）。
	AttachNetns(ctx context.Context, spec NetnsSpec) error
	// DetachNetns 回收 machine 的 slot netns（不存在时 no-op）。
	DetachNetns(ctx context.Context, machineID string) error
	// Check 对单个 slot 做 CNI CHECK 语义校验（幂等补齐内核接线）。
	// slot 不存在时报错——CHECK 不新建网络。
	Check(ctx context.Context, spec NetnsSpec) error
	// Reconcile 启动/周期三方对齐（live 实例 ↔ 持久化状态 ↔ 内核）。
	// 单个 slot 的错误降级不崩溃（M3 纪律），但不可恢复的流程错误要上抛。
	Reconcile(ctx context.Context, live []LiveInstance) error
	// CurrentNetns 返回 machine 当前 netns 观察视图（不存在时 ok=false）。
	CurrentNetns(machineID string) (NetnsState, bool)
}

// PolicyEngine 是一套策略快照的落地插件缝。全量替换 + generation fencing。
type PolicyEngine interface {
	// ApplySnapshot 全量替换 machine 的策略快照。新 generation 小于已应用
	// 值 → 拒绝并保持旧快照；同代幂等。
	ApplySnapshot(ctx context.Context, machineID string, snap PolicySnapshot) error
	// RemoveSnapshot 清空 machine 的策略快照（恢复默认数据面语义）。
	RemoveSnapshot(ctx context.Context, machineID string) error
	// CurrentSnapshot 返回 machine 当前已应用快照（未声明时 ok=false）。
	CurrentSnapshot(machineID string) (PolicySnapshot, bool)
	// RollbackSnapshot 无 fencing 恢复快照，仅供串行化的策略协调器在
	// “内核已提交但上游发布失败”时回滚使用。
	RollbackSnapshot(ctx context.Context, machineID string, snap PolicySnapshot, present bool) error
	// RestoreSnapshot 重启后按持久化状态幂等重放快照。
	RestoreSnapshot(ctx context.Context, machineID string) error
}

// Peer 是一条跨节点 underlay 路由条目（ADR-0040 §9）。AllowedIPs 按 node
// /64 聚合，不逐实例建 peer。MESH=disabled 的节点不注册、不推送（见 fabric reconciler）。
type Peer struct {
	NodeID           string
	Pubkey           string // WireGuard 公钥（base64）
	Endpoint         string // host:port
	NodePrefix       netip.Prefix
	FabricGeneration uint64
}

// Underlay 是跨节点连通插件缝（ADR-0040 §9：内核 WireGuard，WG 自身 UDP
// 封装为唯一隧道层）。实现见 internal/agent/network/wg，由 cmd/agentd 在
// FIREPAAS_MESH=eastwest 时装配；disabled 时不建设备。
type Underlay interface {
	// UpdatePeers 全量替换 peer 集合；fabric_generation 只升不降，
	// 旧 generation 重放必须拒绝（节点级 fencing，G0-2）。
	UpdatePeers(ctx context.Context, generation uint64, peers []Peer) error
	// DialULA 建立到对端 workload ULA 的连接（探测/诊断用；数据面转发
	// 不经过此调用）。
	DialULA(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error)
}

// FabricPolicyEntry 是一条东西向放行条目（G2a §15）：src/dst 稳定身份 +
// 端口集。dst 身份的 mesh_direct 前置由快照组装方保证（缺一不可），
// 此处不再校验。
//
// SrcPorts 区分端口集方向：false = Ports 是目的端口（正向规则）；true =
// Ports 是源端口（对称回程规则，把回程从“任意端口”收窄到服务端口）。
type FabricPolicyEntry struct {
	SrcIdentity uint32
	DstIdentity uint32
	Generation  uint64
	Ports       []uint32
	SrcPorts    bool
}

// FabricPolicySnapshot 是一套 fabric 策略快照（G2a）：ipcache 填充输入 +
// policy 全量。Entries 为空 = 默认 deny（调用方清空已下发条目）。NodeULA
// 是本节点自身 ULA（裸地址；空 = 未启用平台放行）。
type FabricPolicySnapshot struct {
	Generation uint64
	NodeULA    string
	Identities []IdentityMapEntry
	Entries    []FabricPolicyEntry
}

// FabricPolicyWriter 是 fabric 策略快照到 eBPF policy map 的写入插件缝
// （G2a）。全量替换语义（与 ApplyFabric 全量快照对应，幂等可重放）；
// nil 后端（nft emergency，无 v6 数据面）下调用方直接跳过。
type FabricPolicyWriter interface {
	ApplyFabricPolicy(ctx context.Context, snap FabricPolicySnapshot) error
}

// IdentityMapEntry 是 fabric 快照 identity 集合到数据面 ipcache 的映射项
// （ADR-0040 §10：ipcache(ULA → identity_id) 的填充输入；T5 eBPF 后端消费，
// nft 后端忽略——nft 不做 v6 东西向）。
type IdentityMapEntry struct {
	IdentityID uint32
	ULA        string
}
