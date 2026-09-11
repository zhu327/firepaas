// backend.go：slot 数据面后端插件缝（ADR-0040 §10/§11/§12）。
//
// 同一 PolicySnapshot 输入，nft 与 eBPF 后端行为必须一致（§10 四项对等
// 断言：established、私网集、netpolicy 同源、v6 源绑定、代理回流不
// masquerade、限额口径对齐）。slot.Manager 只依赖本接口；具体后端由
// cmd/agentd 按探测结果装配（§12：ebpf 优先、nft fallback）。
package slot

import (
	"context"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// SlotRef 是后端为一个 slot 执行数据面操作所需的全部坐标。
type SlotRef struct {
	Index     int
	VethHost  string // fp-vpN（root 侧）
	VethGuest string // fp-vgN（slot netns 侧）
	HostAddr  string // 10.12.A.B+1（veth host 侧地址 = 网关/代理目标）
	NsAddr    string // 10.12.A.B+2（veth netns 侧地址）
	Netns     string // fp-slot-N
	GuestIP   string // guest IPv4（eBPF host4/egress_slot key）
	// GuestIP6 是 execution 域 ULA（裸地址；空 = 纯 IPv4）。eBPF 后端
	// 用它维护 per-veth 源绑定 map（slot_ula），ADR-0040 §6。
	GuestIP6 string
}

// Backend 是 slot 数据面后端（nft 现役 / eBPF 新后端）。
type Backend interface {
	// EnsureNode 幂等建立节点级设施（nft 隔离表 / bpffs+程序装载）。
	EnsureNode(ctx context.Context) error
	// AttachSlot 为 slot 挂载后端（幂等：重复调用补齐内核状态）。
	AttachSlot(ctx context.Context, ref SlotRef) error
	// DetachSlot 摘除 slot 后端（best-effort；netns 删除随后连带回收）。
	DetachSlot(ctx context.Context, ref SlotRef) error
	// EnsureSlotNAT 幂等建立 slot 内出口 NAT（代理回流不 masquerade 语义）。
	EnsureSlotNAT(ctx context.Context, ref SlotRef) error
	// ApplyEgress 全量替换 slot egress 快照（snap nil = 清除为全放行）。
	ApplyEgress(ctx context.Context, ref SlotRef, snap *api.PolicySnapshot) error
}
