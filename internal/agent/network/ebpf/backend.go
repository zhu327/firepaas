// Package ebpf 是 slot 数据面的 eBPF 后端（ADR-0040 §10/§12，T5）：
// tc-ingress（host veth，隔离+源绑定+东西向策略）、tc-egress（slot veth，
// CIDR/redirect/conn-limit）、tc_host_egress（代理回流反向 NAT）。
//
// 挂载点/port 对照图见 bpf/tc.c 头注释（§10 要求的唯一权威图）。
//
// 后端选择（§12）：FIREPAAS_NETWORK_BACKEND=ebpf 时探测（BTF + 装载），
// 失败按 FIREPAAS_EBPF_FALLBACK（默认 true）回落 nft 并上报
// network.nftfallback.v1；ebpf 可用则上报 network.ebpf.v1（二选一）。
package ebpf

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go@v0.18.0 -cc clang -cflags "-O2 -Wall -Werror -D__TARGET_ARCH_x86" -target amd64 tc bpf/tc.c -- -I bpf

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
)

// PinRoot 是 maps/programs 的 bpffs 固定目录。
const PinRoot = "/sys/fs/bpf/firepaas"

// Options 装配 eBPF 后端。
//
// VethCIDR/RootNATTable（T3）：eBPF 后端不建 nft fp-isolation 表，因此
// 必须自己保证 root 对 slot veth 源段的出口 masquerade（ADR-0040 §7
// “南北出口仍节点 SNAT”）；VethCIDR 缺省 = slot.VethRange，
// RootNATTable 缺省 = "fp-egress"。
type Options struct {
	PinDir         string // 默认 PinRoot
	EgressProxy80  int    // 透明代理 HTTP 端口（0 = 不代理）
	EgressProxy443 int
	// VethCIDR 是 root↔netns veth 源段（slot.Config.VethCIDR 同值）。
	VethCIDR string
	// RootNATTable 是 root 出口 NAT 表名（多 agent 同主机可独立，避免
	// 表名冲突；规则按 CIDR 追加，互不覆盖）。
	RootNATTable string
	// FlowSampleEvery 是 allow flow 事件的采样模数：0 = 默认 128，1 = 全量。
	// deny 事件始终全量。
	FlowSampleEvery uint32
	// Observer（G3，§21）：低基数观测缝；nil = no-op。
	Observer Observer
}

// Backend 实现 slot.Backend（同一 PolicySnapshot 输入，行为与 nft 对等）。
// egress 策略按 slot 分片（W1）：egress_allow/deny 是 map-in-map（外层 key =
// guest IPv4，内层 LPM 做 CIDR 匹配），egress_mode 是 per-slot 默认动作 +
// 代理声明。无外层条目 = 未下发快照 = unrestricted（与 nft 无表 = accept
// 同语义）。内层表不 pin：随 agentd 进程存在，重启后由 slot.Manager 按持久化
// s.Egress 经 Reconcile/RestoreSnapshot 重放（外层 pin 复用到的旧条目指向
// 已死内层 → 查空 → 放行，与重启前“空表=放行”等价，重放后收敛）。
type Backend struct {
	pinDir       string
	port80       int
	port443      int
	vethCIDR     string
	rootNATTable string
	observer     Observer

	objs         *tcObjects
	host4        *ebpf.Map
	egressAllow  *ebpf.Map     // HASH_OF_MAPS<slotKey, LPM>（per-slot allow）
	egressDeny   *ebpf.Map     // HASH_OF_MAPS<slotKey, LPM>（per-slot deny）
	egressMode   *ebpf.Map     // HASH<slotKey, egressModeVal>（per-slot 默认动作）
	egressSlot   *ebpf.Map     // HASH<saddr, slotKey>（saddr 归一，见 tc_egress 注释）
	innerLpmSpec *ebpf.MapSpec // 内层 LPM 模板（loadTc 推导，建表用）
	// egressInners 跟踪已建内层表句柄（FD 回收用；内核对象由外层引用续命，
	// 替换/删除时关闭旧句柄避免 FD 泄漏）。key = 规范 slot key。
	egressInnerAllow map[uint32]*ebpf.Map
	egressInnerDeny  map[uint32]*ebpf.Map
	slotULA          *ebpf.Map // HASH<ifindex, 16B ULA>（per-veth v6 源绑定）
	// fabric 策略双缓冲（W2-2）：datapath 每次查表前读 fabric_active，
	// Go 写非活动集后一次翻转；写失败保持旧集（datapath 不受影响）。
	fabricActive  *ebpf.Map
	ipcacheV0     *ebpf.Map
	ipcacheV1     *ebpf.Map
	policyV0      *ebpf.Map
	policyV1      *ebpf.Map
	policyPortsV0 *ebpf.Map
	policyPortsV1 *ebpf.Map
	nodeULAV0     *ebpf.Map
	nodeULAV1     *ebpf.Map
	flowSample    *ebpf.Map
	hostAddrs4    *ebpf.Map
	flows         *ebpf.Map
	priv4         *ebpf.Map
	// fabricStats 是最近一次成功应用的 fabric 策略统计（状态面；
	// 双 map 原子切换落地前，用于回答“当前生效的是哪一代、规模多大”）。
	// fabricApplyFailures 是 ApplyFabricPolicy 校验/落表失败累计（gauge
	// 语义的单调计数；失败保持旧 map 不动——fail-closed，不断流）。
	statsMu             sync.Mutex
	fabricStats         FabricStats
	fabricApplyFailures uint64
}

// FabricStats 是最近一次成功应用的 fabric 策略快照统计（纯观测）。
type FabricStats struct {
	Generation      uint64
	IdentityEntries int
	PolicyEntries   int
	PortEntries     int
	UpdatedAt       time.Time
}

// FabricStats 返回最近一次成功应用的 fabric 策略统计拷贝（零值 = 尚未应用）。
func (b *Backend) FabricStats() FabricStats {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.fabricStats
}

// FabricApplyFailures 返回 ApplyFabricPolicy 累计失败次数（单调增；
// 失败时旧 map 保持不动——fail-closed，不断流，重试后收敛）。
func (b *Backend) FabricApplyFailures() uint64 {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.fabricApplyFailures
}

// summarizeFabricPolicy 把策略快照折叠为状态面统计（纯函数，可单测；
// 内核落表仍由 ApplyFabricPolicy 完成——统计只在落表成功后更新）。
func summarizeFabricPolicy(snap api.FabricPolicySnapshot, now time.Time) FabricStats {
	ports := 0
	for _, e := range snap.Entries {
		ports += len(e.Ports)
	}
	return FabricStats{
		Generation:      snap.Generation,
		IdentityEntries: len(snap.Identities),
		PolicyEntries:   len(snap.Entries),
		PortEntries:     ports,
		UpdatedAt:       now,
	}
}

// compile-time 契约：Backend 实现 slot.Backend 与 api.FabricPolicyWriter。
var (
	_ slot.Backend           = (*Backend)(nil)
	_ api.FabricPolicyWriter = (*Backend)(nil)
)

// Probe 探测本机 eBPF 能力：BTF 存在、对象可装载、bpffs 可挂载/固定。
// 探测只装载一次对象（不 attach），失败即回落依据（§12 fail closed）。
func Probe() error {
	if err := rlimit.RemoveMemlock(); err != nil {
		return fmt.Errorf("remove memlock: %w", err)
	}
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err != nil {
		return fmt.Errorf("kernel BTF unavailable: %w", err)
	}
	objs := &tcObjects{}
	if err := loadTcObjects(objs, nil); err != nil {
		return fmt.Errorf("load bpf objects: %w", err)
	}
	objs.Close()
	if err := ensureBpffs(PinRoot); err != nil {
		return fmt.Errorf("bpffs: %w", err)
	}
	return nil
}

// New 构造后端并装载/pin 对象（调用前必须先 Probe 成功）。失败 = 调用方回落。
func New(opts Options) (*Backend, error) {
	b := &Backend{
		pinDir:           opts.PinDir,
		port80:           opts.EgressProxy80,
		port443:          opts.EgressProxy443,
		vethCIDR:         strings.TrimSpace(opts.VethCIDR),
		rootNATTable:     strings.TrimSpace(opts.RootNATTable),
		observer:         opts.Observer,
		egressInnerAllow: map[uint32]*ebpf.Map{},
		egressInnerDeny:  map[uint32]*ebpf.Map{},
	}
	if b.vethCIDR == "" {
		b.vethCIDR = slot.VethRange
	}
	if b.rootNATTable == "" {
		b.rootNATTable = "fp-egress"
	}
	if b.observer == nil {
		b.observer = noopObserver{}
	}
	if b.pinDir == "" {
		b.pinDir = PinRoot
	}
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, err
	}
	if err := ensureBpffs(b.pinDir); err != nil {
		return nil, err
	}
	objs := &tcObjects{}
	if err := loadTcObjects(objs, &ebpf.CollectionOptions{
		Maps: ebpf.MapOptions{PinPath: b.pinDir},
	}); err != nil {
		return nil, fmt.Errorf("load bpf objects: %w", err)
	}
	b.objs = objs
	b.host4 = objs.Host4
	b.egressAllow = objs.EgressAllow
	b.egressDeny = objs.EgressDeny
	b.egressMode = objs.EgressMode
	b.egressSlot = objs.EgressSlot
	b.slotULA = objs.SlotUla
	b.fabricActive = objs.FabricActive
	b.ipcacheV0 = objs.IpcacheV0
	b.ipcacheV1 = objs.IpcacheV1
	b.policyV0 = objs.PolicyV0
	b.policyV1 = objs.PolicyV1
	b.policyPortsV0 = objs.PolicyPortsV0
	b.policyPortsV1 = objs.PolicyPortsV1
	b.nodeULAV0 = objs.NodeUlaV0
	b.nodeULAV1 = objs.NodeUlaV1
	b.flowSample = objs.FlowSample
	b.hostAddrs4 = objs.HostAddrs4
	b.flows = objs.FlowEvents
	b.priv4 = objs.Private4

	// 内层 LPM 模板（建 per-slot 内层表用；loadTc 与装载同一 ELF，模板一致）。
	spec, err := loadTc()
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("load bpf spec: %w", err)
	}
	inner := spec.Maps["egress_allow"].InnerMap
	if inner == nil {
		objs.Close()
		return nil, fmt.Errorf("egress_allow has no inner map template")
	}
	b.innerLpmSpec = inner
	// 旧版表/单缓冲表的 pin 残留回收：spec 已更名，留着只是 bpffs 垃圾
	//（运行中的旧程序持有 fd，删除 pin 不影响其工作）。
	for _, stale := range []string{
		"cidr_allow4", "cidr_deny4", "mode_drop",
		"conn_cap", "conn_count", "syn_seen",
		"ipcache", "policy", "policy_ports", "node_ula",
	} {
		_ = os.Remove(filepath.Join(b.pinDir, stale))
	}
	// allow 事件采样模数（<=1 = 全量；默认 128）。
	sampleEvery := opts.FlowSampleEvery
	if sampleEvery == 0 {
		sampleEvery = 128
	}
	if sampleEvery < 1 {
		sampleEvery = 1
	}
	if err := b.flowSample.Put(uint32(0), sampleEvery); err != nil {
		return nil, fmt.Errorf("set flow sample: %w", err)
	}
	// 代理端口配置（一次性写死，全节点同值）。
	if err := objs.ProxyPorts.Put(uint32(0), uint16(b.port80)); err != nil {
		return nil, fmt.Errorf("set proxy port 80: %w", err)
	}
	if err := objs.ProxyPorts.Put(uint32(1), uint16(b.port443)); err != nil {
		return nil, fmt.Errorf("set proxy port 443: %w", err)
	}
	// 程序 pin：tc filter 以 pinned 路径挂载（agentd 重启幂等替换）。
	for name, prog := range map[string]*ebpf.Program{
		"tc_ingress": objs.TcIngress,
		"tc_egress":  objs.TcEgress,
	} {
		path := filepath.Join(b.pinDir, name)
		if err := prog.Pin(path); err != nil {
			// 已存在（前次进程残留）：先摘后 pin。
			_ = os.Remove(path)
			if err2 := prog.Pin(path); err2 != nil {
				return nil, fmt.Errorf("pin program %s: %w", name, err2)
			}
		}
	}
	// 私网/保留集：与 nft 同源（netpolicy canonical 集合）。
	for _, cidr := range slot.PrivateCIDRs() {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("private4 %s: %w", cidr, err)
		}
		if err := b.putLPM(b.priv4, p); err != nil {
			return nil, fmt.Errorf("private4 %s: %w", cidr, err)
		}
	}
	return b, nil
}

// Close 关闭对象（maps 保持 pinned，程序 FDs 释放）。
func (b *Backend) Close() error {
	if b.objs != nil {
		b.objs.Close()
	}
	return nil
}

// EnsureNode 幂等：bpffs + 对象已装载（New 完成），无需每 slot 建表。
func (b *Backend) EnsureNode(ctx context.Context) error {
	// root 出口 SNAT：eBPF 后端不建 nft fp-isolation 表，slot 内一级 NAT
	// 改写后的源（链路地址）必须在本机做 masquerade 才能获得公网回程
	// （ADR-0040 §7）。幂等、按 CIDR 追加，多 agent 同表互不覆盖。
	if err := slot.EnsureRootEgressNAT(ctx, b.rootNATTable, b.vethCIDR); err != nil {
		return err
	}
	// 遗留 ip6 隔离表回收：nft fp-isolation（ip6）的 in/fwd 链对 slot-veths
	// 集合无条件 drop——v6 无 NAT/established 例外，是为纯 v4 时代设计的。
	// ADR-0040 后 v6 策略点在 tc_ingress（identity fail-closed），该表只会在
	// eBPF 节点上残留 nft 时代的陈旧元素：veth 名按 slot 序号复用（fp-vp1），
	// 旧 agent 的集合元素会静默杀死新 agent 同名 slot 的全部入站 v6（NDP 也
	// 被吞，真机实测）。eBPF 模式从不写入该表，删除即收敛；nft-fallback 节点
	//（不入 mesh）由 NftBackend 自建自管，不与 eBPF 双活同主机（ADR-0040）。
	if out, err := exec.CommandContext(ctx, "nft", "delete", "table", "ip6", "fp-isolation").CombinedOutput(); err != nil &&
		!strings.Contains(string(out), "no such table") {
		slog.Warn("ebpf: cleanup legacy nft ip6 isolation table", "error", err,
			"output", strings.TrimSpace(string(out)))
	}
	// host_addrs4：根 netns 已分配 v4 地址集（v4 INPUT 语义的“本机”判定，
	// 见 tc.c host_bound_established）。幂等全量替换（地址增减后 re-ensure
	// 收敛；低频调用）。
	addrs, err := rootIPv4Addrs()
	if err != nil {
		return fmt.Errorf("ebpf: enumerate host addrs: %w", err)
	}
	if err := clearHashMap[uint32, uint8](b.hostAddrs4); err != nil {
		return err
	}
	for _, a := range addrs {
		if err := b.hostAddrs4.Put(a, uint8(1)); err != nil {
			return fmt.Errorf("ebpf: host_addrs4: %w", err)
		}
	}
	return nil
}

// rootIPv4Addrs 枚举根 netns 全部已分配 IPv4 地址（LE u32 序列化，与内核
// daddr 的 map 读值同口径）。
func rootIPv4Addrs() ([]uint32, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []uint32
	seen := map[uint32]bool{}
	for _, ifc := range ifs {
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipn.IP.To4()
			if ip4 == nil || ip4.IsLoopback() {
				continue // loopback 不达 tc；127/8 保留集仍在 private4
			}
			v := binary.LittleEndian.Uint32(ip4)
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
	}
	return out, nil
}

// AttachSlot 为 slot 挂载两个 tc 程序（host veth ingress + ns veth egress）。
func (b *Backend) AttachSlot(ctx context.Context, ref slot.SlotRef) error {
	// G3（§21）：attach 失败计数（低基数，无 label）。defer 简洁胜于每个
	// return 前手动打点（观测不得影响数据面路径）。
	defer func() {
		if err := recover(); err != nil {
			b.observer.AttachError()
			panic(err)
		}
	}()
	err := b.attachSlot(ctx, ref)
	if err != nil {
		b.observer.AttachError()
	}
	return err
}

// attachSlot 是 AttachSlot 的实现体（观测包装之下）。
func (b *Backend) attachSlot(ctx context.Context, ref slot.SlotRef) error {
	if err := ensureClsact(ctx, "", ref.VethHost); err != nil {
		return fmt.Errorf("ebpf: host clsact: %w", err)
	}
	// host 侧 ingress（隔离）；代理回流反向 NAT 由 slot 内 nft DNAT +
	// conntrack 提供（§10 附图注明的机制分工）。
	if err := tcFilter(ctx, "", ref.VethHost, "ingress", "tc_ingress", b.pinDir); err != nil {
		return err
	}
	// slot 内 veth egress（CIDR/redirect/conn-limit）。
	if err := ensureClsact(ctx, ref.Netns, ref.VethGuest); err != nil {
		return fmt.Errorf("ebpf: slot clsact: %w", err)
	}
	if err := tcFilter(ctx, ref.Netns, ref.VethGuest, "egress", "tc_egress", b.pinDir); err != nil {
		return err
	}
	// host4：规范 slot key（链路地址）→ host 侧 veth 地址（BE）+ guest IP →
	// 同值双条目。tc_egress 看到的是 POST-masquerade saddr（链路地址）与未
	// masquerade 的网关/代理包 saddr（guest IP），两种源都要能做网关判定。
	guestKey, err := ipv4Key(ref.GuestIP)
	if err != nil {
		return err
	}
	slotKey, err := slotEgressKey(ref)
	if err != nil {
		return err
	}
	hostAddr := net.ParseIP(ref.HostAddr).To4()
	if hostAddr == nil {
		return fmt.Errorf("ebpf: bad host addr %q", ref.HostAddr)
	}
	hostVal := binary.LittleEndian.Uint32(hostAddr)
	if err := b.host4.Put(guestKey, hostVal); err != nil {
		return fmt.Errorf("ebpf: host4: %w", err)
	}
	if err := b.host4.Put(slotKey, hostVal); err != nil {
		return fmt.Errorf("ebpf: host4: %w", err)
	}
	// saddr 归一：guest IP 与链路地址都指向规范 slot key（见 tc_egress 注释）。
	if err := b.egressSlot.Put(guestKey, slotKey); err != nil {
		return fmt.Errorf("ebpf: egress_slot: %w", err)
	}
	if err := b.egressSlot.Put(slotKey, slotKey); err != nil {
		return fmt.Errorf("ebpf: egress_slot: %w", err)
	}
	// per-veth v6 源绑定：veth ifindex → 本 slot 期望 ULA（空 = 清除）。
	if err := b.putSlotULA(ref); err != nil {
		return err
	}
	return nil
}

// putSlotULA 维护 slot_ula（ADR-0040 §6：v6 源绑定必须绑定到本 veth，
// ipcache 命中不证明报文来自拥有该 ULA 的 slot）。空 GuestIP6 = 删除条目，
// 该 veth 的 v6 全拒（fail closed；纯 v4 slot 零回归）。
func (b *Backend) putSlotULA(ref slot.SlotRef) error {
	idx, err := ifaceIndex(ref.VethHost)
	if err != nil {
		return fmt.Errorf("ebpf: slot_ula resolve %s: %w", ref.VethHost, err)
	}
	raw := strings.TrimSpace(ref.GuestIP6)
	if raw == "" {
		if err := b.slotULA.Delete(idx); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
			return fmt.Errorf("ebpf: slot_ula clear: %w", err)
		}
		return nil
	}
	addr, err := netip.ParseAddr(raw)
	if err != nil || !addr.Is6() || addr.Is4In6() {
		return fmt.Errorf("ebpf: slot_ula invalid ULA %q for %s", raw, ref.VethHost)
	}
	ula := addr.As16()
	if err := b.slotULA.Put(idx, ula); err != nil {
		return fmt.Errorf("ebpf: slot_ula put %s: %w", addr, err)
	}
	return nil
}

// ifaceIndex 解析 host veth 的 ifindex（tc_ingress 以 skb->ifindex 查询）。
func ifaceIndex(name string) (uint32, error) {
	ifc, err := net.InterfaceByName(name)
	if err != nil {
		return 0, err
	}
	return uint32(ifc.Index), nil
}

// slotEgressKey 本 slot 在数据面的规范 key：POST-masquerade 源地址（slot 链路
// 地址 NsAddr）。背景见 tc_egress 头注释的 hook-order 说明：tc egress 看到的是
// masquerade 后的 saddr，nft fwd 看到的是 pre-NAT saddr，两端 key 不同但 1:1
// 对应同一 slot，裁决 outcome 一致。
func slotEgressKey(ref slot.SlotRef) (uint32, error) {
	if ref.NsAddr == "" {
		return 0, fmt.Errorf("ebpf: slot %d has no link addr", ref.Index)
	}
	return ipv4Key(ref.NsAddr)
}

// DetachSlot 摘除 slot 的 per-slot 配置：host4、egress 外层条目 + mode +
// 内层表句柄（netns 删除连带 tc 程序回收）。
// 内层内核对象由外层条目引用续命，删除外层条目后引用释放；Go 句柄关闭防 FD 泄漏。
func (b *Backend) DetachSlot(ctx context.Context, ref slot.SlotRef) error {
	if idx, err := ifaceIndex(ref.VethHost); err == nil {
		_ = b.slotULA.Delete(idx)
	}
	if guestKey, err := ipv4Key(ref.GuestIP); err == nil {
		_ = b.host4.Delete(guestKey)
		_ = b.egressSlot.Delete(guestKey)
	}
	if slotKey, err := slotEgressKey(ref); err == nil {
		_ = b.host4.Delete(slotKey)
		_ = b.egressSlot.Delete(slotKey)
		_ = b.egressAllow.Delete(slotKey)
		_ = b.egressDeny.Delete(slotKey)
		_ = b.egressMode.Delete(slotKey)
		if inner, ok := b.egressInnerAllow[slotKey]; ok {
			_ = inner.Close()
			delete(b.egressInnerAllow, slotKey)
		}
		if inner, ok := b.egressInnerDeny[slotKey]; ok {
			_ = inner.Close()
			delete(b.egressInnerDeny, slotKey)
		}
	}
	return nil
}

// EnsureSlotNAT：slot 内一级 NAT（masquerade）+ 代理 prerouting DNAT 由
// nft fp-slot 表提供（与 nft 后端共享实现）。proxy redirect 不重写 L3/L4
// （TC 位置无法可靠维护校验和），由 conntrack 自动反向 NAT——"代理回流不
// masquerade" 断言在该路径上成立。
func (b *Backend) EnsureSlotNAT(ctx context.Context, ref slot.SlotRef) error {
	return slot.EnsureNetnsNAT(ctx, ref, b.port80, b.port443)
}

// egressModeVal 与 BPF 侧 struct egress_mode_val 同布局。
type egressModeVal struct {
	Drop     uint8
	Proxy80  uint8
	Proxy443 uint8
	_        uint8
}

// connAggKey 已随 BPF 连接计数移除（W2-1）：限额改由 slot netns 的
// nft `ct count` 执行（见 slot.EnsureNetnsTCPLimit）。

// ApplyEgress 全量替换 slot egress 快照（与 nft 同语义：deny → allow →
// mode 默认；redirect 先于 deny/allow，等价 nft prerouting DNAT 顺序）。
// per-slot 分片（W1）：只动本 slot（规范 slot key）的外层条目 + mode，多 slot
// 互不覆盖。snap nil = 清除（删条目回到“未下发=unrestricted”，等价 nft 清表）。
func (b *Backend) ApplyEgress(ctx context.Context, ref slot.SlotRef, snap *api.PolicySnapshot) error {
	slotKey, err := slotEgressKey(ref)
	if err != nil {
		return err
	}
	if snap == nil {
		// 先落限额（标量、幂等、不碰 egress map）：失败即中止，数据面保持旧
		// CIDR 策略，不产生“策略已切但限额还是旧值”的半应用状态。
		if err := slot.EnsureNetnsTCPLimit(ctx, ref, 0); err != nil {
			return err
		}
		b.clearEgressSlot(slotKey)
		return nil
	}
	allow, err := b.egressInner(slotKey, true)
	if err != nil {
		return err
	}
	deny, err := b.egressInner(slotKey, false)
	if err != nil {
		return err
	}
	// 先整体校验再落表：非法 CIDR/未知 mode 直接拒绝整快照（fail closed，
	// 不残留半写表；调用方 ledger claim 保持 in-progress 重试）。
	denied, err := parsePrefixes(snap.DeniedCIDRs, "denied")
	if err != nil {
		return err
	}
	allowed, err := parsePrefixes(snap.AllowedCIDRs, "allowed")
	if err != nil {
		return err
	}
	var mode egressModeVal
	switch snap.Mode {
	case "unrestricted", "":
		allowed = append(allowed, netip.MustParsePrefix("0.0.0.0/0"))
	case "deny_all", "allowlist":
		mode.Drop = 1
	default:
		return fmt.Errorf("ebpf: unknown egress mode %q", snap.Mode)
	}
	if snap.ProxyPort80 > 0 {
		mode.Proxy80 = 1
	}
	if snap.ProxyPort443 > 0 {
		mode.Proxy443 = 1
	}
	// 先落限额（标量、幂等、不碰 egress map）：失败即中止，数据面保持旧
	// CIDR 策略，不产生“策略已切但限额还是旧值”的半应用状态。
	if err := slot.EnsureNetnsTCPLimit(ctx, ref, snap.MaxTCPConns); err != nil {
		return fmt.Errorf("ebpf: tcp limit: %w", err)
	}
	// 落表：内层先清后写，再挂外层，最后写 mode（读端任一中间态仍是旧
	// 快照或空条目；空外层条目 = unrestricted，崩溃窗口 fail-open 等价
	// 重启前空表语义，重放后收敛——见 Backend 注释）。
	if err := clearHashMap[lpm4Key, uint8](deny); err != nil {
		return err
	}
	if err := clearHashMap[lpm4Key, uint8](allow); err != nil {
		return err
	}
	for _, p := range denied {
		if err := b.putLPM(deny, p); err != nil {
			return err
		}
	}
	for _, p := range allowed {
		if err := b.putLPM(allow, p); err != nil {
			return err
		}
	}
	if err := b.egressDeny.Put(slotKey, deny); err != nil {
		return fmt.Errorf("ebpf: attach deny inner: %w", err)
	}
	if err := b.egressAllow.Put(slotKey, allow); err != nil {
		return fmt.Errorf("ebpf: attach allow inner: %w", err)
	}
	if err := b.egressMode.Put(slotKey, mode); err != nil {
		return fmt.Errorf("ebpf: set egress mode: %w", err)
	}
	return nil
}

// egressInner 取本 slot 内层 LPM 表（不存在即建；建表失败 fail closed）。
func (b *Backend) egressInner(slotKey uint32, allow bool) (*ebpf.Map, error) {
	track := b.egressInnerAllow
	if !allow {
		track = b.egressInnerDeny
	}
	if m, ok := track[slotKey]; ok {
		return m, nil
	}
	m, err := ebpf.NewMap(b.innerLpmSpec)
	if err != nil {
		return nil, fmt.Errorf("ebpf: new egress inner map: %w", err)
	}
	track[slotKey] = m
	return m, nil
}

// clearEgressSlot 删除本 slot 的外层条目 + mode + 内层句柄（回到未下发态）。
func (b *Backend) clearEgressSlot(slotKey uint32) {
	_ = b.egressAllow.Delete(slotKey)
	_ = b.egressDeny.Delete(slotKey)
	_ = b.egressMode.Delete(slotKey)
	if inner, ok := b.egressInnerAllow[slotKey]; ok {
		_ = inner.Close()
		delete(b.egressInnerAllow, slotKey)
	}
	if inner, ok := b.egressInnerDeny[slotKey]; ok {
		_ = inner.Close()
		delete(b.egressInnerDeny, slotKey)
	}
}

func parsePrefixes(cidrs []string, what string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, cidr := range cidrs {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("ebpf: %s cidr %q: %w", what, cidr, err)
		}
		if !p.Addr().Is4() {
			return nil, fmt.Errorf("ebpf: %s cidr %q is not IPv4", what, cidr)
		}
		out = append(out, p)
	}
	return out, nil
}

// policyKey/policyVal/policyPortKey 与 BPF 侧 struct policy_key/policy_val/
// policy_port_key 同布局（cilium/ebpf 按声明顺序序列化；_ 填充须具名，
// encoding/binary 会跳过匿名空白字段导致字节错位）。
type policyKey struct {
	Src uint32
	Dst uint32
}

type policyVal struct {
	Generation uint64
	// Flags 是端口白名单方向（与 BPF 侧 policy_val.flags 同布局）：
	//   policyFlagDport = ports 为目的端口（正向规则）
	//   policyFlagSport = ports 为源端口（对称回程规则）
	// flags=0 的条目只放行 ICMPv6；UDP 遇 flags=0 保守 deny。
	Flags uint64
}

const (
	policyFlagDport uint64 = 1 << 0
	policyFlagSport uint64 = 1 << 1
)

type policyPortKey struct {
	Src  uint32
	Dst  uint32
	Port uint16
	Dir  uint16 // 0 = dport 白名单；1 = sport 白名单
}

// mergePolicyEntries 把快照条目折叠为数据面写入形态（纯函数，便于单测）：
// 同一 (src,dst) 可能同时有正向（DPORT）与回程（SPORT）条目（双向 EastWest
// 声明）——flags 必须按位合并，不能让后写覆盖前写（否则丢端口约束或把回程
// 放宽成任意端口）；端口按 dir 分开存储，两个方向互不覆盖。
func mergePolicyEntries(entries []api.FabricPolicyEntry) (map[policyKey]policyVal, []policyPortKey) {
	policies := make(map[policyKey]policyVal, len(entries))
	var ports []policyPortKey
	for _, e := range entries {
		k := policyKey{Src: e.SrcIdentity, Dst: e.DstIdentity}
		v := policies[k]
		if e.Generation > v.Generation {
			v.Generation = e.Generation
		}
		if len(e.Ports) > 0 {
			if e.SrcPorts {
				v.Flags |= policyFlagSport
			} else {
				v.Flags |= policyFlagDport
			}
		}
		policies[k] = v
		dir := uint16(0)
		if e.SrcPorts {
			dir = 1
		}
		for _, p := range e.Ports {
			ports = append(ports, policyPortKey{
				Src: e.SrcIdentity, Dst: e.DstIdentity, Port: uint16(p), Dir: dir,
			})
		}
	}
	return policies, ports
}

// clearHashMap 全量清空一个 HASH map（快照全量替换用；先收集后删除，
// 避免边迭代边删除的游标语义）。
func clearHashMap[K any, V any](m *ebpf.Map) error {
	var zeroK K
	var zeroV V
	it := m.Iterate()
	keys := make([]K, 0)
	for it.Next(&zeroK, &zeroV) {
		keys = append(keys, zeroK)
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("ebpf: iterate map: %w", err)
	}
	for _, k := range keys {
		if err := m.Delete(k); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
			return fmt.Errorf("ebpf: clear map entry: %w", err)
		}
	}
	return nil
}

// ApplyFabricPolicy 全量替换 fabric 策略快照（G2a）：ipcache + policy +
// policy_ports 三表先清后写（快照是全集；下线 execution 的映射/条目必须
// 消失，否则已撤销身份仍被信任）。输入先整体校验再落 map：非法端口直接
// 拒绝整快照（fail closed，调用方 ledger claim 保持 in-progress 重试）。
// 双缓冲（W2-2）：先写非活动集，最后翻转 fabric_active——datapath 只会
// 看到完整旧集或完整新集；中途失败不翻转，旧集继续生效且返回错误重试。
func (b *Backend) ApplyFabricPolicy(ctx context.Context, snap api.FabricPolicySnapshot) (err error) {
	defer func() {
		b.statsMu.Lock()
		defer b.statsMu.Unlock()
		if err != nil {
			b.fabricApplyFailures++
			return
		}
		b.fabricStats = summarizeFabricPolicy(snap, time.Now().UTC())
	}()
	for _, id := range snap.Identities {
		if _, err := netip.ParseAddr(id.ULA); err != nil {
			return fmt.Errorf("ebpf: ula %q: %w", id.ULA, err)
		}
	}
	for i, e := range snap.Entries {
		if e.SrcIdentity == 0 || e.DstIdentity == 0 {
			return fmt.Errorf("ebpf: policy entry %d has zero identity", i)
		}
		// 空 Ports = 仅 ICMPv6 放行的条目；带 Ports 的条目按 SrcPorts 决定
		// 白名单方向（正向 dport / 回程 sport）。
		for _, p := range e.Ports {
			if p == 0 || p > 65535 {
				return fmt.Errorf("ebpf: policy entry %d port %d out of range", i, p)
			}
		}
	}
	target := 1 - b.activeFabricSet()
	ipcache, policies, ports, nodeULA := b.fabricSet(target)
	if err := clearHashMap[[16]byte, uint32](ipcache); err != nil {
		return err
	}
	if err := clearHashMap[policyKey, policyVal](policies); err != nil {
		return err
	}
	if err := clearHashMap[policyPortKey, uint8](ports); err != nil {
		return err
	}
	if err := clearHashMap[[16]byte, uint8](nodeULA); err != nil {
		return err
	}
	for _, id := range snap.Identities {
		addr, _ := netip.ParseAddr(id.ULA)
		if err := ipcache.Put(ulaKey(addr), id.IdentityID); err != nil {
			return fmt.Errorf("ebpf: ipcache: %w", err)
		}
	}
	policiesByKey, portKeys := mergePolicyEntries(snap.Entries)
	for k, v := range policiesByKey {
		if err := policies.Put(k, v); err != nil {
			return fmt.Errorf("ebpf: policy: %w", err)
		}
	}
	for _, pk := range portKeys {
		if err := ports.Put(pk, uint8(1)); err != nil {
			return fmt.Errorf("ebpf: policy_ports: %w", err)
		}
	}
	// 平台流量放行：本节点自身 ULA（/64 基址）。
	if snap.NodeULA != "" {
		addr, err := netip.ParseAddr(snap.NodeULA)
		if err != nil || !addr.Is6() || addr.Is4In6() {
			return fmt.Errorf("ebpf: node ula %q: invalid", snap.NodeULA)
		}
		if err := nodeULA.Put(ulaKey(addr), uint8(1)); err != nil {
			return fmt.Errorf("ebpf: node_ula: %w", err)
		}
	}
	// 一次翻转：datapath 从此读到完整新集。
	if err := b.fabricActive.Put(uint32(0), target); err != nil {
		return fmt.Errorf("ebpf: fabric_active flip: %w", err)
	}
	// G3（§21）：当前已应用 fabric 策略代（gauge；快照 generation）。
	b.observer.PolicyGen(snap.Generation)
	return nil
}

// activeFabricSet 读当前活动集（0/1；读取失败按 0，与 map 初值一致）。
func (b *Backend) activeFabricSet() uint32 {
	var idx uint32
	if err := b.fabricActive.Lookup(uint32(0), &idx); err != nil {
		return 0
	}
	if idx == 1 {
		return 1
	}
	return 0
}

// fabricSet 返回指定缓冲集的四个 fabric 表句柄。
func (b *Backend) fabricSet(idx uint32) (ipcache, policies, ports, nodeULA *ebpf.Map) {
	if idx == 1 {
		return b.ipcacheV1, b.policyV1, b.policyPortsV1, b.nodeULAV1
	}
	return b.ipcacheV0, b.policyV0, b.policyPortsV0, b.nodeULAV0
}

// ulaKey 把 ULA 地址转成 ipcache 的 16 字节 key（big-endian 字节序）。
func ulaKey(addr netip.Addr) [16]byte {
	b := addr.As16()
	return b
}

func (b *Backend) putLPM(m *ebpf.Map, p netip.Prefix) error {
	addr := p.Addr()
	if !addr.Is4() {
		return fmt.Errorf("ebpf: lpm4 %s is not IPv4", p)
	}
	a := addr.As4()
	// cilium/ebpf 按 Go 内存布局（小端）序列化 map key/value；BPF 侧把
	// ip->daddr 当作 u32 原样比较——两侧字节序必须一致（LittleEndian 构造）。
	key := struct {
		Prefixlen uint32
		Data      uint32
	}{uint32(p.Bits()), binary.LittleEndian.Uint32(a[:])}
	var one uint8 = 1
	return m.Put(key, one)
}

// lpm4Key 与 bpf/tc.c 的 struct lpm4_key 一一对应（map 迭代用）。
type lpm4Key struct {
	Prefixlen uint32
	Data      uint32
}

func ipv4Key(ip string) (uint32, error) {
	addr := net.ParseIP(ip).To4()
	if addr == nil {
		return 0, fmt.Errorf("ebpf: bad guest ip %q", ip)
	}
	return binary.LittleEndian.Uint32(addr), nil
}

// tcFilter 挂载 pinned BPF 程序到 tc hook（幂等：重复挂载先 del 再 add）。
// netns 侧必须用 nsenter（--mount=本进程 mount ns）：`ip netns exec` 会切换
// 到新的 mount namespace，看不到 bpffs 固定文件。
// ensureClsact 幂等确保 veth 上有 clsact qdisc（re-ensure 高频路径）。已存在
// 属预期，但 tc 对已存在 clsact 的报错文案随 iproute2 版本变化（旧版
// “File exists”，新版 “Exclusivity flag on, cannot modify”）——文案匹配
// 脆弱（真机 spike 实测两代文案都出现过），改为先查再建：`tc qdisc show`
// 输出含 clsact 即跳过。netns 非空时在其内执行。
func ensureClsact(ctx context.Context, netns, dev string) error {
	var show []string
	if netns != "" {
		show = []string{"ip", "netns", "exec", netns, "tc", "qdisc", "show", "dev", dev}
	} else {
		show = []string{"tc", "qdisc", "show", "dev", dev}
	}
	if out, err := exec.CommandContext(ctx, show[0], show[1:]...).CombinedOutput(); err == nil {
		if strings.Contains(string(out), "clsact") {
			return nil
		}
	} else {
		return fmt.Errorf("show qdisc: %v: %s", err, strings.TrimSpace(string(out)))
	}
	var add []string
	if netns != "" {
		add = []string{"ip", "netns", "exec", netns, "tc", "qdisc", "add", "dev", dev, "clsact"}
	} else {
		add = []string{"tc", "qdisc", "add", "dev", dev, "clsact"}
	}
	if out, err := exec.CommandContext(ctx, add[0], add[1:]...).CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func tcFilter(ctx context.Context, netns, dev, dir, prog, pinDir string) error {
	progPath := filepath.Join(pinDir, prog)
	// replace + 固定 pref：attach 幂等（重复 reconcile 不叠加）。无 pref 的
	// add 在内核自动分配新 pref 且永不冲突报错，del 兑底永不触发——每次
	// attach 泄漏一份程序拷贝（真机实测 5 轮 reconcile 后 8 份叠加，逐包
	// 重复执行）。
	// 先 del 该方向全部 filter 再 replace：重启换程序后，旧运行期的 filter
	// 挂在旧程序实例上，旧程序引用冻结的旧 map（ipcache/policy 停在重启前），
	// 新 identity/规则在旧 map 里查无即 deny——slot 跨重启存活时表现为静默
	// 断流（真机实测）。全部清掉后由本函数重建唯一一份。
	delAll := []string{"tc", "filter", "del", "dev", dev, dir}
	if netns != "" {
		_ = nsenterExec(ctx, netns, delAll) // 不存在属预期
	} else {
		_ = exec.CommandContext(ctx, delAll[0], delAll[1:]...).Run()
	}
	const tcPref = "49142"
	args := []string{
		"tc", "filter", "replace", "dev", dev, dir,
		"pref", tcPref, "bpf", "da", "object-pinned", progPath,
	}
	delArgs := []string{"tc", "filter", "del", "dev", dev, dir, "pref", tcPref}
	if netns != "" {
		if err := nsenterExec(ctx, netns, delArgs); err != nil {
			_ = err // 不存在属预期
		}
		out, err := nsenterExecOutput(ctx, netns, args)
		if err != nil {
			return fmt.Errorf("ebpf: tc filter %s %s %s: %v: %s",
				dev, dir, prog, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err != nil {
		_ = exec.CommandContext(ctx, delArgs[0], delArgs[1:]...).Run()
		if out2, err2 := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err2 != nil {
			return fmt.Errorf("ebpf: tc filter %s %s %s: %v: %s / retry: %v: %s",
				dev, dir, prog, err, strings.TrimSpace(string(out)), err2, strings.TrimSpace(string(out2)))
		}
	}
	return nil
}

// nsenterExec / nsenterExecOutput：进入指定 netns 但保持本进程 mount ns。
func nsenterExec(ctx context.Context, netns string, args []string) error {
	_, err := nsenterExecOutput(ctx, netns, args)
	return err
}

func nsenterExecOutput(ctx context.Context, netns string, args []string) ([]byte, error) {
	full := append([]string{
		"--net=/run/netns/" + netns,
		"--mount=/proc/self/ns/mnt",
	}, args...)
	cmd := exec.CommandContext(ctx, "nsenter", full...)
	return cmd.CombinedOutput()
}

// ensureBpffs 确保 bpffs 挂载并创建 pin 目录。
func ensureBpffs(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	// 目录已存在但未挂载 bpffs 时 pin 会失败，New 时再报错；挂载本身幂等。
	if out, err := exec.Command("mount", "-t", "bpf", "bpf", dir).CombinedOutput(); err != nil {
		// 已挂载或权限不足：已挂载 → 继续；其他 → 错误。
		if !strings.Contains(string(out), "already mounted") {
			return fmt.Errorf("mount bpffs: %v: %s", err, strings.TrimSpace(string(out)))
		}
	}
	return nil
}
