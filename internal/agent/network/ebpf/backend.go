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

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang -cflags "-O2 -Wall -Werror -D__TARGET_ARCH_x86" -target amd64 tc bpf/tc.c -- -I bpf

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

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
)

// PinRoot 是 maps/programs 的 bpffs 固定目录。
const PinRoot = "/sys/fs/bpf/firepaas"

// Options 装配 eBPF 后端。
type Options struct {
	PinDir         string // 默认 PinRoot
	EgressProxy80  int    // 透明代理 HTTP 端口（0 = 不代理）
	EgressProxy443 int
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
	pinDir   string
	port80   int
	port443  int
	observer Observer

	objs         *tcObjects
	host4        *ebpf.Map
	connCap      *ebpf.Map
	connCount    *ebpf.Map
	egressAllow  *ebpf.Map     // HASH_OF_MAPS<slotKey, LPM>（per-slot allow）
	egressDeny   *ebpf.Map     // HASH_OF_MAPS<slotKey, LPM>（per-slot deny）
	egressMode   *ebpf.Map     // HASH<slotKey, egressModeVal>（per-slot 默认动作）
	egressSlot   *ebpf.Map     // HASH<saddr, slotKey>（saddr 归一，见 tc_egress 注释）
	innerLpmSpec *ebpf.MapSpec // 内层 LPM 模板（loadTc 推导，建表用）
	// egressInners 跟踪已建内层表句柄（FD 回收用；内核对象由外层引用续命，
	// 替换/删除时关闭旧句柄避免 FD 泄漏）。key = 规范 slot key。
	egressInnerAllow map[uint32]*ebpf.Map
	egressInnerDeny  map[uint32]*ebpf.Map
	ipcache          *ebpf.Map
	policy           *ebpf.Map
	policyPorts      *ebpf.Map
	nodeULA          *ebpf.Map
	hostAddrs4       *ebpf.Map
	flows            *ebpf.Map
	priv4            *ebpf.Map
	// snapshots 记录已应用的 egress 快照（重启/Reconcile 重放）。
	snapshots map[int]api.PolicySnapshot
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
		observer:         opts.Observer,
		egressInnerAllow: map[uint32]*ebpf.Map{},
		egressInnerDeny:  map[uint32]*ebpf.Map{},
		snapshots:        map[int]api.PolicySnapshot{},
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
	b.connCap = objs.ConnCap
	b.connCount = objs.ConnCount
	b.egressAllow = objs.EgressAllow
	b.egressDeny = objs.EgressDeny
	b.egressMode = objs.EgressMode
	b.egressSlot = objs.EgressSlot
	b.ipcache = objs.Ipcache
	b.policy = objs.Policy
	b.policyPorts = objs.PolicyPorts
	b.nodeULA = objs.NodeUla
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
	// 旧版全局表（W1 前 cidr_allow4/cidr_deny4/mode_drop）的 pin 残留回收：
	// 同名复用已不可能（spec 更名），留着只是 bpffs 垃圾。
	for _, stale := range []string{"cidr_allow4", "cidr_deny4", "mode_drop"} {
		_ = os.Remove(filepath.Join(b.pinDir, stale))
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
	return nil
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

// DetachSlot 摘除 slot 的 per-slot 配置：host4/conn_cap/conn_count 聚合、
// egress 外层条目 + mode + 内层表句柄（netns 删除连带 tc 程序回收）。
// 内层内核对象由外层条目引用续命，删除外层条目后引用释放；Go 句柄关闭防 FD 泄漏。
func (b *Backend) DetachSlot(ctx context.Context, ref slot.SlotRef) error {
	if guestKey, err := ipv4Key(ref.GuestIP); err == nil {
		_ = b.host4.Delete(guestKey)
		_ = b.egressSlot.Delete(guestKey)
	}
	if slotKey, err := slotEgressKey(ref); err == nil {
		_ = b.host4.Delete(slotKey)
		_ = b.egressSlot.Delete(slotKey)
		_ = b.connCap.Delete(slotKey)
		_ = b.connCount.Delete(connAggKey(slotKey))
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
	delete(b.snapshots, ref.Index)
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

// connAggKey 构造 conn_count 聚合 key（{saddr,0,0,0}，与 BPF 侧 agg 同布局）。
type connAggKeyT struct {
	Saddr uint32
	Daddr uint32
	Sport uint16
	Dport uint16
}

func connAggKey(saddr uint32) connAggKeyT { return connAggKeyT{Saddr: saddr} }

// ApplyEgress 全量替换 slot egress 快照（与 nft 同语义：deny → allow →
// mode 默认；redirect 先于 deny/allow，等价 nft prerouting DNAT 顺序）。
// per-slot 分片（W1）：只动本 slot（规范 slot key）的外层条目 + mode，多 slot
// 互不覆盖。snap nil = 清除（删条目回到“未下发=unrestricted”，等价 nft 清表）。
func (b *Backend) ApplyEgress(ctx context.Context, ref slot.SlotRef, snap *api.PolicySnapshot) error {
	idx := ref.Index
	slotKey, err := slotEgressKey(ref)
	if err != nil {
		return err
	}
	if snap == nil {
		b.clearEgressSlot(slotKey)
		_ = b.connCount.Delete(connAggKey(slotKey))
		delete(b.snapshots, idx)
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
	// 落表：内层先清后写，再挂外层，最后写 mode（读端任一中间态仍是旧
	// 快照或空条目；空外层条目 = unrestricted，崩溃窗口 fail-open 等价
	// 重启前空表语义，重放后收敛——见 Backend 注释）。
	if err := b.clearMaps(deny); err != nil {
		return err
	}
	if err := b.clearMaps(allow); err != nil {
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
	if snap.MaxTCPConns > 0 {
		if err := b.connCap.Put(slotKey, snap.MaxTCPConns); err != nil {
			return err
		}
	} else if err := b.connCap.Delete(slotKey); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
		// 快照未声明上限时清除旧上限：残留 cap 会让已结束限额的
		// 陈旧计数继续约束新流量（stale cap bug）。
		return fmt.Errorf("ebpf: clear stale conn_cap: %w", err)
	}
	b.snapshots[idx] = *snap
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
	_ = b.connCap.Delete(slotKey)
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
	// HasPorts：正向端口规则标记（1 = 该方向有 ports 白名单，0 = 纯对称回程
	// 条目）。UDP 凭此区分发起与回包（TCP 回包凭 SYN 标志，不读本字段）。
	// 对应 BPF 侧 policy_val.has_ports（曾为保留字段 ports_bm）。
	HasPorts uint64
}

type policyPortKey struct {
	Src   uint32
	Dst   uint32
	Dport uint16
	Pad   uint16
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
func (b *Backend) ApplyFabricPolicy(ctx context.Context, snap api.FabricPolicySnapshot) error {
	for _, id := range snap.Identities {
		if _, err := netip.ParseAddr(id.ULA); err != nil {
			return fmt.Errorf("ebpf: ula %q: %w", id.ULA, err)
		}
	}
	for i, e := range snap.Entries {
		if e.SrcIdentity == 0 || e.DstIdentity == 0 {
			return fmt.Errorf("ebpf: policy entry %d has zero identity", i)
		}
		// 空 Ports = 对称回程条目（只放行非发起包；端口白名单仅挂在正向）。
		for _, p := range e.Ports {
			if p == 0 || p > 65535 {
				return fmt.Errorf("ebpf: policy entry %d port %d out of range", i, p)
			}
		}
	}
	if err := clearHashMap[[16]byte, uint32](b.ipcache); err != nil {
		return err
	}
	if err := clearHashMap[policyKey, policyVal](b.policy); err != nil {
		return err
	}
	if err := clearHashMap[policyPortKey, uint8](b.policyPorts); err != nil {
		return err
	}
	for _, id := range snap.Identities {
		addr, _ := netip.ParseAddr(id.ULA)
		if err := b.ipcache.Put(ulaKey(addr), id.IdentityID); err != nil {
			return fmt.Errorf("ebpf: ipcache: %w", err)
		}
	}
	for _, e := range snap.Entries {
		var hasPorts uint64
		if len(e.Ports) > 0 {
			hasPorts = 1
		}
		if err := b.policy.Put(
			policyKey{Src: e.SrcIdentity, Dst: e.DstIdentity},
			policyVal{Generation: e.Generation, HasPorts: hasPorts},
		); err != nil {
			return fmt.Errorf("ebpf: policy: %w", err)
		}
		for _, p := range e.Ports {
			if err := b.policyPorts.Put(
				policyPortKey{Src: e.SrcIdentity, Dst: e.DstIdentity, Dport: uint16(p)},
				uint8(1),
			); err != nil {
				return fmt.Errorf("ebpf: policy_ports: %w", err)
			}
		}
	}
	// 平台流量放行：本节点自身 ULA（/64 基址）。全量替换语义（先清后写）。
	if err := clearHashMap[[16]byte, uint8](b.nodeULA); err != nil {
		return err
	}
	if snap.NodeULA != "" {
		addr, err := netip.ParseAddr(snap.NodeULA)
		if err != nil || !addr.Is6() || addr.Is4In6() {
			return fmt.Errorf("ebpf: node ula %q: invalid", snap.NodeULA)
		}
		if err := b.nodeULA.Put(ulaKey(addr), uint8(1)); err != nil {
			return fmt.Errorf("ebpf: node_ula: %w", err)
		}
	}
	// G3（§21）：当前已应用 fabric 策略代（gauge；快照 generation）。
	b.observer.PolicyGen(snap.Generation)
	return nil
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

func (b *Backend) clearMaps(m *ebpf.Map) error {
	iter := m.Iterate()
	var key lpm4Key
	var val uint8
	for iter.Next(&key, &val) {
		if err := m.Delete(key); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
			return fmt.Errorf("ebpf: clear map: %w", err)
		}
	}
	return iter.Err()
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
