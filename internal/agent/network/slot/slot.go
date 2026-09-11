// Package slot 是 agent 的 slot 网络管理器（ADR-0004 的 M3 落地）。
//
// 每个 VM 一个 netns slot：
//
//	root ns                        netns fp-slot-<n>
//	┌──────────────────────┐      ┌───────────────────────────────┐
//	│ fp-vp<n> 10.12.A.B+1 │◄────►│ fp-vg<n> 10.12.A.B+2 (default  │
//	│ (veth host side)     │ veth │  gw via host side)             │
//	│                      │      │   └── fp-br<n> <gw>/<mask>     │
//	│  /32 route: guest IP │      │        ├── TAP (hypeman 移入)  │
//	│  nft fp-isolation    │      │        └── (guest 经 TAP 接入) │
//	└──────────────────────┘      │ nft fp-slot: egress masquerade │
//	                             └───────────────────────────────┘
//
// 隔离由 root ns 的 nftables 表 fp-isolation 保证（O(1) ifname 集合）：
//   - INPUT:  slot veth 入站仅放行 established/related（guest→host 默认拒）
//   - FORWARD: 放行 established/related；drop 私网/组播目标；其余放行（公网 egress）
//   - POSTROUTING: 10.12.0.0/16 → masquerade（二级 NAT 的出口半段）
//
// R2 IPv6 默认拒绝：nft 的 ip family 根本看不到 IPv6 报文，而 slot veth
// 会随内核 auto-config 获得 link-local 地址——不防护则 guest 的 v6 可达
// host 上 ::: 监听的服务。选择同构的 ip6 family 表做 slot veth 入向/转发
// 默认全拒（fail closed），而非逐接口 sysctl disable_ipv6：两者效果等价
// （slot 数据面是纯 IPv4——guest 无 v6 地址与路由，v6 没有管理放行面），
// 但 nft 路径与既有 v4 隔离共享同一实现与审计面，且「表存在即可启动校验」，
// disable_ipv6 还要逐接口 + netns default 配合。
// 私网/保留目标集合统一来自 internal/agent/netpolicy（canonical CIDR）。
//
// slot 内另有一级 NAT（postrouting：非代理回流方向 masquerade 到 veth 地址），
// 保证 guest 真实 IP 不泄漏到 root ns 转发面，同时代理连接（daddr=host veth
// 地址）不被改写、conntrack 可逆。
//
// hypeman 集成约定（真机 spike 验证）：
//   - hypeman 用 bridge 模式创建 VM（TAP 名 = lib/network.GenerateTAPName(id)）；
//   - CreateInstance 返回后本包把 TAP 移入 slot netns（自动脱离 vmbr）；
//   - hypeman 的 release 对找不到 TAP 是 best-effort（WARN 后继续），因此
//     Release 直接删 netns（连带 TAP/veth/bridge 全回收）。
package slot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/shared/pkg/durablewrite"
)

const (
	// 默认名字前缀（fp-slot-<n>/fp-vp<n>/fp-br<n>）；同主机多 agent
	//（ADR-0040 双节点 spike）经 Config.NamePrefix 隔离名字空间。
	defaultNamePrefix = "fp"
	// VethRange 是 root↔netns 点对点链路的默认地址池（10.12.0.0/16）。
	// 导出：agentd 的 egress 保留段检查需要把它列为平台保留段。
	VethRange = "10.12.0.0/16"
)

// Config 是 slot manager 配置。
type Config struct {
	SubnetCIDR string // hypeman 网络子网（guest 侧），如 10.100.0.0/16
	Gateway    string // 子网网关（slot 内 bridge 地址），如 10.100.0.1
	StatePath  string // slots.json 状态文件（agent data_dir 下）
	// EgressProxyPort80/443（v1.3-A，ADR-0027）：root ns 透明 egress 代理的
	// 监听端口。>0 时 fp-isolation INPUT 放行 slot→代理的新连接（否则 SYN 会被
	// “非 established 即 drop”截断）。仅用于构造默认 nft 后端。
	EgressProxyPort80  int
	EgressProxyPort443 int
	// Backend 是数据面后端（ADR-0040 §10/§12；nil = 默认 nft 后端）。
	Backend Backend
	// NamePrefix（同主机多 agent，ADR-0040 G1 spike）：netns/veth/bridge
	// 名字前缀（fp-slot-<n>/fp-vp<n>/fp-br<n>）。默认 "fp"；同一主机网
	// 络命名空间跑多个 agent 时必须互异（否则 reconcile 互扫对方 netns
	// 误删 slot）。合法字符 [a-z0-9-]，长度 ≤ 8。
	NamePrefix string
	// VethCIDR：root↔netns 点对点链路地址池（默认 10.12.0.0/16，必须
	// IPv4 /16）。同主机多 agent 时必须互异（避免 root ns 路由冲突）。
	VethCIDR string
}

// Slot 是一个已分配的 slot。
type Slot struct {
	Index     int    `json:"index"`
	MachineID string `json:"machine_id"`
	Tap       string `json:"tap"`
	GuestIP   string `json:"guest_ip"`
	// GuestIP6（ADR-0040 §7，T6）：execution 域 ULA（裸地址，无前缀）。
	// 空 = 纯 IPv4（legacy 零回归）；非空时 slot 补 v6 三层接线（§9 mesh
	// 转发 + NDP 代理），guest 侧地址由 hypeman vmconfig 注入。
	GuestIP6 string `json:"guest_ip6,omitempty"`
	// Egress（v1.3-A，ADR-0027）：本 slot 当前应用的策略快照
	//（重启后按此重放；空 Mode = 未声明）。
	Egress api.PolicySnapshot `json:"egress,omitempty"`
}

// Manager 管理 slot 的分配/释放/启动回收。所有内核操作串行化。
// 同时实现 network/api 的 Datapath 与 PolicyEngine 插件缝（ADR-0040 §11）；
// 编译期断言防接口漂移。
type Manager struct {
	cfg     Config
	backend Backend
	// namePrefix/vethBase/vethMask：Config.NamePrefix/VethCIDR 的解析形态
	//（名字/地址推导全部经由本 Manager，不再用包级常量）。
	namePrefix string
	vethBase   [2]byte // /16 前两字节
	mu         sync.Mutex
	slots      map[string]Slot // key: machine_id
}

var (
	_ api.Datapath     = (*Manager)(nil)
	_ api.PolicyEngine = (*Manager)(nil)
)

// New 构造 Manager。
func New(cfg Config) (*Manager, error) {
	if cfg.SubnetCIDR == "" {
		return nil, fmt.Errorf("slot: SubnetCIDR is required")
	}
	if cfg.Gateway == "" {
		gw, err := deriveGateway(cfg.SubnetCIDR)
		if err != nil {
			return nil, err
		}
		cfg.Gateway = gw
	}
	if cfg.StatePath == "" {
		return nil, fmt.Errorf("slot: StatePath is required")
	}
	if cfg.EgressProxyPort80 == 0 {
		cfg.EgressProxyPort80 = 18080
	}
	if cfg.EgressProxyPort443 == 0 {
		cfg.EgressProxyPort443 = 18443
	}
	if cfg.NamePrefix == "" {
		cfg.NamePrefix = defaultNamePrefix
	}
	if len(cfg.NamePrefix) > 8 || !validNamePrefix(cfg.NamePrefix) {
		return nil, fmt.Errorf("slot: invalid NamePrefix %q (want [a-z0-9-]{1,8})", cfg.NamePrefix)
	}
	if cfg.VethCIDR == "" {
		cfg.VethCIDR = VethRange
	}
	base, err := parseVethCIDR(cfg.VethCIDR)
	if err != nil {
		return nil, fmt.Errorf("slot: invalid VethCIDR: %w", err)
	}
	backend := cfg.Backend
	if backend == nil {
		backend = NewNftBackend(cfg.EgressProxyPort80, cfg.EgressProxyPort443, cfg.VethCIDR)
	}
	return &Manager{
		cfg: cfg, backend: backend, slots: map[string]Slot{},
		namePrefix: cfg.NamePrefix, vethBase: base,
	}, nil
}

// VethCIDR 返回本 manager 的 veth 地址池（agentd egress 保留段检查用）。
func (m *Manager) VethCIDR() string { return m.cfg.VethCIDR }

func validNamePrefix(p string) bool {
	if p == "" {
		return false
	}
	for _, r := range p {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

func parseVethCIDR(cidr string) (base [2]byte, err error) {
	ip, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return base, err
	}
	if ipNet.Mask.String() != "ffff0000" {
		return base, fmt.Errorf("want IPv4 /16, got %s", cidr)
	}
	b := ip.To4()
	if b == nil {
		return base, fmt.Errorf("want IPv4, got %s", cidr)
	}
	return [2]byte{b[0], b[1]}, nil
}

// refFor 构造 slot 的后端坐标。
func (m *Manager) refFor(s Slot) (SlotRef, error) {
	hostAddr, nsAddr, err := m.vethAddrs(s.Index)
	if err != nil {
		return SlotRef{}, err
	}
	vh, vg := m.vethNames(s.Index)
	return SlotRef{
		Index:     s.Index,
		VethHost:  vh,
		VethGuest: vg,
		HostAddr:  hostAddr,
		NsAddr:    nsAddr,
		Netns:     m.nsName(s.Index),
		GuestIP:   s.GuestIP,
		GuestIP6:  s.GuestIP6,
	}, nil
}

// mustRef 同 refFor，失败 panic（只用于 setupLocked 内坐标已保证合法的路径）。
func (m *Manager) mustRef(s Slot) SlotRef {
	ref, err := m.refFor(s)
	if err != nil {
		panic(fmt.Sprintf("slot: ref for index %d: %v", s.Index, err))
	}
	return ref
}

// ensureNodeBackend 幂等建立节点级后端设施并把 slot 挂入（EnsureNode +
// AttachSlot；重复调用幂等）。
func (m *Manager) ensureNodeBackend(ctx context.Context, s Slot) error {
	if err := m.backend.EnsureNode(ctx); err != nil {
		return err
	}
	return m.backend.AttachSlot(ctx, m.mustRef(s))
}

// StateLockPath 返回 state 文件配套的跨进程锁路径（与 CNI 侧同文件）。
func StateLockPath(statePath string) string { return statePath + ".lock" }

// WithStateFileLock 跨进程串行同一 state 文件的读写（P1 独立评审：锁必须
// 双边拿——CNI 进程与 agentd 进程各持 Manager（mu 跨不了进程），文件锁是
// 唯一的互斥点；slots.json 的 Load/持久化与 CNI 的 Load+操作都经此锁）。
// 锁文件 0600（与 state 文件同目录，含 slot 身份）。
func WithStateFileLock(statePath string, fn func() error) error {
	// 锁文件父目录不存在时先建（state 文件本身由 durablewrite 原子写建目录，
	// 锁不能先于目录存在；0700 与 state 同级目录纪律一致）。
	if dir := filepath.Dir(StateLockPath(statePath)); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("slot state lock dir: %w", err)
		}
	}
	f, err := os.OpenFile(StateLockPath(statePath), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("slot state lock: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("slot state lock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// Load 从状态文件恢复内存视图（Reconcile 前调用）。
func (m *Manager) Load() error {
	// 文件读经跨进程锁（CNI×agentd 串行），解析与内存交换在 mu 下。
	var raw []byte
	if err := WithStateFileLock(m.cfg.StatePath, func() error {
		var err error
		raw, err = os.ReadFile(m.cfg.StatePath)
		if err != nil && os.IsNotExist(err) {
			return nil
		}
		return err
	}); err != nil {
		return fmt.Errorf("slot: read state: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if raw == nil {
		return nil
	}
	var list []Slot
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("slot: parse state: %w", err)
	}
	m.slots = make(map[string]Slot, len(list))
	for _, s := range list {
		if s.MachineID != "" {
			m.slots[s.MachineID] = s
		}
	}
	return nil
}

// List 返回当前状态快照（按 index 排序）。
func (m *Manager) List() []Slot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Slot, 0, len(m.slots))
	for _, s := range m.slots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// AttachNetns 为 machine 建立 slot：创建 netns/veth/bridge，把 hypeman TAP 移入，
// 加 nftables 隔离与 root /32 路由。失败时回收已创建的内核对象。
// 实现 api.Datapath（ADR-0040 §11）。
func (m *Manager) AttachNetns(ctx context.Context, spec api.NetnsSpec) error {
	machineID, tap, guestIP := spec.MachineID, spec.Tap, spec.GuestIP
	guestIP6 := spec.GuestIP6
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.slots[machineID]; ok {
		// 幂等：同 machine 重复 attach 直接复用。若换了 TAP（新 execution 重建
		// 时 hypeman 生成新实例/TAP），先摘除 netns 里可能残留的旧 TAP。
		if s.Tap != tap && tap != "" && s.Tap != "" {
			_ = exec.Command("ip", "netns", "exec", m.nsName(s.Index), "ip", "link", "del", s.Tap).Run()
		}
		if err := m.ensureKernel(ctx, s, tap, guestIP, guestIP6); err != nil {
			return err
		}
		// Reattach changes execution plumbing, not the deployment policy. Preserve
		// the persisted rule set so it can be replayed and rebuilt by egress.Manager.
		s.Tap = tap
		s.GuestIP = guestIP
		if eff6 := effectiveGuestIP6(s.GuestIP6, guestIP6); s.GuestIP6 != eff6 {
			// execution 更替带来新 ULA：旧 /128 路由与 NDP 代理尽力回收，
			// 避免 stale 条目把旧地址继续引入本 slot。
			if s.GuestIP6 != "" {
				m.removeSlotV6Locked(ctx, s)
			}
			s.GuestIP6 = eff6
		}
		m.slots[machineID] = s
		return m.persistLocked()
	}
	if guestIP == "" {
		return fmt.Errorf("slot: guest_ip is required")
	}

	idx, err := m.allocIndexLocked()
	if err != nil {
		return err
	}
	s := Slot{Index: idx, MachineID: machineID, Tap: tap, GuestIP: guestIP, GuestIP6: guestIP6}
	if err := m.setupLocked(ctx, s); err != nil {
		_ = m.releaseLocked(ctx, s)
		return err
	}
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		_ = m.releaseLocked(ctx, s)
		delete(m.slots, machineID)
		return err
	}
	return nil
}

// DetachNetns 删除 slot：先摘后端配置与 v6 接线，再删 netns，最后显式删除
// root 侧 veth（对端销毁不连带删除本端；路由/邻居随设备删除自动回收）。
func (m *Manager) DetachNetns(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return nil
	}
	if err := m.releaseLocked(ctx, s); err != nil {
		return err
	}
	delete(m.slots, machineID)
	return m.persistLocked()
}

// Check 实现 CNI CHECK 语义：对已有 slot 幂等补齐内核接线；slot 不存在时
// 报错（CHECK 不新建网络）。
func (m *Manager) Check(ctx context.Context, spec api.NetnsSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[spec.MachineID]
	if !ok {
		return fmt.Errorf("slot: check: machine %s has no slot", spec.MachineID)
	}
	g6 := effectiveGuestIP6(s.GuestIP6, spec.GuestIP6)
	return m.ensureKernel(ctx, s, spec.Tap, spec.GuestIP, g6)
}

// CurrentNetns 返回 machine 当前 slot 观察视图（api.Datapath）。
func (m *Manager) CurrentNetns(machineID string) (api.NetnsState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return api.NetnsState{}, false
	}
	return api.NetnsState{
		Index:     s.Index,
		MachineID: s.MachineID,
		Tap:       s.Tap,
		GuestIP:   s.GuestIP,
		Snapshot:  s.Egress,
	}, true
}

//  1. 内核有、状态无的 fp-slot-* netns → 删除（attach 中途崩溃残留）；
//  2. 状态有、netns 无 → 丢弃条目（已随内核清理）；
//  3. 状态有、netns 有：live 里没有 → 删除 netns + 条目（VM 已死）；
//     live 里有 → 幂等补 route/nft 元素；
//  4. live 有、状态无 → TAP 还在 root ns（hypeman 创建后 agentd 崩溃窗口），
//     重新 attach。
func (m *Manager) Reconcile(ctx context.Context, live []api.LiveInstance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	liveByID := make(map[string]api.LiveInstance, len(live))
	for _, l := range live {
		if l.MachineID != "" {
			liveByID[l.MachineID] = l
		}
	}

	// 1/2/3：内核 ↔ 状态对齐。错误一律降级（记日志继续）：slot 异常不能
	// 阻塞 agentd 启动（M3 真机事故：僵尸 guest 目录 → reconcile 报错 →
	// agentd crash-loop → 整个节点数据面不可用）。
	for id, s := range m.slots {
		exists, err := m.netnsExists(s.Index)
		if err != nil {
			logf(slog.LevelWarn, "slot: reconcile netns check %s: %v", id, err)
			continue
		}
		if !exists {
			// 内核 netns 已不存在：不能只 delete 内存条目——后端 map
			//（eBPF host4/egress_slot/slot_ula）与 root 侧 veth/路由
			// 残留会在 ifindex 复用窗口内作用于新 slot。releaseLocked
			// 对“netns 已不存在”幂等（deleteNetns 容忍），best-effort
			// 失败记 Warn 后继续。
			if err := m.releaseLocked(ctx, s); err != nil {
				logf(slog.LevelWarn, "slot: reconcile release missing netns %s (degraded): %v", id, err)
			}
			delete(m.slots, id)
			continue
		}
		if l, ok := liveByID[id]; ok && l.Tap == s.Tap {
			if l.GuestIP6 != "" && l.GuestIP6 != s.GuestIP6 {
				// execution 更替带来新 ULA：旧 /128 路由与 NDP 代理若
				// 留到 ifindex 复用窗口会误导新 slot 的 v6 引导，先
				// 回收再更新持久状态。落盘收敛到函数尾的 persistLocked
				// （本分支 continue，不在循环内多次写盘）。
				m.removeSlotV6Locked(ctx, s)
				s.GuestIP6 = l.GuestIP6
				m.slots[id] = s
			}
			g6 := effectiveGuestIP6(s.GuestIP6, l.GuestIP6)
			if err := m.ensureKernel(ctx, s, l.Tap, l.GuestIP, g6); err != nil {
				logf(slog.LevelWarn, "slot: re-ensure %s (degraded): %v", id, err)
			}
			// v1.3-A（ADR-0027）：重启后重放持久化策略快照。
			if s.Egress.Mode != "" {
				// Reconcile already owns m.mu; call the lock-free helper to avoid
				// self-deadlocking through RestoreSnapshot.
				if err := m.backend.ApplyEgress(ctx, m.mustRef(s), &s.Egress); err != nil {
					logf(slog.LevelWarn, "slot: restore egress %s (degraded): %v", id, err)
				}
			}
			continue
		}
		if err := m.releaseLocked(ctx, s); err != nil {
			logf(slog.LevelWarn, "slot: reconcile release %s (degraded): %v", id, err)
			continue
		}
		delete(m.slots, id)
	}

	// 内核残留 netns（状态里没有的）。
	strays, err := m.listStrayNetns()
	if err != nil {
		logf(slog.LevelWarn, "slot: list stray netns: %v", err)
	}
	for _, idx := range strays {
		// 只回收不在状态里的；正在 attach 的窗口由本包串行锁排除。
		if !m.hasIndexLocked(idx) {
			_ = m.deleteNetns(idx)
		}
	}

	// 4：live 实例没有 slot → 重新 attach。TAP 不在 root ns（VM 已死或
	// 已在其它 netns）时跳过：slot 网络故障绝不能让 agentd 退出（降级
	// 运行，机器级 R1-R8 会收敛清理；M3 真机事故：僵尸 guest 目录曾让
	// agentd crash-loop）。
	for id, l := range liveByID {
		if _, ok := m.slots[id]; ok {
			continue
		}
		if !tapExistsInRoot(l.Tap) {
			continue
		}
		idx, err := m.allocIndexLocked()
		if err != nil {
			return err
		}
		s := Slot{Index: idx, MachineID: id, Tap: l.Tap, GuestIP: l.GuestIP, GuestIP6: l.GuestIP6}
		if err := m.setupLocked(ctx, s); err != nil {
			_ = m.releaseLocked(ctx, s)
			delete(m.slots, id)
			logf(slog.LevelWarn, "slot: reconcile attach %s failed (degraded): %v", id, err)
			continue
		}
		m.slots[id] = s
		// 降级后自愈：live VM 曾因创建后崩溃窗口丢了 slot，此处重新接线。
		logf(slog.LevelInfo, "slot: reconcile re-attached live machine %s (self-healed)", id)
	}
	return m.persistLocked()
}

// ApplySnapshot 在 slot netns 内落地策略快照（api.PolicyEngine，ADR-0040 §11）。
// 全量替换 + generation fencing：新 generation 小于已应用值 → 拒绝并保持旧
// 快照。重复应用同 generation 幂等（flush + rebuild）。
func (m *Manager) ApplySnapshot(ctx context.Context, machineID string, snap api.PolicySnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return fmt.Errorf("slot: snapshot apply: machine %s has no slot", machineID)
	}
	if snap.Generation < s.Egress.Generation {
		return fmt.Errorf("slot: snapshot generation fencing: applied %d > requested %d (keep old policy)",
			s.Egress.Generation, snap.Generation)
	}
	if err := m.backend.ApplyEgress(ctx, m.mustRef(s), &snap); err != nil {
		return err
	}
	old := s.Egress
	s.Egress = snap
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		s.Egress = old
		m.slots[machineID] = s
		var rollbackErr error
		if old.Mode == "" {
			rollbackErr = m.backend.ApplyEgress(ctx, m.mustRef(s), nil)
		} else {
			rollbackErr = m.backend.ApplyEgress(ctx, m.mustRef(s), &old)
		}
		return errors.Join(err, rollbackErr)
	}
	return nil
}

// CurrentSnapshot returns the persisted policy snapshot used by the
// egress coordinator for rollback.
func (m *Manager) CurrentSnapshot(machineID string) (api.PolicySnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	return s.Egress, ok && s.Egress.Mode != ""
}

// RollbackSnapshot restores a snapshot without generation fencing. It is
// only exposed to the serialized egress coordinator after a committed nft
// update could not be published by the proxy.
func (m *Manager) RollbackSnapshot(ctx context.Context, machineID string, snap api.PolicySnapshot, present bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return fmt.Errorf("slot: snapshot rollback: machine %s has no slot", machineID)
	}
	if present {
		if err := m.backend.ApplyEgress(ctx, m.mustRef(s), &snap); err != nil {
			return err
		}
		s.Egress = snap
	} else {
		if err := m.backend.ApplyEgress(ctx, m.mustRef(s), nil); err != nil {
			return err
		}
		s.Egress = api.PolicySnapshot{}
	}
	m.slots[machineID] = s
	return m.persistLocked()
}

// RemoveSnapshot 清空 slot 的策略快照（恢复 unrestricted 全通）。
func (m *Manager) RemoveSnapshot(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return nil
	}
	if s.Egress.Mode == "" {
		return nil
	}
	if err := m.backend.ApplyEgress(ctx, m.mustRef(s), nil); err != nil {
		return err
	}
	old := s.Egress
	s.Egress = api.PolicySnapshot{}
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		s.Egress = old
		m.slots[machineID] = s
		return errors.Join(err, m.backend.ApplyEgress(ctx, m.mustRef(s), &old))
	}
	return nil
}

// RestoreSnapshot 重启后按持久化状态重放 slot 策略快照（Reconcile 内用）。
func (m *Manager) RestoreSnapshot(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok || s.Egress.Mode == "" {
		return nil
	}
	return m.backend.ApplyEgress(ctx, m.mustRef(s), &s.Egress)
}

// logf 是包内日志出口（避免 slot 包依赖 slog 的全局 handler 配置）。
// 级别语义：降级但继续的事件用 Warn，自愈成功的可观事件用 Info，只有
// 真正不可恢复的错误才用 Error。
func logf(level slog.Level, format string, args ...any) {
	slog.Log(context.Background(), level, fmt.Sprintf(format, args...))
}

// Count 返回当前 slot 数（测试/观测用）。
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.slots)
}

// SlotFor 返回 machine 当前 slot（不存在时 ok=false）。
func (m *Manager) SlotFor(machineID string) (Slot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	return s, ok
}

// ---------------------------------------------------------------------------
// 内部
// ---------------------------------------------------------------------------

func (m *Manager) allocIndexLocked() (int, error) {
	used := map[int]bool{}
	for _, s := range m.slots {
		used[s.Index] = true
	}
	idx := 0
	for used[idx] {
		idx++
	}
	return idx, nil
}

func (m *Manager) hasIndexLocked(idx int) bool {
	for _, s := range m.slots {
		if s.Index == idx {
			return true
		}
	}
	return false
}

// persistLocked 使用崩溃安全序列（shared/pkg/durablewrite：temp 0600 →
// fsync(temp) → rename → fsync(dir)）。含 egress 域名规则，按 0600 处理；
// rename 前不 fsync 可能掉电后剩空文件，重启时整份 slot 状态作废。
func (m *Manager) persistLocked() error {
	list := make([]Slot, 0, len(m.slots))
	for _, s := range m.slots {
		list = append(list, s)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Index < list[j].Index })
	raw, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return fmt.Errorf("slot: marshal state: %w", err)
	}
	// 文件写经跨进程锁（与 Load/CNI 同锁，防交错写）。
	return WithStateFileLock(m.cfg.StatePath, func() error {
		return durablewrite.WriteFileAtomic(m.cfg.StatePath, "slot state", raw)
	})
}

func (m *Manager) setupLocked(ctx context.Context, s Slot) error {
	if err := execCmd(ctx, "ip", "netns", "add", m.nsName(s.Index)); err != nil {
		return fmt.Errorf("add netns: %w", err)
	}
	cleanupNetns := true
	defer func() {
		if cleanupNetns {
			_ = m.deleteNetns(s.Index)
		}
	}()

	hostAddr, nsAddr, err := m.vethAddrs(s.Index)
	if err != nil {
		return err
	}
	vh, vg := m.vethNames(s.Index)

	if err := execCmd(ctx, "ip", "link", "add", vh, "type", "veth", "peer", "name", vg); err != nil {
		return fmt.Errorf("add veth: %w", err)
	}
	if err := execCmd(ctx, "ip", "link", "set", vg, "netns", m.nsName(s.Index)); err != nil {
		return fmt.Errorf("move veth to netns: %w", err)
	}
	if err := execCmd(ctx, "ip", "addr", "add", hostAddr+"/30", "dev", vh); err != nil {
		return fmt.Errorf("add host veth addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "link", "set", vh, "up"); err != nil {
		return fmt.Errorf("set host veth up: %w", err)
	}

	// netns 内部：bridge（guest 网关）+ veth + 默认路由 + 一级 NAT。
	br := m.brName(s.Index)
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"sysctl", "-qw", "net.ipv4.ip_forward=1"); err != nil {
		return fmt.Errorf("netns ip_forward: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "link", "add", br, "type", "bridge"); err != nil {
		return fmt.Errorf("add slot bridge: %w", err)
	}
	gw, mask, err := m.gatewayAddr()
	if err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "addr", "add", gw+"/"+mask, "dev", br); err != nil {
		return fmt.Errorf("add bridge addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "link", "set", br, "up"); err != nil {
		return fmt.Errorf("set bridge up: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "link", "set", vg, "up"); err != nil {
		return fmt.Errorf("set netns veth up: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "addr", "add", nsAddr+"/30", "dev", vg); err != nil {
		return fmt.Errorf("add netns veth addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
		"ip", "route", "add", "default", "via", hostAddr); err != nil {
		return fmt.Errorf("add netns default route: %w", err)
	}
	if err := m.backend.EnsureSlotNAT(ctx, m.mustRef(s)); err != nil {
		return err
	}

	// TAP 移入 slot（脱离 root bridge）。空 TAP 仅用于无 VM 的泄漏测试。
	if s.Tap != "" {
		if err := execCmd(ctx, "ip", "link", "set", s.Tap, "netns", m.nsName(s.Index)); err != nil {
			return fmt.Errorf("move tap to netns: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
			"ip", "link", "set", s.Tap, "up"); err != nil {
			return fmt.Errorf("set tap up: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
			"ip", "link", "set", s.Tap, "master", br); err != nil {
			return fmt.Errorf("attach tap to bridge: %w", err)
		}
	}

	// root 侧隔离 + guest /32 路由（代理/探针通道）。
	if err := m.ensureNodeBackend(ctx, s); err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "route", "replace", s.GuestIP+"/32", "via", nsAddr, "dev", vh); err != nil {
		return fmt.Errorf("add guest route: %w", err)
	}
	// ULA 三层接线（T6）：guest 侧地址由 hypeman vmconfig 注入，本侧只建
	// 转发 + NDP 代理 + 路由（空 ULA = 纯 IPv4，零回归）。
	if s.GuestIP6 != "" {
		if err := m.ensureSlotV6Locked(ctx, s); err != nil {
			return err
		}
	}

	cleanupNetns = false
	return nil
}

// ensureKernel 幂等补齐已有 slot 的内核状态（Reconcile/重复 Attach 用）。
func (m *Manager) ensureKernel(ctx context.Context, s Slot, tap, guestIP, guestIP6 string) error {
	// 目标态副本：execution 更替时调用方尚未更新 s.GuestIP6，但后端 ref
	// （eBPF slot_ula 源绑定）必须用**目标** ULA；空值 = 保持既有（见
	// effectiveGuestIP6——Resume 复挂不携带 ULA）。
	g6 := effectiveGuestIP6(s.GuestIP6, guestIP6)
	s.Tap, s.GuestIP, s.GuestIP6 = tap, guestIP, g6
	var err error
	_, nsAddr, err := m.vethAddrs(s.Index)
	if err != nil {
		return err
	}
	vh, _ := m.vethNames(s.Index)
	if err := m.ensureNodeBackend(ctx, s); err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "route", "replace", guestIP+"/32", "via", nsAddr, "dev", vh); err != nil {
		return fmt.Errorf("re-add guest route: %w", err)
	}
	// TAP 若还在 root ns（崩溃窗口/restore 后重建），补一次移动。
	// M4.5 restore 场景：hypeman standby 释放的是 root ns 视角的网络（TAP
	// 已被移入 slot netns，root 删除沉默跳过），restore 在 root ns 重建同名
	// TAP——此时 netns 内可能残留旧 TAP，直接 move 会撞 "File exists"。
	// 先清理 netns 内同名残留，再移入。
	if tapExistsInRoot(tap) {
		// 先清理 netns 内同名残留（restore 场景：旧 TAP 残留在 netns 里）；
		// 删除失败（不存在）为正常路径，忽略。
		_ = execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index), "ip", "link", "del", tap)
		if err := execCmd(ctx, "ip", "link", "set", tap, "netns", m.nsName(s.Index)); err != nil {
			return fmt.Errorf("re-move tap: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
			"ip", "link", "set", tap, "master", m.brName(s.Index)); err != nil {
			return fmt.Errorf("re-attach tap: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", m.nsName(s.Index),
			"ip", "link", "set", tap, "up"); err != nil {
			return fmt.Errorf("re-up tap: %w", err)
		}
	}
	// v6 接线幂等补齐（TAP 重建后 ULA 经 TAP 路由需重建；replace 语义幂等）。
	if g6 != "" {
		if err := m.ensureSlotV6Locked(ctx, Slot{Index: s.Index, MachineID: s.MachineID, Tap: tap, GuestIP6: g6}); err != nil {
			return err
		}
	}
	return nil
}

// parseSlotULA6 校验 slot ULA（裸地址，fail closed：非法输入拒绝 attach，
// 绝不把错误地址写入内核路由/邻居表）。
func parseSlotULA6(raw string) (string, error) {
	a, err := netip.ParseAddr(raw)
	if err != nil || !a.Is6() || a.Is4In6() {
		return "", fmt.Errorf("slot: invalid guest ULA %q", raw)
	}
	return a.String(), nil
}

// effectiveGuestIP6 解析“未提供 ULA”（provided 空串）的语义：保持既有 ULA。
// reattachSlot（Resume/autoresume/ConvergeResume，adapter.go）不携带
// GuestIP6；空值若被解读为“清空”，会出现“v6 路由已删但 slot_ula 仍认旧
// ULA”的不一致（且 Resume 后 v6 永久失联）。显式清空只走 DetachNetns/
// 新 execution 的完整 attach。
func effectiveGuestIP6(existing, provided string) string {
	if strings.TrimSpace(provided) == "" {
		return existing
	}
	return provided
}

// slotTapMTU6 是 ULA slot 的 TAP MTU（1280 = IPv6 最小 MTU；WG 封装
// 开销下无分片黑洞；guest TCP MSS 由 MTU 自动派生，无需显式 clamp）。
// 仅 v6 slot 设置（纯 v4 slot 保持 1500 默认，零回归）。
const slotTapMTU6 = "1280"

// ensureSlotV6Locked 为 ULA 建三层接线（T6，幂等：全部 replace/add 语义）：
//
//   - root 侧：veth host 口 LL + ULA/128 dev 路由 + NDP 代理 + 转发。
//     到达 WG 口的远端 ULA 包经主表 /64 聚合路由进 WG、本地 ULA 经此 /128
//     路由进 slot；回程对称。
//   - netns 侧：veth/bridge LL + 默认路由经 root + ULA/128 经 TAP。
//     guest 经 bridge NDP 到 fe80::1（GuestGW6），出向经默认路由上 root。
//
// 数据面说明：eBPF 后端下 v6 准入由 tc 程序按 ipcache/policy 裁决（§10）；
// nft 后端 ip6 隔离表默认 drop slot 口 v6——emergency 回退模式不支持东西向，
// 与 §12 fallback 范围（仅 legacy 南北）一致。
func (m *Manager) ensureSlotV6Locked(ctx context.Context, s Slot) error {
	ula, err := parseSlotULA6(s.GuestIP6)
	if err != nil {
		return err
	}
	vh, vg := m.vethNames(s.Index)
	br := m.brName(s.Index)
	ns := m.nsName(s.Index)
	for _, args := range [][]string{
		{"ip", "addr", "replace", slotVethHostLL6 + "/64", "dev", vh},
		{"ip", "-6", "route", "replace", ula + "/128", "dev", vh},
		{"ip", "-6", "neigh", "replace", "proxy", ula, "dev", vh},
		{"sysctl", "-qw", "net.ipv6.conf.all.forwarding=1"},
		{"sysctl", "-qw", "net.ipv6.conf." + vh + ".proxy_ndp=1"},
	} {
		if err := execCmd(ctx, args[0], args[1:]...); err != nil {
			return fmt.Errorf("slot v6 root %q: %w", args, err)
		}
	}
	nsExec := func(args ...string) error {
		full := append([]string{"netns", "exec", ns, "ip"}, args...)
		return execCmd(ctx, "ip", full...)
	}
	for _, args := range [][]string{
		{"addr", "replace", slotVethNsLL6 + "/64", "dev", vg},
		{"addr", "replace", api.SlotBridgeLL6 + "/64", "dev", br},
		{"-6", "route", "replace", "default", "via", slotVethHostLL6, "dev", vg},
		{"-6", "neigh", "replace", "proxy", ula, "dev", vg},
	} {
		if err := nsExec(args...); err != nil {
			return fmt.Errorf("slot v6 netns %q: %w", args, err)
		}
	}
	if err := execCmd(ctx, "ip", "netns", "exec", ns,
		"sysctl", "-qw", "net.ipv6.conf.all.forwarding=1"); err != nil {
		return fmt.Errorf("slot v6 netns forwarding: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", ns,
		"sysctl", "-qw", "net.ipv6.conf."+vg+".proxy_ndp=1"); err != nil {
		return fmt.Errorf("slot v6 netns proxy_ndp: %w", err)
	}
	if s.Tap != "" {
		// ULA /128 路由走 bridge（非 TAP）：NS 源 = bridge 自身 MAC/LL，
		// guest 的单播 NA 才能回到本 netns 协议栈（TAP 是 bridge 从口，
		// 经 TAP 直发会让 NA 以从口 MAC 为目标，被 bridge 洪泛丢弃，neigh
		// 永久 INCOMPLETE——真机 spike 实测）。bridge 已有 fe80::1（网关）。
		if err := nsExec("-6", "route", "replace", ula+"/128", "dev", br); err != nil {
			return fmt.Errorf("slot v6 bridge route: %w", err)
		}
		// TAP MTU 1280（与 guest eth0 对齐，见 hypeman planNetwork；
		// link set mtu 可重复执行，幂等）。
		if err := nsExec("link", "set", s.Tap, "mtu", slotTapMTU6); err != nil {
			return fmt.Errorf("slot v6 tap mtu: %w", err)
		}
	}
	return nil
}

// removeSlotV6Locked 尽力回收 execution 更替后残留的旧 ULA 路由与 NDP 代理
// （slot 复用场景），以及 releaseLocked 在 netns 删除前的前置回收（root 侧
// 条目引用 vh，netns 删除不连带清理）。
func (m *Manager) removeSlotV6Locked(ctx context.Context, s Slot) {
	ula, err := parseSlotULA6(s.GuestIP6)
	if err != nil {
		return
	}
	vh, vg := m.vethNames(s.Index)
	ns := m.nsName(s.Index)
	_ = execCmd(ctx, "ip", "-6", "route", "del", ula+"/128", "dev", vh)
	_ = execCmd(ctx, "ip", "-6", "neigh", "del", "proxy", ula, "dev", vh)
	_ = execCmd(ctx, "ip", "netns", "exec", ns,
		"ip", "-6", "neigh", "del", "proxy", ula, "dev", vg)
	if s.Tap != "" {
		// 与 ensureSlotV6Locked 对应：/128 路由经 bridge（清理也按 bridge；
		// netns 将删时此满量回收仅是尽力，设备销毁兑底）。
		_ = execCmd(ctx, "ip", "netns", "exec", ns,
			"ip", "-6", "route", "del", ula+"/128", "dev", m.brName(s.Index))
	}
}

func (m *Manager) releaseLocked(ctx context.Context, s Slot) error {
	// netns 删除只销毁 netns 内的设备（对端 veth/bridge/TAP），root 侧 veth
	// 与引用它的路由/邻居/地址不连带消失（W1 实测：ip netns del 后 vh、
	// /32、/128、NDP 代理均残留），必须显式删除。删 vh 时内核自动回收
	// 引用它的路由与邻居条目，此处再补一次显式 v6 回收防 text-book 差异。
	ref, err := m.refFor(s)
	if err == nil {
		_ = m.backend.DetachSlot(ctx, ref)
	}
	if s.GuestIP6 != "" {
		m.removeSlotV6Locked(ctx, s)
	}
	if err := m.deleteNetns(s.Index); err != nil {
		return err
	}
	if err == nil && ref.VethHost != "" {
		// root 侧 veth 显式删除（对端销毁不连带删除本端）。best-effort：
		// 已不存在属预期（幂等重入），后续等待循环做最终确认。
		_ = execCmd(ctx, "ip", "link", "del", ref.VethHost)
	}
	// netlink 清理异步：等 root 侧 veth 真正消失，否则 index 立即复用会撞
	// "File exists"（泄漏测试与高并发 create/delete 都踩过）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat("/sys/class/net/" + ref.VethHost); os.IsNotExist(err) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("slot: veth %s lingered after netns delete", ref.VethHost)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// gatewayAddr 返回 slot 内 bridge 的地址与掩码（点分形式）。
func (m *Manager) gatewayAddr() (string, string, error) {
	_, ipNet, err := net.ParseCIDR(m.cfg.SubnetCIDR)
	if err != nil {
		return "", "", fmt.Errorf("slot: bad SubnetCIDR %q: %w", m.cfg.SubnetCIDR, err)
	}
	gw := net.ParseIP(m.cfg.Gateway)
	if gw == nil {
		return "", "", fmt.Errorf("slot: bad Gateway %q", m.cfg.Gateway)
	}
	mask := fmt.Sprintf("%d.%d.%d.%d", ipNet.Mask[0], ipNet.Mask[1], ipNet.Mask[2], ipNet.Mask[3])
	return gw.String(), mask, nil
}

// vethAddrs 计算 slot index 的 /30 点对点地址（host=块内第一个，netns=第二个）。
// 10.12.A.B：A=idx/63，B=(idx%63)*4。host=…B+1，netns=…B+2。
// v6 链路本地角色（ADR-0040 §7/§9，T6）：网关地址见 api.SlotBridgeLL6
// （guest 默认网关经 vmconfig 下发同值）；veth host 侧 fe80::1（root ns，
// netns 默认路由下一跳），netns 侧 fe80::2。LL 作用域限于各自 netns+链路，
// 跨 slot 无冲突。
const (
	slotVethHostLL6 = "fe80::1"
	slotVethNsLL6   = "fe80::2"
)

func (m *Manager) vethAddrs(idx int) (host, ns string, err error) {
	if idx < 0 || idx > 16000 {
		return "", "", fmt.Errorf("slot: index %d out of range", idx)
	}
	a := idx / 63
	b := (idx % 63) * 4
	return fmt.Sprintf("%d.%d.%d.%d", m.vethBase[0], m.vethBase[1], a, b+1),
		fmt.Sprintf("%d.%d.%d.%d", m.vethBase[0], m.vethBase[1], a, b+2), nil
}

func (m *Manager) nsName(idx int) string { return fmt.Sprintf("%s-slot-%d", m.namePrefix, idx) }

func (m *Manager) vethNames(idx int) (host, guest string) {
	return fmt.Sprintf("%s-vp%d", m.namePrefix, idx), fmt.Sprintf("%s-vpg%d", m.namePrefix, idx)
}

func (m *Manager) brName(idx int) string { return fmt.Sprintf("%s-br%d", m.namePrefix, idx) }

// nsPrefixText 是本 manager 的 netns 名字前缀（fp-slot-/fpb-slot-…）。
func (m *Manager) nsPrefixText() string { return m.namePrefix + "-slot-" }

func deriveGateway(cidr string) (string, error) {
	ip, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", fmt.Errorf("slot: bad subnet %q: %w", cidr, err)
	}
	// 第一个可用地址 = 网络地址 + 1（hypeman DeriveGateway 同语义）。
	gw := make(net.IP, len(ip))
	copy(gw, ip)
	gw = gw.Mask(ipNet.Mask) // 先归零主机位，再 +1
	for i := len(gw) - 1; i >= 0; i-- {
		if gw[i] < 255 {
			gw[i]++
			break
		}
		gw[i] = 0
	}
	return gw.String(), nil
}

// ---------------------------------------------------------------------------
// 内核命令
// ---------------------------------------------------------------------------

func execCmd(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w (%s)", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (m *Manager) netnsExists(idx int) (bool, error) {
	out, err := exec.Command("ip", "netns", "list").Output()
	if err != nil {
		return false, fmt.Errorf("list netns: %w", err)
	}
	want := m.nsName(idx)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), want+" ") || strings.TrimSpace(line) == want {
			return true, nil
		}
	}
	return false, nil
}

// listStrayNetns 只扫描本 manager 前缀的 fp-*-slot-<n> netns（同主机多
// agent 时互不干扰，见 Config.NamePrefix）。
func (m *Manager) listStrayNetns() ([]int, error) {
	out, err := exec.Command("ip", "netns", "list").Output()
	if err != nil {
		return nil, fmt.Errorf("list netns: %w", err)
	}
	var idxs []int
	prefix := m.nsPrefixText()
	for _, line := range strings.Split(string(out), "\n") {
		name := strings.Fields(strings.TrimSpace(line))
		if len(name) == 0 || !strings.HasPrefix(name[0], prefix) {
			continue
		}
		var idx int
		if _, err := fmt.Sscanf(name[0], prefix+"%d", &idx); err == nil {
			idxs = append(idxs, idx)
		}
	}
	return idxs, nil
}

func (m *Manager) deleteNetns(idx int) error {
	if err := exec.Command("ip", "netns", "del", m.nsName(idx)).Run(); err != nil {
		// 已经不存在视为成功。
		if exists, _ := m.netnsExists(idx); exists {
			return fmt.Errorf("delete netns %s: %w", m.nsName(idx), err)
		}
	}
	return nil
}

func tapExistsInRoot(tap string) bool {
	if tap == "" {
		return false
	}
	_, err := os.Stat("/sys/class/net/" + tap)
	return err == nil
}
