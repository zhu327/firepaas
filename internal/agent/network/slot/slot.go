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
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/agent/netpolicy"
	"github.com/zhu327/firepaas/shared/pkg/durablewrite"
)

const (
	nsPrefix   = "fp-slot-"
	vethPrefix = "fp-vp"
	brPrefix   = "fp-br"
	// VethRange 是 root↔netns 点对点链路的地址池（10.12.0.0/16）。
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
	// “非 established 即 drop”截断）。
	EgressProxyPort80  int
	EgressProxyPort443 int
}

// Slot 是一个已分配的 slot。
type Slot struct {
	Index     int    `json:"index"`
	MachineID string `json:"machine_id"`
	Tap       string `json:"tap"`
	GuestIP   string `json:"guest_ip"`
	// Egress（v1.3-A，ADR-0027）：本 slot 当前应用的 egress 规则集
	//（重启后按此重放；空 Mode = 未声明）。
	Egress EgressRuleSet `json:"egress,omitempty"`
}

// EgressRuleSet 描述一个 slot 的 egress 执行规则（nftables 落地）。
// 由 agent 的 egress 层计算，slot 只负责内核落地：
//   - 80/443 域名流量 DNAT 到 root ns 透明代理（ProxyPort80/443）；
//   - 其余流量按 Mode + Allowed/Denied CIDR 在 netns 内过滤；
//   - Generation 是 fencing 水位（只升不降）。
type EgressRuleSet struct {
	Mode         string   `json:"mode"` // unrestricted | deny_all | allowlist
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`
	DeniedCIDRs  []string `json:"denied_cidrs,omitempty"`
	Domains      []string `json:"domains,omitempty"`       // 归一化域名（重启重建 proxy 用）
	ProxyPort80  int      `json:"proxy_port80,omitempty"`  // 0 = 不代理
	ProxyPort443 int      `json:"proxy_port443,omitempty"` // 0 = 不代理
	MaxTCPConns  uint32   `json:"max_tcp_conns,omitempty"` // 0 = 不限
	AuditAll     bool     `json:"audit_all,omitempty"`
	Generation   uint64   `json:"generation,omitempty"`
}

// LiveInstance 是 Reconcile 时 agent 提供的存活实例视图。
type LiveInstance struct {
	MachineID string // firepaas 稳定 machine_id（hypeman instance Name）
	Tap       string // hypeman TAP 名
	GuestIP   string // guest 地址（hypeman allocation IP）
}

// Manager 管理 slot 的分配/释放/启动回收。所有内核操作串行化。
type Manager struct {
	cfg Config

	mu    sync.Mutex
	slots map[string]Slot // key: machine_id
}

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
	return &Manager{cfg: cfg, slots: map[string]Slot{}}, nil
}

// Load 从状态文件恢复内存视图（Reconcile 前调用）。
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := os.ReadFile(m.cfg.StatePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("slot: read state: %w", err)
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

// Attach 为 machine 建立 slot：创建 netns/veth/bridge，把 hypeman TAP 移入，
// 加 nftables 隔离与 root /32 路由。失败时回收已创建的内核对象。
func (m *Manager) Attach(ctx context.Context, machineID, tap, guestIP string) (Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.slots[machineID]; ok {
		// 幂等：同 machine 重复 attach 直接复用。若换了 TAP（新 execution 重建
		// 时 hypeman 生成新实例/TAP），先摘除 netns 里可能残留的旧 TAP。
		if s.Tap != tap && tap != "" && s.Tap != "" {
			_ = exec.Command("ip", "netns", "exec", nsName(s.Index), "ip", "link", "del", s.Tap).Run()
		}
		if err := m.ensureKernel(ctx, s, tap, guestIP); err != nil {
			return Slot{}, err
		}
		// Reattach changes execution plumbing, not the deployment policy. Preserve
		// the persisted rule set so it can be replayed and rebuilt by egress.Manager.
		s.Tap = tap
		s.GuestIP = guestIP
		m.slots[machineID] = s
		if err := m.persistLocked(); err != nil {
			return Slot{}, err
		}
		return m.slots[machineID], nil
	}
	if guestIP == "" {
		return Slot{}, fmt.Errorf("slot: guest_ip is required")
	}

	idx, err := m.allocIndexLocked()
	if err != nil {
		return Slot{}, err
	}
	s := Slot{Index: idx, MachineID: machineID, Tap: tap, GuestIP: guestIP}
	if err := m.setupLocked(ctx, s); err != nil {
		_ = m.releaseLocked(ctx, s.Index)
		return Slot{}, err
	}
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		_ = m.releaseLocked(ctx, s.Index)
		delete(m.slots, machineID)
		return Slot{}, err
	}
	return s, nil
}

// Release 删除 slot netns（连带 TAP/veth/bridge/路由）并移除 nft 集合元素。
func (m *Manager) Release(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return nil
	}
	if err := m.releaseLocked(ctx, s.Index); err != nil {
		return err
	}
	delete(m.slots, machineID)
	return m.persistLocked()
}

// Reconcile 启动/周期回收：
//  1. 内核有、状态无的 fp-slot-* netns → 删除（attach 中途崩溃残留）；
//  2. 状态有、netns 无 → 丢弃条目（已随内核清理）；
//  3. 状态有、netns 有：live 里没有 → 删除 netns + 条目（VM 已死）；
//     live 里有 → 幂等补 route/nft 元素；
//  4. live 有、状态无 → TAP 还在 root ns（hypeman 创建后 agentd 崩溃窗口），
//     重新 attach。
func (m *Manager) Reconcile(ctx context.Context, live []LiveInstance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	liveByID := make(map[string]LiveInstance, len(live))
	for _, l := range live {
		if l.MachineID != "" {
			liveByID[l.MachineID] = l
		}
	}

	// 1/2/3：内核 ↔ 状态对齐。错误一律降级（记日志继续）：slot 异常不能
	// 阻塞 agentd 启动（M3 真机事故：僵尸 guest 目录 → reconcile 报错 →
	// agentd crash-loop → 整个节点数据面不可用）。
	for id, s := range m.slots {
		exists, err := netnsExists(s.Index)
		if err != nil {
			logf(slog.LevelWarn, "slot: reconcile netns check %s: %v", id, err)
			continue
		}
		if !exists {
			delete(m.slots, id)
			continue
		}
		if l, ok := liveByID[id]; ok && l.Tap == s.Tap {
			if err := m.ensureKernel(ctx, s, l.Tap, l.GuestIP); err != nil {
				logf(slog.LevelWarn, "slot: re-ensure %s (degraded): %v", id, err)
			}
			// v1.3-A（ADR-0027）：重启后重放持久化 egress 规则。
			if s.Egress.Mode != "" {
				// Reconcile already owns m.mu; call the lock-free helper to avoid
				// self-deadlocking through RestoreEgress.
				if err := ensureSlotEgress(ctx, s.Index, s.Egress); err != nil {
					logf(slog.LevelWarn, "slot: restore egress %s (degraded): %v", id, err)
				}
			}
			continue
		}
		if err := m.releaseLocked(ctx, s.Index); err != nil {
			logf(slog.LevelWarn, "slot: reconcile release %s (degraded): %v", id, err)
			continue
		}
		delete(m.slots, id)
	}

	// 内核残留 netns（状态里没有的）。
	strays, err := listStrayNetns()
	if err != nil {
		logf(slog.LevelWarn, "slot: list stray netns: %v", err)
	}
	for _, idx := range strays {
		// 只回收不在状态里的；正在 attach 的窗口由本包串行锁排除。
		if !m.hasIndexLocked(idx) {
			_ = deleteNetns(idx)
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
		s := Slot{Index: idx, MachineID: id, Tap: l.Tap, GuestIP: l.GuestIP}
		if err := m.setupLocked(ctx, s); err != nil {
			_ = m.releaseLocked(ctx, s.Index)
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

// ApplyEgressPolicy（v1.3-A，ADR-0027）在 slot netns 内落地 egress 规则集。
// 全量替换 + generation fencing：新 generation 小于已应用值 → 拒绝并保持旧
// 规则。重复应用同 generation 幂等（flush + rebuild）。
func (m *Manager) ApplyEgressPolicy(ctx context.Context, machineID string, rs EgressRuleSet) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return fmt.Errorf("slot: egress apply: machine %s has no slot", machineID)
	}
	if rs.Generation < s.Egress.Generation {
		return fmt.Errorf("slot: egress generation fencing: applied %d > requested %d (keep old policy)",
			s.Egress.Generation, rs.Generation)
	}
	if err := ensureSlotEgress(ctx, s.Index, rs); err != nil {
		return err
	}
	old := s.Egress
	s.Egress = rs
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		s.Egress = old
		m.slots[machineID] = s
		var rollbackErr error
		if old.Mode == "" {
			rollbackErr = clearSlotEgress(ctx, s.Index)
		} else {
			rollbackErr = ensureSlotEgress(ctx, s.Index, old)
		}
		return errors.Join(err, rollbackErr)
	}
	return nil
}

// CurrentEgressPolicy returns the persisted policy snapshot used by the
// egress coordinator for rollback.
func (m *Manager) CurrentEgressPolicy(machineID string) (EgressRuleSet, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	return s.Egress, ok && s.Egress.Mode != ""
}

// RollbackEgressPolicy restores a snapshot without generation fencing. It is
// only exposed to the serialized egress coordinator after a committed nft
// update could not be published by the proxy.
func (m *Manager) RollbackEgressPolicy(ctx context.Context, machineID string, rs EgressRuleSet, present bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return fmt.Errorf("slot: egress rollback: machine %s has no slot", machineID)
	}
	if present {
		if err := ensureSlotEgress(ctx, s.Index, rs); err != nil {
			return err
		}
		s.Egress = rs
	} else {
		if err := clearSlotEgress(ctx, s.Index); err != nil {
			return err
		}
		s.Egress = EgressRuleSet{}
	}
	m.slots[machineID] = s
	return m.persistLocked()
}

// RemoveEgressPolicy 清空 slot 的 egress 规则（恢复 unrestricted 全通）。
func (m *Manager) RemoveEgressPolicy(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok {
		return nil
	}
	if s.Egress.Mode == "" {
		return nil
	}
	if err := clearSlotEgress(ctx, s.Index); err != nil {
		return err
	}
	old := s.Egress
	s.Egress = EgressRuleSet{}
	m.slots[machineID] = s
	if err := m.persistLocked(); err != nil {
		s.Egress = old
		m.slots[machineID] = s
		return errors.Join(err, ensureSlotEgress(ctx, s.Index, old))
	}
	return nil
}

// RestoreEgress 重启后按持久化状态重放 slot egress 规则（Reconcile 内用）。
func (m *Manager) RestoreEgress(ctx context.Context, machineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[machineID]
	if !ok || s.Egress.Mode == "" {
		return nil
	}
	return ensureSlotEgress(ctx, s.Index, s.Egress)
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
	return durablewrite.WriteFileAtomic(m.cfg.StatePath, "slot state", raw)
}

func (m *Manager) setupLocked(ctx context.Context, s Slot) error {
	if err := execCmd(ctx, "ip", "netns", "add", nsName(s.Index)); err != nil {
		return fmt.Errorf("add netns: %w", err)
	}
	cleanupNetns := true
	defer func() {
		if cleanupNetns {
			_ = deleteNetns(s.Index)
		}
	}()

	hostAddr, nsAddr, err := vethAddrs(s.Index)
	if err != nil {
		return err
	}
	vh, vg := vethNames(s.Index)

	if err := execCmd(ctx, "ip", "link", "add", vh, "type", "veth", "peer", "name", vg); err != nil {
		return fmt.Errorf("add veth: %w", err)
	}
	if err := execCmd(ctx, "ip", "link", "set", vg, "netns", nsName(s.Index)); err != nil {
		return fmt.Errorf("move veth to netns: %w", err)
	}
	if err := execCmd(ctx, "ip", "addr", "add", hostAddr+"/30", "dev", vh); err != nil {
		return fmt.Errorf("add host veth addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "link", "set", vh, "up"); err != nil {
		return fmt.Errorf("set host veth up: %w", err)
	}

	// netns 内部：bridge（guest 网关）+ veth + 默认路由 + 一级 NAT。
	br := brName(s.Index)
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"sysctl", "-qw", "net.ipv4.ip_forward=1"); err != nil {
		return fmt.Errorf("netns ip_forward: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "link", "add", br, "type", "bridge"); err != nil {
		return fmt.Errorf("add slot bridge: %w", err)
	}
	gw, mask, err := m.gatewayAddr()
	if err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "addr", "add", gw+"/"+mask, "dev", br); err != nil {
		return fmt.Errorf("add bridge addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "link", "set", br, "up"); err != nil {
		return fmt.Errorf("set bridge up: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "link", "set", vg, "up"); err != nil {
		return fmt.Errorf("set netns veth up: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "addr", "add", nsAddr+"/30", "dev", vg); err != nil {
		return fmt.Errorf("add netns veth addr: %w", err)
	}
	if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
		"ip", "route", "add", "default", "via", hostAddr); err != nil {
		return fmt.Errorf("add netns default route: %w", err)
	}
	if err := ensureNetnsNAT(ctx, s.Index, vg, hostAddr); err != nil {
		return err
	}

	// TAP 移入 slot（脱离 root bridge）。空 TAP 仅用于无 VM 的泄漏测试。
	if s.Tap != "" {
		if err := execCmd(ctx, "ip", "link", "set", s.Tap, "netns", nsName(s.Index)); err != nil {
			return fmt.Errorf("move tap to netns: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
			"ip", "link", "set", s.Tap, "up"); err != nil {
			return fmt.Errorf("set tap up: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
			"ip", "link", "set", s.Tap, "master", br); err != nil {
			return fmt.Errorf("attach tap to bridge: %w", err)
		}
	}

	// root 侧隔离 + guest /32 路由（代理/探针通道）。
	if err := m.ensureIsolationTable(ctx, vh); err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "route", "replace", s.GuestIP+"/32", "via", nsAddr, "dev", vh); err != nil {
		return fmt.Errorf("add guest route: %w", err)
	}

	cleanupNetns = false
	return nil
}

// ensureKernel 幂等补齐已有 slot 的内核状态（Reconcile/重复 Attach 用）。
func (m *Manager) ensureKernel(ctx context.Context, s Slot, tap, guestIP string) error {
	var err error
	_, nsAddr, err := vethAddrs(s.Index)
	if err != nil {
		return err
	}
	vh, _ := vethNames(s.Index)
	if err := m.ensureIsolationTable(ctx, vh); err != nil {
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
		_ = execCmd(ctx, "ip", "netns", "exec", nsName(s.Index), "ip", "link", "del", tap)
		if err := execCmd(ctx, "ip", "link", "set", tap, "netns", nsName(s.Index)); err != nil {
			return fmt.Errorf("re-move tap: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
			"ip", "link", "set", tap, "master", brName(s.Index)); err != nil {
			return fmt.Errorf("re-attach tap: %w", err)
		}
		if err := execCmd(ctx, "ip", "netns", "exec", nsName(s.Index),
			"ip", "link", "set", tap, "up"); err != nil {
			return fmt.Errorf("re-up tap: %w", err)
		}
	}
	return nil
}

func (m *Manager) releaseLocked(ctx context.Context, idx int) error {
	vh, _ := vethNames(idx)
	// netns 删除会连带销毁 veth/TAP/bridge，root 侧 /32 路由随设备自动消失；
	// nft 集合元素摘除是 best-effort（表可能从未建过）。ip/ip6 两个
	// family 的集合都要摘除。
	_ = exec.Command("nft", "delete", "element", "ip", "fp-isolation", "slot-veths", "{", vh, "}").Run()
	_ = exec.Command("nft", "delete", "element", "ip6", "fp-isolation", "slot-veths", "{", vh, "}").Run()
	if err := deleteNetns(idx); err != nil {
		return err
	}
	// netlink 清理异步：等 root 侧 veth 真正消失，否则 index 立即复用会撞
	// "File exists"（泄漏测试与高并发 create/delete 都踩过）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat("/sys/class/net/" + vh); os.IsNotExist(err) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("slot: veth %s lingered after netns delete", vh)
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
func vethAddrs(idx int) (host, ns string, err error) {
	if idx < 0 || idx > 16000 {
		return "", "", fmt.Errorf("slot: index %d out of range", idx)
	}
	a := idx / 63
	b := (idx % 63) * 4
	return fmt.Sprintf("10.12.%d.%d", a, b+1), fmt.Sprintf("10.12.%d.%d", a, b+2), nil
}

func nsName(idx int) string { return fmt.Sprintf("%s%d", nsPrefix, idx) }
func vethNames(idx int) (host, guest string) {
	return fmt.Sprintf("%s%d", vethPrefix, idx), fmt.Sprintf("%sg%d", vethPrefix, idx)
}
func brName(idx int) string { return fmt.Sprintf("%s%d", brPrefix, idx) }

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

func netnsExists(idx int) (bool, error) {
	out, err := exec.Command("ip", "netns", "list").Output()
	if err != nil {
		return false, fmt.Errorf("list netns: %w", err)
	}
	want := nsName(idx)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), want+" ") || strings.TrimSpace(line) == want {
			return true, nil
		}
	}
	return false, nil
}

func listStrayNetns() ([]int, error) {
	out, err := exec.Command("ip", "netns", "list").Output()
	if err != nil {
		return nil, fmt.Errorf("list netns: %w", err)
	}
	var idxs []int
	for _, line := range strings.Split(string(out), "\n") {
		name := strings.Fields(strings.TrimSpace(line))
		if len(name) == 0 || !strings.HasPrefix(name[0], nsPrefix) {
			continue
		}
		var idx int
		if _, err := fmt.Sscanf(name[0], nsPrefix+"%d", &idx); err == nil {
			idxs = append(idxs, idx)
		}
	}
	return idxs, nil
}

func deleteNetns(idx int) error {
	if err := exec.Command("ip", "netns", "del", nsName(idx)).Run(); err != nil {
		// 已经不存在视为成功。
		if exists, _ := netnsExists(idx); exists {
			return fmt.Errorf("delete netns %s: %w", nsName(idx), err)
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

// isolationStepsIPv4 生成 fp-isolation（ip family）的幂等建表步骤。纯函数——
// 规则文本可调试、可单测断言（R2：nft 变更的最低验证是脚本/规则生成级）。
// 私网目标集合来自 netpolicy 的 canonical 集合（Go matcher 与规则文本同源）。
func (m *Manager) isolationStepsIPv4() [][]string {
	return [][]string{
		{"nft", "add", "table", "ip", "fp-isolation"},
		{"nft", "add", "set", "ip", "fp-isolation", "slot-veths", "{", "type", "ifname;", "}"},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "in",
			"{", "type", "filter", "hook", "input", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "fwdchain",
			"{", "type", "filter", "hook", "forward", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip", "fp-isolation", "post",
			"{", "type", "nat", "hook", "postrouting", "priority", "srcnat;", "}",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths",
			"ct", "state", "established,related", "accept",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths",
			"tcp", "dport", "{", fmt.Sprintf("%d,%d", m.cfg.EgressProxyPort80, m.cfg.EgressProxyPort443), "}", "accept",
		},
		{"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths", "drop"},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths",
			"ct", "state", "established,related", "accept",
		},
		{
			"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths",
			"ip", "daddr", netpolicy.IPv4NftSetText(), "drop",
		},
		{"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "accept"},
		{"nft", "add", "rule", "ip", "fp-isolation", "post", "ip", "saddr", VethRange, "masquerade"},
	}
}

// ip6IsolationSteps 生成 fp-isolation（ip6 family）的默认拒绝步骤（R2 IPv6
// 默认拒绝，选择理由见包注释）：slot veth 入向/转发一律 drop，无 established
// 放行——slot 数据面是纯 IPv4，v6 上没有应当存在的流量，全拒即正确语义。
func ip6IsolationSteps() [][]string {
	return [][]string{
		{"nft", "add", "table", "ip6", "fp-isolation"},
		{"nft", "add", "set", "ip6", "fp-isolation", "slot-veths", "{", "type", "ifname;", "}"},
		{
			"nft", "add", "chain", "ip6", "fp-isolation", "in",
			"{", "type", "filter", "hook", "input", "priority", "filter;", "}",
		},
		{
			"nft", "add", "chain", "ip6", "fp-isolation", "fwdchain",
			"{", "type", "filter", "hook", "forward", "priority", "filter;", "}",
		},
		{"nft", "add", "rule", "ip6", "fp-isolation", "in", "iifname", "@slot-veths", "drop"},
		{"nft", "add", "rule", "ip6", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "drop"},
	}
}

// ensureIsolationTable 幂等创建 root 侧 nftables 隔离表并把 veth 加入集合。
func (m *Manager) ensureIsolationTable(ctx context.Context, veth string) error {
	// 表已存在则跳过（检查 exit code：nft list 不存在时非零）。
	if err := exec.Command("nft", "list", "table", "ip", "fp-isolation").Run(); err != nil {
		for _, args := range m.isolationStepsIPv4() {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: nft setup (%s): %w", strings.Join(args, " "), err)
			}
		}
	}
	// ip6 family 独立检查：升级场景（ip 表已存在、ip6 未建）必须补齐。
	if err := exec.Command("nft", "list", "table", "ip6", "fp-isolation").Run(); err != nil {
		for _, args := range ip6IsolationSteps() {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: nft6 setup (%s): %w", strings.Join(args, " "), err)
			}
		}
	}
	// 集合元素幂等：EEXIST 可忽略。两个 family 都要登记（v6 默认拒绝集合）。
	for _, family := range []string{"ip", "ip6"} {
		out, err := exec.Command("nft", "add", "element", family, "fp-isolation", "slot-veths", "{", veth, "}").
			CombinedOutput()
		if err != nil && !strings.Contains(string(out), "exists") {
			return fmt.Errorf("slot: nft add veth %s (%s): %w (%s)", veth, family, err, strings.TrimSpace(string(out)))
		}
	}
	// v1.3-A：升级场景下表已存在但没有 egress 代理 INPUT accept 规则时补齐
	//（必须插在 drop 之前，否则 SYN 被截断）。
	if err := m.ensureEgressProxyInputRule(ctx); err != nil {
		return err
	}
	return nil
}

// ensureEgressProxyInputRule 幂等保证 INPUT 链中存在 slot→egress 代理端口的
// accept 规则（位于 drop 规则之前）。nft 的 position 参数语义是**handle**而非
// 序号，因此先带 handle 列出链，定位 drop 规则后在其前插入。
func (m *Manager) ensureEgressProxyInputRule(ctx context.Context) error {
	marker := fmt.Sprintf("%d", m.cfg.EgressProxyPort80)
	out, err := exec.Command("nft", "-a", "list", "chain", "ip", "fp-isolation", "in").Output()
	if err != nil {
		return fmt.Errorf("slot: list input chain: %w", err)
	}
	listing := string(out)
	if strings.Contains(listing, marker) {
		return nil
	}
	// 解析 drop 规则（@slot-veths 且以 drop 结尾）的 handle。
	dropHandle := ""
	for _, line := range strings.Split(listing, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "@slot-veths") || !strings.HasSuffix(line, "drop") {
			continue
		}
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "handle" {
				dropHandle = fields[i+1]
			}
		}
	}
	if dropHandle == "" {
		return fmt.Errorf("slot: egress input accept: drop rule handle not found in fp-isolation")
	}
	if err := execCmd(ctx, "nft", "insert", "rule", "ip", "fp-isolation", "in", "position", dropHandle,
		"iifname", "@slot-veths", "tcp", "dport", "{", fmt.Sprintf("%d,%d", m.cfg.EgressProxyPort80, m.cfg.EgressProxyPort443), "}", "accept"); err != nil {
		return fmt.Errorf("slot: insert egress proxy input rule: %w", err)
	}
	return nil
}

// ensureNetnsNAT 幂等创建 slot 内一级 NAT（出口 masquerade，代理回流不改写）。
func ensureNetnsNAT(ctx context.Context, idx int, vethGuest, hostAddr string) error {
	ns := nsName(idx)
	if err := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", "ip", "fp-slot").Run(); err != nil {
		steps := [][]string{
			{"ip", "netns", "exec", ns, "nft", "add", "table", "ip", "fp-slot"},
			{
				"ip",
				"netns",
				"exec",
				ns,
				"nft",
				"add",
				"chain",
				"ip",
				"fp-slot",
				"post",
				"{",
				"type",
				"nat",
				"hook",
				"postrouting",
				"priority",
				"srcnat;",
				"}",
			},
			{
				"ip",
				"netns",
				"exec",
				ns,
				"nft",
				"add",
				"rule",
				"ip",
				"fp-slot",
				"post",
				"oifname",
				vethGuest,
				"ip",
				"daddr",
				"!=",
				hostAddr,
				"masquerade",
			},
		}
		for _, args := range steps {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: netns nft setup: %w", err)
			}
		}
	}
	return nil
}
