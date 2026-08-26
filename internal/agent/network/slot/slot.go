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
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	nsPrefix   = "fp-slot-"
	vethPrefix = "fp-vp"
	brPrefix   = "fp-br"
	// vethRange 是 root↔netns 点对点链路的地址池（10.12.0.0/16）。
	vethRange = "10.12.0.0/16"
	// 私有目标集合：guest 永远不可达（host/私网/组播）。
	privateDst = "{ 10.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 224.0.0.0/4 }"
)

// Config 是 slot manager 配置。
type Config struct {
	SubnetCIDR string // hypeman 网络子网（guest 侧），如 10.100.0.0/16
	Gateway    string // 子网网关（slot 内 bridge 地址），如 10.100.0.1
	StatePath  string // slots.json 状态文件（agent data_dir 下）
}

// Slot 是一个已分配的 slot。
type Slot struct {
	Index     int    `json:"index"`
	MachineID string `json:"machine_id"`
	Tap       string `json:"tap"`
	GuestIP   string `json:"guest_ip"`
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
		m.slots[machineID] = Slot{Index: s.Index, MachineID: machineID, Tap: tap, GuestIP: guestIP}
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
			logf("slot: reconcile netns check %s: %v", id, err)
			continue
		}
		if !exists {
			delete(m.slots, id)
			continue
		}
		if l, ok := liveByID[id]; ok && l.Tap == s.Tap {
			if err := m.ensureKernel(ctx, s, l.Tap, l.GuestIP); err != nil {
				logf("slot: re-ensure %s (degraded): %v", id, err)
			}
			continue
		}
		if err := m.releaseLocked(ctx, s.Index); err != nil {
			logf("slot: reconcile release %s (degraded): %v", id, err)
			continue
		}
		delete(m.slots, id)
	}

	// 内核残留 netns（状态里没有的）。
	strays, err := listStrayNetns()
	if err != nil {
		logf("slot: list stray netns: %v", err)
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
			logf("slot: reconcile attach %s failed (degraded): %v", id, err)
			continue
		}
		m.slots[id] = s
	}
	return m.persistLocked()
}

// logf 是包内日志出口（避免 slot 包依赖 slog 的全局 handler 配置）。
func logf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
}

// Count 返回当前 slot 数（测试/观测用）。
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.slots)
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
	if err := os.MkdirAll(filepath.Dir(m.cfg.StatePath), 0o755); err != nil {
		return fmt.Errorf("slot: mkdir state dir: %w", err)
	}
	tmp := m.cfg.StatePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("slot: write state: %w", err)
	}
	return os.Rename(tmp, m.cfg.StatePath)
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
	if err := ensureIsolationTable(ctx, vh); err != nil {
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
	_, nsAddr, err := vethAddrs(s.Index)
	if err != nil {
		return err
	}
	vh, _ := vethNames(s.Index)
	if err := ensureIsolationTable(ctx, vh); err != nil {
		return err
	}
	if err := execCmd(ctx, "ip", "route", "replace", guestIP+"/32", "via", nsAddr, "dev", vh); err != nil {
		return fmt.Errorf("re-add guest route: %w", err)
	}
	// TAP 若还在 root ns（崩溃窗口），补一次移动。
	if tapExistsInRoot(tap) {
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
	// nft 集合元素摘除是 best-effort（表可能从未建过）。
	_ = exec.Command("nft", "delete", "element", "ip", "fp-isolation", "slot-veths", "{", vh, "}").Run()
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

// ensureIsolationTable 幂等创建 root 侧 nftables 隔离表并把 veth 加入集合。
func ensureIsolationTable(ctx context.Context, veth string) error {
	// 表已存在则跳过（检查 exit code：nft list 不存在时非零）。
	if err := exec.Command("nft", "list", "table", "ip", "fp-isolation").Run(); err != nil {
		steps := [][]string{
			{"nft", "add", "table", "ip", "fp-isolation"},
			{"nft", "add", "set", "ip", "fp-isolation", "slot-veths", "{", "type", "ifname;", "}"},
			{"nft", "add", "chain", "ip", "fp-isolation", "in", "{", "type", "filter", "hook", "input", "priority", "filter;", "}"},
			{"nft", "add", "chain", "ip", "fp-isolation", "fwdchain", "{", "type", "filter", "hook", "forward", "priority", "filter;", "}"},
			{"nft", "add", "chain", "ip", "fp-isolation", "post", "{", "type", "nat", "hook", "postrouting", "priority", "srcnat;", "}"},
			{"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths", "ct", "state", "established,related", "accept"},
			{"nft", "add", "rule", "ip", "fp-isolation", "in", "iifname", "@slot-veths", "drop"},
			{"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "ct", "state", "established,related", "accept"},
			{"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "ip", "daddr", privateDst, "drop"},
			{"nft", "add", "rule", "ip", "fp-isolation", "fwdchain", "iifname", "@slot-veths", "accept"},
			{"nft", "add", "rule", "ip", "fp-isolation", "post", "ip", "saddr", vethRange, "masquerade"},
		}
		for _, args := range steps {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: nft setup (%s): %w", strings.Join(args, " "), err)
			}
		}
	}
	// 集合元素幂等：EEXIST 可忽略。
	out, err := exec.Command("nft", "add", "element", "ip", "fp-isolation", "slot-veths", "{", veth, "}").CombinedOutput()
	if err != nil && !strings.Contains(string(out), "exists") {
		return fmt.Errorf("slot: nft add veth %s: %w (%s)", veth, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ensureNetnsNAT 幂等创建 slot 内一级 NAT（出口 masquerade，代理回流不改写）。
func ensureNetnsNAT(ctx context.Context, idx int, vethGuest, hostAddr string) error {
	ns := nsName(idx)
	if err := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", "ip", "fp-slot").Run(); err != nil {
		steps := [][]string{
			{"ip", "netns", "exec", ns, "nft", "add", "table", "ip", "fp-slot"},
			{"ip", "netns", "exec", ns, "nft", "add", "chain", "ip", "fp-slot", "post", "{", "type", "nat", "hook", "postrouting", "priority", "srcnat;", "}"},
			{"ip", "netns", "exec", ns, "nft", "add", "rule", "ip", "fp-slot", "post", "oifname", vethGuest, "ip", "daddr", "!=", hostAddr, "masquerade"},
		}
		for _, args := range steps {
			if err := execCmd(ctx, args[0], args[1:]...); err != nil {
				return fmt.Errorf("slot: netns nft setup: %w", err)
			}
		}
	}
	return nil
}
