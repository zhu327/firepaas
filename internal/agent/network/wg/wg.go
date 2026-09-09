// Package wg 是 api.Underlay 的内核 WireGuard 实现（ADR-0040 §9，T4）：
// WG 自身 UDP 封装为唯一隧道层；peer 按 node /64 聚合（AllowedIPs），不逐
// instance 建 peer。本包只被 cmd/agentd 装配引用；machine/proxy/egress/server
// 只依赖 internal/agent/network/api。
//
// 安全纪律：
//   - 私钥仅以 0600 文件存在 KeyDir/wg-private.key，经 `wg set <iface>
//     private-key <path>` 送入内核，绝不进入命令行参数、日志或配置；
//   - 下发配置（syncconf）只含公钥与 endpoint，不落私钥；
//   - 全量替换幂等：UpdatePeers 可安全重放，配合 fabric 层 generation
//     fencing（旧代重放拒绝）。
package wg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/state"
	"github.com/zhu327/firepaas/shared/pkg/durablewrite"
)

// ErrStaleUnderlayGeneration 表示 UpdatePeers 携带的 generation 早于已应用水位。
var ErrStaleUnderlayGeneration = errors.New("stale underlay generation")

const (
	defaultIface  = "fp-wg0"
	defaultPort   = 51820
	keepaliveSecs = 25 // NAT 映射保活（非失败检测；§14 探测属 G2+）
)

// Runner 抽象命令行执行（单测注入假命令，真机为 ip/wg）。stdin 支持 wg
// pubkey（私钥经 stdin 传入，不出现在 argv）。
type Runner interface {
	Run(ctx context.Context, stdin, name string, args ...string) (string, error)
}

// ExecRunner 是默认 Runner：直接执行命令，stdout/stderr 合并返回。
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, stdin, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// Options 装配 WG underlay。
type Options struct {
	Iface      string        // 默认 fp-wg0
	KeyDir     string        // 私钥与临时配置目录（必填）
	ListenPort uint16        // 默认 51820
	NodePrefix string        // 无 Fabric 时的本节点 /64（测试/降级路径）
	Fabric     *state.Fabric // 快照权威（nil 时用内存水位，仅测试）
	Runner     Runner        // 默认 ExecRunner
}

// Manager 实现 api.Underlay。mu 串行化 Ensure/UpdatePeers（wg syncconf 全量
// 替换，并发调用会产生交叉配置）。
type Manager struct {
	iface      string
	keyDir     string
	keyPath    string
	port       uint16
	nodePrefix string
	fabric     *state.Fabric
	run        Runner

	mu        sync.Mutex
	pubkey    string
	lastGen   uint64
	ownPrefix string         // wg 接口当前已分配的节点前缀地址
	prefixes  []netip.Prefix // 当前已建主表路由的 peer 前缀集
}

// New 构造 Manager（不触发任何内核操作；Ensure 建立设备）。
func New(opts Options) (*Manager, error) {
	if strings.TrimSpace(opts.KeyDir) == "" {
		return nil, errors.New("wg: KeyDir is required")
	}
	m := &Manager{
		iface:      opts.Iface,
		keyDir:     opts.KeyDir,
		port:       opts.ListenPort,
		nodePrefix: strings.TrimSpace(opts.NodePrefix),
		fabric:     opts.Fabric,
		run:        opts.Runner,
	}
	if m.iface == "" {
		m.iface = defaultIface
	}
	if m.port == 0 {
		m.port = defaultPort
	}
	if m.run == nil {
		m.run = ExecRunner{}
	}
	m.keyPath = filepath.Join(m.keyDir, "wg-private.key")
	return m, nil
}

// PublicKey 返回本节点 WG 公钥（base64）。Ensure 前为空串。
func (m *Manager) PublicKey() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pubkey
}

// Ensure 建立（或幂等补齐）WG 设备与密钥：
//   - 私钥缺失时 `wg genkey` 生成并 0600 崩溃安全落盘；
//   - 设备存在则复用，不存在则 `ip link add ... type wireguard`；
//   - private-key 以文件路径送入内核（不进 argv）；设置 listen-port 并 up。
func (m *Manager) Ensure(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureLocked(ctx)
}

func (m *Manager) ensureLocked(ctx context.Context) error {
	if err := os.MkdirAll(m.keyDir, 0o700); err != nil {
		return fmt.Errorf("wg: create key dir: %w", err)
	}
	priv, err := os.ReadFile(m.keyPath)
	if errors.Is(err, os.ErrNotExist) {
		raw, err := m.run.Run(ctx, "", "wg", "genkey")
		if err != nil {
			return fmt.Errorf("wg: generate private key: %w", err)
		}
		priv = []byte(strings.TrimSpace(raw) + "\n")
		if err := durablewrite.WriteFileAtomic(m.keyPath, "wg private key", priv); err != nil {
			return fmt.Errorf("wg: persist private key: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("wg: read private key: %w", err)
	}
	if _, err := m.run.Run(ctx, "", "ip", "link", "add", m.iface, "type", "wireguard"); err != nil {
		// 设备已存在（重启/重复 Ensure）：复用；其余错误上抛。
		if !strings.Contains(err.Error(), "exists") {
			return fmt.Errorf("wg: create device: %w", err)
		}
	}
	if _, err := m.run.Run(ctx, "",
		"wg", "set", m.iface,
		"private-key", m.keyPath,
		"listen-port", fmt.Sprint(m.port)); err != nil {
		return fmt.Errorf("wg: set private key/port: %w", err)
	}
	if _, err := m.run.Run(ctx, "", "ip", "link", "set", m.iface, "up"); err != nil {
		return fmt.Errorf("wg: link up: %w", err)
	}
	pub, err := m.run.Run(ctx, strings.TrimSpace(string(priv)), "wg", "pubkey")
	if err != nil {
		return fmt.Errorf("wg: derive public key: %w", err)
	}
	m.pubkey = strings.TrimSpace(pub)
	return nil
}

// UpdatePeers 实现 api.Underlay：全量替换 peer 集合并同步接口地址与主表路由。
// 幂等：同 generation 同内容重放 = 等价重执行 syncconf（无 diff 时不重发路由）。
func (m *Manager) UpdatePeers(ctx context.Context, generation uint64, peers []api.Peer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureLocked(ctx); err != nil {
		return err
	}
	if generation == 0 {
		return errors.New("wg: generation must be > 0")
	}
	// fencing：水位取 fabric 已应用 generation 与本地 lastGen 的较大者。
	watermark := m.lastGen
	if m.fabric != nil {
		if cur := m.fabric.Current().Generation; cur > watermark {
			watermark = cur
		}
	}
	if generation < watermark {
		return fmt.Errorf("%w: applied %d, request carried %d",
			ErrStaleUnderlayGeneration, watermark, generation)
	}

	nodePrefix := ""
	if m.fabric != nil {
		nodePrefix = m.fabric.Current().NodePrefix
	}
	if nodePrefix == "" {
		nodePrefix = m.nodePrefix
	}

	// 1) 接口地址：节点 /64 的规范地址（::0）作为 wg 出向源地址。VM 实例
	//    从 ::1 起分配（store.AllocateULA），不与该地址冲突。
	if nodePrefix != "" {
		prefix, err := netip.ParsePrefix(nodePrefix)
		if err != nil {
			return fmt.Errorf("wg: parse node prefix: %w", err)
		}
		if m.ownPrefix != prefix.String() {
			if m.ownPrefix != "" {
				if _, err := m.run.Run(ctx, "", "ip", "-6", "addr", "del", m.ownPrefix, "dev", m.iface); err != nil {
					return fmt.Errorf("wg: remove old node address: %w", err)
				}
			}
			if _, err := m.run.Run(ctx, "", "ip", "-6", "addr", "replace", prefix.String(), "dev", m.iface); err != nil {
				return fmt.Errorf("wg: set node address: %w", err)
			}
			m.ownPrefix = prefix.String()
		}
	}

	// 2) peer 全量替换（syncconf 原子替换所有 peer）。经验证（wg-tools
	//    1.0.20210914，本机实测）：setconf/syncconf 会把 Interface 段缺省
	//    字段重置——私钥被清、listen-port 随机化（配置含 [Interface] 缺
	//    PrivateKey 时同样清私钥）；新版本则 strip Interface 段。因此：
	//    peers-only 配置 + syncconf 后无条件重断言 private-key/listen-port
	//    （幂等；旧版恢复被清字段，新版 no-op）。
	confPath := filepath.Join(m.keyDir, "wg-sync.conf")
	if err := m.writeSyncConf(peers); err != nil {
		return err
	}
	defer func() { _ = os.Remove(confPath) }()
	if _, err := m.run.Run(ctx, "", "wg", "syncconf", m.iface, confPath); err != nil {
		return fmt.Errorf("wg: syncconf: %w", err)
	}
	if _, err := m.run.Run(ctx, "",
		"wg", "set", m.iface,
		"private-key", m.keyPath,
		"listen-port", fmt.Sprint(m.port)); err != nil {
		return fmt.Errorf("wg: re-assert private key/port: %w", err)
	}

	// 3) 主表路由：crypto-key routing 只做选择，包要进 wg 设备必须有路由。
	//    按 peer 前缀 replace；被移除的前缀 del。前缀集 diff 避免无谓抖动。
	desired := make([]netip.Prefix, 0, len(peers))
	for _, p := range peers {
		desired = append(desired, p.NodePrefix)
	}
	added, removed := diffPrefixes(m.prefixes, desired)
	for _, p := range added {
		if _, err := m.run.Run(ctx, "", "ip", "-6", "route", "replace", p.String(), "dev", m.iface); err != nil {
			return fmt.Errorf("wg: route replace %s: %w", p, err)
		}
	}
	for _, p := range removed {
		if _, err := m.run.Run(ctx, "", "ip", "-6", "route", "del", p.String(), "dev", m.iface); err != nil {
			return fmt.Errorf("wg: route del %s: %w", p, err)
		}
	}

	m.prefixes = desired
	m.lastGen = generation
	return nil
}

// writeSyncConf 写 syncconf 配置（只含 [Peer]：公钥/endpoint/AllowedIPs；
// 0600 临时文件，用完即删）。不写 [Interface]：旧版 wg-tools 会按缺失字段
// 重置接口配置（清私钥/随机端口），新版本则 strip 该段——统一在 syncconf
// 后由 UpdatePeers 重断言 private-key/listen-port。私钥绝不写入本文件。
func (m *Manager) writeSyncConf(peers []api.Peer) error {
	var b strings.Builder
	for _, p := range peers {
		b.WriteString("[Peer]\n")
		b.WriteString("PublicKey = " + strings.TrimSpace(p.Pubkey) + "\n")
		if p.Endpoint != "" {
			b.WriteString("Endpoint = " + p.Endpoint + "\n")
		}
		b.WriteString("AllowedIPs = " + p.NodePrefix.String() + "\n")
		fmt.Fprintf(&b, "PersistentKeepalive = %d\n", keepaliveSecs)
		b.WriteString("\n")
	}
	if err := os.MkdirAll(m.keyDir, 0o700); err != nil {
		return fmt.Errorf("wg: create config dir: %w", err)
	}
	confPath := filepath.Join(m.keyDir, "wg-sync.conf")
	if err := os.WriteFile(confPath, []byte(b.String()), 0o600); err != nil {
		return fmt.Errorf("wg: write sync config: %w", err)
	}
	return nil
}

func diffPrefixes(old, new []netip.Prefix) (added, removed []netip.Prefix) {
	oldSet := make(map[netip.Prefix]bool, len(old))
	newSet := make(map[netip.Prefix]bool, len(new))
	for _, p := range old {
		oldSet[p] = true
	}
	for _, p := range new {
		newSet[p] = true
	}
	for _, p := range new {
		if !oldSet[p] {
			added = append(added, p)
		}
	}
	for _, p := range old {
		if !newSet[p] {
			removed = append(removed, p)
		}
	}
	return added, removed
}

// DialULA 实现 api.Underlay：经主表路由（UpdatePeers 已建 /64 路由）直连
// 对端 workload ULA。只用于探测/诊断；业务数据面不经此调用。
func (m *Manager) DialULA(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error) {
	d := net.Dialer{}
	return d.DialContext(ctx, "tcp", netip.AddrPortFrom(addr, port).String())
}

// PeersFromSnapshot 把持久化 fabric 快照转换为 api.Peer 集（agentd 启动恢复
// 用）。前缀已在契约边界校验，这里仍 fail closed。
func PeersFromSnapshot(snap state.FabricSnapshot) ([]api.Peer, error) {
	out := make([]api.Peer, 0, len(snap.Peers))
	for _, p := range snap.Peers {
		prefix, err := netip.ParsePrefix(p.NodePrefix)
		if err != nil {
			return nil, fmt.Errorf("wg: peer %s node_prefix %q: %w", p.NodeID, p.NodePrefix, err)
		}
		out = append(out, api.Peer{
			NodeID:           p.NodeID,
			Pubkey:           p.Pubkey,
			Endpoint:         p.Endpoint,
			NodePrefix:       prefix,
			FabricGeneration: p.FabricGeneration,
		})
	}
	return out, nil
}
