// Command cni-firepaas 是 ADR-0040 §20 的 CNI shim（G3）。
//
// 实现 CNI spec 1.0.0 的 ADD/DEL/CHECK/VERSION（含 prevResult 透传与
// Result 输出），数据面复用 slot.Manager（与 agentd 同一 Datapath 实现，
// 不另建网络逻辑）。
//
// 调用约束（§20 v3 加调用约束）：**仅由 agent 以 fenced 方式调用**——
// 直接被外部 container runtime 调用会绕过 operation ledger 与 observed
// 权威（AGENTS.md §4）。落地形态：
//   - 环境变量 FIREPAAS_CNI_FENCE_FILE 必须指向一个 0600 的 fence 文件
//     （agentd 逐调用创建，第一行 = 本次映射的内部 operation_id，可选
//     第二行 = machine_id，不一致拒绝——防 fence 文件跨 machine 重放）；
//   - 无 fence / fence 不可读 / 权限过宽 → 直接拒绝（exit 1，不触碰
//     任何状态）；
//   - fence 中 的 operation_id 随每次调用写日志（审计链）。
//
// 范围声明（P1 独立评审）：本期加固目标 = machine 绑定；ledger 水位校验
// 与 bundle spec-hash 绑定移至 agentd 实际接线 CNI 时（G3——当前无生产
// 调用方，fence 文件由未来 caller 按此格式创建）。
//
// 不实现 portmap/bandwidth 插件链（§20 显式排除）。
//
// 用法（agent 侧协议，非 runtime 直接调用）：
//
//	FIREPAAS_CNI_FENCE_FILE=/run/firepaas/cni-fence \
//	CNI_COMMAND=ADD CNI_CONTAINERID=m-1 CNI_IFNAME=eth0 \
//	CNI_NETNS=/var/run/netns/fp-slot-0 cni-firepaas < bundle.json
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/network/ebpf"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
)

const cniVersion = "1.0.0"

// cniError 是 CNI 错误协议输出（stdout JSON；code 语义见 spec）。
type cniError struct {
	CniVersion string `json:"cniVersion"`
	Code       int    `json:"code"`
	Msg        string `json:"msg"`
}

// cniVersionReply 是 VERSION 命令输出。
type cniVersionReply struct {
	CniVersion        string   `json:"cniVersion"`
	SupportedVersions []string `json:"supportedVersions"`
}

// cniResult 是 ADD 成功输出（CNI Result 1.0.0；无 dns）。
type cniResult struct {
	CniVersion string     `json:"cniVersion"`
	Interfaces []cniIface `json:"interfaces"`
	IPs        []cniIP    `json:"ips"`
}

type cniIface struct {
	Name    string `json:"name"`
	Sandbox string `json:"sandbox"`
}

type cniIP struct {
	Version   string `json:"version"`
	Address   string `json:"address"`
	Interface int    `json:"interface"`
}

func main() {
	if err := run(); err != nil {
		// CNI 错误协议：stdout JSON {cniVersion, code, msg}；code 语义见
		// spec（1xx = CHECK 不一致类，2xx = 资源冲突，4xx = 临时，5xx = 永久）。
		code := 100
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "fence") {
			code = 500 // 拒绝/永久类
		}
		_ = json.NewEncoder(os.Stdout).Encode(cniError{
			CniVersion: cniVersion,
			Code:       code,
			Msg:        err.Error(),
		})
		os.Exit(1)
	}
}

// cniInput 是 CNI 环境变量参数集（spec 1.0.0）。
type cniInput struct {
	Command    string // ADD|DEL|CHECK|VERSION
	Container  string // CNI_CONTAINERID → machine_id
	IfName     string // CNI_IFNAME（记录用）
	NetNS      string // CNI_NETNS（记录用）
	Args       string // CNI_ARGS
	PrevResult json.RawMessage
}

func readInput() (cniInput, error) {
	in := cniInput{
		Command:   os.Getenv("CNI_COMMAND"),
		Container: os.Getenv("CNI_CONTAINERID"),
		IfName:    os.Getenv("CNI_IFNAME"),
		NetNS:     os.Getenv("CNI_NETNS"),
		Args:      os.Getenv("CNI_ARGS"),
	}
	// prevResult：stdin 上随 ADD/CHECK 传入（runtime 序列化的上次 Result）。
	// 从 os.Stdin 读取（而非 /dev/stdin 路径，保证重定向/管道语义）。
	if in.Command == "ADD" || in.Command == "CHECK" {
		raw, err := io.ReadAll(os.Stdin)
		if err == nil && len(strings.TrimSpace(string(raw))) > 0 {
			in.PrevResult = raw
		}
	}
	return in, nil
}

// verifyFence 校验 agent fence（§20：仅 agent fenced 调用）。
// fence 文件必须存在、0600、首行非空（内容 = operation_id，写日志审计）。
// 可选第二行 = machine_id：存在时必须与 bundle 一致（防 fence 文件被重放
// 到其它 machine；W5 加固——单行旧格式仍接受，agent 侧应始终写双行）。
func verifyFence() (opID, boundMachine string, err error) {
	path := os.Getenv("FIREPAAS_CNI_FENCE_FILE")
	if path == "" {
		return "", "", fmt.Errorf(
			"direct runtime invocation not supported: FIREPAAS_CNI_FENCE_FILE unset (agent-fenced calls only, ADR-0040 §20)",
		)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", "", fmt.Errorf("cni fence unavailable: %w", err)
	}
	if info.Mode().Perm() != 0o600 {
		return "", "", fmt.Errorf("cni fence %s must be 0600 (agent-created), got %o", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("cni fence %s unreadable: %w", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return "", "", fmt.Errorf("cni fence %s empty", path)
	}
	opID = strings.TrimSpace(lines[0])
	if len(lines) > 1 {
		boundMachine = strings.TrimSpace(lines[1])
	}
	return opID, boundMachine, nil
}

// slotBundle 是 agent 侧随 stdin 传入的机器/网卡参数（CNI 参数不够表达
// firepaas 的 slot 语义：TAP 名与 guest IP 由 agent 权威给出）。
// 后端对齐（W5）：bundle 携带 agentd 同源的后端描述；缺省 = nft 默认（与
// slot.New 零值一致）。ebpf 后端要求 PinDir（共享 pin 的多进程复用；与
// agentd 并发管理同一 state 文件时以文件锁串行，见 run）。
type slotBundle struct {
	MachineID    string `json:"machine_id"`
	Tap          string `json:"tap"`
	GuestIP      string `json:"guest_ip"`
	GuestIP6     string `json:"guest_ip6,omitempty"`
	StatePath    string `json:"state_path"` // slots.json（agent 数据目录）
	SubnetCIDR   string `json:"subnet_cidr"`
	Gateway      string `json:"gateway,omitempty"`
	NamePrefix   string `json:"name_prefix,omitempty"`
	VethCIDR     string `json:"veth_cidr,omitempty"`
	Backend      string `json:"backend,omitempty"` // "nft"（默认）| "ebpf"
	EbpfPinDir   string `json:"ebpf_pin_dir,omitempty"`
	EbpfNatTable string `json:"ebpf_root_nat_table,omitempty"`
	ProxyPort80  int    `json:"proxy_port80,omitempty"`
	ProxyPort443 int    `json:"proxy_port443,omitempty"`
}

func run() error {
	in, err := readInput()
	if err != nil {
		return err
	}
	if in.Command == "VERSION" {
		// VERSION 无 fence 要求（纯能力声明，不触碰状态）。
		return json.NewEncoder(os.Stdout).Encode(cniVersionReply{
			CniVersion:        cniVersion,
			SupportedVersions: []string{"0.4.0", cniVersion},
		})
	}
	// 其余命令一律要求 fence。
	opID, boundMachine, err := verifyFence()
	if err != nil {
		return err
	}
	// bundle：stdin JSON（agent 协议；CNI 的 prevResult 与本协议共存于
	// 同一 stdin 由 agent 序列化为 bundle.prev_result——简化：bundle 优先）。
	var bundle slotBundle
	if err := json.Unmarshal(in.PrevResult, &bundle); err != nil {
		return fmt.Errorf("bad slot bundle (agent must serialize slotBundle JSON, not a runtime prevResult): %w", err)
	}
	if bundle.MachineID == "" {
		bundle.MachineID = in.Container
	}
	if bundle.MachineID == "" || bundle.StatePath == "" {
		return fmt.Errorf("bundle machine_id/state_path required")
	}
	if boundMachine != "" && boundMachine != bundle.MachineID {
		return fmt.Errorf(
			"cni fence bound to machine %q, bundle wants %q (fence replay rejected)",
			boundMachine,
			bundle.MachineID,
		)
	}
	// 审计链：fence operation_id 进 stderr（agent 聚合到 operation 日志）。
	fmt.Fprintf(os.Stderr, "cni-firepaas op=%s command=%s machine=%s\n", opID, in.Command, bundle.MachineID)
	backend, err := bundleBackend(bundle)
	if err != nil {
		return err
	}
	mgr, err := slot.New(slot.Config{
		SubnetCIDR:         bundle.SubnetCIDR,
		Gateway:            bundle.Gateway,
		StatePath:          bundle.StatePath,
		NamePrefix:         bundle.NamePrefix,
		VethCIDR:           bundle.VethCIDR,
		Backend:            backend,
		EgressProxyPort80:  bundle.ProxyPort80,
		EgressProxyPort443: bundle.ProxyPort443,
	})
	if err != nil {
		return err
	}
	// Load+操作整体加跨进程文件锁（W5/P1：与 agentd 侧 slot.Manager 的
	// Load/persist 拿同一把锁——slot.WithStateFileLock 单实现，路径必同；
	// 串行 slots.json 的 TOCTOU；内核 attach 幂等收敛）。
	return slot.WithStateFileLock(bundle.StatePath, func() error {
		if err := mgr.Load(); err != nil {
			return err
		}
		spec := api.NetnsSpec{
			MachineID: bundle.MachineID,
			Tap:       bundle.Tap,
			GuestIP:   bundle.GuestIP,
			GuestIP6:  bundle.GuestIP6,
		}
		switch in.Command {
		case "ADD":
			// AttachNetns 幂等（CNI ADD 重复调用语义）。
			if err := mgr.AttachNetns(context.Background(), spec); err != nil {
				return fmt.Errorf("attach: %w", err)
			}
			st, ok := mgr.CurrentNetns(bundle.MachineID)
			if !ok {
				return fmt.Errorf("slot not found after attach")
			}
			return writeResult(st, bundle)
		case "DEL":
			// DetachNetns 幂等（不存在 no-op）。
			if err := mgr.DetachNetns(context.Background(), bundle.MachineID); err != nil {
				return fmt.Errorf("detach: %w", err)
			}
			return nil // DEL 成功无输出
		case "CHECK":
			// CHECK 幂等校验（不新建网络；slot 不存在 → 100 类错误）。
			if err := mgr.Check(context.Background(), spec); err != nil {
				return fmt.Errorf("check: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("unsupported CNI_COMMAND %q (ADD/DEL/CHECK/VERSION)", in.Command)
		}
	})
}

// bundleBackend 按 bundle 描述构造数据面后端（W5：与 agentd 同源对齐；
// 缺省 nil = slot.New 默认 nft 后端）。ebpf 要求 PinDir（与 agentd 共享
// pin 的多进程复用；并发管理同一 state 文件时由下文文件锁串行）。
func bundleBackend(bundle slotBundle) (slot.Backend, error) {
	switch bundle.Backend {
	case "", "nft":
		return nil, nil
	case "ebpf":
		if err := ebpf.Probe(); err != nil {
			return nil, fmt.Errorf("cni ebpf backend unavailable: %w", err)
		}
		pinDir := cmp.Or(bundle.EbpfPinDir, ebpf.PinRoot)
		return ebpf.New(ebpf.Options{
			PinDir:         pinDir,
			EgressProxy80:  bundle.ProxyPort80,
			EgressProxy443: bundle.ProxyPort443,
			VethCIDR:       bundle.VethCIDR,
			RootNATTable:   bundle.EbpfNatTable,
		})
	default:
		return nil, fmt.Errorf("unknown bundle backend %q (want nft|ebpf)", bundle.Backend)
	}
}

// writeResult 输出 CNI Result 1.0.0（interfaces/ips；无 dns——.internal
// 由节点本地 DNS 承担，不改 resolver 配置）。
func writeResult(st api.NetnsState, bundle slotBundle) error {
	result := cniResult{
		CniVersion: cniVersion,
		Interfaces: []cniIface{
			{Name: filepath.Base(st.Tap), Sandbox: fmt.Sprintf("fp-slot-%d", st.Index)},
		},
		IPs: []cniIP{},
	}
	if st.GuestIP != "" {
		result.IPs = append(result.IPs, cniIP{
			Version: "4", Address: st.GuestIP, Interface: 0,
		})
	}
	if bundle.GuestIP6 != "" {
		result.IPs = append(result.IPs, cniIP{
			Version: "6", Address: bundle.GuestIP6 + "/128", Interface: 0,
		})
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
