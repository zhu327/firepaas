// Package capabilities 定义 v1.2-A（ADR-0023）runtime capability 的稳定
// feature ID。ID 是小写、带版本后缀的稳定字符串；已发布 ID 语义不可变，
// 未知 ID 必须被忽略（向前兼容）。
package capabilities

// 首批 v1.2 能力（docs/v1.2-plan.md §4）。
const (
	// GuestExecV1：vsock guest 运维通道 exec（ADR-0025）。
	GuestExecV1 = "guest.exec.v1"
	// GuestCopyV1：vsock guest 运维通道单文件 cp（ADR-0025）。
	GuestCopyV1 = "guest.copy.v1"
	// GuestLogsV1：StreamLogs 实时日志（ADR-0025）。
	GuestLogsV1 = "guest.logs.v1"
	// SecretOneShotV1：execution-bound one-shot secret 注入（ADR-0024）。
	// 只有完成 safe guest channel 的 agent 才能上报，fail closed。
	SecretOneShotV1 = "secret.oneshot.v1"
	// SnapshotMemoryV1：memory snapshot/checkpoint 能力。
	SnapshotMemoryV1 = "snapshot.memory.v1"
	// SnapshotFilesystemV1：filesystem-only snapshot 能力。
	SnapshotFilesystemV1 = "snapshot.filesystem.v1"
	// EgressDomainV1（v1.3-A，ADR-0027）：透明 TCP 代理执行域名/SNI egress
	// 策略（HTTP Host + TLS ClientHello SNI、可信 resolver、连接限额）。
	// 只有完成该数据面的 agent 才能上报；deployment 携带 allowed_domains
	// 时调度按此硬过滤（fail closed）。
	EgressDomainV1 = "egress.domain.v1"
	// EgressCidrV1（v1.3-A，ADR-0027）：slot 级 CIDR egress 执行（mode、
	// allowed/denied CIDR、非 80/443 TCP 与 UDP 的默认拒绝）。deployment 携带
	// egress policy 时要求（CIDR-only 无需域名代理）。
	EgressCidrV1 = "egress.cidr.v1"
	// VolumeLocalRWV1: node-local persistent single-writer volume lifecycle.
	VolumeLocalRWV1 = "volume.local_rw.v1"
	// VolumeDatasetROV1 is immutable archive import and shared readonly attach.
	VolumeDatasetROV1 = "volume.dataset_ro.v1"
	// VolumeDatasetOverlayV1 requires genuine per-execution CoW support. The
	// current pinned hypeman dynamic attach API lacks it, so agents must not
	// advertise this capability yet.
	VolumeDatasetOverlayV1 = "volume.dataset_overlay.v1"
	// LocalInventoryV1（v1.4-B）：agent 的 ListSnapshots/ListVolumes 响应携带
	// complete 标志与 observation generation/time（旧 agent 缺字段 → 控制面
	// 只能产生 UNKNOWN，不得推导 MISSING）。
	LocalInventoryV1          = "inventory.local.v1"
	SnapshotScrubV1           = "snapshot.scrub.v1"
	ImageQuarantineV1         = "image.quarantine.v1"
	VolumeDatasetQuarantineV1 = "volume.dataset_quarantine.v1"

	// ADR-0040 §12：网络 fabric 数据面能力。
	// NetworkEbpfV1：节点具备 eBPF datapath（tc-ingress isolation +
	// tc-egress CIDR/redirect/conn-limit + ipcache/policy maps + BTF/CO-RE）。
	// NetworkNftFallbackV1：eBPF 探测不满足、自动回落 legacy nft 后端的
	// emergency 模式。两者二选一上报（ebpf 数据面落地前 agent 不上报任何
	// 一个：nft 仍是现役默认，不是“回落”，虚报会误导调度硬过滤）。
	NetworkEbpfV1        = "network.ebpf.v1"
	NetworkNftFallbackV1 = "network.nftfallback.v1"
	// MeshEastWestV1（ADR-0040 §18）：节点具备东西向 WG mesh 数据面
	//（G2 才可上报；RequiredFeatures 推导必须显式依赖 NetworkEbpfV1，
	// mesh 服务永不调度到回退节点）。§0 gate 满足前不存在任何上报方。
	MeshEastWestV1 = "mesh.eastwest.v1"
)

// Requires 返回 capability 的硬依赖（ADR-0023 能力推导用）。无依赖返回 nil。
// 注意：当前尚无生产调用方——G2 落地 mesh 调度时必须接入
// controlplane/placement.RequiredFeatures 的推导（对 feature ID 做闭包展开），
// 否则 mesh 服务可能被调度到回退节点。
func Requires(id string) []string {
	switch id {
	case MeshEastWestV1:
		return []string{NetworkEbpfV1}
	default:
		return nil
	}
}

// All 返回已知 feature ID 列表（文档用途与校验）。
func All() []string {
	return []string{
		GuestExecV1, GuestCopyV1, GuestLogsV1,
		SecretOneShotV1, SnapshotMemoryV1, SnapshotFilesystemV1,
		EgressDomainV1, EgressCidrV1, VolumeLocalRWV1,
		VolumeDatasetROV1, VolumeDatasetOverlayV1, LocalInventoryV1,
		SnapshotScrubV1, ImageQuarantineV1, VolumeDatasetQuarantineV1,
		NetworkEbpfV1, NetworkNftFallbackV1, MeshEastWestV1,
	}
}

// Valid 校验 feature ID 形态：小写字母/数字/点/下划线，带版本后缀。
func Valid(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// SetOf 把列表转为集合（去重）。空列表返回 nil（区分“未上报”与“无能力”）。
func SetOf(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		if Valid(id) {
			out[id] = true
		}
	}
	return out
}

// IsServingReadiness 判定 observed readiness 是否可服务（ADR-0008）：
// READY 与 UNCONFIGURED 等价可服务（未配置探针视同就绪）；其余值
// （空串、NOT_READY、未知/拼写漂移）一律不可服务。路由发布、切流、
// wait 与 edge 选路四处共用同一白名单，避免语义漂移。
func IsServingReadiness(s string) bool {
	return s == "READY" || s == "UNCONFIGURED"
}
