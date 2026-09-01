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
)

// All 返回 v1.2 已知 feature ID 列表（文档用途与校验）。
func All() []string {
	return []string{
		GuestExecV1, GuestCopyV1, GuestLogsV1,
		SecretOneShotV1, SnapshotMemoryV1, SnapshotFilesystemV1,
		EgressDomainV1, EgressCidrV1, VolumeLocalRWV1,
		VolumeDatasetROV1, VolumeDatasetOverlayV1, LocalInventoryV1,
		SnapshotScrubV1, ImageQuarantineV1, VolumeDatasetQuarantineV1,
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
