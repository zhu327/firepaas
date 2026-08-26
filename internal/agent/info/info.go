// Package info 提供 agent 的 ServiceInfo 数据（M1 最小实现）。
// M2 起改为基于 hypeman lib/resources + lib/vm_metrics 的实时采集。
package info

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const mib = 1024 * 1024

// hostMemReserveMib/Pct（P3-8）：容量上报与硬准入扣除 host 侧保留
// （Nomad client、agentd、hypeman、页缓存安全余量），高密度下不给
// host OOM 留窗口。保留 = max(固定 512MiB, 总量的 3%)。
const (
	hostMemReserveMib = 512
	hostMemReservePct = 32 // 1/32 ≈ 3%
)

// Provider 实现 InfoService 的 ServiceInfo。
type Provider struct {
	NodeID          string
	ServiceVersion  string
	ServiceCommit   string
	NodePool        string
	FirecrackerVer  string
	NetworkCIDR     string
	dataDir         string
	memAllocatedMib func() uint64 // 已承诺给 machine 的内存（M1：实例 Size 之和）
	vcpuAllocated   func() int    // 已承诺给 machine 的 vcpu（实例 Vcpus 之和）
	startedAt       time.Time
	serviceInstance string
	status          pb.ServiceInfoResponse_Status
	statusChangedAt time.Time
}

// New 构造 Provider。dataDir 用于磁盘容量/用量统计（评审 P3：不得用 / 代替
// 数据目录）；memAllocatedMib/vcpuAllocated 可为 nil（视为 0）。
func New(nodeID, version, commit, nodePool, fcVersion, networkCIDR, dataDir string, memAllocatedMib func() uint64, vcpuAllocated func() int) *Provider {
	now := time.Now()
	return &Provider{
		NodeID:          nodeID,
		ServiceVersion:  version,
		ServiceCommit:   commit,
		NodePool:        nodePool,
		FirecrackerVer:  fcVersion,
		NetworkCIDR:     networkCIDR,
		dataDir:         dataDir,
		memAllocatedMib: memAllocatedMib,
		vcpuAllocated:   vcpuAllocated,
		startedAt:       now,
		serviceInstance: uuid.NewString(),
		status:          pb.ServiceInfoResponse_HEALTHY,
		statusChangedAt: now,
	}
}

// AdmissionSnapshot 返回本机硬准入所需的容量/已承诺量（M2.2）。
// 调度器是软决策，这里是与真实 cgroup/进程状态对齐的硬校验双保险（ADR-0002）。
// memTotalMib 已扣除 host 保留（P3-8）。
func (p *Provider) AdmissionSnapshot() (vcpuTotal, memTotalMib, vcpuAllocated, memAllocatedMib uint64) {
	vcpuTotal = uint64(runtime.NumCPU())
	memTotalMib = sellableMemMib(memTotal())
	if p.vcpuAllocated != nil {
		vcpuAllocated = uint64(p.vcpuAllocated())
	}
	if p.memAllocatedMib != nil {
		memAllocatedMib = p.memAllocatedMib()
	}
	return vcpuTotal, memTotalMib, vcpuAllocated, memAllocatedMib
}

// Response 构造 ServiceInfoResponse。
func (p *Provider) Response() *pb.ServiceInfoResponse {
	totalMem := memTotal()
	availMem := memAvailable()
	diskTotal, diskUsed := diskStats(p.dataDir)
	memAllocated := uint64(0)
	if p.memAllocatedMib != nil {
		memAllocated = p.memAllocatedMib()
	}

	return &pb.ServiceInfoResponse{
		NodeId:            p.NodeID,
		ServiceInstanceId: p.serviceInstance,
		ServiceVersion:    p.ServiceVersion,
		ServiceCommit:     p.ServiceCommit,
		Status:            p.status,
		StatusChangedAt:   timestamppb.New(p.statusChangedAt),
		Capacity: &pb.NodeCapacity{
			VcpuTotal:    uint64(runtime.NumCPU()),
			MemTotalMib:  sellableMemMib(totalMem), // P3-8：扣除 host 保留
			DiskTotalMib: diskTotal,
		},
		Usage: &pb.NodeUsage{
			CpuPercent:      loadAvgCPUPercent(),
			MemUsedMib:      (totalMem - availMem) / 1024,
			MemAllocatedMib: memAllocated,
			DiskUsedMib:     diskUsed,
		},
		Labels: map[string]string{
			"node_pool":           p.NodePool,
			"arch":                runtime.GOARCH,
			"os":                  runtime.GOOS,
			"firecracker_version": p.FirecrackerVer,
			"kernel_version":      kernelRelease(),
		},
		NetworkCidr: p.NetworkCIDR,
	}
}

func memTotal() uint64 {
	return readMeminfoField("MemTotal")
}

// sellableMemMib 返回可售内存（MiB）：总量扣除 host 保留（P3-8）。
// 输入为 KiB（meminfo 单位）。
func sellableMemMib(totalKiB uint64) uint64 {
	totalMib := totalKiB / 1024
	reserve := totalMib / hostMemReservePct
	if reserve < hostMemReserveMib {
		reserve = hostMemReserveMib
	}
	if totalMib <= reserve {
		return 0 // 异常小内存：不售，保守拒绝
	}
	return totalMib - reserve
}

func memAvailable() uint64 {
	if v := readMeminfoField("MemAvailable"); v > 0 {
		return v
	}
	return readMeminfoField("MemFree")
}

// diskStats 返回 dataDir 所在文件系统的总量/已用量（MiB）。statfs 失败时返回 0。
func diskStats(dataDir string) (totalMib, usedMib uint64) {
	if dataDir == "" {
		dataDir = "/"
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		return 0, 0
	}
	bsize := uint64(stat.Bsize)
	total := stat.Blocks * bsize
	free := stat.Bfree * bsize
	avail := stat.Bavail * bsize
	totalMib = total / mib
	// used = total - free；非 root 可用量单独反映保留块，不在此区分。
	usedMib = (total - free) / mib
	_ = avail
	return totalMib, usedMib
}

func readMeminfoField(field string) uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, field+":") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		v, _ := strconv.ParseUint(fields[1], 10, 64)
		return v // 单位 kB
	}
	return 0
}

func loadAvgCPUPercent() float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	if n := runtime.NumCPU(); n > 0 {
		return load / float64(n) * 100
	}
	return 0
}

func kernelRelease() string {
	var uts syscall.Utsname
	if err := syscall.Uname(&uts); err != nil {
		return ""
	}
	b := make([]byte, 0, len(uts.Release))
	for _, c := range uts.Release {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}
