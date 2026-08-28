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
	// cachedImageDigests（v1.1，ADR-0018）：节点本地镜像缓存 digest 列表
	//（LRU/创建序，截断上限 512）；nil = 未装配（不上报）。
	cachedImageDigests func() []string
	startedAt          time.Time
	serviceInstance    string
	status             pb.ServiceInfoResponse_Status
	statusChangedAt    time.Time
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

// DataDir 返回数据目录（PullImage 磁盘水位检查用）。
func (p *Provider) DataDir() string { return p.dataDir }

// SetImageDigestsFunc 注入镜像缓存 digest 采集函数（v1.1，ADR-0018）。
// 由 agentd 装配：从 hypeman ListImages 派生 ready 镜像的 digest 集合。
func (p *Provider) SetImageDigestsFunc(f func() []string) { p.cachedImageDigests = f }

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
		NetworkCidr:        p.NetworkCIDR,
		CachedImageDigests: p.cachedImageDigestsList(),
	}
}

// cachedImageDigestsList 返回镜像缓存 digest（nil func = 不上报）。
func (p *Provider) cachedImageDigestsList() []string {
	if p.cachedImageDigests == nil {
		return nil
	}
	return p.cachedImageDigests()
}

func memTotal() uint64 {
	host := readMeminfoField("MemTotal")
	// P3/M3：agent 进程常驻在带 memory limit 的 cgroup 里（Nomad task），
	// firecracker 子进程共享同一限额；硬准入必须按 min(host, cgroup) 计算，
	// 否则 VM 数量超过 cgroup 限额时会触发 cgroup OOM（M3 真机事故：
	// task 限额 1GiB，scale 3 + 发布窗口双代共存 → 成波杀 firecracker）。
	if cg := readCgroupMemMax(); cg > 0 && cg < host {
		return cg
	}
	return host
}

// readCgroupMemMax 读取当前 cgroup v2 的 memory.max（字节）；无限制或不可读
// 返回 0。Nomad raw_exec 任务在自身 cgroup 中运行。
func readCgroupMemMax() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "max" || s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v / 1024 // KiB，与 meminfo 单位对齐
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
