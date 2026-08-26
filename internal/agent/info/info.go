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
	startedAt       time.Time
	serviceInstance string
	status          pb.ServiceInfoResponse_Status
	statusChangedAt time.Time
}

// New 构造 Provider。dataDir 用于磁盘容量/用量统计（评审 P3：不得用 / 代替
// 数据目录）；memAllocatedMib 可为 nil（视为 0）。
func New(nodeID, version, commit, nodePool, fcVersion, networkCIDR, dataDir string, memAllocatedMib func() uint64) *Provider {
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
		startedAt:       now,
		serviceInstance: uuid.NewString(),
		status:          pb.ServiceInfoResponse_HEALTHY,
		statusChangedAt: now,
	}
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
			MemTotalMib:  totalMem / 1024,
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
