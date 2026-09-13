// host_linux.go：M5.2（mvp-plan §9.2）单机宿主资源 gauge 采样。
//
// 实验形态：控制面与宿主同机，直接读 /proc 采样（多节点形态由 agent info
// 上报，见 DEFERRED-MULTI-NODE）。数据入 Prometheus text 端点供告警。
package main

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/observability/metrics"
)

const hostSampleInterval = 15 * time.Second

// hostSampler 每隔 hostSampleInterval 采集一次宿主资源计数写入 registry。
// 只读 /proc，绝不修改系统状态。ctx 取消即退出。
func hostSampler(ctx context.Context, reg *metrics.Registry) {
	t := time.NewTicker(hostSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sampleHostInto(reg)
		}
	}
}

func sampleHostInto(reg *metrics.Registry) {
	// FD 用量：/proc/sys/fs/file-nr = "allocated unused max"。
	if v, ok := readUintField("/proc/sys/fs/file-nr", 0); ok {
		reg.Set("firepaas_host_fds_allocated", nil, v)
		if m, ok2 := readUintField("/proc/sys/fs/file-nr", 2); ok2 {
			reg.Set("firepaas_host_fds_max", nil, m)
		}
	}
	// inode：只取 inode-state 第一列（nr_inodes）。inode-nr 与其重复
	//（双写会以后者覆盖前者），不再单独上报，只保留一次 Set。
	if v, ok := readUintField("/proc/sys/fs/inode-state", 0); ok {
		reg.Set("firepaas_host_inodes_allocated", nil, v)
	}
	// conntrack：计数 + 上限。
	if v, ok := readUintField("/proc/sys/net/netfilter/nf_conntrack_count", 0); ok {
		reg.Set("firepaas_host_conntrack_count", nil, v)
	}
	if v, ok := readUintField("/proc/sys/net/netfilter/nf_conntrack_max", 0); ok {
		reg.Set("firepaas_host_conntrack_max", nil, v)
	}
	// entropy。
	if v, ok := readUintField("/proc/sys/kernel/random/entropy_avail", 0); ok {
		reg.Set("firepaas_host_entropy_avail", nil, v)
	}
	// load1（float → x100 存整型，规则里除以 100）。
	if f, ok := readFloatField("/proc/loadavg", 0); ok {
		reg.Set("firepaas_host_load1_x100", nil, uint64(f*100))
	}
	// 内存可用（MemAvailable kB）。
	if v, ok := memAvailableKB(); ok {
		reg.Set("firepaas_host_mem_available_kb", nil, v)
	}
	slog.Debug("host gauges sampled")
}

// readUintField 返回文件里第 index 个空格分隔的整型（0-based）。
func readUintField(path string, index int) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) <= index || index < 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(f[index], 10, 64)
	return v, err == nil
}

// readFloatField 返回文件里第 index 个空格分隔的浮点（0-based）。
func readFloatField(path string, index int) (float64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) <= index || index < 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(f[index], 64)
	return v, err == nil
}

// memAvailableKB：/proc/meminfo MemAvailable。
func memAvailableKB() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			v, err := strconv.ParseUint(fields[1], 10, 64)
			return v, err == nil
		}
	}
	return 0, false
}
