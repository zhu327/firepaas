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
	if v, ok := firstField("/proc/sys/fs/file-nr"); ok {
		reg.Set("firepaas_host_fds_allocated", nil, v)
		if m, ok2 := thirdField("/proc/sys/fs/file-nr"); ok2 {
			reg.Set("firepaas_host_fds_max", nil, m)
		}
	}
	// inode：/proc/sys/fs/inode-nr = "allocated free"（free 在第二列，
	// 所以上限直接用缺省 3.6M——只上报 allocated/free 供趋势）。
	if v, ok := firstField("/proc/sys/fs/inode-nr"); ok {
		reg.Set("firepaas_host_inodes_allocated", nil, v)
	}
	if v, ok := firstField("/proc/sys/fs/inode-state"); ok {
		// inode-state 第一列 = nr_inodes。
		reg.Set("firepaas_host_inodes_allocated", nil, v)
	}
	// conntrack：计数 + 上限。
	if v, ok := firstField("/proc/sys/net/netfilter/nf_conntrack_count"); ok {
		reg.Set("firepaas_host_conntrack_count", nil, v)
	}
	if v, ok := firstField("/proc/sys/net/netfilter/nf_conntrack_max"); ok {
		reg.Set("firepaas_host_conntrack_max", nil, v)
	}
	// entropy。
	if v, ok := firstField("/proc/sys/kernel/random/entropy_avail"); ok {
		reg.Set("firepaas_host_entropy_avail", nil, v)
	}
	// load1（float → x100 存整型，规则里除以 100）。
	if f, ok := load1(); ok {
		reg.Set("firepaas_host_load1_x100", nil, uint64(f*100))
	}
	// 内存可用（MemAvailable kB）。
	if v, ok := memAvailableKB(); ok {
		reg.Set("firepaas_host_mem_available_kb", nil, v)
	}
	slog.Debug("host gauges sampled")
}

// firstField 返回文件里第一个空格分隔的整型。
func firstField(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) == 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(f[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func thirdField(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) < 3 {
		return 0, false
	}
	v, err := strconv.ParseUint(f[2], 10, 64)
	return v, err == nil
}

func load1() (float64, bool) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(f[0], 64)
	return v, err == nil
}

// memAvailableKB：/proc/meminfo MemAvailable。
func memAvailableKB() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()
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
