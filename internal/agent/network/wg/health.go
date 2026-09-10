// Package wg 的健康观测扩展（本文件）：WireGuard peer 握手新鲜度分级。
//
// 背景：PersistentKeepalive（25s）只维持 NAT 映射，不是失败检测；控制面与
// agent 此前没有任何 handshake-age 采集（仅 soak 脚本 grep 日志）。本文件
// 提供“检测的一半”——采集与分级；**不做自动摘除/回切**：
//
//   - handshake 新鲜 ≠ 业务路径可用（ULA 可达、应用端口仍需独立探测）；
//   - 自动摘除需要 DNS/route/edge-direct 联动设计 + 双节点真机 chaos 证据；
//     在此之前，自动动作只会把“观测缺失”变成“主动断流”。
//
// 状态机（观测侧）：Healthy（<60s）→ Suspect（<180s）→ Failed（超限或持续
// 无握手）；从未握手（timestamp 0）= Unknown（区分“新 peer”与“已失联”）。
// 阈值依据：keepalive 25s 下健康 peer 的 handshake-age 应稳定 <60s；
// 180s 对应 soak 脚本既有告警线（soak-fabric.sh）。
package wg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PeerState 是 WG peer 的握手健康分级（仅观测，不驱动路由变更）。
type PeerState string

const (
	// PeerUnknown 从未完成握手（新 peer 或对端从未上线）。
	PeerUnknown PeerState = "unknown"
	// PeerHealthy 握手新鲜，underlay 疑似可用。
	PeerHealthy PeerState = "healthy"
	// PeerSuspect 握手老化，值得用 ULA 探测复核，不建议据此摘除。
	PeerSuspect PeerState = "suspect"
	// PeerFailed 握手 long-stale：高度疑似失联，需联动 DNS/route 决策
	// （该联动尚未实现，见包注释）。
	PeerFailed PeerState = "failed"
)

// 握手新鲜度阈值（包级变量便于单测锁定语义；生产不调参）。
const (
	healthyHandshakeAge = 60 * time.Second
	failedHandshakeAge  = 180 * time.Second
)

// ClassifyHandshakeAge 按上次握手时间分级（now 由调用方注入，便于测试）。
// last.IsZero()（wg 上报 0）= 从未握手 → Unknown。
func ClassifyHandshakeAge(last time.Time, now time.Time) PeerState {
	if last.IsZero() {
		return PeerUnknown
	}
	age := now.Sub(last)
	if age < 0 {
		age = 0 // 时钟回拨：按最新处理，不误报 Failed
	}
	switch {
	case age < healthyHandshakeAge:
		return PeerHealthy
	case age < failedHandshakeAge:
		return PeerSuspect
	default:
		return PeerFailed
	}
}

// PeerHealth 是单个 WG peer 的握手健康（pubkey 键；node 归因由调用方按
// fabric 快照的 pubkey→node 映射完成——Manager 不持久化该映射）。
type PeerHealth struct {
	Pubkey        string
	LastHandshake time.Time // 零值 = 从未握手
	HandshakeAge  time.Duration
	State         PeerState
}

// ParseLatestHandshakes 解析 `wg show <iface> latest-handshakes` 输出
// （每行 `<pubkey> <unix-secs>`；0 = 从未握手）。非法行跳过（fail-open
// 于观测：脏行不毒化整表，调用方可对比 peer 数发现缺口）。
func ParseLatestHandshakes(output string, now time.Time) map[string]PeerHealth {
	out := map[string]PeerHealth{}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		pubkey := strings.TrimSpace(fields[0])
		ts, err := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil || pubkey == "" || ts < 0 {
			continue
		}
		h := PeerHealth{Pubkey: pubkey}
		if ts > 0 {
			h.LastHandshake = time.Unix(ts, 0).UTC()
			h.HandshakeAge = now.Sub(h.LastHandshake)
			if h.HandshakeAge < 0 {
				h.HandshakeAge = 0
			}
		}
		h.State = ClassifyHandshakeAge(h.LastHandshake, now)
		out[pubkey] = h
	}
	return out
}

// PeerHealth 采集本机 WG 全部 peer 的握手健康（只读观测，不改任何内核状态）。
// 接口不存在等错误直接返回（调用方记日志/指标，不回落——观测失败不是数据面失败）。
func (m *Manager) PeerHealth(ctx context.Context) (map[string]PeerHealth, error) {
	m.mu.Lock()
	iface := m.iface
	run := m.run
	m.mu.Unlock()
	if iface == "" {
		iface = defaultIface
	}
	if run == nil {
		return nil, fmt.Errorf("wg: runner not initialized")
	}
	out, err := run.Run(ctx, "", "wg", "show", iface, "latest-handshakes")
	if err != nil {
		return nil, fmt.Errorf("wg: latest-handshakes: %w", err)
	}
	return ParseLatestHandshakes(out, time.Now().UTC()), nil
}
