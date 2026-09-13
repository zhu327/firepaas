package edge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
)

// autoscale.go：ADR-0041 并发自动弹性——edge 侧信号记账与上报。
//
// 记账口径（§2，每请求单归属，按请求 Host 与 Route{Hostname} 同口径）：
//   - served：选路成功且拿到 token 的判定点（proxiedReqs 同点）+1，
//     handler 出口 -1；内部 forbidden/transport 重试不重复 +1。
//   - served_rps：同一判定点的请求数/采样窗。
//   - hard_rejected：writeSelectionError 的 errHardLimit 分支。
//   - unserved（P0 冷起信号）：零容量——route 查无/Backends 为空/errNoEligible。
//     明确不计入：pin miss、hard limit、429、ErrBeyondStale/catalog/token 错误。
//
// 上报：5s 一拍 `HSET autoscale:{host} {edgeID} <json> + EXPIRE 20s`
// （单个 Lua 原子完成，避免 crash 留下无 TTL 键）。短窗计数上报最近约
// 2×poll 的滚动和（cur+prev），保证 controller 10s 轮询不漏 5s 窗口。
// 有界：沿用 lru.go 口径，独立容量上限；空闲零值不按时间淘汰，仅 LRU
// 压力逐出（否则 idle app 信号丢失会冻结缩容）。

// HostSample 是 autoscale:{hostname} 哈希中单个 edge field 的 JSON 载荷
// （catalog.SignalSample 别名：与 controller 侧同一定义，P2-5）。
type HostSample = catalog.SignalSample

// autoscaleDefaults 是 reporter 的默认节拍（controller 10s 轮询的上游）。
const (
	autoscaleReportInterval = 5 * time.Second
	autoscaleKeyTTL         = 20 * time.Second
	// ewmaAlpha 是 5s 采样、10s EWMA 的平滑系数（1-exp(-5/10)≈0.393）。
	ewmaAlpha = 0.4
	// autoscaleRollingWindows 是短窗计数的滚动窗口数（2×5s≈10s≈2×poll）。
	autoscaleRollingWindows  = 2
	defaultAutoscaleMaxHosts = 10000
)

// hostWindow 是单个 5s 采样窗的短窗计数。
type hostWindow struct {
	rps          int64
	hardRejected int64
	unserved     int64
}

func (w *hostWindow) add(o hostWindow) {
	w.rps += o.rps
	w.hardRejected += o.hardRejected
	w.unserved += o.unserved
}

// hostCounters 是单个 hostname 的记账行。
type hostCounters struct {
	inflight int64
	ewma     float64
	cur      hostWindow
	prev     hostWindow
}

// AutoscaleTracker 是 per-hostname 信号记账表（容量有界 LRU，非并发安全
// 的 lruCache 由本结构互斥锁保护；handler 热路径只做 lock+算术）。
type AutoscaleTracker struct {
	mu       sync.Mutex
	hosts    *lruCache[*hostCounters]
	maxHosts int
	// alpha 是 EWMA 平滑系数（默认 5s 采样/10s 窗口；reporter 按实际
	// 上报间隔推导覆盖——P2-8，不随 FIREPAAS_EDGE_AUTOSCALE_INTERVAL 改
	// 节拍而失真）。
	alpha float64
}

// NewAutoscaleTracker 构造记账表；maxHosts<=0 取默认。
func NewAutoscaleTracker(maxHosts int) *AutoscaleTracker {
	if maxHosts <= 0 {
		maxHosts = defaultAutoscaleMaxHosts
	}
	return &AutoscaleTracker{hosts: newLRUCache[*hostCounters](), maxHosts: maxHosts, alpha: ewmaAlpha}
}

// alphaOrDefault 返回生效系数（零值防御：直接零值构造的 tracker 仍可用）。
func (t *AutoscaleTracker) alphaOrDefault() float64 {
	if t.alpha <= 0 {
		return ewmaAlpha
	}
	return t.alpha
}

// setAlpha 覆盖平滑系数（reporter 装配时调用；并发安全）。
func (t *AutoscaleTracker) setAlpha(a float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.alpha = a
}

// ewmaAlphaForInterval 按上报间隔推导 10s 窗口 EWMA 系数（1-exp(-dt/10s)），
// 钳制到 [0.05,0.95]，避免极端节拍下 EWMA 退化（恒 0 或恒瞬时）。
func ewmaAlphaForInterval(dt time.Duration) float64 {
	if dt <= 0 {
		return ewmaAlpha
	}
	a := 1 - math.Exp(-float64(dt)/float64(10*time.Second))
	if a < 0.05 {
		return 0.05
	}
	if a > 0.95 {
		return 0.95
	}
	return a
}

func (t *AutoscaleTracker) entry(host string) *hostCounters {
	if t == nil {
		return nil
	}
	if e, ok := t.hosts.get(host); ok {
		return e
	}
	e := &hostCounters{}
	t.hosts.set(host, e)
	t.hosts.evict(t.maxHosts)
	return e
}

// Acquire 记录一次 served 进入（与 proxiedReqs 同判定点调用）。
func (t *AutoscaleTracker) Acquire(host string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entry(host)
	e.inflight++
	e.cur.rps++
	t.hosts.touch(host)
}

// Release 记录一次 served 离开（handler 出口 defer，与 Acquire 配对）。
func (t *AutoscaleTracker) Release(host string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.hosts.get(host); ok {
		if e.inflight > 0 {
			e.inflight--
		}
		t.hosts.touch(host)
	}
}

// AddHardRejected 记录一次过载拒绝（errHardLimit 分支；panic 快扩信号）。
func (t *AutoscaleTracker) AddHardRejected(host string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entry(host)
	e.cur.hardRejected++
	t.hosts.touch(host)
}

// AddUnserved 记录一次零容量请求（冷起信号；调用方负责分类排除）。
func (t *AutoscaleTracker) AddUnserved(host string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entry(host)
	e.cur.unserved++
	t.hosts.touch(host)
}

// snapshotForReport 推进一拍：更新各 host EWMA，打包滚动和，并轮转窗口。
// 返回 hostname → 待上报样本。常驻 inflight（WS/SSE 长连接）的 host 被
// touch（不因 LRU 老化在服务中被逐出）；空闲零值 host 自然老化，只在
// 容量压力下逐出（不按时间淘汰，保证 idle app 信号语义）。
func (t *AutoscaleTracker) snapshotForReport(now time.Time) map[string]HostSample {
	out := map[string]HostSample{}
	if t == nil {
		return out
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	ts := now.UnixMilli()
	for host, el := range t.hosts.entries {
		e := el.Value.(lruEntry[*hostCounters]).value
		e.ewma += t.alphaOrDefault() * (float64(e.inflight) - e.ewma)
		sum := e.cur
		sum.add(e.prev)
		out[host] = HostSample{
			EWMA:         e.ewma,
			RPS:          float64(sum.rps),
			HardRejected: sum.hardRejected,
			Unserved:     sum.unserved,
			TsMs:         ts,
			WinMs:        int64(autoscaleReportInterval/time.Millisecond) * autoscaleRollingWindows,
		}
		e.prev = e.cur
		e.cur = hostWindow{}
		if e.inflight > 0 {
			t.hosts.touch(host)
		}
	}
	return out
}

// autoscaleRedis 是 reporter 需要的最小 Redis 面（*redis.Client 原生实现）。
type autoscaleRedis interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
}

// autoscaleReportLua 原子完成 HSET+EXPIRE（避免 crash 留下无 TTL 键）。
const autoscaleReportLua = `
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`

// AutoscaleReporter 把记账表 5s 一拍推到 Redis（失败只记指标，不阻塞转发）。
type AutoscaleReporter struct {
	rdb      autoscaleRedis
	edgeID   string
	interval time.Duration
	ttl      time.Duration
	tracker  *AutoscaleTracker
	counters *Counters
	nowFn    func() time.Time
}

// NewAutoscaleReporter 构造；interval/ttl<=0 取默认（5s/20s）。
func NewAutoscaleReporter(rdb autoscaleRedis, edgeID string, tracker *AutoscaleTracker,
	counters *Counters, interval time.Duration,
) *AutoscaleReporter {
	if interval <= 0 {
		interval = autoscaleReportInterval
	}
	// P2-8 防呆：上报间隔不得超过 TTL 的一半，否则信号恒过期、
	// controller 恒 hold。超限直接钳制并告警（静默放过等于功能关闭）。
	if interval > autoscaleKeyTTL/2 {
		slog.Warn("autoscale report interval exceeds half of signal TTL; clamping",
			"interval", interval, "ttl", autoscaleKeyTTL)
		interval = autoscaleKeyTTL / 2
	}
	if counters == nil {
		counters = &Counters{}
	}
	if tracker != nil {
		tracker.setAlpha(ewmaAlphaForInterval(interval))
	}
	return &AutoscaleReporter{
		rdb: rdb, edgeID: edgeID, tracker: tracker, counters: counters,
		interval: interval, ttl: autoscaleKeyTTL, nowFn: time.Now,
	}
}

// Run 启动上报协程（ctx 取消即停；与 RouteCache/TokenClient 同进程）。
func (r *AutoscaleReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Report(ctx)
		}
	}
}

// Report 执行单拍上报（可单独调用，便于单测）。
func (r *AutoscaleReporter) Report(ctx context.Context) {
	if r.tracker == nil || r.edgeID == "" {
		return
	}
	samples := r.tracker.snapshotForReport(r.nowFn())
	for host, s := range samples {
		raw, err := json.Marshal(s)
		if err != nil {
			continue
		}
		key := catalog.AutoscaleKey(host)
		if err := r.rdb.Eval(ctx, autoscaleReportLua,
			[]string{key}, r.edgeID, string(raw), int64(r.ttl/time.Second)).Err(); err != nil {
			r.counters.autoscaleReportErrors.Add(1)
			continue
		}
		r.counters.autoscaleReports.Add(1)
	}
}

// ResolveEdgeID 解析 reporter 的 edge 身份：FIREPAAS_EDGE_ID 缺省回退
// FIREPAAS_MESH_EDGE_ID/hostname；同 key 内 field 冲突视为配置错误（后写覆盖）。
func ResolveEdgeID(primary, fallback string, hostname func() (string, error)) string {
	if primary != "" {
		return primary
	}
	if fallback != "" {
		return fallback
	}
	if h, err := hostname(); err == nil && h != "" {
		return h
	}
	return fmt.Sprintf("edge-%d", time.Now().UnixNano())
}
