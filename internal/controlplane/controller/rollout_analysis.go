// rollout_analysis.go：CUTOVER 观察窗与指标驱动自动回滚（P1，参考 Argo analysis template）。
//
// 背景：route generation 切换（PREPARING→CUTOVER）后，旧代在 drain grace 内
// 保留（可回退），但此前只有“失败保留旧 route”的被动兜底——CUTOVER 到期而新
// 代始终未全 READY 时无限等待（旧代泄漏），新代切流后退化也无人值守。本文件
// 给该窗口加上健康评估：超阈值自动回退上一代（startRollback → ROLLING_BACK，
// from 代本就在线 serving，回退不断流）。
//
// 评估面是可插拔的 AnalysisProvider（Argo AnalysisTemplate 类比）：
//   - 默认 serving-ratio 内建实现（PG observed 状态：可服务比例 + 零可服务
//     灾难快线；错误率/延迟类外部信号——edge/Prometheus——由后续 Provider
//     实现同一接口接入，无需改状态机）；
//   - VerdictWait（窗内继续观察）/ VerdictProceed（到期完成）/ VerdictRollback
//     （回退）三态，调用方（reconcileRollout CUTOVER 分支）只做状态迁移。
package controller

import (
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// CutoverVerdict 是 CUTOVER 评估的三态结论。
type CutoverVerdict int

const (
	// VerdictWait 继续观察（窗内未到期、无灾难信号）。
	VerdictWait CutoverVerdict = iota
	// VerdictProceed 窗到期且达标：走既有完成路径（旧代回收）。
	VerdictProceed
	// VerdictRollback 回退上一代：灾难信号或窗到期未达标。
	VerdictRollback
)

// AnalysisProvider 评估 CUTOVER 中新代的健康度（Argo AnalysisTemplate 类比）。
// atWindowEnd=false 是窗内中间态（只应报 Wait，或灾难性 Rollback）；
// atWindowEnd=true 是窗到期决策点（必须报 Proceed 或 Rollback，禁止 Wait——
// 调用方在到期点把 Wait 当 Proceed 处理，不等待无界）。
type AnalysisProvider interface {
	AssessCutover(
		app *store.App,
		r *store.Rollout,
		toMachines []store.Machine,
		atWindowEnd bool,
		now time.Time,
	) (CutoverVerdict, string)
}

// CutoverAnalysisConfig 是观察窗参数（controller.Config 透传；零值 = 历史行为）。
type CutoverAnalysisConfig struct {
	// MinServingRatio 是窗到期时新代可服务比例下限 [0,1]；<=0 → 1.0（与
	// 既有 allReady 完成门控一致）。
	MinServingRatio float64
	// ZeroServingGrace 是 cutover 后允许零可服务的宽限（切流传播抖动）；
	// <=0 → 30s。超宽限仍零可服务 = 灾难信号，提前回退不等窗到期。
	ZeroServingGrace time.Duration
	// 注：观察窗长度（Window）不在此——窗终点由调用方按
	// cutoverAnalysisEnd 统一计算后以 atWindowEnd 传入，避免两处各算一遍发散。
}

// DefaultCutoverAnalysisConfig 返回归一后的默认配置。
func DefaultCutoverAnalysisConfig() CutoverAnalysisConfig {
	return CutoverAnalysisConfig{MinServingRatio: 1.0, ZeroServingGrace: 30 * time.Second}
}

func (c CutoverAnalysisConfig) normalized() CutoverAnalysisConfig {
	if c.MinServingRatio <= 0 {
		c.MinServingRatio = 1.0
	}
	if c.MinServingRatio > 1 {
		c.MinServingRatio = 1
	}
	if c.ZeroServingGrace <= 0 {
		c.ZeroServingGrace = 30 * time.Second
	}
	return c
}

// ServingRatioAnalysis 是内建的 AnalysisProvider：基于 PG observed 状态的新代
// 可服务比例（serving ordinals / desired）。错误率/延迟等外部信号由自定义
// Provider 接入同一接口（调用方只认三态结论）。
type ServingRatioAnalysis struct{ cfg CutoverAnalysisConfig }

// NewServingRatioAnalysis 构造内建评估（零值 cfg 自动归一）。
func NewServingRatioAnalysis(cfg CutoverAnalysisConfig) *ServingRatioAnalysis {
	return &ServingRatioAnalysis{cfg: cfg.normalized()}
}

// AssessCutover 实现 AnalysisProvider。
func (a *ServingRatioAnalysis) AssessCutover(
	app *store.App,
	r *store.Rollout,
	toMachines []store.Machine,
	atWindowEnd bool,
	now time.Time,
) (CutoverVerdict, string) {
	desired := 0
	if app != nil {
		desired = app.DesiredReplicas
	}
	if desired <= 0 {
		return VerdictProceed, "no desired replicas: nothing to observe"
	}
	serving := servingOrdinals(toMachines)
	// 灾难快线：超宽限仍零可服务（cutover 只在全 READY 时发生，掉零是明确退化）。
	if serving == 0 && !now.Before(cutoverBase(r).Add(a.cfg.ZeroServingGrace)) {
		return VerdictRollback, "new generation serves zero traffic past grace; auto-rollback"
	}
	if !atWindowEnd {
		return VerdictWait, "within observation window"
	}
	if float64(serving)/float64(desired) >= a.cfg.MinServingRatio {
		return VerdictProceed, "new generation meets serving ratio"
	}
	return VerdictRollback, "new generation below serving ratio at window end; auto-rollback"
}

// servingOrdinals 统计可服务的不重复 ordinal 数（与 allReady 同口径：
// RUNNING/PAUSED + READY/UNCONFIGURED）。
func servingOrdinals(ms []store.Machine) int {
	seen := map[int]bool{}
	for _, m := range ms {
		if machineServing(m) {
			seen[m.ReplicaOrdinal] = true
		}
	}
	return len(seen)
}

// cutoverBase 返回观察窗起点：cutover_at 缺失（历史脏行）时回退 started_at，
// 再缺失回退零时间（调用方此时视为已到期，不无限等待）。
func cutoverBase(r *store.Rollout) time.Time {
	if r == nil {
		return time.Time{}
	}
	if r.CutoverAt != nil && !r.CutoverAt.IsZero() {
		return *r.CutoverAt
	}
	return r.StartedAt
}

// cutoverAnalysisEnd 计算观察窗终点：Window>0 时 cutover+Window，否则 drain
// deadline（历史行为）。drain deadline 缺失（防御）时回退观察窗终点。
func cutoverAnalysisEnd(r *store.Rollout, window time.Duration, drainGraceFallback time.Time) time.Time {
	if window > 0 {
		return cutoverBase(r).Add(window)
	}
	return drainGraceFallback
}
