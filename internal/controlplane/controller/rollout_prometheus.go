package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// rollout_prometheus.go：Wave3 Stage A——Prometheus 错误率/延迟 Provider
//（Flagger canary analysis / Argo AnalysisTemplate 对标）。
//
// serving-ratio 只看"可服务比例"，看不见"服务着但全 500"的退化。本 Provider
// 查 edge 代级指标（internal/edge/genmetrics.go）：
//   - 错误率 = 5xx / 全部（to-generation，指定 hostname）；
//   - p99 = to-generation 端到端延迟分位。
// 启用条件（零回归）：PrometheusAddr 非空且至少一个阈值 >0，否则保持纯
// serving-ratio。查询失败 fail-closed：窗内 Wait（不误杀抖动），到期
// Rollback（与"零 started_at 按超时回滚"的 violation-safe 方向一致）。

// promQuerier 是 PromQL 瞬时查询的最小面（*promquery.Client 原生实现；
// 单测用假实现）。
type promQuerier interface {
	Query(ctx context.Context, expr string) (float64, error)
}

// PrometheusAnalysisConfig 是外部指标门的参数。
type PrometheusAnalysisConfig struct {
	// Querier 为 nil → 本 Provider 恒 Wait（调用方不应启用；防御）。
	Querier promQuerier
	// MaxErrorRate 是 to 代错误率上限 [0,1]；<=0 = 不启用错误率门。
	MaxErrorRate float64
	// P99LatencySec 是 to 代 p99 上限（秒）；<=0 = 不启用延迟门。
	P99LatencySec float64
	// Window 是 rate 窗（PromQL [..]）；<=0 → 5m。
	Window time.Duration
	// HostLabel 是 edge 代级指标的 host 标签与 app.Hostname 同口径时无需覆盖。
}

func (c PrometheusAnalysisConfig) window() time.Duration {
	if c.Window > 0 {
		return c.Window
	}
	return 5 * time.Minute
}

// enabled 报告本 Provider 是否被真正启用（缺查询器或双门全关 = 未启用）。
func (c PrometheusAnalysisConfig) enabled() bool {
	return c.Querier != nil && (c.MaxErrorRate > 0 || c.P99LatencySec > 0)
}

// PrometheusAnalysis 是基于 edge 代级指标的 AnalysisProvider。
type PrometheusAnalysis struct{ cfg PrometheusAnalysisConfig }

// NewPrometheusAnalysis 构造（未启用配置同样可构造，评估恒 Wait 不误判）。
func NewPrometheusAnalysis(cfg PrometheusAnalysisConfig) *PrometheusAnalysis {
	return &PrometheusAnalysis{cfg: cfg}
}

func promLabelEscape(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}

// errorRateQuery 生成 to 代错误率 PromQL（5xx/全部；分母为零时 Prom
// 零流量时无 series（ErrNoData→窗内 Wait/到期 Rollback）；零可服务判定仍由 serving-ratio 门承担）。
func errorRateQuery(host string, generation int64, window time.Duration) string {
	h, g := promLabelEscape(host), strconv.FormatInt(generation, 10)
	return fmt.Sprintf(
		`sum(rate(firepaas_edge_gen_responses_total{host=%q,generation=%q,code_class="5xx"}[%s]))`+
			` / sum(rate(firepaas_edge_gen_responses_total{host=%q,generation=%q}[%s]))`,
		h, g, window, h, g, window)
}

// p99Query 生成 to 代 p99 PromQL。
func p99Query(host string, generation int64, window time.Duration) string {
	return fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(firepaas_edge_gen_latency_seconds_bucket{host=%q,generation=%q}[%s])))`,
		promLabelEscape(host),
		strconv.FormatInt(generation, 10),
		window,
	)
}

// AssessCutover 实现 AnalysisProvider。
func (a *PrometheusAnalysis) AssessCutover(
	app *store.App,
	r *store.Rollout,
	_ []store.Machine,
	atWindowEnd bool,
	_ time.Time,
) (CutoverVerdict, string) {
	if !a.cfg.enabled() {
		return VerdictWait, "prometheus analysis disabled"
	}
	host := ""
	if app != nil {
		host = app.Hostname
	}
	var gen int64
	if r != nil {
		gen = r.ToGeneration
	}
	if host == "" || gen <= 0 {
		return VerdictWait, "prometheus analysis: missing host/generation"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if a.cfg.MaxErrorRate > 0 {
		rate, err := a.cfg.Querier.Query(ctx, errorRateQuery(host, gen, a.cfg.window()))
		if err != nil {
			return a.queryFailure(atWindowEnd, "error-rate", err)
		}
		if rate > a.cfg.MaxErrorRate {
			return VerdictRollback, fmt.Sprintf(
				"to-generation error rate %.3f exceeds %.3f; auto-rollback", rate, a.cfg.MaxErrorRate)
		}
	}
	if a.cfg.P99LatencySec > 0 {
		p99, err := a.cfg.Querier.Query(ctx, p99Query(host, gen, a.cfg.window()))
		if err != nil {
			return a.queryFailure(atWindowEnd, "p99", err)
		}
		if p99 > a.cfg.P99LatencySec {
			return VerdictRollback, fmt.Sprintf(
				"to-generation p99 %.2fs exceeds %.2fs; auto-rollback", p99, a.cfg.P99LatencySec)
		}
	}
	if !atWindowEnd {
		return VerdictWait, "within observation window"
	}
	return VerdictProceed, "to-generation meets error-rate/latency gates"
}

// queryFailure 把查询失败映射为三态：窗内 Wait（抖动不误杀），到期
// Rollback（fail-closed：外部信号不可用时不放行全量）。
func (a *PrometheusAnalysis) queryFailure(atWindowEnd bool, what string, err error) (CutoverVerdict, string) {
	if !atWindowEnd {
		return VerdictWait, fmt.Sprintf("prometheus %s query failed (waiting): %v", what, err)
	}
	return VerdictRollback, fmt.Sprintf("prometheus %s unavailable at window end; auto-rollback: %v", what, err)
}

// CompositeAnalysis 是多个 Provider 的 AND 组合（任一 Rollback 即 Rollback；
// Proceed 需全体 Proceed；Wait 透传；到期点的 Wait 按调用方既有语义——
// apps.go CUTOVER 分支把 Wait 当 Proceed 处理——此处原样透出，不私自升级）。
type CompositeAnalysis struct {
	providers []AnalysisProvider
}

// NewCompositeAnalysis 构造（空 providers 恒 Proceed：组合未配置 = 不挡路）。
func NewCompositeAnalysis(providers ...AnalysisProvider) *CompositeAnalysis {
	return &CompositeAnalysis{providers: providers}
}

// AssessCutover 实现 AnalysisProvider。
func (c *CompositeAnalysis) AssessCutover(
	app *store.App,
	r *store.Rollout,
	toMachines []store.Machine,
	atWindowEnd bool,
	now time.Time,
) (CutoverVerdict, string) {
	reasons := []string{}
	wait := false
	for _, p := range c.providers {
		if p == nil {
			continue
		}
		v, reason := p.AssessCutover(app, r, toMachines, atWindowEnd, now)
		switch v {
		case VerdictRollback:
			return VerdictRollback, reason
		case VerdictWait:
			wait = true
			reasons = append(reasons, reason)
		}
	}
	if wait {
		return VerdictWait, "waiting: " + strings.Join(reasons, "; ")
	}
	return VerdictProceed, "all analyses proceed"
}
