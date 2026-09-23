package controller

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/promquery"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// autoscale_multi.go：Wave4 多信号——max-of-wants（KPA/HPA 语义）。
//
// want = clamp(max(并发, RPS?, CPU?, 自定义?), min, max)。带 ? 的维度缺省
// 关闭（policy 0/空），不参与 max。panic 仍只看并发（KPA 同理：panic 是
// 并发快扩专线）。缩容稳定窗、冻结、CAS 写路径全部复用既有机制。

// extraSignals 是可选维度的已归一输入。ok=false 表示该维度无信号：
// 关闭的维度恒 false；开启但查询失败/无样本时也为 false（调用方此时整体
// hold，见 fetchExtraSignals——缺信号不猜方向）。
type extraSignals struct {
	rpsRate     float64 // ~滚动窗口速率（TotalRPS/WindowSec）
	rpsOK       bool
	cpuUtil     float64 // app 平均 CPU 利用率 (0,∞)
	cpuOK       bool
	customValue float64
	customOK    bool
}

// computeAutoscaleWantMulti 是多信号决策核（纯函数）：并发 want 与既有
// computeAutoscaleWant 同公式；各开启维度取 max；最后 min/max clamp。
// byDim 返回各维度 want（排障进事件 payload，不进指标 label）。
func computeAutoscaleWantMulti(policy store.AutoscalePolicy,
	sig AutoscaleSignal, ready, warming int, extra extraSignals,
) (want int, panicTriggered bool, byDim map[string]int) {
	byDim = map[string]int{}
	eff := ready + warming
	load := sig.Served + sig.Unserved
	wantConcurrency := ceilDiv(int(math.Ceil(load)), policy.TargetConcurrency)
	byDim["concurrency"] = wantConcurrency
	want = wantConcurrency
	if policy.TargetRPS > 0 && extra.rpsOK {
		w := ceilDiv(int(math.Ceil(extra.rpsRate)), policy.TargetRPS)
		byDim["rps"] = w
		if w > want {
			want = w
		}
	}
	if policy.TargetCPURatio > 0 && extra.cpuOK {
		w := int(math.Ceil(float64(eff) * extra.cpuUtil / policy.TargetCPURatio))
		byDim["cpu"] = w
		if w > want {
			want = w
		}
	}
	if policy.CustomTarget > 0 && policy.CustomPromQuery != "" && extra.customOK {
		var w int
		if policy.CustomMode == "absolute" {
			w = int(math.Ceil(extra.customValue / policy.CustomTarget))
		} else {
			w = int(math.Ceil(float64(eff) * extra.customValue / policy.CustomTarget))
		}
		byDim["custom"] = w
		if w > want {
			want = w
		}
	}
	if want < policy.MinReplicas {
		want = policy.MinReplicas
	}
	if want > policy.MaxReplicas {
		want = policy.MaxReplicas
	}
	// panic：只看并发（与既有语义逐字一致）。
	if eff > 0 && (sig.PanicReject ||
		load > policy.PanicThreshold*float64(policy.TargetConcurrency*eff)) {
		boosted := max(wantConcurrency, int(math.Ceil(policy.PanicThreshold*float64(eff))), eff+2)
		if boosted > policy.MaxReplicas {
			boosted = policy.MaxReplicas
		}
		if boosted > want {
			want = boosted
		}
		return want, true, byDim
	}
	return want, false, byDim
}

// cpuUtilQuery 生成 app 级平均 CPU 利用率 PromQL：instance_name 即 firepaas
// machine_id（{app}-r{ordinal}-g{generation}，见 agent/machine 注释），按
// app 前缀正则聚合，rate/allocated_vcpus 即单机利用率再 avg。
func cpuUtilQuery(appID string, window time.Duration) string {
	// QuoteMeta：appID 含正则元字符（点等）时不误匹配无关 series。
	re := regexp.QuoteMeta(appID) + `-r[0-9]+-g[0-9]+`
	return fmt.Sprintf(
		`avg(rate(hypeman_vm_cpu_seconds_total{instance_name=~%q}[%s]) / on(instance_name) hypeman_vm_allocated_vcpus{instance_name=~%q})`,
		re,
		window,
		re,
	)
}

// renderCustomQuery 渲染自定义查询的 {hostname}/{app_id} 模板变量。
func renderCustomQuery(tmpl, hostname, appID string) string {
	out := strings.ReplaceAll(tmpl, "{hostname}", promLabelEscape(hostname))
	return strings.ReplaceAll(out, "{app_id}", promLabelEscape(appID))
}

// promQuerierFor 返回外部指标查询器（PrometheusAddr 空 = 未配置 nil）。
func (c *Controller) promQuerierFor() promQuerier {
	if c.cfg.PrometheusAddr == "" {
		return nil
	}
	return &promquery.Client{BaseURL: c.cfg.PrometheusAddr}
}

// fetchExtraSignals 拉取可选维度信号。返回 hold=true 时调用方整体 hold
// （hold_signal）：开启的维度查不到信号且有效容量 >0——盲目扩缩都不安全；
// 冷起（eff==0）时跳过外部维度（并发 path 的 unserved 负载自然 want>=1）。
func (c *Controller) fetchExtraSignals(ctx context.Context,
	app *store.App, policy store.AutoscalePolicy, sig AutoscaleSignal, eff int,
) (extra extraSignals, holdReason string, hold bool) {
	if policy.TargetRPS > 0 && sig.WindowSec > 0 {
		extra.rpsRate = sig.TotalRPS / sig.WindowSec
		extra.rpsOK = true
	}
	needProm := policy.TargetCPURatio > 0 ||
		(policy.CustomTarget > 0 && policy.CustomPromQuery != "")
	if !needProm {
		return extra, "", false
	}
	if eff == 0 {
		return extra, "", false
	}
	q := c.promQuerierFor()
	if q == nil {
		return extra, "prometheus not configured for cpu/custom signals", true
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if policy.TargetCPURatio > 0 {
		v, err := q.Query(ctx, cpuUtilQuery(app.ID, 2*time.Minute))
		if err != nil {
			return extra, fmt.Sprintf("cpu signal unavailable: %v", err), true
		}
		extra.cpuUtil, extra.cpuOK = v, true
	}
	if policy.CustomTarget > 0 && policy.CustomPromQuery != "" {
		v, err := q.Query(ctx, renderCustomQuery(policy.CustomPromQuery, app.Hostname, app.ID))
		if err != nil {
			return extra, fmt.Sprintf("custom signal unavailable: %v", err), true
		}
		extra.customValue, extra.customOK = v, true
	}
	return extra, "", false
}
