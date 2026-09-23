package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// fakeQuerier 按查询子串返回预设值/错误。
type fakeQuerier struct {
	values map[string]float64
	err    error
}

func (f *fakeQuerier) Query(_ context.Context, expr string) (float64, error) {
	if f.err != nil {
		return 0, f.err
	}
	for sub, v := range f.values {
		if strings.Contains(expr, sub) {
			return v, nil
		}
	}
	return 0, errors.New("no data")
}

func rolloutFixture() (*store.App, *store.Rollout) {
	cutover := time.Now().Add(-10 * time.Minute)
	return &store.App{ID: "app", Hostname: "app.test", DesiredReplicas: 2},
		&store.Rollout{AppID: "app", FromGeneration: 1, ToGeneration: 2, Status: "CUTOVER", CutoverAt: &cutover}
}

func TestPrometheusAnalysisGates(t *testing.T) {
	app, r := rolloutFixture()
	now := time.Now()
	// 错误率超阈 → Rollback（窗内也一样，退化不等到期）。
	a := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier: &fakeQuerier{values: map[string]float64{"code_class": 0.2}}, MaxErrorRate: 0.05,
	})
	if v, _ := a.AssessCutover(app, r, nil, false, now); v != VerdictRollback {
		t.Fatalf("high error rate mid-window = %v, want Rollback", v)
	}
	// 达标：窗内 Wait，到期 Proceed。
	ok := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier:      &fakeQuerier{values: map[string]float64{"code_class": 0.01, "histogram_quantile": 0.2}},
		MaxErrorRate: 0.05, P99LatencySec: 1.0,
	})
	if v, _ := ok.AssessCutover(app, r, nil, false, now); v != VerdictWait {
		t.Fatalf("healthy mid-window = %v, want Wait", v)
	}
	if v, _ := ok.AssessCutover(app, r, nil, true, now); v != VerdictProceed {
		t.Fatalf("healthy at-end = %v, want Proceed", v)
	}
	// p99 超阈 → Rollback。
	slow := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier: &fakeQuerier{values: map[string]float64{"histogram_quantile": 2.5}}, P99LatencySec: 1.0,
	})
	if v, reason := slow.AssessCutover(app, r, nil, true, now); v != VerdictRollback ||
		!strings.Contains(reason, "p99") {
		t.Fatalf("slow at-end = %v (%s), want Rollback", v, reason)
	}
	// 查询失败：窗内 Wait，到期 Rollback（fail-closed）。
	broken := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier: &fakeQuerier{err: errors.New("connection refused")}, MaxErrorRate: 0.05,
	})
	if v, _ := broken.AssessCutover(app, r, nil, false, now); v != VerdictWait {
		t.Fatalf("query failure mid-window = %v, want Wait", v)
	}
	if v, _ := broken.AssessCutover(app, r, nil, true, now); v != VerdictRollback {
		t.Fatalf("query failure at-end = %v, want Rollback", v)
	}
	// 未启用（无阈值）→ 恒 Wait，不挡路。
	off := NewPrometheusAnalysis(PrometheusAnalysisConfig{Querier: &fakeQuerier{}})
	if v, _ := off.AssessCutover(app, r, nil, true, now); v != VerdictWait {
		t.Fatalf("disabled = %v, want Wait", v)
	}
}

func TestCompositeAnalysis(t *testing.T) {
	app, r := rolloutFixture()
	now := time.Now()
	serving := NewServingRatioAnalysis(CutoverAnalysisConfig{})
	healthy := func() []store.Machine {
		return []store.Machine{
			{ReplicaOrdinal: 0, ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ReplicaOrdinal: 1, ObservedState: "RUNNING", ObservedReadiness: "READY"},
		}
	}
	// 任一 Rollback 即 Rollback。
	bad := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier: &fakeQuerier{values: map[string]float64{"code_class": 0.5}}, MaxErrorRate: 0.05,
	})
	c := NewCompositeAnalysis(serving, bad)
	if v, _ := c.AssessCutover(app, r, healthy(), true, now); v != VerdictRollback {
		t.Fatalf("composite with bad prom = %v, want Rollback", v)
	}
	// 全体 Proceed → Proceed。
	good := NewPrometheusAnalysis(PrometheusAnalysisConfig{
		Querier: &fakeQuerier{values: map[string]float64{"code_class": 0.0}}, MaxErrorRate: 0.05,
	})
	c2 := NewCompositeAnalysis(serving, good)
	if v, _ := c2.AssessCutover(app, r, healthy(), true, now); v != VerdictProceed {
		t.Fatalf("composite healthy = %v, want Proceed", v)
	}
	// 空组合恒 Proceed。
	if v, _ := NewCompositeAnalysis().AssessCutover(app, r, nil, true, now); v != VerdictProceed {
		t.Fatalf("empty composite = %v, want Proceed", v)
	}
}

// B1 回归：label matcher 值必须加引号（PromQL 要求 STRING；裸数字 parse error）。
func TestPromQLLabelQuoting(t *testing.T) {
	errQ := errorRateQuery("app.test", 7, 5*time.Minute)
	for _, want := range []string{`host="app.test"`, `generation="7"`, `code_class="5xx"`} {
		if !strings.Contains(errQ, want) {
			t.Fatalf("errorRateQuery = %s, missing %s", errQ, want)
		}
	}
	if strings.Contains(errQ, "generation=7,") || strings.Contains(errQ, "generation=7}") {
		t.Fatalf("unquoted generation in %s", errQ)
	}
	p99 := p99Query("app.test", 7, 5*time.Minute)
	if !strings.Contains(p99, `generation="7"`) {
		t.Fatalf("p99Query = %s, missing quoted generation", p99)
	}
}
