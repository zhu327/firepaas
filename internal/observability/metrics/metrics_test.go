package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// P2-8：gauge family 清理——label 组合消失后旧序列不得残留。
func TestResetFamilyRemovesStaleSeries(t *testing.T) {
	r := New()
	r.Set("firepaas_machines_observed", map[string]string{"state": "RUNNING"}, 3)
	r.Set("firepaas_machines_observed", map[string]string{"state": "PAUSED"}, 1)
	r.Set("firepaas_nodes_total", nil, 2)

	r.ResetFamily("firepaas_machines_observed")
	r.Set("firepaas_machines_observed", map[string]string{"state": "RUNNING"}, 2)

	out := render(t, r)
	if strings.Contains(out, `state="PAUSED"`) {
		t.Fatalf("stale PAUSED series survived reset:\n%s", out)
	}
	if !strings.Contains(out, `firepaas_machines_observed{state="RUNNING"} 2`) {
		t.Fatalf("fresh series missing:\n%s", out)
	}
	if !strings.Contains(out, "firepaas_nodes_total 2") {
		t.Fatalf("other family must be untouched:\n%s", out)
	}
}

func render(t *testing.T, r *Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, nil)
	return rec.Body.String()
}

// W4：每个指标名恰好一条 HELP/TYPE（跨 label set 聚合）；显式描述输出
// HELP+TYPE，未描述指标只按操作自检 TYPE。
func TestExpositionHeadersOncePerMetric(t *testing.T) {
	r := New()
	r.DescribeCounter("firepaas_up", "process up")
	r.Inc("firepaas_up", map[string]string{"a": "1"}, 1)
	r.Inc("firepaas_up", map[string]string{"a": "2"}, 1)
	r.Inc("firepaas_inc_only", nil, 2)
	r.Set("firepaas_gauge_only", nil, 7)

	out := render(t, r)
	if got := strings.Count(out, "# HELP firepaas_up process up"); got != 1 {
		t.Fatalf("want exactly one HELP line, got %d:\n%s", got, out)
	}
	if got := strings.Count(out, "# TYPE firepaas_up counter"); got != 1 {
		t.Fatalf("want exactly one TYPE line, got %d:\n%s", got, out)
	}
	if !strings.Contains(out, "# TYPE firepaas_inc_only counter") {
		t.Fatalf("Inc-only metric must auto-detect counter:\n%s", out)
	}
	if strings.Contains(out, "# HELP firepaas_inc_only") {
		t.Fatalf("undescribed metric must not emit HELP:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE firepaas_gauge_only gauge") {
		t.Fatalf("Set-only metric must auto-detect gauge:\n%s", out)
	}
}

// W4：Handler 与 ObserveHistogram 并发安全（直方图快照必须在锁内深拷贝）。
func TestHistogramConcurrentSnapshot(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			r.ObserveHistogram("firepaas_conc", map[string]string{"k": "v"}, 0.01)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			rec := httptest.NewRecorder()
			r.Handler().ServeHTTP(rec, nil)
		}
	}()
	wg.Wait()
}

// W4：label 值转义在直方图路径同样生效。
func TestLabelEscaping(t *testing.T) {
	r := New()
	r.Inc("esc", map[string]string{"k": "a\"b\\c\nd"}, 1)
	r.ObserveHistogram("esc_h", map[string]string{"k": "a\"b\\c"}, 0.001)

	out := render(t, r)
	if !strings.Contains(out, `esc{k="a\"b\\c\nd"} 1`) {
		t.Fatalf("counter label not escaped:\n%s", out)
	}
	if !strings.Contains(out, `esc_h_bucket{k="a\"b\\c",le="0.005"} 1`) {
		t.Fatalf("histogram label not escaped:\n%s", out)
	}
}

// W4：固定桶直方图渲染为累计桶 + +Inf + sum/count，且只发一组 HELP/TYPE。
func TestHistogramExposition(t *testing.T) {
	r := New()
	r.DescribeHistogram("firepaas_api_request_duration_seconds", "api request duration")
	for _, v := range []float64{0.003, 0.02, 4, 20} {
		r.ObserveHistogram("firepaas_api_request_duration_seconds", map[string]string{"route": "x"}, v)
	}

	out := render(t, r)
	if got := strings.Count(out, "# TYPE firepaas_api_request_duration_seconds histogram"); got != 1 {
		t.Fatalf("want exactly one histogram TYPE, got %d:\n%s", got, out)
	}
	if !strings.Contains(out, "# HELP firepaas_api_request_duration_seconds api request duration") {
		t.Fatalf("histogram HELP missing:\n%s", out)
	}
	wants := []string{
		`firepaas_api_request_duration_seconds_bucket{route="x",le="0.005"} 1`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="0.01"} 1`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="0.025"} 2`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="0.1"} 2`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="5"} 3`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="10"} 3`,
		`firepaas_api_request_duration_seconds_bucket{route="x",le="+Inf"} 4`,
		`firepaas_api_request_duration_seconds_count{route="x"} 4`,
		`firepaas_api_request_duration_seconds_sum{route="x"} 24.023`,
	}
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

// W4：ResetFamily 清除序列但保留显式 HELP/TYPE 元数据。
func TestResetFamilyKeepsMetadata(t *testing.T) {
	r := New()
	r.DescribeGauge("firepaas_keep_gauge", "kept help")
	r.Set("firepaas_keep_gauge", nil, 1)
	r.DescribeHistogram("firepaas_keep_hist", "kept hist help")
	r.ObserveHistogram("firepaas_keep_hist", nil, 1)

	r.ResetFamily("firepaas_keep_gauge")
	r.ResetFamily("firepaas_keep_hist")
	if out := render(t, r); strings.Contains(out, "firepaas_keep_gauge 1") ||
		strings.Contains(out, "firepaas_keep_hist_count") {
		t.Fatalf("series survived reset:\n%s", out)
	}

	r.Set("firepaas_keep_gauge", nil, 2)
	r.ObserveHistogram("firepaas_keep_hist", nil, 1)
	out := render(t, r)
	for _, want := range []string{
		"# HELP firepaas_keep_gauge kept help",
		"# TYPE firepaas_keep_gauge gauge",
		"firepaas_keep_gauge 2",
		"# HELP firepaas_keep_hist kept hist help",
		"# TYPE firepaas_keep_hist histogram",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q after reset (metadata must persist):\n%s", want, out)
		}
	}
}
