package edge

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestGenMetricsExposition(t *testing.T) {
	c := &Counters{}
	c.observeGenResponse("a.test", 7, 200, 0.02)
	c.observeGenResponse("a.test", 7, 500, 0.3)
	c.observeGenResponse("a.test", 8, 200, 0.01)
	// 无代请求不进表。
	c.observeGenResponse("a.test", 0, 500, 0.1)

	out := httptest.NewRecorder()
	c.WritePrometheus(out)
	body := out.Body.String()
	for _, want := range []string{
		`firepaas_edge_gen_responses_total{host="a.test",generation="7",code_class="2xx"} 1`,
		`firepaas_edge_gen_responses_total{host="a.test",generation="7",code_class="5xx"} 1`,
		`firepaas_edge_gen_responses_total{host="a.test",generation="8",code_class="2xx"} 1`,
		`firepaas_edge_gen_latency_seconds_count{host="a.test",generation="7"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, `generation="0"`) {
		t.Fatal("gen<=0 must not be recorded")
	}
}

func TestGenMetricsLRUBound(t *testing.T) {
	c := &Counters{}
	for i := 1; i <= maxGenEntries+100; i++ {
		c.observeGenResponse("h.test", int64(i), 200, 0.01)
	}
	c.genMu.RLock()
	n := len(c.genTable)
	c.genMu.RUnlock()
	if n > maxGenEntries {
		t.Fatalf("gen table = %d, want <= %d", n, maxGenEntries)
	}
	// 最新代必须保留。
	c.genMu.RLock()
	_, ok := c.genTable["h.test\x00"+itoa(maxGenEntries+100)]
	c.genMu.RUnlock()
	if !ok {
		t.Fatal("newest generation evicted")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// B2 回归：桶输出必须单调非减且任一有限桶 ≤ +Inf（存储非累计、写出累计）。
func TestGenHistogramBucketsMonotone(t *testing.T) {
	c := &Counters{}
	c.observeGenResponse("a.test", 7, 200, 0.02)
	c.observeGenResponse("a.test", 7, 500, 0.3)
	c.observeGenResponse("a.test", 7, 200, 30) // 超最大桶，只进 count/+Inf
	out := httptest.NewRecorder()
	c.WritePrometheus(out)
	body := out.Body.String()
	var prev uint64
	var inf, count uint64
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "firepaas_edge_gen_latency_seconds_bucket{") {
			continue
		}
		n++
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("bad line %q", line)
		}
		var v uint64
		if _, err := fmt.Sscanf(fields[1], "%d", &v); err != nil {
			t.Fatalf("bad value %q", line)
		}
		if strings.Contains(line, `le="+Inf"`) {
			inf = v
			continue
		}
		if v < prev {
			t.Fatalf("non-monotone buckets at %q (prev %d)", line, prev)
		}
		prev = v
		if v > 3 {
			t.Fatalf("finite bucket %q exceeds observations", line)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "firepaas_edge_gen_latency_seconds_count{") {
			if _, err := fmt.Sscanf(strings.Fields(line)[1], "%d", &count); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n == 0 || inf != 3 || count != 3 || prev > inf {
		t.Fatalf("buckets n=%d inf=%d count=%d last=%d", n, inf, count, prev)
	}
}

// B3 回归：整个 /metrics 输出必须能被 Prometheus exposition parser 吃下
// （le 等 label 值加引号；一个坏行会丢整个 scrape）。
func TestGenMetricsExpositionParses(t *testing.T) {
	c := &Counters{}
	c.observeGenResponse("a.test", 7, 200, 0.02)
	c.observeGenResponse("a.test", 7, 500, 0.3)
	c.req5xx.Add(1)
	out := httptest.NewRecorder()
	c.WritePrometheus(out)
	dec := expfmt.NewDecoder(strings.NewReader(out.Body.String()), expfmt.NewFormat(expfmt.TypeTextPlain))
	families := 0
	for {
		var mf dto.MetricFamily
		if err := dec.Decode(&mf); err != nil {
			if err.Error() == "EOF" || strings.Contains(err.Error(), "EOF") {
				break
			}
			t.Fatalf("exposition parse error: %v\n%s", err, out.Body.String())
		}
		families++
	}
	if families == 0 {
		t.Fatal("no metric families decoded")
	}
}
