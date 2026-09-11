// Package metrics 是 M2.5 最低可观测的手写计数器（避免引入完整 Prometheus 依赖）。
// 输出 Prometheus text format，接入 /metrics。agent 侧使用 OTel SDK；控制面保持
// 这一小体积实现（元数据/直方图/转义已满足需求），迁移 OTel 需具体收益而非风格。
//
// P3-4：指标名与 label 分离。旧实现把 label 内联在名字里且不转义，
// 值里出现空格/等号/引号时产出非法 exposition 文本。
//
// W4 可观测性：新增显式 HELP/TYPE 元数据与固定桶直方图（请求时延）。
// metadata 不随 ResetFamily 清除——指标类型由调用点约定，不随 label 集合
// 波动（见 ResetFamily 注释）。
package metrics

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var nameRe = regexp.MustCompile(`[^a-zA-Z0-9_:]`)

// histogramBuckets 是请求时延（秒）的固定桶上界；+Inf 由 count 总量表达。
var histogramBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type entry struct {
	name   string
	labels map[string]string
	value  uint64
}

// histogram 是按 name+labels 聚合的固定桶累计分布。counts 存每桶增量，
// 渲染时累加为 Prometheus 的累计形式。
type histogram struct {
	name   string
	labels map[string]string
	counts []uint64
	sum    float64
	count  uint64
}

// metricMeta 是 DescribeCounter/DescribeGauge/DescribeHistogram 附加的元数据。
type metricMeta struct {
	kind string
	help string
}

// Registry 持有命名计数器（key 为 name+labels 的缓存键）。
type Registry struct {
	mu         sync.Mutex
	entries    map[string]entry
	histograms map[string]*histogram
	meta       map[string]metricMeta // 显式描述，key 为规范化后的指标名
	kinds      map[string]string     // 未描述指标的自检类型（counter/gauge/histogram）
}

// New 构造 Registry。
func New() *Registry {
	return &Registry{
		entries:    map[string]entry{},
		histograms: map[string]*histogram{},
		meta:       map[string]metricMeta{},
		kinds:      map[string]string{},
	}
}

// DescribeCounter 声明计数器并附加 HELP 文本（幂等；重复声明后者覆盖）。
func (r *Registry) DescribeCounter(name, help string) { r.describe(name, "counter", help) }

// DescribeGauge 声明仪表并附加 HELP 文本。
func (r *Registry) DescribeGauge(name, help string) { r.describe(name, "gauge", help) }

// DescribeHistogram 声明直方图并附加 HELP 文本。
func (r *Registry) DescribeHistogram(name, help string) { r.describe(name, "histogram", help) }

func (r *Registry) describe(name, kind, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.meta[sanitizeName(name)] = metricMeta{kind: kind, help: help}
}

// Inc 增加计数。labels 可为 nil；name 与 label key 会规范化，label 值会转义。
func (r *Registry) Inc(name string, labels map[string]string, n uint64) {
	r.add(name, labels, n, false)
}

// Set 置绝对值（仪表类）。
func (r *Registry) Set(name string, labels map[string]string, v uint64) {
	r.add(name, labels, v, true)
}

// ObserveHistogram 记录一次观测（秒）。桶上界固定为 histogramBuckets。
func (r *Registry) ObserveHistogram(name string, labels map[string]string, seconds float64) {
	if seconds < 0 {
		seconds = 0
	}
	key := cacheKey(name, labels)
	san := sanitizeName(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kinds[san]; !ok {
		r.kinds[san] = "histogram"
	}
	h, ok := r.histograms[key]
	if !ok {
		h = &histogram{name: san, labels: labels, counts: make([]uint64, len(histogramBuckets))}
	}
	h.count++
	h.sum += seconds
	for i, ub := range histogramBuckets {
		if seconds <= ub {
			h.counts[i]++
			break
		}
	}
	r.histograms[key] = h
}

// ResetFamily 删除某指标名的全部序列（P2-8，M5 评审）：label 集合随时间
// 收缩的 gauge（如 machines_observed{state=...}）必须每轮先清再 Set，
// 否则消失的 label 组合永远残留旧值（幽灵机器/告警噪声）。
//
// 显式 Describe* 元数据与自检类型不从 ResetFamily 清除：类型是调用点的
// 静态约定，不应随某轮 label 集合的存亡而改变（否则同一指标名会在
// counter/gauge 间摇摆，甚至发出错误的 # TYPE）。
func (r *Registry) ResetFamily(name string) {
	san := sanitizeName(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, e := range r.entries {
		if e.name == san {
			delete(r.entries, k)
		}
	}
	for k, h := range r.histograms {
		if h.name == san {
			delete(r.histograms, k)
		}
	}
}

func (r *Registry) add(name string, labels map[string]string, n uint64, absolute bool) {
	if n == 0 && !absolute {
		return
	}
	key := cacheKey(name, labels)
	san := sanitizeName(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kinds[san]; !ok {
		if absolute {
			r.kinds[san] = "gauge"
		} else {
			r.kinds[san] = "counter"
		}
	}
	e, ok := r.entries[key]
	if !ok {
		e = entry{name: san, labels: labels}
	}
	if absolute {
		e.value = n
	} else {
		e.value += n
	}
	r.entries[key] = e
}

func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unnamed"
	}
	return nameRe.ReplaceAllString(s, "_")
}

func cacheKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteString(";")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

// escapeLabelValue 转义 exposition 文本里的 label 值。
func escapeLabelValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// Snapshot 返回缓存键 → 当前值副本（仅计数器/gauge；直方图经 Handler 导出）。
func (r *Registry) Snapshot() map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]uint64, len(r.entries))
	for k, e := range r.entries {
		out[k] = e.value
	}
	return out
}

// Handler 返回 /metrics 的 HTTP handler（Prometheus text format）。
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		entries := make([]entry, 0, len(r.entries))
		for _, e := range r.entries {
			entries = append(entries, e)
		}
		hists := make([]histogram, 0, len(r.histograms))
		for _, h := range r.histograms {
			cp := *h
			cp.counts = append([]uint64(nil), h.counts...)
			hists = append(hists, cp)
		}
		meta := make(map[string]metricMeta, len(r.meta))
		for k, v := range r.meta {
			meta[k] = v
		}
		kinds := make(map[string]string, len(r.kinds))
		for k, v := range r.kinds {
			kinds[k] = v
		}
		r.mu.Unlock()

		names := map[string]bool{}
		for _, e := range entries {
			names[e.name] = true
		}
		for _, h := range hists {
			names[h.name] = true
		}
		sorted := make([]string, 0, len(names))
		for n := range names {
			sorted = append(sorted, n)
		}
		sort.Strings(sorted)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, name := range sorted {
			kind := meta[name].kind
			if kind == "" {
				kind = kinds[name]
			}
			if kind == "" {
				kind = "counter"
			}
			writeHeader(w, name, kind, meta[name].help)

			var fam []entry
			for _, e := range entries {
				if e.name == name {
					fam = append(fam, e)
				}
			}
			sort.Slice(fam, func(i, j int) bool {
				return cacheKey(fam[i].name, fam[i].labels) < cacheKey(fam[j].name, fam[j].labels)
			})
			for _, e := range fam {
				_, _ = fmt.Fprintf(w, "%s%s %d\n", e.name, renderLabels(e.labels), e.value)
			}

			var famH []histogram
			for _, h := range hists {
				if h.name == name {
					famH = append(famH, h)
				}
			}
			sort.Slice(famH, func(i, j int) bool {
				return cacheKey(famH[i].name, famH[i].labels) < cacheKey(famH[j].name, famH[j].labels)
			})
			for _, h := range famH {
				writeHistogram(w, h)
			}
		}
	})
}

// writeHeader 为每个指标名恰好输出一次 HELP/TYPE（跨 label set 聚合）。
// 未显式描述的指标只输出自检 TYPE；直方图无显式 HELP 时回退到通用文本。
func writeHeader(w http.ResponseWriter, name, kind, help string) {
	if kind == "histogram" {
		if help == "" {
			help = name + " histogram"
		}
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
		return
	}
	if help != "" {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	}
	_, _ = fmt.Fprintf(w, "# TYPE %s %s\n", name, kind)
}

func writeHistogram(w http.ResponseWriter, h histogram) {
	cumulative := uint64(0)
	for i, ub := range histogramBuckets {
		cumulative += h.counts[i]
		le := strconv.FormatFloat(ub, 'g', -1, 64)
		_, _ = fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, renderLabelsWithLE(h.labels, le), cumulative)
	}
	_, _ = fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, renderLabelsWithLE(h.labels, "+Inf"), h.count)
	_, _ = fmt.Fprintf(
		w, "%s_sum%s %s\n", h.name, renderLabels(h.labels), strconv.FormatFloat(h.sum, 'g', -1, 64),
	)
	_, _ = fmt.Fprintf(w, "%s_count%s %d\n", h.name, renderLabels(h.labels), h.count)
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(sanitizeName(k))
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

// renderLabelsWithLE 渲染基础 label 并追加直方图桶标签 le（始终置于末尾，
// 与 Prometheus 约定一致）。
func renderLabelsWithLE(labels map[string]string, le string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("{")
	for _, k := range keys {
		b.WriteString(sanitizeName(k))
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteString(`",`)
	}
	b.WriteString(`le="`)
	b.WriteString(escapeLabelValue(le))
	b.WriteString(`"}`)
	return b.String()
}
