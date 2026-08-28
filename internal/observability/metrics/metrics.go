// Package metrics 是 M2.5 最低可观测的手写计数器（避免引入完整 Prometheus 依赖）。
// 输出 Prometheus text format，接入 /metrics；生产形态 M5 换 OTel SDK。
//
// P3-4：指标名与 label 分离。旧实现把 label 内联在名字里且不转义，
// 值里出现空格/等号/引号时产出非法 exposition 文本。
package metrics

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var nameRe = regexp.MustCompile(`[^a-zA-Z0-9_:]`)

type entry struct {
	name   string
	labels map[string]string
	value  uint64
}

// Registry 持有命名计数器（key 为 name+labels 的缓存键）。
type Registry struct {
	mu      sync.Mutex
	entries map[string]entry
}

// New 构造 Registry。
func New() *Registry { return &Registry{entries: map[string]entry{}} }

// Inc 增加计数。labels 可为 nil；name 与 label key 会规范化，label 值会转义。
func (r *Registry) Inc(name string, labels map[string]string, n uint64) {
	r.add(name, labels, n, false)
}

// Set 置绝对值（仪表类）。
func (r *Registry) Set(name string, labels map[string]string, v uint64) {
	r.add(name, labels, v, true)
}

// ResetFamily 删除某指标名的全部序列（P2-8，M5 评审）：label 集合随时间
// 收缩的 gauge（如 machines_observed{state=...}）必须每轮先清再 Set，
// 否则消失的 label 组合永远残留旧值（幽灵机器/告警噪声）。
func (r *Registry) ResetFamily(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, e := range r.entries {
		if e.name == sanitizeName(name) {
			delete(r.entries, k)
		}
	}
}

func (r *Registry) add(name string, labels map[string]string, n uint64, absolute bool) {
	if n == 0 && !absolute {
		return
	}
	key := cacheKey(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		e = entry{name: sanitizeName(name), labels: labels}
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

// Snapshot 返回缓存键 → 当前值副本。
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
		snap := make([]entry, 0, len(r.entries))
		for _, e := range r.entries {
			snap = append(snap, e)
		}
		r.mu.Unlock()
		sort.Slice(snap, func(i, j int) bool {
			if snap[i].name != snap[j].name {
				return snap[i].name < snap[j].name
			}
			return cacheKey(snap[i].name, snap[i].labels) < cacheKey(snap[j].name, snap[j].labels)
		})
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, e := range snap {
			fmt.Fprintf(w, "%s%s %d\n", e.name, renderLabels(e.labels), e.value)
		}
	})
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
