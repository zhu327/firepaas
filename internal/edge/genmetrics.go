package edge

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
)

// genmetrics.go：按 (host, generation) 的代级观测（Wave3 rollout 分析源）。
//
// canary 错误率/p99 必须按代归因，全局计数给不出代维度。选路时 backend
// 自带 DeploymentGeneration（catalog.Backend.DeploymentGeneration，= 该
// backend 所属 deployment 的发布代；legacy 与 mesh 路径都填充）。
// 恰好一次记账（与 observeRequest 同调用点，重试不重复）。
//
// 基数控制：键空间以 maxGenEntries 为界 LRU 逐出（generation 随发布单调涨，
// 不封顶会泄漏；逐出只丢 Prometheus 可见性，不影响转发）。延迟是端到端
// 用户视角（含路由/token 开销）——canary SLO 要的就是这个。

// maxGenEntries 是代级观测表的键上限（host × generation；逐出最老 key）。
// firepaas 规模下 host 数十、每 host 在飞代数个位数，4096 是宽松上界。
const maxGenEntries = 4096

// genEntry 是单个 (host, generation) 的代级观测行：状态码分类 + 端到端
// 延迟直方图。桶与 defaultLatencyBuckets 一致；全部原子字段，建表后无锁并发写。
type genEntry struct {
	host, generation string
	c2xx, c4xx, c5xx atomic.Uint64
	buckets          []atomic.Uint64
	count            atomic.Uint64
	sumMicros        atomic.Uint64
}

func (c *Counters) genTableFor(host string, gen int64) *genEntry {
	if c == nil || host == "" || gen <= 0 {
		return nil
	}
	key := host + "\x00" + strconv.FormatInt(gen, 10)
	c.genMu.RLock()
	e := c.genTable[key]
	c.genMu.RUnlock()
	if e != nil {
		return e
	}
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if e = c.genTable[key]; e != nil {
		return e
	}
	e = &genEntry{host: host, generation: strconv.FormatInt(gen, 10)}
	e.buckets = make([]atomic.Uint64, len(defaultLatencyBuckets))
	if c.genTable == nil {
		c.genTable = map[string]*genEntry{}
	}
	c.genTable[key] = e
	c.genOrder = append(c.genOrder, key)
	for len(c.genOrder) > maxGenEntries {
		oldest := c.genOrder[0]
		c.genOrder = c.genOrder[1:]
		// 当前 key 永不为 oldest（刚 append），直接删安全。
		delete(c.genTable, oldest)
	}
	return e
}

// observeGenResponse 记录一次客户端请求的代级归因（每请求恰好一次，与
// observeRequest 同调用点）。gen<=0（无路由/零容量/选路失败）时跳过——
// canary 分析只需要"被某代服务过"的请求。
func (c *Counters) observeGenResponse(host string, gen int64, status int, latencySec float64) {
	e := c.genTableFor(host, gen)
	if e == nil {
		return
	}
	switch status / 100 {
	case 2:
		e.c2xx.Add(1)
	case 4:
		e.c4xx.Add(1)
	case 5:
		e.c5xx.Add(1)
	}
	for i, bound := range defaultLatencyBuckets {
		if latencySec <= bound {
			e.buckets[i].Add(1)
			break // 非累计存储：只进第一个命中的桶，累计在写出时做
		}
	}
	e.count.Add(1)
	e.sumMicros.Add(uint64(latencySec * 1e6))
}

// writeGenMetrics 以 Prometheus 文本格式写出代级观测：
// firepaas_edge_gen_responses_total{host,generation,code_class} 与
// firepaas_edge_gen_latency_seconds{host,generation} 直方图族。
func (c *Counters) writeGenMetrics(w http.ResponseWriter) {
	c.genMu.RLock()
	entries := make([]*genEntry, 0, len(c.genTable))
	for _, e := range c.genTable {
		entries = append(entries, e)
	}
	c.genMu.RUnlock()
	if len(entries) == 0 {
		return
	}
	_, _ = fmt.Fprint(w,
		"# HELP firepaas_edge_gen_responses_total client requests by serving generation (one per request)\n"+
			"# TYPE firepaas_edge_gen_responses_total counter\n")
	for _, e := range entries {
		for _, kv := range []struct {
			class string
			v     uint64
		}{
			{"2xx", e.c2xx.Load()},
			{"4xx", e.c4xx.Load()},
			{"5xx", e.c5xx.Load()},
		} {
			_, _ = fmt.Fprintf(w, "firepaas_edge_gen_responses_total{host=%q,generation=%q,code_class=%q} %d\n",
				e.host, e.generation, kv.class, kv.v)
		}
	}
	_, _ = fmt.Fprint(w,
		"# HELP firepaas_edge_gen_latency_seconds end-to-end request latency by serving generation\n"+
			"# TYPE firepaas_edge_gen_latency_seconds histogram\n")
	for _, e := range entries {
		var cumulative uint64
		for i, bound := range defaultLatencyBuckets {
			cumulative += e.buckets[i].Load()
			_, _ = fmt.Fprintf(w,
				"firepaas_edge_gen_latency_seconds_bucket{host=%q,generation=%q,le=%q} %d\n",
				e.host, e.generation, strconv.FormatFloat(bound, 'g', -1, 64), cumulative)
		}
		_, _ = fmt.Fprintf(w,
			"firepaas_edge_gen_latency_seconds_bucket{host=%q,generation=%q,le=\"+Inf\"} %d\n"+
				"firepaas_edge_gen_latency_seconds_sum{host=%q,generation=%q} %g\n"+
				"firepaas_edge_gen_latency_seconds_count{host=%q,generation=%q} %d\n",
			e.host, e.generation, e.count.Load(),
			e.host, e.generation, float64(e.sumMicros.Load())/1e6,
			e.host, e.generation, e.count.Load())
	}
}
