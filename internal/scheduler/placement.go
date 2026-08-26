// Package scheduler 是 firepaas 的自研放置调度器(P2 移植 e2b Best-of-K)。
package scheduler

// Score 计算节点放置分。默认 R=4、K=3、Alpha=0.5(参考
// infra/packages/api/internal/orchestrator/placement/placement_best_of_K.go)。
//
//	score = Σ_w w_i * (req_i + allocated_i + pending_i + alpha_i * usage_i) / (R_i * capacity_i)
//
// MVP 只对 CPU/内存打分;磁盘、带宽、GPU 用过滤/约束表达。
type Score struct {
	CPU float64
	Mem float64
}

// BestOfKConfig 放置参数(P2 支持 feature flag 热更新)。
type BestOfKConfig struct {
	R     float64 // 集群级最大超售比(CPU 维度),默认 4
	K     int     // 每轮随机采样数,默认 3
	Alpha float64 // 实时用量的权重,默认 0.5
}

// Node 是调度器看到的节点视图(由 nodemanager 同步)。
// TODO(P2.1): 字段对齐 agent.v1.ServiceInfoResponse + PlacementMetrics。
type Node struct {
	ID      string
	Metrics NodeMetrics
	Pending map[string]Score // in-progress 分配,防重复记账
}

// NodeMetrics 是最近一次 ServiceInfo 上报的容量/承诺量。
type NodeMetrics struct {
	CPUAllocated float64
	CPUPercent   float64
	CPUTotal     float64
	MemAllocated uint64
	MemTotalMiB  uint64
}

// CanAcceptNewRequests 返回节点是否还能接新 machine(P2.1 实现)。
func (n *Node) CanAcceptNewRequests() bool {
	// TODO: Healthy 状态 + 非 draining + 容量未硬超限
	return false
}

// Filter 是调度管线的第一层:先过滤后打分(ADR-0009)。
// 顺序:状态 → 资源 → label/node_pool → DEPLOYMENT 反亲和(尽力而为,候选不足
// 可降级并记录调度事件)。M2 实现状态/资源过滤,M3 补齐 label 与反亲和。
// TODO(M2): 定义 Filter(spec PlacementConstraints, nodes []Node) []Node 及
// 每个过滤器的跳过原因(接入调度事件/遥测)。
type Filter interface{}

// DefaultBestOfKConfig 返回默认参数(对齐 e2b)。
func DefaultBestOfKConfig() BestOfKConfig {
	return BestOfKConfig{R: 4, K: 3, Alpha: 0.5}
}
