// Package scheduler 是 firepaas 的自研放置调度器（移植 e2b Best-of-K，ADR-0002）。
//
// 管线（ADR-0009，先过滤后打分）：
//
//	状态过滤 → node_pool/labels 过滤 → 资源硬准入过滤
//	→ DEPLOYMENT 反亲和过滤（尽力而为，候选为空回退）
//	→ 随机抽 K → Best-of-K 打分 → 最低分
//
// 打分公式（CPU/内存分维度，MVP 只对这两维打分）：
//
//	score = Σ_w w_i * (req_i + allocated_i + pending_i + alpha_i * usage_i) / (R_i * capacity_i)
package scheduler

import (
	"fmt"
	"math/rand"
	"sort"
)

// 节点状态（与 agent ServiceInfoResponse.Status 对齐，外加 UNKNOWN）。
const (
	StatusUnknown   = "UNKNOWN"
	StatusHealthy   = "HEALTHY"
	StatusDraining  = "DRAINING"
	StatusUnhealthy = "UNHEALTHY"
)

// BestOfKConfig 放置参数。
type BestOfKConfig struct {
	R         float64 // 集群级最大超售比（CPU 维度），默认 4
	MemR      float64 // 内存超售比，默认 1.0（内存不超售）
	K         int     // 每轮随机采样数，默认 3
	Alpha     float64 // 实时用量的权重，默认 0.5
	WeightCPU float64 // 打分权重，默认 0.5
	WeightMem float64 // 打分权重，默认 0.5
}

// DefaultBestOfKConfig 返回默认参数（对齐 e2b）。
func DefaultBestOfKConfig() BestOfKConfig {
	return BestOfKConfig{R: 4, MemR: 1.0, K: 3, Alpha: 0.5, WeightCPU: 0.5, WeightMem: 0.5}
}

// Options 是 Place 的可观测/可注入选项。
type Options struct {
	// ScoreHook 在每个候选节点进入打分前被调用一次；仿真用它在
	// “打分前”校验该节点已通过全部过滤（断言过滤先于打分）。
	ScoreHook func(nodeID string)
}

// Node 是调度器看到的节点视图（nodemanager 同步，纯数据）。
type Node struct {
	ID     string
	Pool   string
	Labels map[string]string
	Status string

	CPUTotal    uint64
	MemTotalMib uint64

	CPUAllocated uint64  // 已承诺 vcpu
	MemAllocated uint64  // 已承诺 MiB（agent 上报校正）
	CPUPending   uint64  // 在途 create 的 vcpu 承诺
	MemPending   uint64  // 在途 create 的 MiB 承诺
	CPUPercent   float64 // 实时用量（0-100）
	MemUsedMib   uint64  // 实时用量
}

// Request 是一次放置请求（来自 MachineSpec.placement + 资源需求）。
type Request struct {
	VCPU   uint64
	MemMib uint64

	DeploymentID string            // 反亲和范围；空 = 不启用
	Pool         string            // 空 = compute
	Labels       map[string]string // key=value 硬过滤

	AntiAffinity bool
	// ExistingDeploymentNodes 是同一 deployment 已占节点集合
	// （控制面从 PG 推导，ADR-0009）。
	ExistingDeploymentNodes map[string]bool
	// ExcludedNodes 是本次放置不可选节点（如 ResourceExhausted 换节点重试）。
	ExcludedNodes map[string]bool
}

// Event 是调度事件（写 scheduler_events，审计可解释）。
type Event struct {
	Kind   string // placement|filter_rejection
	NodeID string
	Reason string
}

// Placement 是一次放置的结果。
type Placement struct {
	NodeID string
	Score  float64
	Events []Event
}

// Placer 执行放置。
type Placer struct {
	cfg  BestOfKConfig
	opts Options
}

// New 构造 Placer。cfg 零值字段会被默认值补齐。
func New(cfg BestOfKConfig, opts Options) *Placer {
	if cfg.R == 0 {
		cfg.R = DefaultBestOfKConfig().R
	}
	if cfg.MemR == 0 {
		cfg.MemR = 1.0
	}
	if cfg.K == 0 {
		cfg.K = DefaultBestOfKConfig().K
	}
	if cfg.Alpha == 0 {
		cfg.Alpha = DefaultBestOfKConfig().Alpha
	}
	if cfg.WeightCPU == 0 && cfg.WeightMem == 0 {
		cfg.WeightCPU = 0.5
		cfg.WeightMem = 0.5
	}
	return &Placer{cfg: cfg, opts: opts}
}

// ErrNoCandidates 表示过滤后无可用节点。
type ErrNoCandidates struct{ Reason string }

func (e ErrNoCandidates) Error() string { return "scheduler: no candidates: " + e.Reason }

// Place 执行一次放置。rnd 为 nil 时使用随机采样源。
func (p *Placer) Place(req Request, nodes []Node, rnd *rand.Rand) (Placement, error) {
	if rnd == nil {
		rnd = rand.New(rand.NewSource(rand.Int63()))
	}
	var events []Event
	addEvent := func(kind, nodeID, reason string) {
		events = append(events, Event{Kind: kind, NodeID: nodeID, Reason: reason})
	}

	pool := req.Pool
	if pool == "" {
		pool = "compute"
	}

	// 1) 状态过滤：只接受 HEALTHY，排除重试黑名单。
	var statusPass []Node
	for _, n := range nodes {
		if n.Status == StatusHealthy {
			statusPass = append(statusPass, n)
		} else {
			addEvent("filter_rejection", n.ID, "status="+n.Status)
		}
	}

	// 1.5) 重试排除（ResourceExhausted 换节点，ADR-0002）。
	var retryPass []Node
	for _, n := range statusPass {
		if req.ExcludedNodes[n.ID] {
			addEvent("filter_rejection", n.ID, "excluded by retry policy")
			continue
		}
		retryPass = append(retryPass, n)
	}

	// 2) node_pool + labels 硬过滤。
	var poolPass []Node
	for _, n := range retryPass {
		if n.Pool != pool {
			addEvent("filter_rejection", n.ID, "pool mismatch")
			continue
		}
		if !labelsMatch(n.Labels, req.Labels) {
			addEvent("filter_rejection", n.ID, "labels mismatch")
			continue
		}
		poolPass = append(poolPass, n)
	}

	// 3) 资源硬准入（agent 侧同样有硬校验，双保险，ADR-0002）。
	var resourcePass []Node
	for _, n := range poolPass {
		if !canFit(n, req, p.cfg) {
			addEvent("filter_rejection", n.ID,
				fmt.Sprintf("resources: need vcpu=%d mem=%dMiB (allocated+pending+req exceeds R*capacity)",
					req.VCPU, req.MemMib))
			continue
		}
		resourcePass = append(resourcePass, n)
	}

	// 4) DEPLOYMENT 反亲和（尽力而为）：排除集内节点先剔除；候选为空则回退
	//    到资源通过集合，并记录降级事件（ADR-0009 不为反亲和牺牲可用性）。
	candidates := resourcePass
	if req.AntiAffinity && req.DeploymentID != "" && len(req.ExistingDeploymentNodes) > 0 {
		var spread []Node
		for _, n := range resourcePass {
			if req.ExistingDeploymentNodes[n.ID] {
				continue
			}
			spread = append(spread, n)
		}
		if len(spread) == 0 {
			addEvent("placement", "", "anti_affinity degraded: no node outside deployment set")
		} else {
			candidates = spread
		}
	}

	if len(candidates) == 0 {
		return Placement{Events: events}, ErrNoCandidates{Reason: "all filters exhausted"}
	}

	// 5) 随机抽 K → 打分 → 最低分。ScoreHook 在打分前调用（顺序断言）。
	sample := sampleK(candidates, p.cfg.K, rnd)
	type scored struct {
		node  Node
		score float64
	}
	scoredList := make([]scored, 0, len(sample))
	for _, n := range sample {
		if p.opts.ScoreHook != nil {
			p.opts.ScoreHook(n.ID)
		}
		scoredList = append(scoredList, scored{node: n, score: p.score(n, req)})
	}
	sort.Slice(scoredList, func(i, j int) bool {
		if scoredList[i].score != scoredList[j].score {
			return scoredList[i].score < scoredList[j].score
		}
		return scoredList[i].node.ID < scoredList[j].node.ID // 确定性 tie-break
	})
	best := scoredList[0]
	addEvent("placement", best.node.ID, fmt.Sprintf("score=%.6f candidates=%d sampled=%d",
		best.score, len(candidates), len(sample)))
	return Placement{NodeID: best.node.ID, Score: best.score, Events: events}, nil
}

// score 计算单节点放置分（Best-of-K，ADR-0002）。
func (p *Placer) score(n Node, req Request) float64 {
	cpuCap := float64(n.CPUTotal) * p.cfg.R
	memCap := float64(n.MemTotalMib) * p.cfg.MemR
	usageCPU := n.CPUPercent / 100 * float64(n.CPUTotal) // 折算为“占用 vcpu 数”
	cpu := (float64(req.VCPU+uint64(n.CPUAllocated)+uint64(n.CPUPending)) + p.cfg.Alpha*usageCPU) / cpuCap
	mem := (float64(req.MemMib+n.MemAllocated+n.MemPending) + p.cfg.Alpha*float64(n.MemUsedMib)) / memCap
	return p.cfg.WeightCPU*cpu + p.cfg.WeightMem*mem
}

// canFit 硬准入：allocated+pending+req ≤ R·capacity（内存 R=MemR）。
func canFit(n Node, req Request, cfg BestOfKConfig) bool {
	if n.CPUTotal == 0 || n.MemTotalMib == 0 {
		return false // 容量未知的节点不接新 machine（保守）
	}
	usedVCPU := uint64(n.CPUAllocated) + n.CPUPending + req.VCPU
	if float64(usedVCPU) > float64(n.CPUTotal)*cfg.R {
		return false
	}
	usedMem := n.MemAllocated + n.MemPending + req.MemMib
	if float64(usedMem) > float64(n.MemTotalMib)*cfg.MemR {
		return false
	}
	return true
}

func labelsMatch(nodeLabels, want map[string]string) bool {
	for k, v := range want {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

func sampleK(nodes []Node, k int, rnd *rand.Rand) []Node {
	if k <= 0 || k >= len(nodes) {
		out := make([]Node, len(nodes))
		copy(out, nodes)
		return out
	}
	perm := rnd.Perm(len(nodes))
	out := make([]Node, 0, k)
	for _, idx := range perm[:k] {
		out = append(out, nodes[idx])
	}
	return out
}
